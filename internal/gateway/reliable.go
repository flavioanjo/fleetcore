package gateway

import (
	"container/list"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flavioanjo/fleetcore/internal/eventstore"
	"github.com/flavioanjo/fleetcore/internal/reliable"
	"github.com/flavioanjo/fleetcore/internal/security"
	"github.com/flavioanjo/fleetcore/internal/sqlitedb"
)

// # Reliable (protocol v2) ingestion
//
// For each v2 datagram the gateway:
//
//  1. authenticates it (reliable.Open: structure, key lookup, HMAC). A
//     datagram that fails is dropped without an answer, since it may be
//     forged;
//  2. checks the device session against the durable per-device high-water
//     mark shared by every gateway (eventstore.ObserveSession), so frames
//     of an older session are rejected, across gateways and restarts;
//  3. checks the per-session counter against a sliding replay window;
//  4. validates the event body; an authenticated invalid event is answered
//     with a signed AckInvalid bound to its UID and fingerprint;
//  5. stores the batch in one transaction (eventstore.AppendReliable);
//  6. only after the commit returns, sends a signed ACK per event:
//     AckStored or AckDuplicate (success), AckConflict, or AckRetry if the
//     batch could not be committed.
//
// A success ACK therefore always means "an identical event with this
// device and UID is durably committed". If the process dies between the
// commit and the ACK, or the ACK datagram is lost, the device retransmits;
// any gateway classifies the retransmission as a Duplicate of the
// committed event and ACKs it.
//
// The gateway also remembers, per device, the address and session of its
// latest authenticated frame, so it can address command deliveries
// (CommandPacket) to the device's current session.

// ReliableStore is the part of the event store the reliable path needs.
// *eventstore.Store implements it.
type ReliableStore interface {
	AppendReliable(ctx context.Context, evs []eventstore.ReliableEvent) ([]eventstore.ReliableResult, error)
	ObserveSession(ctx context.Context, deviceID string, keyID uint32, gen uint64, at time.Time, gateway string) (eventstore.SessionState, error)
	DeviceSession(ctx context.Context, deviceID string) (eventstore.SessionState, error)
}

// ReliableConfig enables protocol v2 on a gateway.
type ReliableConfig struct {
	// Keys resolves device keys. Required.
	Keys security.KeyStore
	// Store persists events and sessions. Required.
	Store ReliableStore
	// GatewayID names this gateway in stored records. Required.
	GatewayID string
	// SessionRecheck is how often the device's last-seen time and gateway
	// are refreshed in the shared session table while its session does not
	// change. Default 5s. (The session generation itself is checked against
	// the shared table on every batch; see admit.)
	SessionRecheck time.Duration
	// MaxDevices bounds the per-device state (replay windows, addresses).
	// The least recently seen device is evicted beyond it; an evicted
	// device's next frame is re-validated against the store. Default 100000.
	MaxDevices int
	// StoreRetries bounds retries of a failed batch commit before the batch
	// is answered with AckRetry. Default 3.
	StoreRetries int
	// RetryBackoff is the first retry delay (doubling). Default 50ms.
	RetryBackoff time.Duration
	// WriteTimeout bounds one store transaction attempt. Default 10s.
	WriteTimeout time.Duration
	// Hooks injects faults in tests. Nil in production.
	Hooks *ReliableHooks
}

// ReliableHooks are fault-injection points (tests and the simulator only).
type ReliableHooks struct {
	// BeforeCommit runs before each store attempt; a non-nil error fails it
	// (simulated busy or failing storage).
	BeforeCommit func() error
	// AfterCommit runs after a successful commit; returning false discards
	// the batch's ACKs, simulating a crash between commit and ACK.
	AfterCommit func() bool
}

func (c ReliableConfig) withDefaults() (ReliableConfig, error) {
	if c.Keys == nil || c.Store == nil || c.GatewayID == "" {
		return c, errors.New("gateway: reliable config needs Keys, Store and GatewayID")
	}
	if c.SessionRecheck <= 0 {
		c.SessionRecheck = 5 * time.Second
	}
	if c.MaxDevices <= 0 {
		c.MaxDevices = 100_000
	}
	if c.StoreRetries <= 0 {
		c.StoreRetries = 3
	}
	if c.RetryBackoff <= 0 {
		c.RetryBackoff = 50 * time.Millisecond
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = 10 * time.Second
	}
	return c, nil
}

// Inbound is a received datagram.
type Inbound struct {
	Data []byte
	From netip.AddrPort
	At   time.Time
}

// Outbound is a datagram to send.
type Outbound struct {
	Data []byte
	To   netip.AddrPort
}

// Reliable is the protocol v2 ingestion core. It does no socket I/O: the
// gateway (or the simulator) feeds it datagrams and sends what it returns.
// Prepare is safe for concurrent use; Process must be called from one
// goroutine at a time; CommandPacket is safe for concurrent use.
type Reliable struct {
	cfg ReliableConfig
	log *slog.Logger
	now func() time.Time

	mu      sync.Mutex // guards devices and lru
	devices map[string]*list.Element
	lru     *list.List // of *deviceState, front = most recent

	stats ReliableStats
}

type deviceState struct {
	id        string
	keyID     uint32
	gen       uint64
	checkedAt time.Time
	window    security.ReplayWindow
	addr      netip.AddrPort
}

// ReliableStats are the v2 counters. All fields are updated atomically.
type ReliableStats struct {
	Received      atomic.Uint64
	Rejected      [32]atomic.Uint64 // indexed by reliable.Code
	Heartbeats    atomic.Uint64
	Events        atomic.Uint64 // authenticated, valid event uploads handed to the store
	Stored        atomic.Uint64 // AckStored
	Duplicates    atomic.Uint64 // AckDuplicate
	Conflicts     atomic.Uint64 // AckConflict
	Invalid       atomic.Uint64 // AckInvalid
	RetryAcks     atomic.Uint64 // AckRetry (batch could not be committed)
	StoreRetries  atomic.Uint64
	AcksDiscarded atomic.Uint64 // dropped by the AfterCommit hook
	AckSendErrors atomic.Uint64
	CommandsSent  atomic.Uint64
	Unreachable   atomic.Uint64
	Critical      atomic.Uint64 // critical events stored or duplicate
}

// RejectedBy returns the number of datagrams rejected with code c.
func (s *ReliableStats) RejectedBy(c reliable.Code) uint64 { return s.Rejected[c].Load() }

// NewReliable validates cfg and returns the core.
func NewReliable(cfg ReliableConfig, now func() time.Time, log *slog.Logger) (*Reliable, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	if now == nil {
		now = time.Now
	}
	if log == nil {
		log = slog.Default()
	}
	return &Reliable{cfg: cfg, log: log, now: now, devices: map[string]*list.Element{}, lru: list.New()}, nil
}

// Stats returns the live counters.
func (r *Reliable) Stats() *ReliableStats { return &r.stats }

// Prepared is an authenticated, decoded datagram awaiting Process.
type Prepared struct {
	in       Inbound
	frame    reliable.Frame
	err      error
	upload   *eventstore.ReliableEvent
	invalid  *reliable.EventAck // signed negative ACK to send (authenticated but invalid event)
	critical bool
}

// Critical reports whether the datagram carries a critical event (the
// gateway flushes its batch immediately for those).
func (p Prepared) Critical() bool { return p.critical }

// Prepare authenticates and decodes a datagram. It has no side effects.
func (r *Reliable) Prepare(in Inbound) Prepared {
	p := Prepared{in: in}
	f, err := reliable.Open(in.Data, security.Uplink, r.cfg.Keys)
	if err != nil {
		p.err = err
		return p
	}
	p.frame = f
	if f.Type != reliable.TypeEventUpload {
		if f.Type == reliable.TypeHeartbeat {
			if _, err := reliable.DecodeHeartbeat(f.Payload); err != nil {
				p.err = err
			}
		}
		return p
	}
	up, err := reliable.DecodeEventUpload(f.Payload)
	if err != nil {
		p.err = err // no trustworthy UID to answer
		return p
	}
	ev, err := eventstore.NewReliableEvent(f.DeviceID, up, f.KeyID, f.SessionGen, in.At, r.cfg.GatewayID)
	if err != nil {
		p.invalid = &reliable.EventAck{UID: up.UID, Fingerprint: reliable.EventFingerprint(f.DeviceID, f.Payload), Status: reliable.AckInvalid}
		return p
	}
	p.upload = &ev
	p.critical = up.Type.Class().Critical()
	return p
}

func (r *Reliable) reject(err error) {
	c := reliable.CodeOf(err)
	if int(c) >= len(r.stats.Rejected) {
		c = 0
	}
	r.stats.Rejected[c].Add(1)
}

// device returns the state of a device, creating it (LRU-bounded).
func (r *Reliable) device(id string) *deviceState {
	if e, ok := r.devices[id]; ok {
		r.lru.MoveToFront(e)
		return e.Value.(*deviceState)
	}
	d := &deviceState{id: id}
	r.devices[id] = r.lru.PushFront(d)
	for r.lru.Len() > r.cfg.MaxDevices {
		old := r.lru.Back()
		r.lru.Remove(old)
		delete(r.devices, old.Value.(*deviceState).id)
	}
	return d
}

// admit applies the session and replay checks to an authenticated frame
// and records the sender's address. It returns a reliable.Error on
// rejection.
//
// The device's session generation is read from the shared store once per
// batch (durable maps it for the rest of the batch), so a frame of an
// older session is rejected by every gateway as soon as any gateway has
// recorded the newer one, and after any restart. The store is written only
// when the session advances (and every SessionRecheck to refresh the
// device's last-seen time).
//
// Residual: the replay window is per gateway, so the very same datagram
// of the current session, replayed to a second gateway, passes that
// gateway's window once. It cannot change state: every v2 message is
// idempotent (event uploads deduplicate by UID and fingerprint; commands
// by CommandID on the device).
func (r *Reliable) admit(ctx context.Context, f reliable.Frame, from netip.AddrPort, at time.Time, durable map[string]eventstore.SessionState) error {
	st, ok := durable[f.DeviceID]
	if !ok {
		var err error
		st, err = r.cfg.Store.DeviceSession(ctx, f.DeviceID)
		switch {
		case errors.Is(err, eventstore.ErrNotFound):
			st = eventstore.SessionState{}
		case err != nil:
			return err // storage failure: not the device's fault; no answer
		}
		durable[f.DeviceID] = st
	}
	r.mu.Lock()
	d := r.device(f.DeviceID)
	checked := d.checkedAt
	r.mu.Unlock()

	switch {
	case f.SessionGen < st.SessionGen:
		return &reliable.Error{Code: reliable.CodeStaleSession, Detail: "older than the device's latest session"}
	case f.SessionGen == st.SessionGen && f.KeyID != st.KeyID:
		return &reliable.Error{Code: reliable.CodeStaleSession, Detail: "session generation reused with another key"}
	case f.SessionGen == st.SessionGen && at.Sub(checked) < r.cfg.SessionRecheck:
		// Current session; last-seen recently refreshed.
	default:
		st, err := r.cfg.Store.ObserveSession(ctx, f.DeviceID, f.KeyID, f.SessionGen, at, r.cfg.GatewayID)
		switch {
		case errors.Is(err, eventstore.ErrStaleSession), errors.Is(err, eventstore.ErrSessionKeyMismatch):
			durable[f.DeviceID] = st
			return &reliable.Error{Code: reliable.CodeStaleSession, Err: err}
		case err != nil:
			return err // storage failure: not the device's fault; no answer
		}
		durable[f.DeviceID] = st
		r.mu.Lock()
		d.checkedAt = at
		r.mu.Unlock()
	}

	r.mu.Lock()
	if f.SessionGen != d.gen || f.KeyID != d.keyID {
		d.window = security.ReplayWindow{} // new session: fresh window
		d.gen, d.keyID = f.SessionGen, f.KeyID
	}
	r.mu.Unlock()

	r.mu.Lock()
	defer r.mu.Unlock()
	if d.gen != f.SessionGen || !d.window.Accept(f.Counter) {
		return &reliable.Error{Code: reliable.CodeReplay}
	}
	d.addr = from
	return nil
}

// Process applies the stateful checks to prepared datagrams, stores the
// valid events in one transaction and returns the ACKs to send. It is
// called by one goroutine.
func (r *Reliable) Process(ctx context.Context, batch []Prepared) []Outbound {
	var out []Outbound
	var evs []eventstore.ReliableEvent
	var evFrames []Prepared
	durable := map[string]eventstore.SessionState{}
	for _, p := range batch {
		r.stats.Received.Add(1)
		if p.err != nil {
			r.reject(p.err)
			continue
		}
		if err := r.admit(ctx, p.frame, p.in.From, p.in.At, durable); err != nil {
			if reliable.CodeOf(err) == 0 {
				r.log.Warn("reliable: session check failed", "device_id", p.frame.DeviceID, "err", err)
			}
			r.reject(err)
			continue
		}
		switch {
		case p.frame.Type == reliable.TypeHeartbeat:
			r.stats.Heartbeats.Add(1)
		case p.invalid != nil:
			r.stats.Invalid.Add(1)
			if o, err := r.ack(p.frame, p.in.From, *p.invalid); err == nil {
				out = append(out, o)
			}
		case p.upload != nil:
			r.stats.Events.Add(1)
			evs = append(evs, *p.upload)
			evFrames = append(evFrames, p)
		}
	}
	if len(evs) == 0 {
		return out
	}
	results, err := r.store(ctx, evs)
	if err != nil {
		r.log.Error("reliable: events not stored; answering retry", "events", len(evs), "err", err)
		for i, p := range evFrames {
			r.stats.RetryAcks.Add(1)
			if o, err := r.ack(p.frame, p.in.From, reliable.EventAck{UID: evs[i].Upload.UID, Fingerprint: evs[i].Fingerprint, Status: reliable.AckRetry}); err == nil {
				out = append(out, o)
			}
		}
		return out
	}
	if h := r.cfg.Hooks; h != nil && h.AfterCommit != nil && !h.AfterCommit() {
		r.stats.AcksDiscarded.Add(uint64(len(evs)))
		return out
	}
	for i, res := range results {
		a := reliable.EventAck{UID: evs[i].Upload.UID, Fingerprint: evs[i].Fingerprint}
		switch res.Outcome {
		case eventstore.Inserted:
			a.Status, a.Position = reliable.AckStored, res.Position
			r.stats.Stored.Add(1)
		case eventstore.Duplicate:
			a.Status, a.Position = reliable.AckDuplicate, res.Position
			r.stats.Duplicates.Add(1)
		default:
			a.Status = reliable.AckConflict
			r.stats.Conflicts.Add(1)
			r.log.Warn("reliable: conflicting event; stored event kept", "device_id", evs[i].DeviceID,
				"uid", evs[i].Upload.UID, "reason", res.ConflictReason)
		}
		if a.Status.Success() && evs[i].Upload.Type.Class().Critical() {
			r.stats.Critical.Add(1)
		}
		if o, err := r.ack(evFrames[i].frame, evFrames[i].in.From, a); err == nil {
			out = append(out, o)
		}
	}
	return out
}

// HandleBatch is Prepare followed by Process (used by the simulator).
func (r *Reliable) HandleBatch(ctx context.Context, in []Inbound) []Outbound {
	ps := make([]Prepared, len(in))
	for i, d := range in {
		ps[i] = r.Prepare(d)
	}
	return r.Process(ctx, ps)
}

func (r *Reliable) store(ctx context.Context, evs []eventstore.ReliableEvent) ([]eventstore.ReliableResult, error) {
	backoff := r.cfg.RetryBackoff
	var err error
	for attempt := 0; ; attempt++ {
		if h := r.cfg.Hooks; h != nil && h.BeforeCommit != nil {
			err = h.BeforeCommit()
		} else {
			err = nil
		}
		if err == nil {
			wctx, cancel := context.WithTimeout(ctx, r.cfg.WriteTimeout)
			var res []eventstore.ReliableResult
			res, err = r.cfg.Store.AppendReliable(wctx, evs)
			cancel()
			if err == nil {
				return res, nil
			}
		}
		if attempt >= r.cfg.StoreRetries || ctx.Err() != nil {
			return nil, err
		}
		r.stats.StoreRetries.Add(1)
		if !sqlitedb.IsBusy(err) {
			r.log.Warn("reliable: store failed; retrying", "attempt", attempt+1, "err", err)
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		backoff *= 2
	}
}

func nonce() uint64 {
	var b [8]byte
	for {
		if _, err := rand.Read(b[:]); err != nil {
			panic(err) // crypto/rand does not fail on supported platforms
		}
		if n := binary.BigEndian.Uint64(b[:]); n != 0 {
			return n
		}
	}
}

// ack seals an ACK in the session of the frame it answers, addressed to
// the frame's sender.
func (r *Reliable) ack(f reliable.Frame, to netip.AddrPort, a reliable.EventAck) (Outbound, error) {
	p, err := a.Encode()
	if err != nil {
		return Outbound{}, err
	}
	k, err := r.cfg.Keys.Lookup(f.DeviceID, f.KeyID)
	if err != nil {
		return Outbound{}, err
	}
	pkt, err := reliable.Seal(reliable.Header{Type: reliable.TypeEventAck, KeyID: f.KeyID, SessionGen: f.SessionGen,
		Counter: nonce(), DeviceID: f.DeviceID}, p, &k.Secret)
	if err != nil {
		return Outbound{}, err
	}
	return Outbound{Data: pkt, To: to}, nil
}

// ErrUnreachable is returned by CommandPacket when this gateway has not
// heard an authenticated frame from the device (since start or eviction),
// so it knows neither its address nor its current session.
var ErrUnreachable = errors.New("gateway: device not reachable through this gateway")

// CommandPacket seals a command delivery for the device's current session
// and returns it with the device's last authenticated address.
func (r *Reliable) CommandPacket(d reliable.CommandDelivery) (Outbound, error) {
	r.mu.Lock()
	e, ok := r.devices[d.DeviceID]
	var st deviceState
	if ok {
		st = *e.Value.(*deviceState)
	}
	r.mu.Unlock()
	if !ok || !st.addr.IsValid() || st.gen == 0 {
		r.stats.Unreachable.Add(1)
		return Outbound{}, ErrUnreachable
	}
	p, err := d.Encode()
	if err != nil {
		return Outbound{}, err
	}
	k, err := r.cfg.Keys.Lookup(d.DeviceID, st.keyID)
	if err != nil {
		return Outbound{}, fmt.Errorf("gateway: key for %s: %w", d.DeviceID, err)
	}
	pkt, err := reliable.Seal(reliable.Header{Type: reliable.TypeCommandDelivery, KeyID: st.keyID, SessionGen: st.gen,
		Counter: nonce(), DeviceID: d.DeviceID}, p, &k.Secret)
	if err != nil {
		return Outbound{}, err
	}
	r.stats.CommandsSent.Add(1)
	return Outbound{Data: pkt, To: st.addr}, nil
}

// GatewayID returns the configured gateway id.
func (r *Reliable) GatewayID() string { return r.cfg.GatewayID }

// reliableLoop batches prepared v2 datagrams, processes them and sends the
// answers. A critical event flushes the batch at once. Answers are written
// synchronously by this goroutine (at most one per datagram in the batch),
// so ACK generation needs no queue of its own and cannot grow without
// bound; a failed send is counted and the device's retransmission is
// answered later. During shutdown the socket is already closed, so events
// drained then are stored but not ACKed: devices retransmit and the next
// run (or another gateway) answers them as duplicates.
func (g *Gateway) reliableLoop(writeCtx context.Context) {
	batch := make([]Prepared, 0, g.cfg.BatchSize)
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	for {
		p, ok := <-g.relQ
		if !ok {
			return
		}
		batch = append(batch, p)
		closed := false
		if !p.Critical() {
			timer.Reset(g.cfg.BatchDelay)
		fill:
			for len(batch) < g.cfg.BatchSize {
				select {
				case p, ok := <-g.relQ:
					if !ok {
						closed = true
						break fill
					}
					batch = append(batch, p)
					if p.Critical() {
						break fill
					}
				case <-timer.C:
					break fill
				}
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
		for _, o := range g.rel.Process(writeCtx, batch) {
			if _, err := g.conn.WriteToUDPAddrPort(o.Data, o.To); err != nil {
				g.rel.stats.AckSendErrors.Add(1)
			}
		}
		batch = batch[:0]
		if closed {
			return
		}
	}
}

// ReliableStats returns the v2 counters, or nil if v2 is disabled.
func (g *Gateway) ReliableStats() *ReliableStats {
	if g.rel == nil {
		return nil
	}
	return g.rel.Stats()
}

// SendCommand transmits a command delivery to the device through this
// gateway's socket. It fails with ErrUnreachable if this gateway has not
// heard from the device in its current session. A nil error means the
// datagram was handed to the network, not that it was delivered.
func (g *Gateway) SendCommand(d reliable.CommandDelivery) error {
	if g.rel == nil {
		return errors.New("gateway: protocol v2 is disabled")
	}
	o, err := g.rel.CommandPacket(d)
	if err != nil {
		return err
	}
	_, err = g.conn.WriteToUDPAddrPort(o.Data, o.To)
	return err
}
