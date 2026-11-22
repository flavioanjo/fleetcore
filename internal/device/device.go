// Package device is a simulated tracking device speaking protocol v2: a
// durable outbound event queue with prioritized, bounded retransmission,
// and command reception and execution against simulated hardware.
//
// # Event delivery guarantee
//
// An event is queued when Enqueue returns nil: it is then committed to the
// device store with a UID never used before. It stays pending, and is
// retransmitted with backoff, until the device receives an authenticated
// ACK in its current session whose UID and fingerprint match the event and
// whose status confirms durable server storage. Under the assumptions that
// the device store survives and the network is eventually available long
// enough, every queued critical event is eventually stored by the server
// exactly once (as one row; retransmissions are deduplicated) and the
// device learns it. Routine and diagnostic events may instead expire after
// Config.RoutineMaxAge (counted, kept, not deleted). Transport-level
// exactly-once delivery is not claimed: the server may receive an event
// many times; it stores it once.
//
// The device clock is the simulator's (Config.Now); the device never needs
// it to be correct for delivery, only for command expiry checks.
package device

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	mrand "math/rand"
	"sync"
	"time"

	"github.com/flavioanjo/fleetcore/internal/reliable"
	"github.com/flavioanjo/fleetcore/internal/security"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// Transport sends one datagram towards a gateway. A nil error means the
// datagram was handed to the network, not that it arrived.
type Transport interface {
	Send(pkt []byte) error
}

// Config configures a Device.
type Config struct {
	DeviceID string
	KeyID    uint32
	Key      security.Secret
	// Authority verifies command authorization signatures. Required.
	Authority *security.Verifier
	StorePath string
	Transport Transport
	Hardware  Hardware

	// Capacity bounds pending events. CriticalReserve of it is reserved for
	// critical events (PANIC, SECURITY_VIOLATION) and command reports, so
	// routine traffic can never prevent a panic from being queued.
	// Defaults 10000 and 1000.
	Capacity, CriticalReserve int
	// RoutineMaxAge expires routine and diagnostic events not acknowledged
	// within it. Critical events and command reports never expire. Default
	// 7 days.
	RoutineMaxAge time.Duration
	// Retransmission backoff: InitialBackoff doubling per attempt up to
	// MaxBackoff, with ±Jitter (fraction) randomization. Defaults 2s, 2m, 0.2.
	InitialBackoff, MaxBackoff time.Duration
	Jitter                     float64
	// CriticalMaxBackoff caps the backoff of critical events. Default 15s.
	CriticalMaxBackoff time.Duration
	// MaxSendsPerTick bounds transmissions per Tick. Default 32.
	MaxSendsPerTick int
	// RoutineShare: at least one transmission slot in RoutineShare is
	// given to routine/diagnostic traffic when any is due, so critical
	// floods cannot starve it indefinitely. Default 4.
	RoutineShare int
	// HeartbeatInterval: an idle device sends a heartbeat so gateways keep
	// its current session and address for command delivery. Default 30s.
	HeartbeatInterval time.Duration
	// RetainDone bounds acknowledged rows kept for audit; older ones are
	// purged. Failed and expired rows are kept up to RetainFailed.
	// Defaults 1000 and 10000.
	RetainDone, RetainFailed int
	// StatusRepeat is the minimum interval between unsolicited status
	// re-reports for a command the server keeps redelivering. Default 30s.
	StatusRepeat time.Duration
	// InitialSequence is the first v1 telemetry sequence number of each
	// boot (tests use it to exercise wrap-around). Default 0.
	InitialSequence telemetry.Sequence

	Now    func() time.Time
	Rand   *mrand.Rand // jitter source; default seeded from crypto/rand
	Logger *slog.Logger
	// Hooks injects faults (tests and simulator only).
	Hooks *Hooks
}

// Hooks are fault-injection points.
type Hooks struct {
	// BeforeEnqueueCommit fails the enqueue transaction (crash or storage
	// failure before the event is persisted).
	BeforeEnqueueCommit func() error
	// DuringExecution runs after the execution decision was committed and
	// the actuator driven, before the outcome is committed; returning an
	// error aborts the device there (simulated crash mid-execution).
	DuringExecution func(reliable.CommandID) error
}

func (c *Config) defaults() error {
	if c.DeviceID == "" || telemetry.DeviceID(c.DeviceID).Validate() != nil || c.KeyID == 0 || c.Key.IsZero() {
		return errors.New("device: needs a valid DeviceID, KeyID and Key")
	}
	if c.Authority == nil || c.StorePath == "" || c.Transport == nil || c.Hardware == nil {
		return errors.New("device: needs Authority, StorePath, Transport and Hardware")
	}
	def := func(v *int, d int) {
		if *v <= 0 {
			*v = d
		}
	}
	defD := func(v *time.Duration, d time.Duration) {
		if *v <= 0 {
			*v = d
		}
	}
	def(&c.Capacity, 10000)
	if c.CriticalReserve <= 0 {
		c.CriticalReserve = c.Capacity / 10
	}
	if c.CriticalReserve >= c.Capacity {
		return errors.New("device: CriticalReserve must be below Capacity")
	}
	defD(&c.RoutineMaxAge, 7*24*time.Hour)
	defD(&c.InitialBackoff, 2*time.Second)
	defD(&c.MaxBackoff, 2*time.Minute)
	defD(&c.CriticalMaxBackoff, 15*time.Second)
	if c.Jitter < 0 || c.Jitter >= 1 {
		return errors.New("device: Jitter must be in [0, 1)")
	}
	if c.Jitter == 0 {
		c.Jitter = 0.2
	}
	def(&c.MaxSendsPerTick, 32)
	def(&c.RoutineShare, 4)
	defD(&c.HeartbeatInterval, 30*time.Second)
	def(&c.RetainDone, 1000)
	def(&c.RetainFailed, 10000)
	defD(&c.StatusRepeat, 30*time.Second)
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.Rand == nil {
		var seed [8]byte
		rand.Read(seed[:]) //nolint:errcheck
		c.Rand = mrand.New(mrand.NewSource(int64(binary.BigEndian.Uint64(seed[:]))))
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return nil
}

// Device is a simulated tracking device. Its methods are safe for
// concurrent use (they are serialized internally).
type Device struct {
	cfg   Config
	store *store
	log   *slog.Logger

	mu       sync.Mutex
	booted   bool
	gen      uint64
	counter  uint64
	sequence telemetry.Sequence
	lastFix  *reliable.PositionFix
	lastSend time.Time
	// rate limit for SECURITY_VIOLATION reports
	lastViolation map[reliable.SecurityCode]time.Time
	authFailures  int
	m             Metrics
}

// Open opens the device store. Call Boot before using the device.
func Open(ctx context.Context, cfg Config) (*Device, error) {
	if err := cfg.defaults(); err != nil {
		return nil, err
	}
	st, err := openStore(ctx, cfg.StorePath, cfg.Now)
	if err != nil {
		return nil, err
	}
	return &Device{cfg: cfg, store: st, log: cfg.Logger, lastViolation: map[reliable.SecurityCode]time.Time{},
		m: Metrics{Queued: map[reliable.EventType]uint64{}}}, nil
}

// Close closes the store. It models a power loss as well as a clean
// stop: nothing is flushed that was not already committed.
func (d *Device) Close() error { return d.store.close() }

// ErrNotBooted is returned before Boot.
var ErrNotBooted = errors.New("device: not booted")

// Boot starts a new session: it increments and commits the session
// generation (so it is never reused, and frames of earlier sessions are
// recognisably stale), recovers commands whose execution was interrupted,
// and queues the current actuator states.
func (d *Device) Boot(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := d.cfg.Now().UTC()
	tx, err := d.store.db.W.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	st, err := readState(ctx, tx)
	if err != nil {
		return err
	}
	gen := st.sessionGen + 1
	if gen > reliable.MaxIdentity {
		return errors.New("device: session generation exhausted")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE device_state SET session_gen = ?, boots = boots + 1 WHERE id = 1`, int64(gen)); err != nil {
		return err
	}
	// Pending events are due immediately after a reboot (connectivity may
	// have returned); their attempt counts are kept.
	if _, err := tx.ExecContext(ctx, `UPDATE outbox SET next_attempt_ns = min(next_attempt_ns, ?) WHERE state = 'pending'`, now.UnixNano()); err != nil {
		return err
	}
	if err := d.recoverInterrupted(ctx, tx, now); err != nil {
		return err
	}
	// Boot state reports are informational: a full queue skips them (and
	// counts it) rather than preventing the device from booting.
	for _, body := range []reliable.Body{
		reliable.BlockState{State: d.cfg.Hardware.ImmobilizerState(), Cause: reliable.CauseBoot},
		reliable.CargoState{Door: d.cfg.Hardware.CargoDoor(1), Latch: d.cfg.Hardware.CargoLatch(1), Cause: reliable.CauseBoot},
	} {
		b := body
		if _, _, err := enqueueTx(ctx, tx, &d.cfg, now, func(uint64) (reliable.Body, error) { return b, nil }); err != nil {
			if !errors.Is(err, ErrQueueFull) {
				return fmt.Errorf("device: queue boot state: %w", err)
			}
			d.m.QueueFull++
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	d.booted, d.gen, d.counter, d.sequence = true, gen, 0, d.cfg.InitialSequence
	d.m.Boots++
	d.lastSend = time.Time{}
	return nil
}

// SessionGen returns the current session generation (0 before Boot).
func (d *Device) SessionGen() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.gen
}

// Enqueue durably queues an event built by build (which receives the
// event's UID). It returns ErrQueueFull when the class's capacity is
// exhausted; nothing is evicted.
func (d *Device) Enqueue(ctx context.Context, build func(uid uint64, now time.Time) (reliable.Body, error)) (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.enqueue(ctx, build)
}

func (d *Device) enqueue(ctx context.Context, build func(uid uint64, now time.Time) (reliable.Body, error)) (uint64, error) {
	if !d.booted {
		return 0, ErrNotBooted
	}
	now := d.cfg.Now().UTC()
	tx, err := d.store.db.W.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck
	uid, typ, err := enqueueTx(ctx, tx, &d.cfg, now, func(uid uint64) (reliable.Body, error) { return build(uid, now) })
	if err != nil {
		if errors.Is(err, ErrQueueFull) {
			d.m.QueueFull++
		}
		return 0, err
	}
	if h := d.cfg.Hooks; h != nil && h.BeforeEnqueueCommit != nil {
		if err := h.BeforeEnqueueCommit(); err != nil {
			d.m.EnqueueFailed++
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		d.m.EnqueueFailed++
		return 0, err
	}
	d.m.Queued[typ]++
	return uid, nil
}

// EnqueueTelemetry queues a v1 telemetry sample; its DeviceID, EventID
// (= UID), BootID (= session generation, low 32 bits) and Sequence are set
// by the device.
func (d *Device) EnqueueTelemetry(ctx context.Context, s telemetry.Sample) (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.enqueue(ctx, func(uid uint64, now time.Time) (reliable.Body, error) {
		s.DeviceID, s.EventID = telemetry.DeviceID(d.cfg.DeviceID), telemetry.EventID(uid)
		s.BootID = telemetry.BootID(uint32(d.gen))
		if s.BootID == 0 {
			s.BootID = 1 // the low 32 bits wrapped to the reserved value
		}
		s.Sequence = d.sequence
		s.DeviceTime = time.UnixMilli(now.UnixMilli()).UTC()
		s.IngestedAt = telemetry.None[time.Time]()
		d.sequence++ // wraps at 2^32, as the v1 domain allows
		return reliable.Telemetry{Sample: s}, nil
	})
}

// ReadPosition reads the GNSS and remembers the fix if valid.
func (d *Device) readFix() (reliable.PositionFix, bool) {
	fix, ok := d.cfg.Hardware.Fix()
	if ok && fix.Valid() {
		f := fix
		d.lastFix = &f
		return fix, true
	}
	return reliable.PositionFix{}, false
}

// ReportPosition queues a periodic POSITION if a valid fix is available.
// Without a fix nothing is queued and ok is false: a position is never
// fabricated.
func (d *Device) ReportPosition(ctx context.Context) (uid uint64, ok bool, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	fix, valid := d.readFix()
	if !valid {
		return 0, false, nil
	}
	uid, err = d.enqueue(ctx, func(uint64, time.Time) (reliable.Body, error) {
		return reliable.Position{PositionFix: fix, Reason: reliable.PositionPeriodic}, nil
	})
	return uid, err == nil, err
}

// Panic queues a PANIC event, linked to the most recent valid fix (with
// its age) when one is known. It uses the critical reserve, so routine
// traffic cannot prevent it from being queued.
func (d *Device) Panic(ctx context.Context) (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.readFix()
	return d.enqueue(ctx, func(_ uint64, now time.Time) (reliable.Body, error) {
		p := reliable.Panic{Trigger: reliable.PanicButton}
		if d.lastFix != nil {
			age := now.Sub(d.lastFix.FixTime).Truncate(time.Millisecond)
			if age >= 0 && age <= 24*time.Hour {
				p.HasPosition, p.Position, p.PositionAge = true, *d.lastFix, age
			}
		}
		return p, nil
	})
}

// backoff returns the delay before attempt number attempts+1.
func (d *Device) backoff(attempts int, class reliable.Class) time.Duration {
	max := d.cfg.MaxBackoff
	if class.Critical() {
		max = d.cfg.CriticalMaxBackoff
	}
	b := d.cfg.InitialBackoff
	for i := 1; i < attempts && b < max; i++ {
		b *= 2
	}
	if b > max {
		b = max
	}
	j := 1 + d.cfg.Jitter*(2*d.cfg.Rand.Float64()-1)
	return time.Duration(float64(b) * j)
}

func (d *Device) seal(typ reliable.MessageType, payload []byte) ([]byte, error) {
	d.counter++
	return reliable.Seal(reliable.Header{Type: typ, KeyID: d.cfg.KeyID, SessionGen: d.gen, Counter: d.counter, DeviceID: d.cfg.DeviceID}, payload, &d.cfg.Key)
}

// Tick expires stale routine events, transmits due events (priority by
// class, with the anti-starvation share), sends a heartbeat when idle, and
// purges old acknowledged rows. It returns the number of event datagrams
// handed to the transport.
func (d *Device) Tick(ctx context.Context) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.booted {
		return 0, ErrNotBooted
	}
	now := d.cfg.Now().UTC()
	tx, err := d.store.db.W.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck

	res, err := tx.ExecContext(ctx, `UPDATE outbox SET state = 'expired', done_ns = ?
		WHERE state = 'pending' AND class IN (?, ?) AND created_ns <= ?`,
		now.UnixNano(), int64(reliable.ClassRoutine), int64(reliable.ClassDiagnostic), now.Add(-d.cfg.RoutineMaxAge).UnixNano())
	if err != nil {
		return 0, err
	}
	if n, _ := res.RowsAffected(); n > 0 {
		d.m.Expired += uint64(n)
	}

	lim := d.cfg.MaxSendsPerTick
	rows, err := tx.QueryContext(ctx, `SELECT `+outboxCols+` FROM outbox WHERE state = 'pending' AND next_attempt_ns <= ? AND class <= ?
		ORDER BY class, uid LIMIT ?`, now.UnixNano(), int64(reliable.ClassControl), lim)
	if err != nil {
		return 0, err
	}
	high, err := scanOutbox(rows)
	if err != nil {
		return 0, err
	}
	rows, err = tx.QueryContext(ctx, `SELECT `+outboxCols+` FROM outbox WHERE state = 'pending' AND next_attempt_ns <= ? AND class > ?
		ORDER BY class, uid LIMIT ?`, now.UnixNano(), int64(reliable.ClassControl), lim)
	if err != nil {
		return 0, err
	}
	low, err := scanOutbox(rows)
	if err != nil {
		return 0, err
	}
	var plan []outboxRow
	for slot := 0; slot < lim && (len(high) > 0 || len(low) > 0); slot++ {
		routineTurn := slot%d.cfg.RoutineShare == d.cfg.RoutineShare-1
		if (routineTurn && len(low) > 0) || len(high) == 0 {
			plan, low = append(plan, low[0]), low[1:]
		} else {
			plan, high = append(plan, high[0]), high[1:]
		}
	}

	sent := 0
	for _, r := range plan {
		pkt, err := d.seal(reliable.TypeEventUpload, r.payload)
		if err != nil {
			return sent, err
		}
		attempts := r.attempts + 1
		next := now.Add(d.backoff(attempts, r.class))
		if err := d.cfg.Transport.Send(pkt); err != nil {
			d.m.SendErrors++
			// Not transmitted: retry after the backoff without counting an
			// attempt.
			if _, err := tx.ExecContext(ctx, `UPDATE outbox SET next_attempt_ns = ? WHERE uid = ?`, next.UnixNano(), int64(r.uid)); err != nil {
				return sent, err
			}
			continue
		}
		sent++
		d.m.Transmissions++
		if r.attempts > 0 {
			d.m.Retransmissions++
		}
		if r.firstSent.IsZero() && r.class.Critical() {
			d.m.CriticalQueueDelay.add(now.Sub(r.created))
		}
		if _, err := tx.ExecContext(ctx, `UPDATE outbox SET attempts = ?, last_sent_ns = ?, first_sent_ns = coalesce(first_sent_ns, ?),
			next_attempt_ns = ? WHERE uid = ?`, attempts, now.UnixNano(), now.UnixNano(), next.UnixNano(), int64(r.uid)); err != nil {
			return sent, err
		}
	}
	if sent > 0 {
		d.lastSend = now
	} else if d.lastSend.IsZero() || now.Sub(d.lastSend) >= d.cfg.HeartbeatInterval {
		var pending int
		tx.QueryRowContext(ctx, `SELECT count(*) FROM outbox WHERE state = 'pending'`).Scan(&pending) //nolint:errcheck
		hb, _ := reliable.Heartbeat{DeviceTime: now, Pending: uint32(pending)}.Encode()
		if pkt, err := d.seal(reliable.TypeHeartbeat, hb); err == nil {
			if d.cfg.Transport.Send(pkt) == nil {
				d.m.Heartbeats++
				d.lastSend = now
			}
		}
	}
	// Retention: purge the oldest finished rows beyond the limits.
	if _, err := tx.ExecContext(ctx, `DELETE FROM outbox WHERE state = 'acked' AND uid NOT IN
		(SELECT uid FROM outbox WHERE state = 'acked' ORDER BY uid DESC LIMIT ?)`, d.cfg.RetainDone); err != nil {
		return sent, err
	}
	res, err = tx.ExecContext(ctx, `DELETE FROM outbox WHERE state IN ('failed', 'expired') AND uid NOT IN
		(SELECT uid FROM outbox WHERE state IN ('failed', 'expired') ORDER BY uid DESC LIMIT ?)`, d.cfg.RetainFailed)
	if err != nil {
		return sent, err
	}
	if n, _ := res.RowsAffected(); n > 0 {
		d.m.PurgedFailed += uint64(n)
	}
	return sent, tx.Commit()
}

// HandleDownlink authenticates and processes one datagram from a gateway.
// Datagrams that fail authentication, belong to another session or are
// malformed are dropped (and counted); they never change state.
func (d *Device) HandleDownlink(ctx context.Context, pkt []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.booted {
		return ErrNotBooted
	}
	f, err := reliable.OpenWithKey(pkt, security.Downlink, d.cfg.DeviceID, d.cfg.KeyID, &d.cfg.Key)
	if err != nil {
		d.m.DownlinkRejected++
		if reliable.CodeOf(err) == reliable.CodeBadMAC {
			d.authFailures++
			if d.authFailures%16 == 0 {
				d.violation(ctx, reliable.SecurityDownlinkAuthFailure, uint32(d.authFailures))
			}
		}
		return err
	}
	if f.SessionGen != d.gen {
		d.m.DownlinkStale++
		return &reliable.Error{Code: reliable.CodeStaleSession}
	}
	switch f.Type {
	case reliable.TypeEventAck:
		a, err := reliable.DecodeEventAck(f.Payload)
		if err != nil {
			d.m.DownlinkRejected++
			return err
		}
		return d.handleAck(ctx, a)
	case reliable.TypeCommandDelivery:
		cd, err := reliable.DecodeCommandDelivery(d.cfg.DeviceID, f.Payload)
		if err != nil {
			d.m.DownlinkRejected++
			return err
		}
		return d.handleCommand(ctx, cd)
	}
	d.m.DownlinkRejected++
	return &reliable.Error{Code: reliable.CodeUnknownType}
}

func (d *Device) handleAck(ctx context.Context, a reliable.EventAck) error {
	now := d.cfg.Now().UTC()
	var fp []byte
	var state string
	var created, class int64
	var firstSent sql.NullInt64
	err := d.store.db.W.QueryRowContext(ctx, `SELECT fingerprint, state, created_ns, class, first_sent_ns FROM outbox WHERE uid = ?`, int64(a.UID)).
		Scan(&fp, &state, &created, &class, &firstSent)
	if errors.Is(err, sql.ErrNoRows) {
		d.m.AcksUnmatched++
		return nil
	}
	if err != nil {
		return err
	}
	if string(fp) != string(a.Fingerprint[:]) {
		d.m.AcksUnmatched++ // authenticated, but for other content: never completes this event
		return nil
	}
	if state != "pending" {
		d.m.AcksDuplicate++
		return nil
	}
	switch {
	case a.Status.Success():
		if _, err := d.store.db.W.ExecContext(ctx, `UPDATE outbox SET state = 'acked', done_ns = ?, ack_status = ?, ack_position = ?
			WHERE uid = ? AND state = 'pending'`, now.UnixNano(), int64(a.Status), a.Position, int64(a.UID)); err != nil {
			return err
		}
		d.m.Acked++
		lat := now.Sub(time.Unix(0, created))
		if reliable.Class(class).Critical() {
			d.m.CriticalAckLatency.add(lat)
		} else {
			d.m.AckLatency.add(lat)
		}
	case a.Status.Permanent():
		if _, err := d.store.db.W.ExecContext(ctx, `UPDATE outbox SET state = 'failed', done_ns = ?, ack_status = ? WHERE uid = ? AND state = 'pending'`,
			now.UnixNano(), int64(a.Status), int64(a.UID)); err != nil {
			return err
		}
		d.m.Failed++
		d.log.Warn("device: event permanently rejected by the server; kept as failed", "uid", a.UID, "status", a.Status.String())
	default: // AckRetry: the server could not commit; retry after a backoff
		d.m.RetryAcks++
	}
	return nil
}

// violation queues a rate-limited SECURITY_VIOLATION (at most one per code
// per minute), so an attacker cannot flood the queue through it.
func (d *Device) violation(ctx context.Context, code reliable.SecurityCode, detail uint32) {
	now := d.cfg.Now()
	if t, ok := d.lastViolation[code]; ok && now.Sub(t) < time.Minute {
		return
	}
	d.lastViolation[code] = now
	if _, err := d.enqueue(ctx, func(uint64, time.Time) (reliable.Body, error) {
		return reliable.SecurityViolation{Code: code, Detail: detail}, nil
	}); err != nil {
		d.log.Warn("device: could not queue security violation", "err", err)
	}
}

// Tamper queues a SECURITY_VIOLATION for the simulated tamper switch.
func (d *Device) Tamper(ctx context.Context) (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.enqueue(ctx, func(uint64, time.Time) (reliable.Body, error) {
		return reliable.SecurityViolation{Code: reliable.SecurityTamper}, nil
	})
}
