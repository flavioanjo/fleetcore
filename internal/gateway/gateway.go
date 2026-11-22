// Package gateway receives FleetCore protocol packets over UDP, validates
// them, stamps their ingestion time, persists them through the event store
// and reports session diagnostics.
//
// # Pipeline
//
//	read loop ──queue──► decode workers ──store queue──► writer ──► event store
//	(1 goroutine)        (Config.Workers)                (1)        └► session registry
//
// The goroutine count is fixed at 2+Workers for the lifetime of Run; no
// goroutine is created per packet.
//
// # Backpressure
//
// UDP has no flow control, so the gateway sheds load at exactly one place:
// the read loop hands each datagram to the bounded decode queue without
// blocking and, when the queue is full, drops that (newest) datagram and
// counts it in Stats.QueueFull. Everything downstream blocks instead of
// dropping: decode workers wait for room in the bounded store queue, so a
// slow store fills the store queue, then the decode queue, and only then
// causes drops at the socket. The kernel socket buffer (Config.ReadBuffer)
// absorbs bursts before that. Dropped datagrams are never acknowledged
// (the protocol has no acknowledgements); devices that retransmit are
// deduplicated by the event store.
//
// # Ingestion time
//
// IngestedAt is taken from Config.Now immediately after the datagram is
// read, before queueing, so queueing and storage delays do not inflate it.
// A retransmission keeps the IngestedAt of its first stored receipt.
//
// # Security
//
// The CRC-32C in every packet detects accidental corruption only. It does
// not authenticate the sender: anyone who can reach the socket can submit
// well-formed packets for any DeviceID. Source addresses are logged for
// diagnosis, not trusted.
package gateway

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"runtime"
	"sync"
	"time"

	"github.com/flavioanjo/fleetcore/internal/eventstore"
	"github.com/flavioanjo/fleetcore/internal/protocol"
	"github.com/flavioanjo/fleetcore/internal/sessions"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// Appender is the part of the event store the gateway needs.
// *eventstore.Store implements it.
type Appender interface {
	AppendBatch(ctx context.Context, evs []eventstore.Event) ([]eventstore.Result, error)
}

// Observer is the part of the session registry the gateway needs.
// *sessions.Registry implements it.
type Observer interface {
	Observe(s telemetry.Sample) (sessions.Observation, error)
	Expire(now time.Time) int
}

// Config configures a Gateway. Zero fields select the defaults.
type Config struct {
	// Addr is the UDP listen address. Default ":7400".
	Addr string
	// MaxDatagramSize: larger datagrams are rejected without decoding.
	// Must be in [protocol.MinPacketSize, protocol.MaxPacketSize].
	// Default protocol.MaxPacketSize.
	MaxDatagramSize int
	// ReadBuffer sets the socket receive buffer (SO_RCVBUF) in bytes;
	// 0 keeps the OS default.
	ReadBuffer int
	// QueueSize is the capacity of the decode queue. Default 4096.
	QueueSize int
	// Workers is the number of decode workers. Default GOMAXPROCS.
	Workers int
	// StoreQueueSize is the capacity of the store queue. Default 4096.
	StoreQueueSize int
	// BatchSize is the maximum number of events per store transaction.
	// Default 256.
	BatchSize int
	// BatchDelay is the longest an event waits for its batch to fill.
	// Default 10ms.
	BatchDelay time.Duration
	// WriteTimeout bounds one store transaction attempt. Default 10s.
	WriteTimeout time.Duration
	// StoreRetries is how many times a failed batch is retried before its
	// events are dropped and counted in Stats.StoreFailed. Default 3.
	StoreRetries int
	// RetryBackoff is the delay before the first retry; it doubles per
	// attempt. Default 100ms.
	RetryBackoff time.Duration
	// DrainTimeout bounds how long shutdown waits for queued events to be
	// stored. Default 10s.
	DrainTimeout time.Duration
	// DedupCacheSize is the number of recently stored identities kept in
	// memory to answer retransmissions without a database round trip.
	// It is an optimization only; the store remains authoritative.
	// Default 65536; negative disables the cache.
	DedupCacheSize int
	// ExpireInterval is how often the session registry is expired.
	// Default 1m.
	ExpireInterval time.Duration
	// Now is the ingestion clock. Default time.Now.
	Now func() time.Time
	// Logger receives structured logs. Default slog.Default().
	Logger *slog.Logger
}

func (c Config) withDefaults() (Config, error) {
	if c.Addr == "" {
		c.Addr = ":7400"
	}
	if c.MaxDatagramSize == 0 {
		c.MaxDatagramSize = protocol.MaxPacketSize
	}
	if c.MaxDatagramSize < protocol.MinPacketSize || c.MaxDatagramSize > protocol.MaxPacketSize {
		return c, fmt.Errorf("gateway: MaxDatagramSize %d outside [%d, %d]", c.MaxDatagramSize, protocol.MinPacketSize, protocol.MaxPacketSize)
	}
	if c.ReadBuffer < 0 {
		return c, fmt.Errorf("gateway: negative ReadBuffer")
	}
	def := func(v *int, d int) {
		if *v <= 0 {
			*v = d
		}
	}
	defDur := func(v *time.Duration, d time.Duration) {
		if *v <= 0 {
			*v = d
		}
	}
	def(&c.QueueSize, 4096)
	def(&c.Workers, runtime.GOMAXPROCS(0))
	def(&c.StoreQueueSize, 4096)
	def(&c.BatchSize, 256)
	if c.StoreRetries < 0 {
		c.StoreRetries = 0
	} else if c.StoreRetries == 0 {
		c.StoreRetries = 3
	}
	if c.DedupCacheSize == 0 {
		c.DedupCacheSize = 65536
	}
	defDur(&c.BatchDelay, 10*time.Millisecond)
	defDur(&c.WriteTimeout, 10*time.Second)
	defDur(&c.RetryBackoff, 100*time.Millisecond)
	defDur(&c.DrainTimeout, 10*time.Second)
	defDur(&c.ExpireInterval, time.Minute)
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return c, nil
}

// Gateway is a UDP ingestion gateway. Create it with New, then call Run once.
type Gateway struct {
	cfg   Config
	conn  *net.UDPConn
	store Appender
	reg   Observer
	log   *slog.Logger
	stats *counters
	cache *dedupCache
	// writerLogs rate-limits per-event warnings and diagnostics emitted by
	// the writer goroutine (which owns it), so hostile or broken input cannot
	// turn into a log flood. Counters are never rate-limited.
	writerLogs *burstLimiter

	queue  chan datagram
	storeQ chan item

	runOnce sync.Once
}

type datagram struct {
	data []byte
	from netip.AddrPort
	at   time.Time
}

type item struct {
	ev   eventstore.Event
	from netip.AddrPort
}

// New validates cfg and binds the UDP socket, so address errors surface
// before Run.
func New(cfg Config, store Appender, reg Observer) (*Gateway, error) {
	if store == nil || reg == nil {
		return nil, errors.New("gateway: store and registry are required")
	}
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	addr, err := net.ResolveUDPAddr("udp", cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("gateway: resolve %q: %w", cfg.Addr, err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("gateway: listen: %w", err)
	}
	if cfg.ReadBuffer > 0 {
		if err := conn.SetReadBuffer(cfg.ReadBuffer); err != nil {
			conn.Close()
			return nil, fmt.Errorf("gateway: set read buffer: %w", err)
		}
	}
	return &Gateway{
		cfg:        cfg,
		conn:       conn,
		store:      store,
		reg:        reg,
		log:        cfg.Logger,
		stats:      &counters{},
		cache:      newDedupCache(cfg.DedupCacheSize),
		writerLogs: newBurstLimiter(time.Second, 20),
		queue:      make(chan datagram, cfg.QueueSize),
		storeQ:     make(chan item, cfg.StoreQueueSize),
	}, nil
}

// Addr returns the bound local address.
func (g *Gateway) Addr() net.Addr { return g.conn.LocalAddr() }

// Stats returns a snapshot of the gateway counters.
func (g *Gateway) Stats() Stats { return g.stats.snapshot() }

// ErrAlreadyRun is returned by a second call to Run.
var ErrAlreadyRun = errors.New("gateway: Run called twice")

// Run receives packets until ctx is cancelled, then shuts down gracefully:
// it stops reading, closes the socket, and stores every queued event, giving
// up after Config.DrainTimeout (events still queued then are counted in
// Stats.ShutdownDropped). It returns nil after a clean shutdown.
func (g *Gateway) Run(ctx context.Context) error {
	err := ErrAlreadyRun
	g.runOnce.Do(func() { err = g.run(ctx) })
	return err
}

func (g *Gateway) run(ctx context.Context) error {
	g.log.Info("gateway started", "addr", g.Addr().String(), "max_datagram", g.cfg.MaxDatagramSize,
		"queue", g.cfg.QueueSize, "workers", g.cfg.Workers, "store_queue", g.cfg.StoreQueueSize, "batch", g.cfg.BatchSize)

	// writeCtx governs store operations. It outlives ctx so queued events
	// can still be stored during shutdown, and is cancelled when the drain
	// deadline passes.
	writeCtx, cancelWrite := context.WithCancel(context.Background())
	defer cancelWrite()

	readDone := make(chan error, 1)
	go func() { readDone <- g.readLoop() }()

	var workers sync.WaitGroup
	for i := 0; i < g.cfg.Workers; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			g.decodeLoop(writeCtx)
		}()
	}
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		g.writeLoop(writeCtx)
	}()
	go func() {
		workers.Wait()
		close(g.storeQ)
	}()

	var readErr error
	select {
	case <-ctx.Done():
		g.conn.Close() // unblocks the read loop
		readErr = <-readDone
	case readErr = <-readDone: // fatal socket error
		g.conn.Close()
	}
	close(g.queue)

	drain := time.AfterFunc(g.cfg.DrainTimeout, func() {
		g.log.Warn("drain timeout reached; abandoning queued events", "timeout", g.cfg.DrainTimeout)
		cancelWrite()
	})
	<-writerDone
	drain.Stop()

	st := g.Stats()
	g.log.Info("gateway stopped", "received", st.Received, "inserted", st.Inserted,
		"duplicates", st.Duplicates, "conflicts", st.Conflicts, "rejected", st.Rejected(),
		"queue_full", st.QueueFull, "store_failed", st.StoreFailed, "shutdown_dropped", st.ShutdownDropped)
	return readErr
}

// readLoop reads datagrams until the socket is closed. It returns nil on
// close and an error only for a persistent socket failure.
func (g *Gateway) readLoop() error {
	// One extra byte detects oversize datagrams on platforms that truncate
	// silently.
	buf := make([]byte, g.cfg.MaxDatagramSize+1)
	drops := newLogLimiter(time.Second)
	var consecutiveErrs int
	for {
		n, from, err := g.conn.ReadFromUDPAddrPort(buf)
		at := g.cfg.Now().UTC()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			if isMessageTooLarge(err) { // Windows reports oversize as an error
				g.stats.received.Add(1)
				g.reject(rejOversize, from, "datagram larger than receive buffer")
				continue
			}
			consecutiveErrs++
			g.log.Warn("udp read failed", "err", err, "consecutive", consecutiveErrs)
			if consecutiveErrs >= 100 {
				return fmt.Errorf("gateway: read: %w", err)
			}
			time.Sleep(10 * time.Millisecond) // avoid spinning on a failing socket
			continue
		}
		consecutiveErrs = 0
		g.stats.received.Add(1)
		if n > g.cfg.MaxDatagramSize {
			g.reject(rejOversize, from, fmt.Sprintf("datagram exceeds %d bytes", g.cfg.MaxDatagramSize))
			continue
		}
		d := datagram{data: append([]byte(nil), buf[:n]...), from: from, at: at}
		select {
		case g.queue <- d:
		default:
			total := g.stats.queueFull.Add(1)
			if drops.allow(at) {
				g.log.Warn("decode queue full; dropping datagrams", "dropped_total", total, "queue", g.cfg.QueueSize)
			}
		}
	}
}

func (g *Gateway) decodeLoop(writeCtx context.Context) {
	for d := range g.queue {
		s, err := protocol.DecodeSample(d.data)
		if err != nil {
			var pe *protocol.Error
			code := rejMalformed
			if errors.As(err, &pe) {
				code = rejectReason(pe.Code)
			}
			g.reject(code, d.from, err.Error())
			continue
		}
		s.IngestedAt = telemetry.Some(d.at)
		ev, err := eventstore.NewEvent(s) // re-validates with IngestedAt (clock skew)
		if err != nil {
			g.reject(rejInconsistent, d.from, err.Error())
			continue
		}
		select {
		case g.storeQ <- item{ev: ev, from: d.from}:
		case <-writeCtx.Done():
			g.stats.shutdownDropped.Add(1)
		}
	}
}

func (g *Gateway) reject(r reason, from netip.AddrPort, detail string) {
	g.stats.rejected[r].Add(1)
	if g.log.Enabled(context.Background(), slog.LevelDebug) {
		g.log.Debug("packet rejected", "reason", r.String(), "from", from.String(), "detail", detail)
	}
}

func (g *Gateway) writeLoop(writeCtx context.Context) {
	batch := make([]item, 0, g.cfg.BatchSize)
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	expire := time.NewTicker(g.cfg.ExpireInterval)
	defer expire.Stop()

	for {
		// Wait for the first item of a batch.
		select {
		case it, ok := <-g.storeQ:
			if !ok {
				return
			}
			batch = append(batch, it)
		case <-expire.C:
			if n := g.reg.Expire(g.cfg.Now().UTC()); n > 0 {
				g.log.Debug("session registry expired devices", "devices", n)
			}
			continue
		}
		// Fill it until full, delayed or closed.
		timer.Reset(g.cfg.BatchDelay)
		closed := false
	fill:
		for len(batch) < g.cfg.BatchSize {
			select {
			case it, ok := <-g.storeQ:
				if !ok {
					closed = true
					break fill
				}
				batch = append(batch, it)
			case <-timer.C:
				break fill
			}
		}
		if !timer.Stop() {
			select { // drain a fired timer so Reset is safe on all Go versions
			case <-timer.C:
			default:
			}
		}
		g.flush(writeCtx, batch)
		batch = batch[:0]
		if closed {
			return
		}
	}
}

// flush stores a batch and processes the results.
func (g *Gateway) flush(writeCtx context.Context, batch []item) {
	// Answer retransmissions of recently stored events from the cache;
	// everything else (including possible conflicts) goes to the store.
	pending := make([]item, 0, len(batch))
	for _, it := range batch {
		if g.cache.isStoredDuplicate(it.ev.Key(), it.ev.Fingerprint) {
			g.stats.duplicates.Add(1)
			g.stats.duplicatesCached.Add(1)
			continue
		}
		pending = append(pending, it)
	}
	if len(pending) == 0 {
		return
	}
	evs := make([]eventstore.Event, len(pending))
	for i, it := range pending {
		evs[i] = it.ev
	}

	results, err := g.appendWithRetry(writeCtx, evs)
	if err != nil {
		if writeCtx.Err() != nil {
			g.stats.shutdownDropped.Add(uint64(len(evs)))
		} else {
			g.stats.storeFailed.Add(uint64(len(evs)))
		}
		g.log.Error("events not stored", "events", len(evs), "err", err)
		return
	}
	for i, res := range results {
		it := pending[i]
		s := it.ev.Sample
		switch res.Outcome {
		case eventstore.Inserted:
			// The outcome counter is published last, so a caller that sees
			// the pipeline settled also sees this event's diagnostics.
			g.cache.add(it.ev.Key(), it.ev.Fingerprint)
			if len(res.EventIDAlsoInBoots) > 0 {
				g.stats.eventIDReused.Add(1)
				if g.logAllowed() {
					boots := make([]uint32, len(res.EventIDAlsoInBoots))
					for i, b := range res.EventIDAlsoInBoots {
						boots[i] = uint32(b)
					}
					g.log.Warn("EventID reused across boots (violates per-device uniqueness); event stored",
						"device_id", string(s.DeviceID), "event_id", uint64(s.EventID), "boot_id", uint32(s.BootID),
						"also_in_boots", boots)
				}
			}
			g.observe(s)
			g.stats.inserted.Add(1)
		case eventstore.Duplicate:
			g.stats.duplicates.Add(1)
			g.cache.add(it.ev.Key(), it.ev.Fingerprint)
		case eventstore.Conflict:
			g.stats.conflicts.Add(1)
			if !g.logAllowed() {
				continue
			}
			g.log.Warn("conflicting event: identity reused with a different payload; stored event kept",
				"device_id", string(s.DeviceID), "event_id", uint64(s.EventID), "boot_id", uint32(s.BootID),
				"seq", uint32(s.Sequence), "from", it.from.String(),
				"stored_fingerprint", res.Existing.String(), "conflicting_fingerprint", it.ev.Fingerprint.String())
		}
	}
}

func (g *Gateway) appendWithRetry(writeCtx context.Context, evs []eventstore.Event) ([]eventstore.Result, error) {
	backoff := g.cfg.RetryBackoff
	var err error
	for attempt := 0; ; attempt++ {
		ctx, cancel := context.WithTimeout(writeCtx, g.cfg.WriteTimeout)
		var res []eventstore.Result
		res, err = g.store.AppendBatch(ctx, evs)
		cancel()
		if err == nil {
			return res, nil
		}
		if attempt >= g.cfg.StoreRetries || writeCtx.Err() != nil {
			return nil, err
		}
		g.stats.storeRetries.Add(1)
		g.log.Warn("store write failed; retrying", "attempt", attempt+1, "events", len(evs), "backoff", backoff, "err", err)
		select {
		case <-time.After(backoff):
		case <-writeCtx.Done():
			return nil, err
		}
		backoff *= 2
	}
}

// logAllowed applies the writer's log rate limit, reporting suppressed lines.
func (g *Gateway) logAllowed() bool {
	ok, suppressed := g.writerLogs.allow(time.Now())
	if suppressed > 0 {
		g.log.Warn("log lines suppressed by rate limit", "suppressed", suppressed, "limit_per_second", g.writerLogs.max)
	}
	return ok
}

func (g *Gateway) observe(s telemetry.Sample) {
	obs, err := g.reg.Observe(s)
	if err != nil {
		g.log.Error("session registry rejected stored event", "err", err)
		return
	}
	for _, d := range obs.Diagnostics {
		g.stats.countDiagnostic(d.Kind)
		level := slog.LevelDebug
		switch d.Kind {
		case sessions.NewSession, sessions.RetiredSession, sessions.ChronologyAmbiguous, sessions.SequenceReuse:
			level = slog.LevelInfo
		}
		if !g.log.Enabled(context.Background(), level) || !g.logAllowed() {
			continue
		}
		attrs := []any{"diagnostic", d.Kind.String(), "provisional", true,
			"device_id", string(s.DeviceID), "boot_id", uint32(s.BootID), "seq", uint32(s.Sequence), "event_id", uint64(s.EventID)}
		if d.OtherBoot != 0 {
			attrs = append(attrs, "other_boot_id", uint32(d.OtherBoot))
		}
		if d.Kind == sessions.SequenceGap {
			attrs = append(attrs, "missing", d.Count, "expected_seq", uint32(d.Expected))
		}
		if d.Detail != "" {
			attrs = append(attrs, "detail", d.Detail)
		}
		g.log.Log(context.Background(), level, "session diagnostic", attrs...)
	}
}
