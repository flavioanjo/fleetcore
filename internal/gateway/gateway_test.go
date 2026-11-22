package gateway

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/eventstore"
	"github.com/flavioanjo/fleetcore/internal/protocol"
	"github.com/flavioanjo/fleetcore/internal/sessions"
	"github.com/flavioanjo/fleetcore/internal/simulator"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

var clock = time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)

func quietLogger() *slog.Logger {
	// Debug level exercises every logging path without printing.
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func baseConfig() Config {
	return Config{
		Addr:       "127.0.0.1:0",
		Now:        func() time.Time { return clock },
		Logger:     quietLogger(),
		BatchDelay: time.Millisecond,
	}
}

// running is a gateway under test.
type running struct {
	g      *Gateway
	cancel context.CancelFunc
	done   chan error
	client net.Conn
}

func start(t *testing.T, cfg Config, store Appender, reg Observer) *running {
	t.Helper()
	g, err := New(cfg, store, reg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &running{g: g, cancel: cancel, done: make(chan error, 1)}
	go func() { r.done <- g.Run(ctx) }()
	r.client, err = net.Dial("udp", g.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		r.client.Close()
		r.stop(t)
	})
	return r
}

func (r *running) send(t *testing.T, pkts ...[]byte) {
	t.Helper()
	for i, p := range pkts {
		if _, err := r.client.Write(p); err != nil {
			t.Fatalf("send: %v", err)
		}
		if i%32 == 31 {
			time.Sleep(time.Millisecond) // stay well inside the socket buffer
		}
	}
}

// stop cancels the gateway and waits for Run to return (idempotent).
func (r *running) stop(t *testing.T) error {
	t.Helper()
	r.cancel()
	select {
	case err, ok := <-r.done:
		if ok {
			close(r.done)
		}
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after cancellation")
		return nil
	}
}

func settled(s Stats) uint64 {
	return s.Rejected() + s.QueueFull + s.Inserted + s.Duplicates + s.Conflicts + s.StoreFailed + s.ShutdownDropped
}

// waitFor polls the gateway stats until cond holds.
func waitFor(t *testing.T, g *Gateway, what string, cond func(Stats) bool) Stats {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		s := g.Stats()
		if cond(s) {
			return s
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s: %+v", what, s)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func waitSettled(t *testing.T, g *Gateway, received uint64) Stats {
	t.Helper()
	return waitFor(t, g, "pipeline to settle", func(s Stats) bool {
		return s.Received >= received && settled(s) == s.Received
	})
}

func simSamples(t *testing.T, vehicles, samples int) []telemetry.Sample {
	t.Helper()
	cfg := simulator.DefaultConfig()
	cfg.Vehicles, cfg.Samples = vehicles, samples
	var out []telemetry.Sample
	if err := simulator.Run(cfg, func(s telemetry.Sample) error {
		out = append(out, s)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func encode(t *testing.T, s telemetry.Sample) []byte {
	t.Helper()
	p, err := protocol.EncodeSample(s)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestEndToEndWithSQLite(t *testing.T) {
	ctx := context.Background()
	store, err := eventstore.Open(ctx, filepath.Join(t.TempDir(), "events.db"), eventstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	reg := sessions.New(sessions.Config{})
	r := start(t, baseConfig(), store, reg)

	samples := simSamples(t, 2, 5) // 30 samples, all before `clock`
	var pkts [][]byte
	for _, s := range samples {
		pkts = append(pkts, encode(t, s))
	}

	// Stage 1: originals and some malformed input.
	r.send(t, pkts...)
	flipped := append([]byte(nil), pkts[0]...)
	flipped[20] ^= 0x01
	future := samples[0]
	future.EventID = 1 << 40
	future.DeviceTime = clock.Add(10 * time.Minute) // beyond MaxClockSkew of ingestion time
	future.IngestedAt = telemetry.None[time.Time]() // valid on the wire by itself
	r.send(t, flipped, []byte("not a fleetcore packet"), make([]byte, 600), pkts[1][:20], encode(t, future))
	st := waitSettled(t, r.g, 35)
	if st.Inserted != 30 || st.Rejected() != 5 {
		t.Fatalf("stage 1: %+v", st)
	}
	for reason, n := range map[string]uint64{"checksum_mismatch": 1, "bad_magic": 1, "oversize": 1, "truncated": 1, "inconsistent_with_ingestion_time": 1} {
		if st.RejectedBy[reason] != n {
			t.Errorf("RejectedBy[%s] = %d, want %d (%v)", reason, st.RejectedBy[reason], n, st.RejectedBy)
		}
	}

	// Stage 2: retransmissions and a conflicting reuse of an identity.
	r.send(t, pkts...)
	conflict := samples[3]
	conflict.Quality = telemetry.QualityUnavailable
	conflict.Distance = telemetry.None[telemetry.Distance]()
	r.send(t, encode(t, conflict))
	st = waitSettled(t, r.g, 66)
	if st.Inserted != 30 || st.Duplicates != 30 || st.Conflicts != 1 {
		t.Fatalf("stage 2: %+v", st)
	}
	if st.Diagnostics["new_session"] != 2 {
		t.Errorf("diagnostics %v, want 2 new sessions", st.Diagnostics)
	}

	if err := r.stop(t); err != nil {
		t.Fatalf("Run: %v", err)
	}
	recs, err := store.ReadAfter(ctx, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 30 {
		t.Fatalf("%d stored events, want 30", len(recs))
	}
	for _, rec := range recs {
		if ing, _ := rec.Sample.IngestedAt.Get(); !ing.Equal(clock) {
			t.Fatalf("IngestedAt %v, want gateway receipt time %v", ing, clock)
		}
	}
	key := eventstore.Key{DeviceID: conflict.DeviceID, BootID: conflict.BootID, EventID: conflict.EventID}
	rec, _ := store.Get(ctx, key)
	if !rec.Sample.Distance.IsPresent() {
		t.Fatal("conflicting payload overwrote the stored event")
	}
	if cs, _ := store.Conflicts(ctx, key); len(cs) != 1 {
		t.Fatalf("%d conflicts recorded, want 1", len(cs))
	}
}

// memStore is an in-memory Appender with controllable failure modes.
type memStore struct {
	mu       sync.Mutex
	stored   map[eventstore.Key]eventstore.Fingerprint
	events   int           // events passed to AppendBatch
	block    chan struct{} // if non-nil, AppendBatch waits for it to close
	failN    int           // fail this many calls first
	attempts int
}

func newMemStore() *memStore { return &memStore{stored: map[eventstore.Key]eventstore.Fingerprint{}} }

// eventCount returns the number of events passed to AppendBatch.
func (m *memStore) eventCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.events
}

func (m *memStore) AppendBatch(ctx context.Context, evs []eventstore.Event) ([]eventstore.Result, error) {
	if m.block != nil {
		select {
		case <-m.block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.attempts++
	if m.failN > 0 {
		m.failN--
		return nil, errors.New("disk on fire")
	}
	m.events += len(evs)
	out := make([]eventstore.Result, len(evs))
	for i, ev := range evs {
		fp, ok := m.stored[ev.Key()]
		switch {
		case !ok:
			m.stored[ev.Key()] = ev.Fingerprint
			out[i].Outcome = eventstore.Inserted
		case fp == ev.Fingerprint:
			out[i].Outcome = eventstore.Duplicate
		default:
			out[i].Outcome = eventstore.Conflict
		}
	}
	return out, nil
}

func distinctPackets(t *testing.T, n int) [][]byte {
	t.Helper()
	var out [][]byte
	for i := 1; i <= n; i++ {
		out = append(out, encode(t, telemetry.Sample{
			DeviceID: "bp-1", EventID: telemetry.EventID(i), BootID: 1, Sequence: telemetry.Sequence(i),
			DeviceTime: clock.Add(-time.Hour), Source: telemetry.SourceCAN, Quality: telemetry.QualityGood,
			Distance: telemetry.Some(telemetry.Distance{Kind: telemetry.DistanceCumulative, Value: telemetry.Millimetres(i)}),
		}))
	}
	return out
}

func TestBackpressureShedsAtSocketOnly(t *testing.T) {
	store := newMemStore()
	store.block = make(chan struct{})
	cfg := baseConfig()
	cfg.QueueSize, cfg.StoreQueueSize, cfg.Workers, cfg.BatchSize = 4, 2, 2, 1
	base := runtime.NumGoroutine()
	r := start(t, cfg, store, sessions.New(sessions.Config{}))

	const n = 300
	r.send(t, distinctPackets(t, n)...)
	st := waitFor(t, r.g, "all datagrams read", func(s Stats) bool { return s.Received == n })
	// The store is blocked: at most queue + store queue + one per worker +
	// one in the writer can be held; the rest must have been shed.
	held := uint64(cfg.QueueSize + cfg.StoreQueueSize + cfg.Workers + 1)
	if st.QueueFull < n-held {
		t.Fatalf("QueueFull = %d, want >= %d", st.QueueFull, n-held)
	}
	if g := runtime.NumGoroutine(); g > base+2+cfg.Workers+3 {
		t.Fatalf("%d goroutines (base %d): goroutine count grows with load", g, base)
	}

	close(store.block)
	st = waitSettled(t, r.g, n)
	if st.Inserted+st.QueueFull != n || st.Inserted == 0 {
		t.Fatalf("accounting: %+v", st)
	}
}

func TestGracefulShutdownDrainsQueues(t *testing.T) {
	store := newMemStore()
	store.block = make(chan struct{})
	cfg := baseConfig()
	cfg.BatchSize = 8
	r := start(t, cfg, store, sessions.New(sessions.Config{}))

	const n = 100
	r.send(t, distinctPackets(t, n)...)
	waitFor(t, r.g, "all datagrams read", func(s Stats) bool { return s.Received == n })

	stopped := make(chan error, 1)
	go func() { stopped <- r.stop(t) }()
	time.Sleep(20 * time.Millisecond) // shutdown has begun while the store is blocked
	close(store.block)
	if err := <-stopped; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if st := r.g.Stats(); st.Inserted != n || store.eventCount() != n {
		t.Fatalf("after shutdown: %+v, store saw %d", st, store.eventCount())
	}
}

func TestDrainTimeoutAbandonsBlockedWrites(t *testing.T) {
	store := newMemStore()
	store.block = make(chan struct{}) // never released
	cfg := baseConfig()
	cfg.DrainTimeout = 100 * time.Millisecond
	cfg.StoreRetries = -1
	r := start(t, cfg, store, sessions.New(sessions.Config{}))

	r.send(t, distinctPackets(t, 20)...)
	waitFor(t, r.g, "all datagrams read", func(s Stats) bool { return s.Received == 20 })
	begin := time.Now()
	if err := r.stop(t); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if d := time.Since(begin); d > 3*time.Second {
		t.Fatalf("shutdown took %s despite 100ms drain timeout", d)
	}
	st := r.g.Stats()
	if settled(st) != st.Received || st.ShutdownDropped == 0 || st.Inserted != 0 {
		t.Fatalf("stats %+v", st)
	}
}

func TestStoreRetry(t *testing.T) {
	store := newMemStore()
	store.failN = 2
	cfg := baseConfig()
	cfg.RetryBackoff = time.Millisecond
	r := start(t, cfg, store, sessions.New(sessions.Config{}))
	r.send(t, distinctPackets(t, 1)...)
	st := waitSettled(t, r.g, 1)
	if st.Inserted != 1 || st.StoreRetries != 2 {
		t.Fatalf("stats %+v", st)
	}

	store.mu.Lock()
	store.failN = 100
	store.mu.Unlock()
	r.send(t, distinctPackets(t, 2)[1])
	st = waitSettled(t, r.g, 2)
	if st.StoreFailed != 1 {
		t.Fatalf("exhausted retries: %+v", st)
	}
}

func TestDedupCacheShortCircuitsRetransmissions(t *testing.T) {
	store := newMemStore()
	r := start(t, baseConfig(), store, sessions.New(sessions.Config{}))
	pkt := distinctPackets(t, 1)[0]
	r.send(t, pkt)
	waitSettled(t, r.g, 1)
	r.send(t, pkt, pkt)
	st := waitSettled(t, r.g, 3)
	if st.Duplicates != 2 || st.DuplicatesCached != 2 {
		t.Fatalf("stats %+v", st)
	}
	if store.eventCount() != 1 {
		t.Fatalf("store saw %d events; retransmissions were not answered from the cache", store.eventCount())
	}

	// A conflicting payload for a cached identity still reaches the store.
	s := telemetry.Sample{
		DeviceID: "bp-1", EventID: 1, BootID: 1, Sequence: 1,
		DeviceTime: clock.Add(-time.Hour), Source: telemetry.SourceCAN, Quality: telemetry.QualityGood,
		Distance: telemetry.Some(telemetry.Distance{Kind: telemetry.DistanceCumulative, Value: 999}),
	}
	r.send(t, encode(t, s))
	if st := waitSettled(t, r.g, 4); st.Conflicts != 1 {
		t.Fatalf("stats %+v", st)
	}
}

func TestMaxDatagramSize(t *testing.T) {
	cfg := baseConfig()
	cfg.MaxDatagramSize = 60
	r := start(t, cfg, newMemStore(), sessions.New(sessions.Config{}))
	long := encode(t, telemetry.Sample{
		DeviceID: telemetry.DeviceID(strings.Repeat("x", 32)), EventID: 1, BootID: 1, Sequence: 1,
		DeviceTime: clock.Add(-time.Hour), Source: telemetry.SourceGPS, Quality: telemetry.QualityUnavailable,
	})
	if len(long) <= 60 {
		t.Fatalf("test packet is %d bytes", len(long))
	}
	r.send(t, long, distinctPackets(t, 1)[0])
	st := waitSettled(t, r.g, 2)
	if st.RejectedBy["oversize"] != 1 || st.Inserted != 1 {
		t.Fatalf("stats %+v", st)
	}
}

func TestConfigValidation(t *testing.T) {
	for _, size := range []int{5, protocol.MaxPacketSize + 1} {
		cfg := baseConfig()
		cfg.MaxDatagramSize = size
		if _, err := New(cfg, newMemStore(), sessions.New(sessions.Config{})); err == nil {
			t.Errorf("MaxDatagramSize %d accepted", size)
		}
	}
	if _, err := New(baseConfig(), nil, sessions.New(sessions.Config{})); err == nil {
		t.Error("nil store accepted")
	}
	cfg := baseConfig()
	cfg.Addr = "not an address"
	if _, err := New(cfg, newMemStore(), sessions.New(sessions.Config{})); err == nil {
		t.Error("bad address accepted")
	}
}

func TestRunTwice(t *testing.T) {
	r := start(t, baseConfig(), newMemStore(), sessions.New(sessions.Config{}))
	if err := r.stop(t); err != nil {
		t.Fatal(err)
	}
	if err := r.g.Run(context.Background()); !errors.Is(err, ErrAlreadyRun) {
		t.Fatalf("second Run: %v", err)
	}
}
