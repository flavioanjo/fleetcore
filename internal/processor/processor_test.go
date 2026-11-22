package processor

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/chrono"
	"github.com/flavioanjo/fleetcore/internal/eventstore"
	"github.com/flavioanjo/fleetcore/internal/fusion"
	"github.com/flavioanjo/fleetcore/internal/resultstore"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

var t0 = time.Date(2026, 6, 1, 8, 0, 0, 0, time.UTC)

const m = telemetry.Metre

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

type harness struct {
	t       testing.TB
	dir     string
	events  *eventstore.Store
	results *resultstore.Store
	clock   *fakeClock
}

func newHarness(t testing.TB) *harness {
	t.Helper()
	h := &harness{t: t, dir: t.TempDir(), clock: &fakeClock{t: t0.Add(time.Hour)}}
	h.reopen()
	return h
}

// reopen (re)opens both databases, as a restarted process would.
func (h *harness) reopen() {
	h.t.Helper()
	if h.events != nil {
		h.events.Close()
		h.results.Close()
	}
	var err error
	if h.events, err = eventstore.Open(context.Background(), filepath.Join(h.dir, "events.db"), eventstore.Options{}); err != nil {
		h.t.Fatal(err)
	}
	if h.results, err = resultstore.Open(context.Background(), filepath.Join(h.dir, "results.db"), resultstore.Options{}); err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { h.events.Close(); h.results.Close() })
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func (h *harness) processor(mutate ...func(*Config)) *Processor {
	h.t.Helper()
	cfg := Config{Now: h.clock.now, Logger: quiet(), Chrono: chrono.Config{Hold: 2 * time.Second}}
	for _, f := range mutate {
		f(&cfg)
	}
	p, err := New(cfg, h.events, h.results)
	if err != nil {
		h.t.Fatal(err)
	}
	if _, err := p.Recover(context.Background()); err != nil {
		h.t.Fatal(err)
	}
	return p
}

// processorB is processor for benchmarks.
func (h *harness) processorB(b *testing.B) *Processor {
	b.Helper()
	p, err := New(Config{Now: h.clock.now, Logger: quiet(), ReadBatch: 8192}, h.events, h.results)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := p.Recover(context.Background()); err != nil {
		b.Fatal(err)
	}
	return p
}

// sample builds a GPS sample; device time t0 + devSec.
func sample(dev string, boot telemetry.BootID, seq telemetry.Sequence, devSec int, mm telemetry.Millimetres) telemetry.Sample {
	return telemetry.Sample{
		DeviceID: telemetry.DeviceID(dev), BootID: boot, Sequence: seq,
		EventID:    telemetry.EventID(uint64(boot)<<32|uint64(seq)) + 1,
		DeviceTime: t0.Add(time.Duration(devSec) * time.Second),
		IngestedAt: telemetry.Some(t0.Add(time.Duration(devSec)*time.Second + 300*time.Millisecond)),
		Source:     telemetry.SourceGPS, Quality: telemetry.QualityGood,
		Distance: telemetry.Some(telemetry.Distance{Kind: telemetry.DistanceDelta, Value: mm}),
	}
}

func (h *harness) store(ss ...telemetry.Sample) {
	h.t.Helper()
	for _, s := range ss {
		ev, err := eventstore.NewEvent(s)
		if err != nil {
			h.t.Fatal(err)
		}
		if _, err := h.events.Append(context.Background(), ev); err != nil {
			h.t.Fatal(err)
		}
	}
}

func step(t testing.TB, p *Processor) int {
	t.Helper()
	n, err := p.Step(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// settle reads what is stored, lets the hold expire and releases it.
func (h *harness) settle(p *Processor) {
	h.t.Helper()
	step(h.t, p)
	h.clock.advance(3 * time.Second)
	step(h.t, p)
}

func journal(t *testing.T, rs *resultstore.Store) []resultstore.Entry {
	t.Helper()
	es, err := rs.Journal(context.Background(), 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	return es
}

func eventOrder(es []resultstore.Entry) []telemetry.Sequence {
	var out []telemetry.Sequence
	for _, e := range es {
		if e.Kind == resultstore.KindEvent {
			out = append(out, e.Sequence)
		}
	}
	return out
}

func TestHoldThenSequenceOrder(t *testing.T) {
	h := newHarness(t)
	p := h.processor()
	// Stored (ingested) out of order: 3, 1, 2.
	h.store(sample("d", 1, 3, 30, 300*m), sample("d", 1, 1, 10, 100*m), sample("d", 1, 2, 20, 200*m))
	step(t, p)
	if got := eventOrder(journal(t, h.results)); len(got) != 0 {
		t.Fatalf("released before the hold expired: %v", got)
	}
	h.clock.advance(2 * time.Second)
	step(t, p)
	if got := eventOrder(journal(t, h.results)); len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("journal order %v, want [1 2 3]", got)
	}
	// A new session buffers until it learns its first sequence number (one
	// reorder window of device time, fusion.md §3.2), so the three events
	// are buffered until a tick drains the idle device.
	c, _ := h.results.Count(context.Background())
	if c.Outcomes["buffered"] != 3 {
		t.Fatalf("outcomes before the idle drain %v", c.Outcomes)
	}
	for i := 0; i < 65; i++ {
		h.clock.advance(time.Second)
		step(t, p)
	}
	c, _ = h.results.Count(context.Background())
	if c.Outcomes["accepted"] != 3 {
		t.Fatalf("outcomes after the idle drain %v", c.Outcomes)
	}
}

func TestSessionsByDeviceTimeAcrossGatewaysOrder(t *testing.T) {
	h := newHarness(t)
	p := h.processor()
	// Boot 7 was generated first (device time 0..20) but stored after boot 9.
	h.store(sample("d", 9, 1, 100, 10*m), sample("d", 9, 2, 110, 10*m))
	h.store(sample("d", 7, 1, 0, 10*m), sample("d", 7, 2, 10, 10*m))
	h.settle(p)
	var boots []telemetry.BootID
	for _, e := range journal(t, h.results) {
		if e.Kind == resultstore.KindEvent {
			boots = append(boots, e.BootID)
		}
	}
	if len(boots) != 4 || boots[0] != 7 || boots[1] != 7 || boots[2] != 9 {
		t.Fatalf("boot order %v, want 7 7 9 9", boots)
	}
	c, _ := h.results.Count(context.Background())
	if c.Outcomes["rejected/stale_session"] != 0 {
		t.Fatalf("device-time ordering should avoid stale-session rejections: %v", c.Outcomes)
	}
}

func TestLateEventIsJournaledWithDiagnostic(t *testing.T) {
	h := newHarness(t)
	p := h.processor()
	h.store(sample("d", 1, 1, 10, 10*m), sample("d", 1, 3, 30, 10*m))
	h.settle(p)
	h.store(sample("d", 1, 2, 20, 10*m)) // arrives after 3 was released
	h.settle(p)
	es := journal(t, h.results)
	last := es[len(es)-1]
	for i := len(es) - 1; i >= 0; i-- {
		if es[i].Kind == resultstore.KindEvent {
			last = es[i]
			break
		}
	}
	if last.Sequence != 2 || last.Flags&chrono.FlagLate == 0 {
		t.Fatalf("late event journaled as seq %d flags %v", last.Sequence, last.Flags)
	}
	c, _ := h.results.Count(context.Background())
	if c.Diagnostics["chronology:late"] != 1 {
		t.Fatalf("diagnostics %v", c.Diagnostics)
	}
}

func TestWrapAroundThroughProcessor(t *testing.T) {
	h := newHarness(t)
	p := h.processor()
	max := ^telemetry.Sequence(0)
	var ss []telemetry.Sample
	for i, seq := range []telemetry.Sequence{max - 2, max - 1, max, 0, 1, 2} {
		s := sample("w", 1, seq, i*10, 50*m)
		s.EventID = telemetry.EventID(1000 + i) // a wrap-safe producer: EventIDs stay unique
		ss = append(ss, s)
	}
	h.store(ss[3], ss[0], ss[5], ss[1], ss[4], ss[2]) // ingested shuffled
	h.settle(p)
	if _, err := p.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := eventOrder(journal(t, h.results))
	want := []telemetry.Sequence{max - 2, max - 1, max, 0, 1, 2}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order %v, want %v", got, want)
		}
	}
	c, _ := h.results.Count(context.Background())
	if c.Outcomes["accepted"] != 6 || c.Diagnostics["fusion:sequence_gap"] != 0 {
		t.Fatalf("outcomes %v diagnostics %v", c.Outcomes, c.Diagnostics)
	}
}

func TestTicksAreJournaledAndDrainIdleDevices(t *testing.T) {
	h := newHarness(t)
	p := h.processor()
	h.store(sample("d", 1, 1, 10, 10*m), sample("d", 1, 3, 30, 10*m)) // 2 is lost
	for i := 0; i < 70; i++ {                                         // 70 s of processing time, ticking every second
		h.clock.advance(time.Second)
		step(t, p)
	}
	c, _ := h.results.Count(context.Background())
	if c.TickSteps < 60 {
		t.Fatalf("%d ticks journaled", c.TickSteps)
	}
	if c.Diagnostics["fusion:idle_flush"] == 0 {
		t.Fatalf("silent device was not drained by Tick: %v", c.Diagnostics)
	}
	// The drain is part of the journal: a replay reproduces it.
	h.reopen()
	h.processor()
}

func TestRestartReplaysAndContinuesWithoutLossOrRepeat(t *testing.T) {
	h := newHarness(t)
	p := h.processor()
	var ss []telemetry.Sample
	for i := 1; i <= 40; i++ {
		ss = append(ss, sample("r", 1, telemetry.Sequence(i), i*10, telemetry.Millimetres(i)*m))
	}
	h.store(ss[:20]...)
	h.settle(p)
	h.store(ss[20:30]...)
	step(t, p) // 20..29 are read and held, not released: the process "stops" here

	h.reopen()
	h.clock.advance(time.Second)
	p2 := h.processor() // replays and verifies the journal
	if st := p2.Stats(); st.ReplaySteps == 0 {
		t.Fatal("nothing replayed")
	}
	h.store(ss[30:]...)
	h.settle(p2)

	got := eventOrder(journal(t, h.results))
	if len(got) != 40 {
		t.Fatalf("%d events journaled, want 40", len(got))
	}
	for i, s := range got {
		if s != telemetry.Sequence(i+1) {
			t.Fatalf("journal position %d has seq %d", i, s)
		}
	}
	// A third run verifies the whole journal again.
	h.reopen()
	h.processor()
}

func TestReplayMismatchIsDetectedAndRebuildable(t *testing.T) {
	h := newHarness(t)
	p := h.processor()
	h.store(sample("d", 1, 1, 10, 10*m), sample("d", 1, 2, 20, 10*m))
	h.settle(p)
	// Simulate outputs produced by a different engine version.
	if _, err := writeDB(h).Exec(`UPDATE journal SET output_hash = zeroblob(32) WHERE step = 2`); err != nil {
		t.Fatal(err)
	}
	h.reopen()
	np, _ := New(Config{Now: h.clock.now, Logger: quiet()}, h.events, h.results)
	if _, err := np.Recover(context.Background()); !errors.Is(err, ErrReplayMismatch) {
		t.Fatalf("recover: %v, want ErrReplayMismatch", err)
	}
	rp := h.processor(func(c *Config) { c.Rebuild = true })
	if rp.Stats().Rewritten != 1 {
		t.Fatalf("rewritten %d, want 1", rp.Stats().Rewritten)
	}
	h.reopen()
	h.processor() // consistent again
}

func TestConfigChangeIsDetected(t *testing.T) {
	h := newHarness(t)
	p := h.processor()
	h.store(sample("d", 1, 1, 10, 10*m))
	h.clock.advance(3 * time.Second)
	step(t, p)
	cfg := fusion.DefaultConfig()
	cfg.ReorderWindow = 10 * time.Second
	np, _ := New(Config{Fusion: cfg, Now: h.clock.now, Logger: quiet()}, h.events, h.results)
	if _, err := np.Recover(context.Background()); !errors.Is(err, resultstore.ErrConfigChanged) {
		t.Fatalf("recover with another fusion config: %v", err)
	}
}

func TestSecondProcessorFencesFirst(t *testing.T) {
	h := newHarness(t)
	p1 := h.processor()
	h.processor() // claims ownership
	h.store(sample("d", 1, 1, 10, 10*m))
	h.clock.advance(3 * time.Second)
	if _, err := p1.Step(context.Background()); !errors.Is(err, resultstore.ErrNotOwner) || !errors.Is(err, ErrFatal) {
		t.Fatalf("first processor after takeover: %v", err)
	}
}

func TestWrongEventStoreIsDetected(t *testing.T) {
	h := newHarness(t)
	p := h.processor()
	h.store(sample("d", 1, 1, 10, 10*m))
	h.settle(p)
	other, err := eventstore.Open(context.Background(), filepath.Join(t.TempDir(), "other.db"), eventstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	ev, _ := eventstore.NewEvent(sample("d", 1, 1, 10, 99*m)) // same position, different content
	other.Append(context.Background(), ev)
	np, _ := New(Config{Now: h.clock.now, Logger: quiet()}, other, h.results)
	if _, err := np.Recover(context.Background()); !errors.Is(err, ErrWrongEventStore) {
		t.Fatalf("recover against another event store: %v", err)
	}
}

func TestStepBeforeRecover(t *testing.T) {
	h := newHarness(t)
	p, _ := New(Config{Now: h.clock.now, Logger: quiet()}, h.events, h.results)
	if _, err := p.Step(context.Background()); err == nil {
		t.Fatal("Step before Recover succeeded")
	}
}

// writeDB opens a direct write connection to the results database, for
// tests that tamper with stored data.
func writeDB(h *harness) *sql.DB {
	h.t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(h.dir, "results.db")+"?_pragma=busy_timeout(5000)")
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { db.Close() })
	return db
}
