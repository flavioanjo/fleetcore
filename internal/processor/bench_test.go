package processor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/eventstore"
	"github.com/flavioanjo/fleetcore/internal/simulator"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// storeSimulated stores n simulator samples (10 vehicles, 3 sources) and
// returns how many were stored.
func storeSimulated(b *testing.B, h *harness, n int) int {
	b.Helper()
	cfg := simulator.DefaultConfig()
	cfg.Vehicles, cfg.Samples, cfg.Interval = 10, n/30+1, 10*time.Second
	var evs []eventstore.Event
	if err := simulator.Run(cfg, func(s telemetry.Sample) error {
		if len(evs) < n {
			ev, err := eventstore.NewEvent(s)
			if err != nil {
				return err
			}
			evs = append(evs, ev)
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < len(evs); i += 256 {
		if _, err := h.events.AppendBatch(context.Background(), evs[i:min(i+256, len(evs))]); err != nil {
			b.Fatal(err)
		}
	}
	return len(evs)
}

// BenchmarkProcessorThroughput measures the processing path for stored
// events: read from the event store, chronological ordering, fusion, and the
// durable commit of journal, reports and diagnostics (SQLite WAL,
// synchronous=FULL). Each iteration is one event.
func BenchmarkProcessorThroughput(b *testing.B) {
	h := newHarness(b)
	n := storeSimulated(b, h, b.N)
	p := h.processorB(b)
	b.ReportAllocs()
	b.ResetTimer()
	begin := time.Now()
	for p.Stats().Processed < uint64(n) {
		if _, err := p.Step(context.Background()); err != nil {
			b.Fatal(err)
		}
		h.clock.advance(3 * time.Second)
	}
	b.ReportMetric(float64(n)/time.Since(begin).Seconds(), "events/s")
}

// BenchmarkRecoveryReplay measures restart recovery: replaying and verifying
// a journal of b.N events.
func BenchmarkRecoveryReplay(b *testing.B) {
	h := newHarness(b)
	n := storeSimulated(b, h, b.N)
	p := h.processorB(b)
	for p.Stats().Processed < uint64(n) {
		if _, err := p.Step(context.Background()); err != nil {
			b.Fatal(err)
		}
		h.clock.advance(3 * time.Second)
	}
	b.ReportAllocs()
	b.ResetTimer()
	begin := time.Now()
	p2, err := New(Config{Now: h.clock.now, Logger: quiet()}, h.events, h.results)
	if err != nil {
		b.Fatal(err)
	}
	rs, err := p2.Recover(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	if rs.Events != int64(n) {
		b.Fatal(fmt.Sprintf("replayed %d of %d", rs.Events, n))
	}
	b.ReportMetric(float64(n)/time.Since(begin).Seconds(), "events/s")
}
