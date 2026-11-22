package processor

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/chrono"
	"github.com/flavioanjo/fleetcore/internal/eventstore"
	"github.com/flavioanjo/fleetcore/internal/fusion"
	"github.com/flavioanjo/fleetcore/internal/gateway"
	"github.com/flavioanjo/fleetcore/internal/protocol"
	"github.com/flavioanjo/fleetcore/internal/resultstore"
	"github.com/flavioanjo/fleetcore/internal/sessions"
	"github.com/flavioanjo/fleetcore/internal/simulator"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// runningGateway is one gateway "process": its own event store connection
// pool on the shared database file, its own socket and session registry.
type runningGateway struct {
	g      *gateway.Gateway
	store  *eventstore.Store
	conn   net.Conn
	cancel context.CancelFunc
	done   chan error
}

func startGateway(t *testing.T, dbPath string) *runningGateway {
	t.Helper()
	st, err := eventstore.Open(context.Background(), dbPath, eventstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	g, err := gateway.New(gateway.Config{Addr: "127.0.0.1:0", Logger: quiet(), BatchDelay: 2 * time.Millisecond, ReadBuffer: 4 << 20},
		st, sessions.New(sessions.Config{}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	rg := &runningGateway{g: g, store: st, cancel: cancel, done: make(chan error, 1)}
	go func() { rg.done <- g.Run(ctx) }()
	if rg.conn, err = net.Dial("udp", g.Addr().String()); err != nil {
		t.Fatal(err)
	}
	return rg
}

func (rg *runningGateway) stop(t *testing.T) gateway.Stats {
	t.Helper()
	rg.conn.Close()
	rg.cancel()
	if err := <-rg.done; err != nil {
		t.Fatalf("gateway: %v", err)
	}
	st := rg.g.Stats()
	rg.store.Close()
	return st
}

func settledGateway(s gateway.Stats) uint64 {
	return s.Rejected() + s.QueueFull + s.Inserted + s.Duplicates + s.Conflicts + s.StoreFailed + s.ShutdownDropped
}

func TestEndToEndTwoGatewaysSimulatorUDPSQLite(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	eventsPath, resultsPath := filepath.Join(dir, "events.db"), filepath.Join(dir, "results.db")

	simCfg := simulator.DefaultConfig()
	simCfg.Vehicles, simCfg.Samples, simCfg.Interval = 4, 150, 10*time.Second
	var samples []telemetry.Sample
	if err := simulator.Run(simCfg, func(s telemetry.Sample) error { samples = append(samples, s); return nil }); err != nil {
		t.Fatal(err)
	}

	gws := []*runningGateway{startGateway(t, eventsPath), startGateway(t, eventsPath)}
	sent := 0
	send := func(gw int, p []byte) {
		if _, err := gws[gw].conn.Write(p); err != nil {
			t.Fatal(err)
		}
		sent++
		if sent%64 == 0 {
			time.Sleep(time.Millisecond)
		}
	}
	for i, s := range samples {
		p, err := protocol.EncodeSample(s)
		if err != nil {
			t.Fatal(err)
		}
		send(i%2, p)
		if i%10 == 0 { // retransmission through the other gateway
			send((i+1)%2, p)
		}
	}
	waitSettled := func() {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for settledGateway(gws[0].g.Stats())+settledGateway(gws[1].g.Stats()) < uint64(sent) {
			if time.Now().After(deadline) {
				t.Fatalf("gateways did not settle: %+v / %+v", gws[0].g.Stats(), gws[1].g.Stats())
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	waitSettled()
	// A conflicting payload reusing a stored identity, through the other
	// gateway. It is sent only after the original is committed: across
	// gateways the store's "first committed wins" follows commit order,
	// which need not be send order (two processes compete for SQLite's
	// write lock, which is not fair), so a concurrent race would have no
	// defined winner.
	original := samples[5]
	conflict := original
	conflict.Quality, conflict.Distance = telemetry.QualityUnavailable, telemetry.None[telemetry.Distance]()
	cp, _ := protocol.EncodeSample(conflict)
	send(0, cp)
	waitSettled()
	s0, s1 := gws[0].stop(t), gws[1].stop(t)
	inserted := s0.Inserted + s1.Inserted
	if inserted != uint64(len(samples)) || s0.Conflicts+s1.Conflicts != 1 || s0.QueueFull+s1.QueueFull != 0 {
		t.Fatalf("gateways: inserted %d of %d, conflicts %d; %+v / %+v", inserted, len(samples), s0.Conflicts+s1.Conflicts, s0, s1)
	}
	if s0.Duplicates+s1.Duplicates != uint64(len(samples)+9)/10 {
		t.Fatalf("cross-gateway duplicates %d", s0.Duplicates+s1.Duplicates)
	}

	// Processor "process": its own connections to both databases.
	events, err := eventstore.Open(ctx, eventsPath, eventstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer events.Close()
	// Both payloads are preserved: the original as the event, the
	// conflicting one in the conflict table.
	key := eventstore.Key{DeviceID: original.DeviceID, BootID: original.BootID, EventID: original.EventID}
	if rec, err := events.Get(ctx, key); err != nil || !rec.Sample.Distance.IsPresent() {
		t.Fatalf("stored event for the conflicting identity: %+v %v", rec.Sample, err)
	}
	if cs, err := events.Conflicts(ctx, key); err != nil || len(cs) != 1 {
		t.Fatalf("conflicts %d %v, want 1", len(cs), err)
	}
	results, err := resultstore.Open(ctx, resultsPath, resultstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	p, err := New(Config{Logger: quiet(), Chrono: chrono.Config{Hold: 50 * time.Millisecond}}, events, results)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	for p.Stats().Processed < inserted {
		if _, err := p.Step(ctx); err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if p.Stats().Processed != inserted {
		t.Fatalf("processed %d, stored %d", p.Stats().Processed, inserted)
	}

	// Each stored event is journaled exactly once (UNIQUE(position) also
	// enforces it), and the store's conflict was not fed to fusion.
	c, err := results.Count(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if c.EventSteps != int64(inserted) {
		t.Fatalf("%d event steps, %d stored events", c.EventSteps, inserted)
	}

	// The persisted estimates equal the engine fed directly with the same
	// samples: the pipeline (UDP, two gateways, SQLite, chronological
	// ordering) changes nothing about distance estimation.
	direct, _ := fusion.New(fusion.DefaultConfig())
	want := map[telemetry.DeviceID]telemetry.Millimetres{}
	for _, s := range samples {
		for _, r := range direct.Process(s) {
			if r.Type == fusion.ReportEstimate {
				want[r.Estimate.DeviceID] = r.Estimate.TripMM
			}
		}
	}
	latest, err := results.LatestEstimates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(latest) != simCfg.Vehicles {
		t.Fatalf("%d devices with estimates, want %d", len(latest), simCfg.Vehicles)
	}
	for _, e := range latest {
		if e.TripMM != want[e.DeviceID] {
			t.Errorf("%s: persisted trip %d mm, direct fusion %d mm", e.DeviceID, e.TripMM, want[e.DeviceID])
		}
	}
	if t.Failed() {
		t.Logf("orderer flags %v; outcomes %v; diagnostics %v", p.Stats().Flags, c.Outcomes, c.Diagnostics)
	}

	// Restart: a new processor replays and verifies every journal step.
	results.Close()
	results2, err := resultstore.Open(ctx, resultsPath, resultstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer results2.Close()
	p2, err := New(Config{Logger: quiet()}, events, results2)
	if err != nil {
		t.Fatal(err)
	}
	rs, err := p2.Recover(ctx)
	if err != nil {
		t.Fatalf("deterministic replay after restart failed: %v", err)
	}
	if rs.Events != int64(inserted) {
		t.Fatalf("replayed %d events, want %d", rs.Events, inserted)
	}
}

// TestLiveProcessingWhileGatewaysReceive runs the processor (Run, real
// clock, real ticks) while two gateways receive paced simulator traffic.
// Every stored event must be processed exactly once and the journal must
// replay after the stop. If no event exceeded the hold, the live estimates
// must equal direct fusion; otherwise the late events must be flagged and
// have a final fusion outcome (preserved, not lost).
//
// The test compresses time (10 s of device time per 25 ms), so a
// cross-gateway commit skew that exceeds the hold, as under the race
// detector's slowdown, also exceeds fusion's device-time window. In
// production device time and wall time advance together.
func TestLiveProcessingWhileGatewaysReceive(t *testing.T) {
	if testing.Short() {
		t.Skip("paced live test")
	}
	ctx := context.Background()
	dir := t.TempDir()
	eventsPath, resultsPath := filepath.Join(dir, "events.db"), filepath.Join(dir, "results.db")

	simCfg := simulator.DefaultConfig()
	simCfg.Vehicles, simCfg.Samples, simCfg.Interval = 3, 60, 10*time.Second
	var samples []telemetry.Sample
	if err := simulator.Run(simCfg, func(s telemetry.Sample) error { samples = append(samples, s); return nil }); err != nil {
		t.Fatal(err)
	}

	gws := []*runningGateway{startGateway(t, eventsPath), startGateway(t, eventsPath)}
	events, err := eventstore.Open(ctx, eventsPath, eventstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer events.Close()
	results, err := resultstore.Open(ctx, resultsPath, resultstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer results.Close()
	p, err := New(Config{Logger: quiet(), PollInterval: 10 * time.Millisecond, TickInterval: 100 * time.Millisecond,
		Chrono: chrono.Config{Hold: 2 * time.Second}}, events, results)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- p.Run(runCtx) }()

	// One simulator round (3 vehicles x 3 sources) every 25 ms, alternating
	// gateways; each round is 10 s of device time, so the 30 s fusion window
	// spans 75 ms of wall time, well above the expected cross-gateway skew.
	perRound := simCfg.Vehicles * 3
	for i, s := range samples {
		pkt, _ := protocol.EncodeSample(s)
		if _, err := gws[i%2].conn.Write(pkt); err != nil {
			t.Fatal(err)
		}
		if i%perRound == perRound-1 {
			time.Sleep(25 * time.Millisecond)
		}
	}
	deadline := time.Now().Add(20 * time.Second)
	for settledGateway(gws[0].g.Stats())+settledGateway(gws[1].g.Stats()) < uint64(len(samples)) {
		if time.Now().After(deadline) {
			t.Fatal("gateways did not settle")
		}
		time.Sleep(5 * time.Millisecond)
	}
	s0, s1 := gws[0].stop(t), gws[1].stop(t)
	inserted := s0.Inserted + s1.Inserted
	for p.Stats().Processed < inserted {
		if time.Now().After(deadline) {
			t.Fatalf("processed %d of %d", p.Stats().Processed, inserted)
		}
		time.Sleep(10 * time.Millisecond)
	}
	stop()
	if err := <-runDone; err != nil {
		t.Fatalf("Run: %v", err)
	}
	st := p.Stats()
	if st.Processed != inserted || inserted != uint64(len(samples)) || st.Ticks == 0 {
		t.Fatalf("processed %d, inserted %d of %d, ticks %d", st.Processed, inserted, len(samples), st.Ticks)
	}
	c, err := results.Count(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if c.EventSteps != int64(inserted) {
		t.Fatalf("%d event steps for %d stored events", c.EventSteps, inserted)
	}
	var final int64
	for _, n := range c.Outcomes {
		final += n
	}
	if final != int64(inserted) {
		t.Fatalf("%d events with a fusion outcome, %d stored (%v)", final, inserted, c.Outcomes)
	}
	if late := st.Flags["late"]; late != 0 {
		if c.Diagnostics["chronology:late"] != int64(late) {
			t.Fatalf("%d late events, %d persisted late diagnostics", late, c.Diagnostics["chronology:late"])
		}
		t.Logf("%d events exceeded the hold (machine slowdown); all were journaled, flagged and given an outcome: %v", late, c.Outcomes)
		p2, _ := New(Config{Logger: quiet()}, events, results)
		if _, err := p2.Recover(ctx); err != nil {
			t.Fatalf("replay after live run: %v", err)
		}
		return
	}

	direct, _ := fusion.New(fusion.DefaultConfig())
	want := map[telemetry.DeviceID]telemetry.Millimetres{}
	for _, s := range samples {
		for _, r := range direct.Process(s) {
			if r.Type == fusion.ReportEstimate {
				want[r.Estimate.DeviceID] = r.Estimate.TripMM
			}
		}
	}
	latest, err := results.LatestEstimates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range latest {
		if e.TripMM != want[e.DeviceID] {
			t.Errorf("%s: live trip %d mm, direct fusion %d mm (flags %v)", e.DeviceID, e.TripMM, want[e.DeviceID], st.Flags)
		}
	}

	p2, _ := New(Config{Logger: quiet()}, events, results)
	if _, err := p2.Recover(ctx); err != nil {
		t.Fatalf("replay after live run: %v", err)
	}
}
