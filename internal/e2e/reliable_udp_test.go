package e2e

import (
	"context"
	"crypto/ed25519"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/alerting"
	"github.com/flavioanjo/fleetcore/internal/commandstore"
	"github.com/flavioanjo/fleetcore/internal/device"
	"github.com/flavioanjo/fleetcore/internal/dispatcher"
	"github.com/flavioanjo/fleetcore/internal/eventstore"
	"github.com/flavioanjo/fleetcore/internal/gateway"
	"github.com/flavioanjo/fleetcore/internal/processor"
	"github.com/flavioanjo/fleetcore/internal/reliable"
	"github.com/flavioanjo/fleetcore/internal/resultstore"
	"github.com/flavioanjo/fleetcore/internal/security"
	"github.com/flavioanjo/fleetcore/internal/sessions"
	"github.com/flavioanjo/fleetcore/internal/simhw"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type gw struct {
	g      *gateway.Gateway
	store  *eventstore.Store
	cancel context.CancelFunc
	done   chan error
}

func startGateway(t *testing.T, dbPath, id string, keys security.KeyStore, hooks *gateway.ReliableHooks) *gw {
	t.Helper()
	st, err := eventstore.Open(context.Background(), dbPath, eventstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	g, err := gateway.New(gateway.Config{Addr: "127.0.0.1:0", Logger: quiet(), BatchDelay: 5 * time.Millisecond,
		Reliable: &gateway.ReliableConfig{Keys: keys, Store: st, GatewayID: id, Hooks: hooks}}, st, sessions.New(sessions.Config{}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &gw{g: g, store: st, cancel: cancel, done: make(chan error, 1)}
	go func() { r.done <- g.Run(ctx) }()
	return r
}

func (g *gw) stop(t *testing.T) {
	t.Helper()
	g.cancel()
	if err := <-g.done; err != nil {
		t.Fatalf("gateway: %v", err)
	}
	g.store.Close()
}

func (g *gw) addr() netip.AddrPort { return g.g.Addr().(*net.UDPAddr).AddrPort() }

// swappable lets the dispatcher keep a stable transport while a gateway
// process is replaced.
type swappable struct {
	mu sync.Mutex
	g  *gateway.Gateway
}

func (s *swappable) set(g *gateway.Gateway) { s.mu.Lock(); s.g = g; s.mu.Unlock() }
func (s *swappable) SendCommand(d reliable.CommandDelivery) error {
	s.mu.Lock()
	g := s.g
	s.mu.Unlock()
	if g == nil {
		return reliable.ErrDeviceUnreachable
	}
	return g.SendCommand(d)
}

type rdev struct {
	id  string
	dev *device.Device
	veh *simhw.Vehicle
	tr  *device.UDPTransport
}

func startDevice(t *testing.T, ctx context.Context, dir, id string, keys *security.MemoryKeyStore, auth *security.Authority, to netip.AddrPort, wg *sync.WaitGroup) *rdev {
	t.Helper()
	key, _ := security.GenerateSecret()
	keys.Put(security.DeviceKey{DeviceID: id, KeyID: 1, Secret: key})
	veh := simhw.New(time.Now)
	veh.SetFix(reliable.PositionFix{Fix: reliable.Fix3D, LatE7: -235505200, LonE7: -466333100, HDOPx10: 8, Satellites: 10}, true)
	tr, err := device.NewUDPTransport("127.0.0.1:0", to)
	if err != nil {
		t.Fatal(err)
	}
	d, err := device.Open(ctx, device.Config{DeviceID: id, KeyID: 1, Key: key,
		Authority: security.NewVerifier(map[uint32]ed25519.PublicKey{1: auth.Public()}),
		StorePath: filepath.Join(dir, "device-"+id+".db"), Transport: tr, Hardware: veh, Logger: quiet(),
		InitialBackoff: 200 * time.Millisecond, MaxBackoff: time.Second, CriticalMaxBackoff: 500 * time.Millisecond,
		HeartbeatInterval: 500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Boot(ctx); err != nil {
		t.Fatal(err)
	}
	wg.Add(2)
	go func() { defer wg.Done(); tr.Receive(ctx, d) }() //nolint:errcheck
	go func() {
		defer wg.Done()
		tk := time.NewTicker(20 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tk.C:
				d.Tick(ctx) //nolint:errcheck
			}
		}
	}()
	return &rdev{id: id, dev: d, veh: veh, tr: tr}
}

func eventually(t *testing.T, limit time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestReliableEndToEndOverUDP(t *testing.T) {
	if testing.Short() {
		t.Skip("real-time UDP end-to-end test")
	}
	dir := t.TempDir()
	eventsPath := filepath.Join(dir, "events.db")
	keys := security.NewMemoryKeyStore()
	auth, _ := security.NewAuthority(1)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var devs []*rdev
	defer func() {
		cancel()
		wg.Wait() // device loops, receivers, dispatcher and alert loop have stopped
		for _, d := range devs {
			d.tr.Close()
			if err := d.dev.Close(); err != nil {
				t.Errorf("close device %s: %v", d.id, err)
			}
		}
	}()

	// Gateway A loses the ACKs of its first 5 committed batches (crash
	// between commit and ACK); gateway B is healthy.
	var crashes atomic.Int32
	crashes.Store(5)
	gwA := startGateway(t, eventsPath, "gw-a", keys, &gateway.ReliableHooks{AfterCommit: func() bool { return crashes.Add(-1) < 0 }})
	gwB := startGateway(t, eventsPath, "gw-b", keys, nil)
	defer gwB.stop(t)
	ta, tb := &swappable{}, &swappable{}
	ta.set(gwA.g)
	tb.set(gwB.g)

	events, err := eventstore.Open(ctx, eventsPath, eventstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer events.Close()
	cmds, err := commandstore.Open(ctx, filepath.Join(dir, "commands.db"), auth, commandstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer cmds.Close()
	disp, err := dispatcher.New(dispatcher.Config{Commands: cmds, Events: events, Logger: quiet(), Interval: 20 * time.Millisecond,
		InitialBackoff: 300 * time.Millisecond, MaxBackoff: time.Second, PollInterval: time.Second,
		Transports: []dispatcher.Named{{Name: "gw-a", Transport: ta}, {Name: "gw-b", Transport: tb}}})
	if err != nil {
		t.Fatal(err)
	}
	wg.Add(1)
	go func() { defer wg.Done(); disp.Run(ctx) }() //nolint:errcheck
	sink := &countingSink{seen: map[string]int{}}
	alerts, err := alerting.Open(ctx, alerting.Config{Path: filepath.Join(dir, "alerts.db"), Events: events, Sink: sink, Logger: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	defer alerts.Close()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			alerts.Step(ctx) //nolint:errcheck
			time.Sleep(20 * time.Millisecond)
		}
	}()

	d1 := startDevice(t, ctx, dir, "TRK-1", keys, auth, gwA.addr(), &wg)
	d2 := startDevice(t, ctx, dir, "TRK-2", keys, auth, gwB.addr(), &wg)
	devs = append(devs, d1, d2)

	// Telemetry and critical events from both devices.
	const samples = 30
	var tel []uint64
	for i := 0; i < samples; i++ {
		for _, d := range []*rdev{d1, d2} {
			u, err := d.dev.EnqueueTelemetry(ctx, telemetry.Sample{Source: telemetry.SourceGPS, Quality: telemetry.QualityGood,
				Distance: telemetry.Some(telemetry.Distance{Kind: telemetry.DistanceDelta, Value: telemetry.Millimetres(1000 * (i + 1))})})
			if err != nil {
				t.Fatal(err)
			}
			tel = append(tel, u)
		}
		if i == samples/2 {
			d2.tr.SetGateway(gwA.addr()) // TRK-2 fails over to A mid-stream
		}
	}
	p1, _ := d1.dev.Panic(ctx)
	p2, _ := d2.dev.Panic(ctx)

	drained := func(d *rdev) func() bool {
		return func() bool { q, _ := d.dev.Queue(ctx); return q.PendingTotal() == 0 }
	}
	eventually(t, 20*time.Second, "TRK-1 queue drained", drained(d1))
	eventually(t, 20*time.Second, "TRK-2 queue drained", drained(d2))
	if crashes.Load() >= 0 {
		t.Fatal("crash-after-commit fault was not exercised")
	}

	// Commands through the dispatcher and the gateways.
	sec := commandstore.Principal{ID: "ops:alice", Roles: []string{"security_operator", "cargo_operator", "fleet_viewer"}}
	create := func(id, dev string, typ reliable.CommandType, args []byte) commandstore.Command {
		c, _, err := cmds.Create(ctx, commandstore.Request{RequestID: id, Principal: sec, DeviceID: dev, Type: typ, Args: args,
			TTL: time.Minute, Justification: "e2e"})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	final := func(c commandstore.Command, want commandstore.State) {
		t.Helper()
		eventually(t, 20*time.Second, c.Type.String()+" "+string(want), func() bool {
			got, _ := cmds.Get(ctx, c.ID)
			return got.State == want
		})
	}
	blk := create("blk", "TRK-1", reliable.CommandBlock, nil)
	pos := create("pos", "TRK-2", reliable.CommandRequestPosition, reliable.PositionArgs(time.Minute))
	oc := create("oc", "TRK-2", reliable.CommandOpenCargo, reliable.CargoArgs(1))
	final(blk, commandstore.Succeeded)
	final(pos, commandstore.Succeeded)
	final(oc, commandstore.Succeeded)

	// Gateway A reboots (new process, new port); TRK-1 fails over to B and
	// UNBLOCK is delivered through B.
	ta.set(nil)
	gwA.stop(t)
	d1.tr.SetGateway(gwB.addr())
	ub := create("ub", "TRK-1", reliable.CommandUnblock, nil)
	final(ub, commandstore.Succeeded)

	if tr := d1.veh.Truth(); tr.Blocked || tr.ImmobilizerDrives != 2 || tr.ImmobilizerMoves != 2 {
		t.Fatalf("TRK-1 truth %+v: BLOCK and UNBLOCK must each drive the actuator once", tr)
	}
	if tr := d2.veh.Truth(); !tr.CargoOpen || tr.LatchReleases != 1 {
		t.Fatalf("TRK-2 truth %+v", tr)
	}
	eventually(t, 10*time.Second, "queues drained after commands", func() bool { return drained(d1)() && drained(d2)() })

	// Database contents: every event stored once.
	n, _ := events.CountReliable(ctx)
	if n[reliable.EventTelemetry] != 2*samples || n[reliable.EventPanic] != 2 || n[reliable.EventPosition] != 1 {
		t.Fatalf("stored %v", n)
	}
	v1, _ := events.ReadAfter(ctx, 0, 10000)
	if len(v1) != 2*samples {
		t.Fatalf("%d v1 telemetry rows for %d samples", len(v1), 2*samples)
	}
	for _, d := range []struct {
		id  string
		uid uint64
	}{{"TRK-1", p1}, {"TRK-2", p2}} {
		eventually(t, 5*time.Second, "alert notified", func() bool {
			st, err := alerts.Get(ctx, d.id, d.uid)
			return err == nil && st.State == alerting.StateNotified
		})
	}
	if sink.count() != 2 {
		t.Fatalf("sink notified %d times for 2 panics", sink.count())
	}
	// Downstream: the chronological processor and fusion apply each sample once.
	results, err := resultstore.Open(ctx, filepath.Join(dir, "results.db"), resultstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer results.Close()
	p, err := processor.New(processor.Config{Logger: quiet()}, events, results)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := p.Step(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := p.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	c, _ := results.Count(ctx)
	if c.EventSteps != int64(2*samples) {
		t.Fatalf("fusion applied %d events for %d stored samples", c.EventSteps, 2*samples)
	}
	t.Logf("gateway A: %+v", statsLine(gwA.g.ReliableStats()))
	t.Logf("gateway B: %+v", statsLine(gwB.g.ReliableStats()))
}

type countingSink struct {
	mu   sync.Mutex
	seen map[string]int
}

func (s *countingSink) Notify(_ context.Context, a alerting.Alert) error {
	s.mu.Lock()
	s.seen[a.ID]++
	s.mu.Unlock()
	return nil
}

func (s *countingSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, v := range s.seen {
		n += v
	}
	return n
}

func statsLine(s *gateway.ReliableStats) map[string]uint64 {
	return map[string]uint64{"received": s.Received.Load(), "stored": s.Stored.Load(), "duplicates": s.Duplicates.Load(),
		"acks_discarded": s.AcksDiscarded.Load(), "commands_sent": s.CommandsSent.Load(), "critical": s.Critical.Load()}
}
