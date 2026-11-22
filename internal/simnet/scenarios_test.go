package simnet

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/alerting"
	"github.com/flavioanjo/fleetcore/internal/commandstore"
	"github.com/flavioanjo/fleetcore/internal/device"
	"github.com/flavioanjo/fleetcore/internal/reliable"
	"github.com/flavioanjo/fleetcore/internal/security"
)

var (
	secOp   = commandstore.Principal{ID: "ops:alice", Roles: []string{"security_operator", "fleet_viewer"}}
	cargoOp = commandstore.Principal{ID: "ops:bob", Roles: []string{"cargo_operator"}}
	viewer  = commandstore.Principal{ID: "ops:carol", Roles: []string{"fleet_viewer"}}
)

const step = 500 * time.Millisecond

func newWorld(t *testing.T, seed int64, devCfg func(*device.Config)) *World {
	t.Helper()
	w, err := New(context.Background(), Config{Dir: t.TempDir(), Seed: seed, Device: devCfg})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Close)
	return w
}

func addDevice(t *testing.T, w *World, id string) *DeviceNode {
	t.Helper()
	n, err := w.AddDevice(id)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func mustRun(t *testing.T, w *World, d time.Duration) {
	t.Helper()
	if err := w.Run(d, step); err != nil {
		t.Fatal(err)
	}
}

func waitState(t *testing.T, w *World, id reliable.CommandID, limit time.Duration, states ...commandstore.State) commandstore.Command {
	t.Helper()
	ok, err := w.RunUntil(limit, step, func() bool {
		s := w.Command(id).State
		for _, want := range states {
			if s == want {
				return true
			}
		}
		return false
	})
	if err != nil {
		t.Fatal(err)
	}
	c := w.Command(id)
	if !ok {
		t.Fatalf("command %s %s stuck in %s (want %v)", c.Type, c.ID, c.State, states)
	}
	return c
}

func request(t *testing.T, w *World, id string, p commandstore.Principal, dev string, typ reliable.CommandType, args []byte) commandstore.Command {
	t.Helper()
	c, err := w.Request(id, p, dev, typ, args, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func historyStates(t *testing.T, w *World, id reliable.CommandID) []commandstore.State {
	h, err := w.Commands.History(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	var out []commandstore.State
	for _, e := range h {
		out = append(out, e.To)
	}
	return out
}

// BLOCK: requested → eligible → sent → received → accepted → succeeded;
// BLOCK_STATE and COMMAND_RESULT persisted; the vehicle is blocked; one
// physical operation. UNBLOCK then reverses it.
func TestScenarioBlockAndUnblock(t *testing.T) {
	w := newWorld(t, 1, nil)
	d := addDevice(t, w, "TRK-1")
	mustRun(t, w, 2*time.Second) // device announces its session
	blk := request(t, w, "blk-1", secOp, "TRK-1", reliable.CommandBlock, nil)
	c := waitState(t, w, blk.ID, time.Minute, commandstore.Succeeded)
	if tr := d.Vehicle.Truth(); !tr.Blocked || tr.ImmobilizerDrives != 1 {
		t.Fatalf("truth %+v", tr)
	}
	want := []commandstore.State{commandstore.Requested, commandstore.Eligible, commandstore.Sent, commandstore.Received,
		commandstore.Accepted, commandstore.Succeeded}
	if got := historyStates(t, w, blk.ID); len(got) != len(want) {
		t.Fatalf("history %v, want %v", got, want)
	} else {
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("history %v, want %v", got, want)
			}
		}
	}
	if c.Observed != uint8(reliable.Blocked) || c.NeedsAttention {
		t.Fatalf("command %+v", c)
	}
	mustRun(t, w, 5*time.Second)
	rs, _ := w.Commands.DeviceReportedState(context.Background(), "TRK-1")
	if rs.Block != reliable.Blocked || rs.BlockCause != reliable.CauseCommand {
		t.Fatalf("reported state %+v", rs)
	}
	reps, _ := w.Commands.Reports(context.Background(), blk.ID)
	if len(reps) != 3 {
		t.Fatalf("%d COMMAND_RESULT reports applied, want 3", len(reps))
	}

	ub := request(t, w, "ub-1", secOp, "TRK-1", reliable.CommandUnblock, nil)
	waitState(t, w, ub.ID, time.Minute, commandstore.Succeeded)
	mustRun(t, w, 5*time.Second)
	if tr := d.Vehicle.Truth(); tr.Blocked || tr.ImmobilizerMoves != 2 {
		t.Fatalf("after unblock %+v", tr)
	}
	if rs, _ := w.Commands.DeviceReportedState(context.Background(), "TRK-1"); rs.Block != reliable.Unblocked {
		t.Fatalf("reported state after unblock %v", rs.Block)
	}
}

func TestScenarioOpenCargoAndRequestPosition(t *testing.T) {
	w := newWorld(t, 2, nil)
	d := addDevice(t, w, "TRK-1")
	mustRun(t, w, 2*time.Second)
	oc := request(t, w, "oc", cargoOp, "TRK-1", reliable.CommandOpenCargo, reliable.CargoArgs(1))
	waitState(t, w, oc.ID, time.Minute, commandstore.Succeeded)
	if tr := d.Vehicle.Truth(); !tr.CargoOpen || tr.LatchReleases != 1 {
		t.Fatalf("cargo truth %+v", tr)
	}
	mustRun(t, w, 5*time.Second)
	if rs, _ := w.Commands.DeviceReportedState(context.Background(), "TRK-1"); rs.Door != reliable.DoorOpen || rs.Latch != reliable.LatchReleased {
		t.Fatalf("cargo state %+v", rs)
	}

	// No GNSS fix: explicit failure, no position fabricated.
	p1 := request(t, w, "pos-1", viewer, "TRK-1", reliable.CommandRequestPosition, reliable.PositionArgs(30*time.Second))
	c := waitState(t, w, p1.ID, time.Minute, commandstore.Failed)
	if c.DeviceReason != reliable.ReasonPositionUnavailable {
		t.Fatalf("reason %s", c.DeviceReason)
	}
	if recs, _ := w.Events.ReadReliableAfter(context.Background(), 0, 100, reliable.EventPosition); len(recs) != 0 {
		t.Fatal("position reported without a fix")
	}
	d.Vehicle.SetFix(reliable.PositionFix{Fix: reliable.Fix3D, LatE7: -229068000, LonE7: -431729000, HDOPx10: 7, Satellites: 12}, true)
	p2 := request(t, w, "pos-2", viewer, "TRK-1", reliable.CommandRequestPosition, reliable.PositionArgs(30*time.Second))
	waitState(t, w, p2.ID, time.Minute, commandstore.Succeeded)
	mustRun(t, w, 5*time.Second)
	recs, _ := w.Events.ReadReliableAfter(context.Background(), 0, 100, reliable.EventPosition)
	if len(recs) != 1 {
		t.Fatalf("%d positions", len(recs))
	}
	b, _ := recs[0].Body()
	if pos := b.(reliable.Position); pos.RequestID != p2.ID || pos.LatE7 != -229068000 {
		t.Fatalf("position %+v", pos)
	}
}

// PANIC while offline; device reboots; connectivity returns; the panic is
// stored exactly once, the alert is notified once and stays unhandled until
// an operator acknowledges it.
func TestScenarioPanicOfflineRebootReconnect(t *testing.T) {
	w := newWorld(t, 3, nil)
	d := addDevice(t, w, "TRK-1")
	d.Vehicle.SetFix(reliable.PositionFix{Fix: reliable.Fix3D, LatE7: 1, LonE7: 2, HDOPx10: 9, Satellites: 8}, true)
	mustRun(t, w, 2*time.Second)
	d.Offline = true
	uid, err := d.Dev.Panic(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	mustRun(t, w, 30*time.Second)
	d.Crash()
	mustRun(t, w, 10*time.Second)
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	mustRun(t, w, 10*time.Second)
	if st, _, _ := d.Dev.EventState(context.Background(), uid); st != "pending" {
		t.Fatalf("panic %s while offline", st)
	}
	d.Offline = false
	ok, _ := w.RunUntil(time.Minute, step, func() bool { st, _, _ := d.Dev.EventState(context.Background(), uid); return st == "acked" })
	if !ok {
		t.Fatal("panic not acknowledged after reconnection")
	}
	mustRun(t, w, 5*time.Second)
	n, _ := w.Events.CountReliable(context.Background())
	if n[reliable.EventPanic] != 1 {
		t.Fatalf("%d panic rows", n[reliable.EventPanic])
	}
	rec, _ := w.Events.GetReliable(context.Background(), "TRK-1", uid)
	b, _ := rec.Body()
	if p := b.(reliable.Panic); !p.HasPosition {
		t.Fatal("panic lost its position")
	}
	// Stored and notified, but not handled.
	st, err := w.Alerts.Get(context.Background(), "TRK-1", uid)
	if err != nil || st.State != alerting.StateNotified || w.Sink.Seen["TRK-1/"+itoa(uid)] != 1 {
		t.Fatalf("alert %+v %v (sink %v)", st, err, w.Sink.Seen)
	}
	if err := w.Alerts.Acknowledge(context.Background(), "TRK-1", uid, "ops:dave", "called driver"); err != nil {
		t.Fatal(err)
	}
	if st, _ := w.Alerts.Get(context.Background(), "TRK-1", uid); st.State != alerting.StateAcknowledged || st.AckedBy != "ops:dave" {
		t.Fatalf("after acknowledgement %+v", st)
	}
	if m := d.Dev.Metrics(); m.CriticalAckLatency.Count != 1 {
		t.Fatalf("critical ack latency not measured: %+v", m.CriticalAckLatency)
	}
}

func itoa(u uint64) string {
	if u == 0 {
		return "0"
	}
	var b []byte
	for u > 0 {
		b = append([]byte{byte('0' + u%10)}, b...)
		u /= 10
	}
	return string(b)
}

// Command deliveries and result ACKs are lost: retransmissions on both
// sides never repeat the physical operation.
func TestScenarioLostCommandAndResultAcks(t *testing.T) {
	w := newWorld(t, 4, nil)
	d := addDevice(t, w, "TRK-1")
	mustRun(t, w, 2*time.Second)
	lostDeliveries, lostAcks := 0, 0
	w.Net.AddFilter(func(dg Datagram) bool {
		if dg.Uplink {
			return true
		}
		h, err := reliable.ParseHeader(dg.Data)
		if err != nil {
			return true
		}
		switch {
		case h.Type == reliable.TypeCommandDelivery && lostDeliveries < 2:
			lostDeliveries++
			return false
		case h.Type == reliable.TypeEventAck && lostAcks < 12:
			lostAcks++ // lose the ACKs of the COMMAND_RESULT reports
			return false
		}
		return true
	})
	blk := request(t, w, "blk", secOp, "TRK-1", reliable.CommandBlock, nil)
	waitState(t, w, blk.ID, 3*time.Minute, commandstore.Succeeded)
	mustRun(t, w, 30*time.Second)
	if tr := d.Vehicle.Truth(); tr.ImmobilizerDrives != 1 || !tr.Blocked {
		t.Fatalf("truth %+v (lost %d deliveries, %d acks)", tr, lostDeliveries, lostAcks)
	}
	at, _ := w.Commands.Attempts(context.Background(), blk.ID)
	if len(at) < 3 {
		t.Fatalf("%d attempts; the lost deliveries were not retried", len(at))
	}
	if m := d.Dev.Metrics(); m.Retransmissions == 0 {
		t.Fatal("no event retransmissions despite lost ACKs")
	}
	// Each COMMAND_RESULT event is stored once although transmitted several times.
	recs, _ := w.Events.ReadReliableAfter(context.Background(), 0, 1000, reliable.EventCommandResult)
	stages := map[reliable.Stage]int{}
	for _, r := range recs {
		b, _ := r.Body()
		if cr := b.(reliable.CommandResult); cr.CommandID == blk.ID {
			stages[cr.Stage]++
		}
	}
	if stages[reliable.StageSucceeded] < 1 || stages[reliable.StageReceived] < 1 {
		t.Fatalf("stages %v", stages)
	}
	if c := w.Command(blk.ID); c.State != commandstore.Succeeded || c.NeedsAttention {
		t.Fatalf("final %s attention=%v", c.State, c.NeedsAttention)
	}
}

func TestScenarioExpiredForgedUnauthorizedDuplicate(t *testing.T) {
	w := newWorld(t, 5, nil)
	d := addDevice(t, w, "TRK-1")
	ctx := context.Background()
	mustRun(t, w, 2*time.Second)

	// Unauthorized: never stored, never sent.
	if _, err := w.Request("u", viewer, "TRK-1", reliable.CommandBlock, nil, time.Minute); !errors.Is(err, commandstore.ErrUnauthorized) {
		t.Fatalf("unauthorized: %v", err)
	}
	// Expired while the device is unreachable and never transmitted: expired, not executed.
	d.Offline = true
	for _, g := range w.Gateways {
		g.Reboot() // forget the device's address: deliveries become unreachable
	}
	exp, _ := w.Request("e", secOp, "TRK-1", reliable.CommandBlock, nil, 10*time.Second)
	mustRun(t, w, 30*time.Second)
	if c := w.Command(exp.ID); c.State != commandstore.Expired || !c.NeedsAttention {
		t.Fatalf("expired command %s", c.State)
	}
	d.Offline = false
	mustRun(t, w, 5*time.Second)

	// Forged: a command delivered with a valid transport tag but signed by
	// another authority (e.g. a compromised gateway) is not executed.
	other, _ := security.NewAuthority(1)
	cmd := reliable.Command{ID: reliable.CommandID{0xbe, 0xef}, DeviceID: "TRK-1", Type: reliable.CommandBlock,
		IssuedAt: time.UnixMilli(w.Now().UnixMilli()).UTC(), ExpiresAt: w.Now().Add(time.Minute).Truncate(time.Millisecond), Principal: "attacker"}
	del := reliable.CommandDelivery{Command: cmd, Attempt: 1, AttemptSentAt: cmd.IssuedAt, AttemptNotAfter: cmd.IssuedAt.Add(30 * time.Second),
		AuthorityKeyID: 1, Signature: other.Sign(cmd.Statement())}
	if err := w.Gateways[d.Gateway].SendCommand(del); err != nil {
		t.Fatal(err)
	}
	mustRun(t, w, 10*time.Second)
	if d.Vehicle.Truth().ImmobilizerDrives != 0 {
		t.Fatal("forged command executed")
	}
	if recs, _ := w.Events.ReadReliableAfter(ctx, 0, 100, reliable.EventSecurityViolation); len(recs) != 1 {
		t.Fatalf("%d security violations", len(recs))
	}

	// Duplicate request id: one command; duplicate deliveries: one execution.
	c1, created1, _ := w.Commands.Create(ctx, commandstore.Request{RequestID: "dup", Principal: secOp, DeviceID: "TRK-1",
		Type: reliable.CommandBlock, TTL: time.Minute, Justification: "x"})
	c2, created2, _ := w.Commands.Create(ctx, commandstore.Request{RequestID: "dup", Principal: secOp, DeviceID: "TRK-1",
		Type: reliable.CommandBlock, TTL: time.Minute, Justification: "x"})
	if !created1 || created2 || c1.ID != c2.ID {
		t.Fatal("duplicate request created two commands")
	}
	w.Net.Down.Duplicate = 1 // every downlink datagram arrives twice
	waitState(t, w, c1.ID, time.Minute, commandstore.Succeeded)
	mustRun(t, w, 20*time.Second)
	if tr := d.Vehicle.Truth(); tr.ImmobilizerDrives != 1 {
		t.Fatalf("drives %d", tr.ImmobilizerDrives)
	}
	if d.Dev.Metrics().DuplicateDeliveries == 0 {
		t.Fatal("duplicates were not exercised")
	}
}

// A device switches gateway while retransmitting: both gateways see the
// same events; each is stored once.
func TestScenarioCrossGatewayDeduplication(t *testing.T) {
	w := newWorld(t, 6, nil)
	d := addDevice(t, w, "TRK-1")
	ctx := context.Background()
	var uids []uint64
	for i := 0; i < 20; i++ {
		u, _ := d.Dev.Panic(ctx)
		uids = append(uids, u)
	}
	// ACKs from gateway A are lost; the device fails over to B.
	w.Net.AddFilter(func(dg Datagram) bool { return dg.Uplink || dg.From != w.Gateways[0].Addr })
	mustRun(t, w, 10*time.Second)
	d.Gateway = 1
	ok, _ := w.RunUntil(time.Minute, step, func() bool {
		q, _ := d.Dev.Queue(ctx)
		return q.PendingTotal() == 0
	})
	if !ok {
		t.Fatal("events not delivered after failover")
	}
	n, _ := w.Events.CountReliable(ctx)
	if n[reliable.EventPanic] != 20 {
		t.Fatalf("%d panic rows for 20 panics", n[reliable.EventPanic])
	}
	sa, sb := w.Gateways[0].Core.Stats(), w.Gateways[1].Core.Stats()
	if int64(sa.Stored.Load()+sb.Stored.Load()) != n[reliable.EventPanic]+n[reliable.EventBlockState]+n[reliable.EventCargoState] || sb.Duplicates.Load() < 20 {
		t.Fatalf("A stored %d, B stored %d dup %d", sa.Stored.Load(), sb.Stored.Load(), sb.Duplicates.Load())
	}
	for _, u := range uids {
		rec, err := w.Events.GetReliable(ctx, "TRK-1", u)
		if err != nil || rec.Gateway != "gw-a" {
			t.Fatalf("uid %d: %v (stored by %s)", u, err, rec.Gateway)
		}
	}
}

// Crash between commit and ACK, storage failures, gateway reboot and
// dispatcher restart during a command: no event lost or duplicated, no
// repeated execution.
func TestScenarioServerFaults(t *testing.T) {
	w := newWorld(t, 7, nil)
	d := addDevice(t, w, "TRK-1")
	ctx := context.Background()
	mustRun(t, w, 2*time.Second)
	g := w.Gateways[0]
	crashes, busy := 3, 4
	g.Hooks.AfterCommit = func() bool {
		if crashes > 0 {
			crashes--
			return false
		}
		return true
	}
	g.Hooks.BeforeCommit = func() error {
		if busy > 0 {
			busy--
			return errors.New("database is locked (simulated)")
		}
		return nil
	}
	var uids []uint64
	for i := 0; i < 5; i++ {
		u, _ := d.Dev.Panic(ctx)
		uids = append(uids, u)
	}
	blk := request(t, w, "blk", secOp, "TRK-1", reliable.CommandBlock, nil)
	mustRun(t, w, 3*time.Second)
	g.Reboot()
	if err := w.RestartDispatcher(); err != nil {
		t.Fatal(err)
	}
	waitState(t, w, blk.ID, 2*time.Minute, commandstore.Succeeded)
	ok, _ := w.RunUntil(2*time.Minute, step, func() bool {
		q, _ := d.Dev.Queue(ctx)
		return q.PendingTotal() == 0
	})
	if !ok {
		t.Fatal("queue not drained")
	}
	n, _ := w.Events.CountReliable(ctx)
	if n[reliable.EventPanic] != 5 {
		t.Fatalf("%d panic rows", n[reliable.EventPanic])
	}
	if tr := d.Vehicle.Truth(); tr.ImmobilizerDrives != 1 {
		t.Fatalf("drives %d", tr.ImmobilizerDrives)
	}
	if crashes != 0 || busy != 0 {
		t.Fatalf("faults not exercised: %d crashes and %d storage failures left", crashes, busy)
	}
}

// Seeded random impairments on both directions, several devices, many
// events: every queued event ends acknowledged and stored exactly once.
func TestScenarioLossyNetworkInvariants(t *testing.T) {
	for _, seed := range []int64{11, 12, 13} {
		w := newWorld(t, seed, func(c *device.Config) { c.InitialBackoff, c.MaxBackoff = time.Second, 8*time.Second })
		w.Net.Up = Faults{Loss: 0.3, Duplicate: 0.2, Delay: 100 * time.Millisecond, Jitter: 2 * time.Second}
		w.Net.Down = Faults{Loss: 0.3, Duplicate: 0.2, Delay: 100 * time.Millisecond, Jitter: 2 * time.Second}
		ctx := context.Background()
		queued := map[string][]uint64{}
		var devs []*DeviceNode
		for _, id := range []string{"TRK-1", "TRK-2", "TRK-3"} {
			devs = append(devs, addDevice(t, w, id))
		}
		for round := 0; round < 10; round++ {
			for _, d := range devs {
				u, err := d.Dev.Enqueue(ctx, func(uint64, time.Time) (reliable.Body, error) {
					return reliable.Diagnostic{Code: 7, Value: int64(round)}, nil
				})
				if err != nil {
					t.Fatal(err)
				}
				queued[d.ID] = append(queued[d.ID], u)
				if round%3 == 0 {
					p, _ := d.Dev.Panic(ctx)
					queued[d.ID] = append(queued[d.ID], p)
				}
			}
			mustRun(t, w, 2*time.Second)
		}
		ok, err := w.RunUntil(10*time.Minute, step, func() bool {
			for _, d := range devs {
				if q, _ := d.Dev.Queue(ctx); q.PendingTotal() > 0 {
					return false
				}
			}
			return true
		})
		if err != nil || !ok {
			t.Fatalf("seed %d: queues not drained (%v)", seed, err)
		}
		for _, d := range devs {
			for _, u := range queued[d.ID] {
				if st, _, _ := d.Dev.EventState(ctx, u); st != "acked" {
					t.Fatalf("seed %d: %s/%d %s", seed, d.ID, u, st)
				}
				if _, err := w.Events.GetReliable(ctx, d.ID, u); err != nil {
					t.Fatalf("seed %d: acked event %s/%d not stored", seed, d.ID, u)
				}
			}
		}
		total := 0
		recs, _ := w.Events.ReadReliableAfter(ctx, 0, 100000)
		seen := map[string]bool{}
		for _, r := range recs {
			k := r.DeviceID + "/" + itoa(r.Upload.UID)
			if seen[k] {
				t.Fatalf("seed %d: %s stored twice", seed, k)
			}
			seen[k] = true
			total++
		}
		if w.Net.Duplicated == 0 || w.Net.Dropped == 0 {
			t.Fatalf("seed %d: impairments not exercised", seed)
		}
		w.Close()
	}
}

// Queue pressure: routine traffic fills its share while offline; panics
// are still queued and are delivered first when connectivity returns.
func TestScenarioPanicUnderQueuePressure(t *testing.T) {
	w := newWorld(t, 8, func(c *device.Config) { c.Capacity, c.CriticalReserve, c.MaxSendsPerTick = 200, 20, 8 })
	d := addDevice(t, w, "TRK-1")
	ctx := context.Background()
	mustRun(t, w, 2*time.Second)
	d.Offline = true
	full := 0
	for i := 0; i < 400; i++ {
		if _, err := d.Dev.Enqueue(ctx, func(uint64, time.Time) (reliable.Body, error) { return reliable.Diagnostic{Code: 1}, nil }); errors.Is(err, device.ErrQueueFull) {
			full++
		}
	}
	if full == 0 {
		t.Fatal("routine traffic never hit its limit")
	}
	p, err := d.Dev.Panic(ctx)
	if err != nil {
		t.Fatalf("panic refused under routine pressure: %v", err)
	}
	d.Offline = false
	ok, _ := w.RunUntil(time.Minute, step, func() bool { st, _, _ := d.Dev.EventState(ctx, p); return st == "acked" })
	if !ok {
		t.Fatal("panic not delivered")
	}
	q, _ := d.Dev.Queue(ctx)
	if q.PendingTotal() < 100 {
		t.Fatalf("panic was not prioritized: only %d routine events still pending", q.PendingTotal())
	}
	// Routine traffic still drains (not starved).
	ok, _ = w.RunUntil(5*time.Minute, step, func() bool { q, _ := d.Dev.Queue(ctx); return q.PendingTotal() == 0 })
	if !ok {
		t.Fatal("routine traffic starved")
	}
}

// Device clock regressions and a prolonged outage: delivery does not
// depend on the device clock; commands expire by deadline.
func TestScenarioClockRegressionAndOutage(t *testing.T) {
	w := newWorld(t, 9, nil)
	d := addDevice(t, w, "TRK-1")
	ctx := context.Background()
	mustRun(t, w, 2*time.Second)
	d.Skew = -3 * time.Hour // device clock jumps back
	u1, _ := d.Dev.Panic(ctx)
	d.Skew = 0
	d.Offline = true
	u2, _ := d.Dev.Panic(ctx)
	blk := request(t, w, "blk", secOp, "TRK-1", reliable.CommandBlock, nil)
	mustRun(t, w, 30*time.Minute) // prolonged outage
	if c := w.Command(blk.ID); c.State != commandstore.Unknown && c.State != commandstore.Expired {
		t.Fatalf("command after outage: %s", c.State)
	}
	if d.Vehicle.Truth().ImmobilizerDrives != 0 {
		t.Fatal("executed during outage")
	}
	d.Offline = false
	ok, _ := w.RunUntil(2*time.Minute, step, func() bool {
		a, _, _ := d.Dev.EventState(ctx, u1)
		b, _, _ := d.Dev.EventState(ctx, u2)
		return a == "acked" && b == "acked"
	})
	if !ok {
		t.Fatal("panics not delivered after the outage")
	}
	// The gateway still knew the device's address, so attempts were
	// transmitted and lost: the outcome is unknown (not "failed") and needs
	// attention; the device never executed it.
	if c := w.Command(blk.ID); c.State != commandstore.Unknown || !c.NeedsAttention {
		t.Fatalf("command %s attention=%v, want unknown with attention", c.State, c.NeedsAttention)
	}
	mustRun(t, w, time.Minute)
	if d.Vehicle.Truth().ImmobilizerDrives != 0 {
		t.Fatal("expired command executed after reconnection")
	}
}

// A device that is moving refuses BLOCK; the server records the rejection
// with its reason; the vehicle is not blocked.
func TestScenarioBlockRefusedWhileMoving(t *testing.T) {
	w := newWorld(t, 10, nil)
	d := addDevice(t, w, "TRK-1")
	d.Vehicle.Drive(800, true)
	mustRun(t, w, 2*time.Second)
	blk := request(t, w, "blk", secOp, "TRK-1", reliable.CommandBlock, nil)
	c := waitState(t, w, blk.ID, time.Minute, commandstore.Rejected)
	if c.DeviceReason != reliable.ReasonVehicleMoving || d.Vehicle.Truth().ImmobilizerDrives != 0 {
		t.Fatalf("command %+v truth %+v", c, d.Vehicle.Truth())
	}
}
