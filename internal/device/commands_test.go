package device_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/device"
	"github.com/flavioanjo/fleetcore/internal/reliable"
	"github.com/flavioanjo/fleetcore/internal/security"
	"github.com/flavioanjo/fleetcore/internal/simhw"
)

var nextCmd byte

func (r *rig) command(typ reliable.CommandType, args []byte, ttl time.Duration, signer *security.Authority) reliable.CommandDelivery {
	nextCmd++
	now := time.UnixMilli(r.clk.now().UnixMilli()).UTC()
	c := reliable.Command{ID: reliable.CommandID{0xc0, nextCmd}, DeviceID: "DEV-1", Type: typ, Args: args,
		IssuedAt: now, ExpiresAt: now.Add(ttl), Principal: "ops:alice"}
	return reliable.CommandDelivery{Command: c, Attempt: 1, AttemptSentAt: now, AttemptNotAfter: now.Add(30 * time.Second),
		AuthorityKeyID: signer.KeyID(), Signature: signer.Sign(c.Statement())}
}

// deliver seals the delivery for the device's current session through the
// gateway (as the dispatcher does) and hands it to the device.
func (r *rig) deliver(d reliable.CommandDelivery) {
	r.t.Helper()
	o, err := r.gw.CommandPacket(d)
	if err != nil {
		r.t.Fatal(err)
	}
	if err := r.dev.HandleDownlink(context.Background(), o.Data); err != nil {
		r.t.Fatalf("device: %v", err)
	}
}

// reports returns the COMMAND_RESULT stages the server stored for id.
func (r *rig) reports(id reliable.CommandID) []reliable.Stage {
	r.t.Helper()
	for i := 0; i < 4; i++ {
		r.exchange(false)
	}
	recs, err := r.events.ReadReliableAfter(context.Background(), 0, 1000, reliable.EventCommandResult)
	if err != nil {
		r.t.Fatal(err)
	}
	var out []reliable.Stage
	for _, rec := range recs {
		b, _ := rec.Body()
		if c := b.(reliable.CommandResult); c.CommandID == id {
			out = append(out, c.Stage)
		}
	}
	return out
}

func (r *rig) lastResult(id reliable.CommandID) reliable.CommandResult {
	recs, _ := r.events.ReadReliableAfter(context.Background(), 0, 1000, reliable.EventCommandResult)
	var last reliable.CommandResult
	for _, rec := range recs {
		b, _ := rec.Body()
		if c := b.(reliable.CommandResult); c.CommandID == id {
			last = c
		}
	}
	return last
}

func stagesEqual(a []reliable.Stage, b ...reliable.Stage) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestBlockExecutesOnceDespiteDuplicateDeliveries(t *testing.T) {
	r := newRig(t, func(c *device.Config) { c.StatusRepeat = time.Second })
	r.exchange(false)
	blk := r.command(reliable.CommandBlock, nil, time.Minute, r.auth)
	r.deliver(blk)
	for i := 0; i < 5; i++ { // the server, not hearing back, redelivers
		blk.Attempt++
		r.clk.advance(2 * time.Second)
		blk.AttemptSentAt, blk.AttemptNotAfter = r.clk.now(), r.clk.now().Add(30*time.Second)
		r.deliver(blk)
	}
	tr := r.veh.Truth()
	if !tr.Blocked || tr.ImmobilizerDrives != 1 {
		t.Fatalf("truth %+v: one BLOCK must drive the actuator exactly once", tr)
	}
	st := r.reports(blk.ID)
	if st[0] != reliable.StageReceived || st[1] != reliable.StageAccepted || st[2] != reliable.StageSucceeded {
		t.Fatalf("reports %v", st)
	}
	if res := r.lastResult(blk.ID); res.Observed != uint8(reliable.Blocked) {
		t.Fatalf("observed %d", res.Observed)
	}
	if m := r.dev.Metrics(); m.DuplicateDeliveries != 5 || m.StatusReReports == 0 {
		t.Fatalf("metrics %+v", m)
	}
	// BLOCK_STATE confirmed by the sensor, linked to the command.
	recs, _ := r.events.ReadReliableAfter(context.Background(), 0, 100, reliable.EventBlockState)
	var linked bool
	for _, rec := range recs {
		b, _ := rec.Body()
		if s := b.(reliable.BlockState); s.CommandID == blk.ID && s.State == reliable.Blocked && s.Cause == reliable.CauseCommand {
			linked = true
		}
	}
	if !linked {
		t.Fatal("no BLOCK_STATE for the command")
	}

	// UNBLOCK: verifiable transition back.
	ub := r.command(reliable.CommandUnblock, nil, time.Minute, r.auth)
	r.deliver(ub)
	if tr := r.veh.Truth(); tr.Blocked || tr.ImmobilizerMoves != 2 {
		t.Fatalf("after unblock %+v", tr)
	}
	if !stagesEqual(r.reports(ub.ID), reliable.StageReceived, reliable.StageAccepted, reliable.StageSucceeded) {
		t.Fatal("unblock reports")
	}
}

func TestBlockRefusedWhileMoving(t *testing.T) {
	r := newRig(t, nil)
	r.exchange(false)
	r.veh.Drive(600, true) // 60 km/h
	blk := r.command(reliable.CommandBlock, nil, time.Minute, r.auth)
	r.deliver(blk)
	if tr := r.veh.Truth(); tr.Blocked || tr.ImmobilizerDrives != 0 {
		t.Fatalf("immobilizer driven on a moving vehicle: %+v", tr)
	}
	if !stagesEqual(r.reports(blk.ID), reliable.StageReceived, reliable.StageRejected) || r.lastResult(blk.ID).Reason != reliable.ReasonVehicleMoving {
		t.Fatalf("reports %v reason %s", r.reports(blk.ID), r.lastResult(blk.ID).Reason)
	}
	// The same CommandID later (vehicle stopped) is still not executed: the
	// decision is final.
	r.veh.Drive(0, false)
	blk.Attempt++
	r.deliver(blk)
	if r.veh.Truth().ImmobilizerDrives != 0 {
		t.Fatal("rejected command executed on redelivery")
	}
}

func TestInvalidAuthorityExpiredAndStaleAttempts(t *testing.T) {
	r := newRig(t, nil)
	ctx := context.Background()
	r.exchange(false)
	rogue, _ := security.NewAuthority(1) // same key id, different key
	forged := r.command(reliable.CommandBlock, nil, time.Minute, rogue)
	r.deliver(forged) // authenticated transport, forged authority
	if _, _, ok := r.dev.CommandStage(ctx, forged.ID); ok || r.veh.Truth().ImmobilizerDrives != 0 {
		t.Fatal("command with an invalid authority signature recorded or executed")
	}
	r.exchange(false)
	recs, _ := r.events.ReadReliableAfter(ctx, 0, 100, reliable.EventSecurityViolation)
	if len(recs) != 1 {
		t.Fatalf("%d security violations reported", len(recs))
	}
	// Tampered arguments invalidate the signature too.
	cargo := r.command(reliable.CommandOpenCargo, reliable.CargoArgs(1), time.Minute, r.auth)
	cargo.Args = reliable.CargoArgs(2)
	r.deliver(cargo)
	if r.veh.Truth().LatchReleases != 0 {
		t.Fatal("tampered command executed")
	}

	// Expired before it reached the device.
	exp := r.command(reliable.CommandBlock, nil, 5*time.Second, r.auth)
	r.clk.advance(10 * time.Second)
	exp.AttemptSentAt, exp.AttemptNotAfter = r.clk.now(), r.clk.now().Add(30*time.Second)
	r.deliver(exp)
	if st, reason, _ := r.dev.CommandStage(ctx, exp.ID); st != reliable.StageRejected || reason != reliable.ReasonExpired {
		t.Fatalf("expired command: %s %s", st, reason)
	}
	// A delayed attempt (past its freshness bound) is ignored entirely.
	late := r.command(reliable.CommandBlock, nil, time.Hour, r.auth)
	r.clk.advance(31 * time.Second)
	r.deliver(late)
	if _, _, ok := r.dev.CommandStage(ctx, late.ID); ok || r.dev.Metrics().StaleAttempts != 1 {
		t.Fatal("stale attempt processed")
	}
	if r.veh.Truth().ImmobilizerDrives != 0 {
		t.Fatal("actuator driven")
	}
}

func TestCrashDuringExecutionIsReportedUnknownNeverRepeated(t *testing.T) {
	crash := true
	r := newRig(t, func(c *device.Config) {
		c.Hooks = &device.Hooks{DuringExecution: func(reliable.CommandID) error {
			if crash {
				return errors.New("power loss")
			}
			return nil
		}}
	})
	ctx := context.Background()
	r.exchange(false)
	blk := r.command(reliable.CommandBlock, nil, time.Minute, r.auth)
	o, _ := r.gw.CommandPacket(blk)
	if err := r.dev.HandleDownlink(ctx, o.Data); err == nil {
		t.Fatal("simulated crash not surfaced")
	}
	crash = false
	r.start() // reboot
	if st, reason, _ := r.dev.CommandStage(ctx, blk.ID); st != reliable.StageUnknown || reason != reliable.ReasonInterrupted {
		t.Fatalf("after reboot: %s %s", st, reason)
	}
	// Redelivered after the reboot: not executed again.
	r.exchange(false)
	r.deliver(blk)
	if tr := r.veh.Truth(); tr.ImmobilizerDrives != 1 {
		t.Fatalf("actuator driven %d times", tr.ImmobilizerDrives)
	}
	st := r.reports(blk.ID)
	if !stagesEqual(st, reliable.StageReceived, reliable.StageAccepted, reliable.StageUnknown) {
		t.Fatalf("reports %v", st)
	}
	// The interrupted report carries what the sensor shows (it did block).
	if res := r.lastResult(blk.ID); res.Reason != reliable.ReasonInterrupted || res.Observed != uint8(reliable.Blocked) {
		t.Fatalf("interrupted report %+v", res)
	}
}

func TestOpenCargoNeedsSensorConfirmation(t *testing.T) {
	r := newRig(t, nil)
	ctx := context.Background()
	r.exchange(false)
	// Ignition on: refused.
	r.veh.Drive(0, true)
	c1 := r.command(reliable.CommandOpenCargo, reliable.CargoArgs(1), time.Minute, r.auth)
	r.deliver(c1)
	if st, reason, _ := r.dev.CommandStage(ctx, c1.ID); st != reliable.StageRejected || reason != reliable.ReasonIgnitionOn {
		t.Fatalf("ignition on: %s %s", st, reason)
	}
	// Latch released, nobody opens the door: unknown, not success.
	r.veh.Drive(0, false)
	r.veh.SetFaults(simhw.Faults{DoorStaysClosed: true})
	c2 := r.command(reliable.CommandOpenCargo, reliable.CargoArgs(1), time.Minute, r.auth)
	r.deliver(c2)
	if st, reason, _ := r.dev.CommandStage(ctx, c2.ID); st != reliable.StageUnknown || reason != reliable.ReasonNotConfirmed {
		t.Fatalf("unconfirmed opening: %s %s", st, reason)
	}
	// Door opens: success, confirmed by the door sensor; CARGO_STATE reports it.
	r.veh.CloseCargo(1)
	r.veh.SetFaults(simhw.Faults{})
	c3 := r.command(reliable.CommandOpenCargo, reliable.CargoArgs(1), time.Minute, r.auth)
	r.deliver(c3)
	if st, _, _ := r.dev.CommandStage(ctx, c3.ID); st != reliable.StageSucceeded || !r.veh.Truth().CargoOpen {
		t.Fatal("cargo not opened")
	}
	r.exchange(false)
	recs, _ := r.events.ReadReliableAfter(ctx, 0, 100, reliable.EventCargoState)
	b, _ := recs[len(recs)-1].Body()
	if cs := b.(reliable.CargoState); cs.Door != reliable.DoorOpen || cs.Latch != reliable.LatchReleased || cs.CommandID != c3.ID {
		t.Fatalf("cargo state %+v", cs)
	}
	if r.veh.Truth().LatchReleases != 2 {
		t.Fatalf("latch released %d times", r.veh.Truth().LatchReleases)
	}
}

func TestRequestPositionNeverFabricatesAFix(t *testing.T) {
	r := newRig(t, nil)
	ctx := context.Background()
	r.exchange(false)
	// No fix: explicit failure, no POSITION event.
	c1 := r.command(reliable.CommandRequestPosition, reliable.PositionArgs(30*time.Second), time.Minute, r.auth)
	r.deliver(c1)
	if st, reason, _ := r.dev.CommandStage(ctx, c1.ID); st != reliable.StageFailed || reason != reliable.ReasonPositionUnavailable {
		t.Fatalf("no fix: %s %s", st, reason)
	}
	r.exchange(false)
	if recs, _ := r.events.ReadReliableAfter(ctx, 0, 100, reliable.EventPosition); len(recs) != 0 {
		t.Fatal("a position was reported without a fix")
	}
	// With a fix: a fresh POSITION answering the request.
	r.veh.SetFix(reliable.PositionFix{Fix: reliable.Fix3D, LatE7: -235505200, LonE7: -466333100, HDOPx10: 8, Satellites: 9}, true)
	c2 := r.command(reliable.CommandRequestPosition, reliable.PositionArgs(30*time.Second), time.Minute, r.auth)
	r.deliver(c2)
	r.exchange(false)
	recs, _ := r.events.ReadReliableAfter(ctx, 0, 100, reliable.EventPosition)
	if len(recs) != 1 {
		t.Fatalf("%d positions", len(recs))
	}
	b, _ := recs[0].Body()
	p := b.(reliable.Position)
	if p.Reason != reliable.PositionRequested || p.RequestID != c2.ID || !p.FixTime.Equal(time.UnixMilli(r.clk.now().UnixMilli())) {
		t.Fatalf("position %+v", p)
	}
	if st, _, _ := r.dev.CommandStage(ctx, c2.ID); st != reliable.StageSucceeded {
		t.Fatal("request not succeeded")
	}

}

func TestPanicCarriesLastValidFix(t *testing.T) {
	r := newRig(t, nil)
	ctx := context.Background()
	r.veh.SetFix(reliable.PositionFix{Fix: reliable.Fix2D, LatE7: 10, LonE7: 20, HDOPx10: 20, Satellites: 3}, true)
	r.dev.ReportPosition(ctx)
	r.veh.SetFix(reliable.PositionFix{}, false) // fix lost (tunnel)
	r.clk.advance(90 * time.Second)
	r.panicEvent()
	for i := 0; i < 3; i++ {
		r.exchange(false)
	}
	recs, _ := r.events.ReadReliableAfter(ctx, 0, 100, reliable.EventPanic)
	if len(recs) != 1 {
		t.Fatalf("%d panics", len(recs))
	}
	b, _ := recs[0].Body()
	p := b.(reliable.Panic)
	if !p.HasPosition || p.Position.LatE7 != 10 || p.PositionAge != 90*time.Second {
		t.Fatalf("panic %+v", p)
	}
}
