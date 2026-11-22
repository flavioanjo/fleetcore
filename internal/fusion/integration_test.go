package fusion

import (
	"reflect"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// Tests for the contract with an upstream event pipeline (docs/fusion.md §11).

var wall = time.Date(2026, time.November, 22, 12, 0, 0, 0, time.UTC)

// The reorder window is measured in device time, so it cannot fire for a
// device that goes silent: device time stops with it. Without Tick the
// buffered events and the open epoch wait until Flush.
func TestSilentDeviceAfterOutOfOrderEvents(t *testing.T) {
	newScenario := func() (*Engine, []telemetry.Sample) {
		e := newEngine(t)
		ss := gpsPulseDrive(newStream("Q", 1), 10)
		for _, s := range ss[:16] { // establishes the session (start-up window)
			e.Process(s)
		}
		// seq 17 is lost; 18 and 19 arrive and wait for it; then silence.
		e.Process(ss[17])
		e.Process(ss[18])
		return e, ss
	}

	t.Run("device-time window alone never fires", func(t *testing.T) {
		e, _ := newScenario()
		if got := e.Stats().Buffered; got != 2 {
			t.Fatalf("buffered %d, want 2", got)
		}
		// Hours of processing time pass, but no Tick is called.
		if got := e.Stats().Buffered; got != 2 {
			t.Fatalf("buffered %d after silence, want still 2 (only Flush or Tick release them)", got)
		}
		rs := e.Flush()
		requireDiag(t, rs, CodeSequenceGap)
		if e.Stats().Buffered != 0 {
			t.Fatal("Flush left events buffered")
		}
	})

	t.Run("processing-time Tick drains after IdleTimeout", func(t *testing.T) {
		e, _ := newScenario()
		if rs := e.Tick(wall); len(rs) != 0 {
			t.Fatalf("first tick after activity must only observe it, got %d reports", len(rs))
		}
		if rs := e.Tick(wall.Add(59 * time.Second)); len(rs) != 0 || e.Stats().Buffered != 2 {
			t.Fatal("drained before IdleTimeout")
		}
		rs := e.Tick(wall.Add(60 * time.Second))
		requireDiag(t, rs, CodeIdleFlush)
		requireDiag(t, rs, CodeSequenceGap)
		if e.Stats().Buffered != 0 {
			t.Fatalf("still buffered: %d", e.Stats().Buffered)
		}
		accepted := 0
		for _, ev := range events(rs) {
			if ev.Outcome == OutcomeAccepted {
				accepted++
			}
		}
		if accepted != 2 || len(estimates(rs, "Q")) == 0 {
			t.Fatalf("accepted %d events and %d estimates, want 2 and the closed epochs", accepted, len(estimates(rs, "Q")))
		}
		// Nothing more to do: further ticks are silent.
		if rs := e.Tick(wall.Add(10 * time.Minute)); len(rs) != 0 {
			t.Fatalf("idle device drained twice: %d reports", len(rs))
		}
	})

	t.Run("the session continues after an idle drain", func(t *testing.T) {
		e, ss := newScenario()
		e.Tick(wall)
		e.Tick(wall.Add(time.Minute))
		late := e.Process(ss[16]) // the lost event finally arrives
		if ev := late[0].Event; ev.Outcome != OutcomeRequiresReconciliation || ev.Reason != ReasonLateAfterGap {
			t.Fatalf("late event after idle drain: %s/%s", ev.Outcome, ev.Reason)
		}
		next := e.Process(ss[19])
		if ev := events(next)[0]; ev.Outcome != OutcomeAccepted {
			t.Fatalf("next in-order event: %s", ev.Outcome)
		}
	})
}

// A device that sends less than one reorder window of data and stops is
// still waiting for its session start; Tick releases it.
func TestShortLivedDeviceIsReleasedByTick(t *testing.T) {
	e := newEngine(t)
	st := newStream("SHORT", 1)
	for i := 0; i < 2; i++ {
		st.tick(5 * time.Second)
		e.Process(st.gps(50 * m))
	}
	if e.Stats().Buffered != 2 {
		t.Fatalf("buffered %d, want 2 (session start not yet established)", e.Stats().Buffered)
	}
	e.Tick(wall)
	rs := e.Tick(wall.Add(time.Minute))
	if got := lastTrip(t, rs, "SHORT"); got != 100*m {
		t.Fatalf("trip %d, want 100 m", got)
	}
}

// Tick must not disturb devices that keep sending, and must not change the
// estimates of a stream it never had to drain.
func TestTickLeavesActiveDevicesAlone(t *testing.T) {
	ss := gpsPulseDrive(newStream("A", 1), 30)
	plain := runAll(newEngine(t), ss)

	e := newEngine(t)
	var ticked []Report
	for i, s := range ss {
		ticked = append(ticked, e.Process(s)...)
		if i%2 == 1 { // a tick every epoch: the device is never idle for a minute
			ticked = append(ticked, e.Tick(wall.Add(time.Duration(i)*5*time.Second))...)
		}
	}
	ticked = append(ticked, e.Flush()...)
	if hasDiag(ticked, CodeIdleFlush) {
		t.Fatal("active device was drained")
	}
	if !reflect.DeepEqual(estimates(plain, "A"), estimates(ticked, "A")) {
		t.Fatal("ticks changed the estimates of an active device")
	}
}

// EventID is payload, not identity, for the engine: the reference producer
// derives it from the per-boot sequence, so it repeats across reboots.
func TestEventIDReuseAcrossBootsIsNormal(t *testing.T) {
	a := newStream("E", 1)
	first := gpsPulseDrive(a, 5)
	b := newStream("E", 2)
	b.now = a.now.Add(time.Minute)
	second := gpsPulseDrive(b, 5)
	for i := range second { // same EventIDs as the first boot
		second[i].EventID = first[i].EventID
	}
	rs := runAll(newEngine(t), append(first, second...))
	for _, ev := range events(rs) {
		if ev.Outcome != OutcomeAccepted && ev.Outcome != OutcomeBuffered {
			t.Fatalf("EventID reuse across boots: seq %d boot %d got %s/%s", ev.Sequence, ev.BootID, ev.Outcome, ev.Reason)
		}
	}
	if got := lastTrip(t, rs, "E"); got != 1000*m {
		t.Fatalf("trip %d, want 1000 m", got)
	}
}

// The engine's identity is (DeviceID, BootID, Sequence) plus a content
// fingerprint that includes EventID.
func TestIdentityRulesAtTheBoundary(t *testing.T) {
	st := newStream("I", 1)
	ss := gpsPulseDrive(st, 4)

	t.Run("same sequence, different EventID is a conflict", func(t *testing.T) {
		other := ss[2]
		other.EventID += 1000
		rs := runAll(newEngine(t), append(append([]telemetry.Sample(nil), ss...), other))
		last := events(rs)[len(events(rs))-1]
		if last.Outcome != OutcomeRejected || last.Reason != ReasonSequenceConflict {
			t.Fatalf("got %s/%s", last.Outcome, last.Reason)
		}
	})
	t.Run("same EventID in two sequence slots is two events", func(t *testing.T) {
		twin := append([]telemetry.Sample(nil), ss...)
		twin[3].EventID = twin[2].EventID // a producer bug the event store reports as a conflict
		rs := runAll(newEngine(t), twin)
		for _, ev := range events(rs) {
			if ev.Outcome != OutcomeAccepted && ev.Outcome != OutcomeBuffered {
				t.Fatalf("seq %d: %s/%s", ev.Sequence, ev.Outcome, ev.Reason)
			}
		}
	})
	t.Run("retransmission with a later ingestion time is a duplicate", func(t *testing.T) {
		again := ss[1]
		again.IngestedAt = telemetry.Some(again.DeviceTime.Add(time.Hour))
		rs := runAll(newEngine(t), append(append([]telemetry.Sample(nil), ss...), again))
		if last := events(rs)[len(events(rs))-1]; last.Outcome != OutcomeDuplicate {
			t.Fatalf("got %s/%s", last.Outcome, last.Reason)
		}
	})
}

// Replaying the same ordered input (for example from an append-only event
// store, by position) into a fresh engine reproduces the output exactly.
func TestReplayReproducesOutput(t *testing.T) {
	ss := gpsPulseDrive(newStream("R", 1), 40)
	ss[7], ss[9] = ss[9], ss[7]
	ss = append(ss, ss[3])
	if !reflect.DeepEqual(runAll(newEngine(t), ss), runAll(newEngine(t), ss)) {
		t.Fatal("replay differs from the original run")
	}
}
