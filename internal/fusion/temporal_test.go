package fusion

import (
	"reflect"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// gpsPulseDrive returns n epochs (10 s) of GPS + pulse at 100 m each.
func gpsPulseDrive(st *stream, n int) []telemetry.Sample {
	var ss []telemetry.Sample
	for i := 0; i < n; i++ {
		st.tick(10 * time.Second)
		ss = append(ss, st.gps(100*m), st.pulse(100*m))
	}
	return ss
}

func TestLateEventsWithinWindowAreReordered(t *testing.T) {
	inOrder := gpsPulseDrive(newStream("D", 1), 10)
	shuffled := append([]telemetry.Sample(nil), inOrder...)
	// Swap neighbours and delay one event by two positions (all within 30 s).
	shuffled[4], shuffled[5] = shuffled[5], shuffled[4]
	shuffled[10], shuffled[11], shuffled[12] = shuffled[11], shuffled[12], shuffled[10]

	want := runAll(newEngine(t), inOrder)
	got := runAll(newEngine(t), shuffled)
	if !reflect.DeepEqual(estimates(want, "D"), estimates(got, "D")) {
		t.Fatal("reordering within the window changed the estimates")
	}
	for k, ev := range finalOutcomes(got) {
		if ev.Outcome != OutcomeAccepted {
			t.Fatalf("event %v final outcome %s", k, ev.Outcome)
		}
	}
}

func TestOutOfOrderEventIsReportedAsBuffered(t *testing.T) {
	e := newEngine(t)
	ss := gpsPulseDrive(newStream("D", 1), 10)
	// Establish the session (start-up buffering) with the first 8 epochs.
	for _, s := range ss[:16] {
		e.Process(s)
	}
	rs := e.Process(ss[17]) // skips seq 17 (ss[16])
	if len(rs) != 1 || rs[0].Event.Outcome != OutcomeBuffered {
		t.Fatalf("got %+v, want a single buffered report", rs)
	}
	rs = e.Process(ss[16])
	evs := events(rs)
	if len(evs) != 2 || evs[0].Sequence != 17 || evs[1].Sequence != 18 || evs[1].Outcome != OutcomeAccepted {
		t.Fatalf("release order/outcomes wrong: %+v", evs)
	}
	requireDiag(t, rs, CodeReleasedFromBuffer)
}

func TestDuplicatesAreIgnored(t *testing.T) {
	ss := gpsPulseDrive(newStream("D", 1), 10)
	dup := append([]telemetry.Sample(nil), ss...)
	dup = append(dup[:7], append([]telemetry.Sample{ss[3], ss[6]}, dup[7:]...)...) // one released, one buffered copy
	dup = append(dup, ss[0])                                                       // a much later copy

	base := runAll(newEngine(t), ss)
	rs := runAll(newEngine(t), dup)
	if lastTrip(t, rs, "D") != lastTrip(t, base, "D") {
		t.Fatal("duplicates changed the estimate")
	}
	n := 0
	for _, ev := range events(rs) {
		if ev.Outcome == OutcomeDuplicate {
			n++
		}
	}
	if n != 3 {
		t.Fatalf("%d duplicates reported, want 3", n)
	}
}

func TestSequenceReuseWithDifferentContentIsRejected(t *testing.T) {
	st := newStream("D", 1)
	ss := gpsPulseDrive(st, 5)
	forged := ss[2]
	forged.Distance = telemetry.Some(telemetry.Distance{Kind: telemetry.DistanceDelta, Value: 999 * m})
	rs := runAll(newEngine(t), append(ss, forged))
	last := events(rs)[len(events(rs))-1]
	if last.Outcome != OutcomeRejected || last.Reason != ReasonSequenceConflict {
		t.Fatalf("got %s/%s, want rejected/sequence_conflict", last.Outcome, last.Reason)
	}
}

func TestEventsBeyondWindowRequireReconciliation(t *testing.T) {
	ss := gpsPulseDrive(newStream("D", 1), 12)
	late := ss[5]
	stream := append(append([]telemetry.Sample(nil), ss[:5]...), ss[6:]...)
	stream = append(stream, late) // arrives ~70 s of device time later: beyond the 30 s window
	rs := runAll(newEngine(t), stream)

	requireDiag(t, rs, CodeSequenceGap)
	requireDiag(t, rs, CodeSpanAmbiguous)
	final := finalOutcomes(rs)[[2]uint64{1, uint64(late.Sequence)}]
	if final.Outcome != OutcomeRequiresReconciliation || final.Reason != ReasonLateAfterGap {
		t.Fatalf("late event: %s/%s, want requires_reconciliation/late_after_gap", final.Outcome, final.Reason)
	}
	// The engine cannot tell which source the missing event belonged to, so
	// both delta sources only re-anchor in the next epoch. That epoch's
	// travel is then interpolated from the speeds around it (here constant).
	requireDiag(t, rs, CodeGapInterpolated)
	near(t, "trip", lastTrip(t, rs, "D"), 1200*m, 0)

	// With a CAN odometer the bound recovers the lost epoch.
	st := newStream("E", 2)
	odo := 400 * km
	var withCAN []telemetry.Sample
	for i := 0; i < 12; i++ {
		st.tick(10 * time.Second)
		odo += 100 * m
		withCAN = append(withCAN, st.gps(100*m), st.pulse(100*m), st.can(odo))
	}
	lateCAN := withCAN[16]
	stream = append(append(append([]telemetry.Sample(nil), withCAN[:16]...), withCAN[17:]...), lateCAN)
	rs = runAll(newEngine(t), stream)
	near(t, "trip with CAN", lastTrip(t, rs, "E"), 1200*m, 100*m)
}

func TestMissingSequenceNumbers(t *testing.T) {
	ss := gpsPulseDrive(newStream("D", 1), 10)
	var withGap []telemetry.Sample
	for i, s := range ss {
		if i == 8 || i == 9 { // epoch 5 lost entirely
			continue
		}
		withGap = append(withGap, s)
	}
	rs := runAll(newEngine(t), withGap)
	requireDiag(t, rs, CodeSequenceGap)
	// The lost epoch's 100 m are unknown: no source covered it, and the
	// next deltas only re-anchor. The estimate is low, never inflated.
	trip := lastTrip(t, rs, "D")
	if trip > 1000*m || trip < 800*m {
		t.Fatalf("trip %d after a lost epoch, want 800..1000 m", trip)
	}
}

func TestRebootKeepsTripAndRestartsBaselines(t *testing.T) {
	a := newStream("D", 0xAAAA)
	ss := gpsPulseDrive(a, 6)
	b := newStream("D", 0x1111) // numerically smaller BootID: order is not inferred from it
	b.now = a.now.Add(2 * time.Minute)
	ss = append(ss, gpsPulseDrive(b, 6)...)

	rs := runAll(newEngine(t), ss)
	requireDiag(t, rs, CodeSessionTransition)
	if got := lastTrip(t, rs, "D"); got != 1200*m {
		t.Fatalf("trip %d, want 1200 m across the reboot", got)
	}
}

func TestPreviousSessionEventsAfterReboot(t *testing.T) {
	a := newStream("D", 7)
	old := gpsPulseDrive(a, 6)
	b := newStream("D", 8)
	b.now = a.now.Add(time.Minute)
	cur := gpsPulseDrive(b, 6)

	// Old session's last epoch is delivered after the new session started.
	stream := append(append(append([]telemetry.Sample(nil), old[:10]...), cur...), old[10:]...)
	rs := runAll(newEngine(t), stream)
	for _, s := range old[10:] {
		ev := finalOutcomes(rs)[[2]uint64{7, uint64(s.Sequence)}]
		if ev.Outcome != OutcomeRejected || ev.Reason != ReasonStaleSession {
			t.Fatalf("old-session event after reboot: %s/%s", ev.Outcome, ev.Reason)
		}
	}
	if got := lastTrip(t, rs, "D"); got != 1100*m {
		t.Fatalf("trip %d, want 1100 m (old tail not applied)", got)
	}
}

func TestUnknownOlderSessionIsAmbiguousThenAdopted(t *testing.T) {
	a := newStream("D", 1)
	a.now = t0.Add(time.Hour)
	ss := gpsPulseDrive(a, 5)
	// A boot whose clock is an hour behind: a delayed old session, or a
	// device whose clock reset. It is rejected until AdoptAfter events.
	b := newStream("D", 2)
	ss = append(ss, gpsPulseDrive(b, 3)...)
	rs := runAll(newEngine(t, func(c *Config) { c.AdoptAfter = 4 }), ss)
	requireDiag(t, rs, CodeSessionOrderUnclear)
	requireDiag(t, rs, CodeSessionAdopted)
	stale := 0
	for _, ev := range events(rs) {
		if ev.BootID == 2 && ev.Reason == ReasonStaleSession {
			stale++
		}
	}
	if stale != 3 {
		t.Fatalf("%d events rejected before adoption, want 3", stale)
	}
}

func TestRepeatedTimestamps(t *testing.T) {
	st := newStream("D", 1)
	ss := []telemetry.Sample{
		st.tick(10 * time.Second).gps(100 * m), st.pulse(100 * m), st.can(10 * km), // same time, three sensors: one epoch
		st.gps(5 * m), // same source again with the same timestamp
		st.tick(10 * time.Second).gps(100 * m),
	}
	rs := runAll(newEngine(t), ss)
	requireDiag(t, rs, CodeRepeatedTimestamp)
	es := estimates(rs, "D")
	if len(es) != 3 || es[0].LastSeq != 3 {
		t.Fatalf("epochs %+v: want 3 epochs, the first holding all three sensors", es)
	}
	// The repeated GPS reading spans zero time; 5 m is within the slack.
	if got := lastTrip(t, rs, "D"); got != 205*m {
		t.Fatalf("trip %d, want 205 m", got)
	}
}

func TestClockRegressionIsReported(t *testing.T) {
	st := newStream("D", 1)
	ss := []telemetry.Sample{st.tick(10 * time.Second).gps(100 * m), st.tick(10 * time.Second).gps(100 * m)}
	st.now = st.now.Add(-15 * time.Second)
	ss = append(ss, st.gps(100*m))
	rs := runAll(newEngine(t), ss)
	requireDiag(t, rs, CodeClockRegression)
	if got := lastTrip(t, rs, "D"); got != 300*m {
		t.Fatalf("trip %d, want 300 m: ordering follows sequence, not time", got)
	}
}

func TestDelayedDeliveryAndClockSkewAreNoted(t *testing.T) {
	st := newStream("D", 1)
	late := st.tick(10 * time.Second).gps(100 * m)
	late.IngestedAt = telemetry.Some(late.DeviceTime.Add(10 * time.Minute))
	ahead := st.tick(10 * time.Second).gps(100 * m)
	ahead.IngestedAt = telemetry.Some(ahead.DeviceTime.Add(-30 * time.Second))
	rs := runAll(newEngine(t), []telemetry.Sample{late, ahead})
	requireDiag(t, rs, CodeDelayedDelivery)
	requireDiag(t, rs, CodeDeviceClockAhead)
	if got := lastTrip(t, rs, "D"); got != 200*m {
		t.Fatalf("trip %d", got)
	}
}

func TestEverySampleGetsOneFinalOutcome(t *testing.T) {
	st := newStream("D", 1)
	ss := gpsPulseDrive(st, 20)
	ss[3], ss[4] = ss[4], ss[3]
	ss = append(ss, ss[2], ss[10])
	bad := ss[0]
	bad.EventID = 0
	ss = append(ss, bad)

	rs := runAll(newEngine(t), ss)
	final := 0
	for _, ev := range events(rs) {
		if ev.Outcome != OutcomeBuffered {
			final++
		}
	}
	if final != len(ss) {
		t.Fatalf("%d final outcomes for %d inputs", final, len(ss))
	}
}
