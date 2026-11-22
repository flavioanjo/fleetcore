package fusion

import (
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// Regression tests for defects found in code review before the first
// release of the fusion engine.

// Health sums accumulated before a reset (reboot, gap) must not leak into
// the next checkpoint and make a correct source look suspect.
func TestHealthCheckpointAfterRebootStartsClean(t *testing.T) {
	a := newStream("H", 1)
	odo := 100 * km
	var ss []telemetry.Sample
	for i := 0; i < 20; i++ {
		a.tick(10 * time.Second)
		odo += 200 * m
		ss = append(ss, a.gps(200*m), a.can(odo/(100*m)*(100*m)))
	}
	b := newStream("H", 2)
	b.now = a.now.Add(time.Minute)
	for i := 0; i < 30; i++ {
		b.tick(10 * time.Second)
		odo += 200 * m
		if i < 10 {
			ss = append(ss, b.gps(200*m), b.canNone())
		} else {
			ss = append(ss, b.gps(200*m), b.can(odo/(100*m)*(100*m)))
		}
	}
	rs := runAll(newEngine(t), ss)
	if hasDiag(rs, CodeSourceSuspect) {
		t.Fatal("accurate GPS marked suspect after a reboot")
	}
}

// A suspect pulse counter stuck at zero must not keep suppressing GPS as
// "stationary drift".
func TestSuspectPulseDoesNotSuppressGPS(t *testing.T) {
	st := newStream("S", 1)
	odo := 10 * km
	var ss []telemetry.Sample
	for i := 0; i < 150; i++ {
		st.tick(10 * time.Second)
		odo += 20 * m
		ss = append(ss, st.gps(20*m))
		if i >= 120 {
			ss = append(ss, st.canNone())
		} else {
			ss = append(ss, st.can(odo/(100*m)*(100*m)))
		}
		ss = append(ss, st.pulse(0))
	}
	rs := runAll(newEngine(t), ss)
	requireDiag(t, rs, CodeSourceSuspect)
	near(t, "trip", lastTrip(t, rs, "S"), 150*20*m, 2*100*m)
	for _, r := range rs {
		if r.Estimate == nil {
			continue
		}
		for _, d := range r.Estimate.Diagnostics {
			if d.Code == CodeSourceSuspect && d.Source == telemetry.SourceGPS {
				t.Fatal("GPS marked suspect because a broken pulse suppressed it")
			}
		}
	}
}

// While CAN is stale, a lower reading is a decrease, not a new baseline.
func TestDecreaseWhileStaleIsIgnored(t *testing.T) {
	st := newStream("C", 1)
	var ss []telemetry.Sample
	for i := 0; i < 8; i++ {
		ss = append(ss, st.tick(10*time.Second).gps(100*m), st.can(400*km))
	}
	ss = append(ss, st.tick(10*time.Second).gps(100*m), st.can(350*km))
	rs := runAll(newEngine(t), ss)
	requireDiag(t, rs, CodeCANStale)
	requireDiag(t, rs, CodeCANDecreased)
	if hasDiag(rs, CodeCANRebaselined) {
		t.Fatal("decreasing reading became a new baseline")
	}
}

// Sequence wrap-around is unsupported: it must be reported, never
// silently re-applied.
func TestSequenceExhaustionIsReported(t *testing.T) {
	st := newStream("W", 1)
	st.seq = ^telemetry.Sequence(0) - 15
	var ss []telemetry.Sample
	for i := 0; i < 15; i++ {
		ss = append(ss, st.tick(10*time.Second).gps(100*m))
	}
	resend := ss[4]
	after := st.tick(10 * time.Second).gps(100 * m) // wraps to sequence 0
	rs := runAll(newEngine(t), append(ss, resend, after))
	evs := events(rs)
	if got := evs[len(evs)-2]; got.Outcome != OutcomeDuplicate {
		t.Fatalf("resend after exhaustion: %s/%s, want duplicate", got.Outcome, got.Reason)
	}
	if got := evs[len(evs)-1]; got.Outcome != OutcomeRejected || got.Reason != ReasonSequenceExhausted {
		t.Fatalf("wrapped event: %s/%s, want rejected/sequence_exhausted", got.Outcome, got.Reason)
	}
	if hasDiag(rs, CodeSequenceGap) {
		t.Fatal("wrap produced a spurious sequence gap")
	}
}

// An event just below the session's chosen first sequence number is late,
// not "beyond tracking".
func TestLateEventBeforeSessionStart(t *testing.T) {
	ss := gpsPulseDrive(newStream("L", 1), 10)
	rs := runAll(newEngine(t), append(append([]telemetry.Sample(nil), ss[1:]...), ss[0]))
	ev := finalOutcomes(rs)[[2]uint64{1, uint64(ss[0].Sequence)}]
	if ev.Outcome != OutcomeRequiresReconciliation || ev.Reason != ReasonLateAfterGap {
		t.Fatalf("got %s/%s, want requires_reconciliation/late_after_gap", ev.Outcome, ev.Reason)
	}
}

// Interpolated travel widens the uncertainty linearly (by half of it).
func TestInterpolationWidensUncertaintyLinearly(t *testing.T) {
	st := newStream("U", 1)
	ss := []telemetry.Sample{
		st.tick(10 * time.Second).gps(300 * m), // first epoch: no elapsed time, so no speed yet
		st.tick(10 * time.Second).gps(300 * m), st.tick(10 * time.Second).gpsNone(),
		st.tick(10 * time.Second).gpsNone(), st.tick(10 * time.Second).gps(300 * m),
	}
	es := estimates(runAll(newEngine(t), ss), "U")
	last, before := es[len(es)-1], es[len(es)-2]
	if last.InterpolatedMM != 600*m {
		t.Fatalf("interpolated %d", last.InterpolatedMM)
	}
	if grow := last.UncertaintyMM - before.UncertaintyMM; grow < last.InterpolatedMM/2 {
		t.Fatalf("uncertainty grew by %d mm, want at least half of %d mm", grow, last.InterpolatedMM)
	}
}
