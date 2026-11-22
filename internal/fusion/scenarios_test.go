package fusion

import (
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// A hand-made drive: standstill, acceleration, cruising, braking, standstill.
// Distances per 10 s epoch.
func drive() []telemetry.Millimetres {
	var out []telemetry.Millimetres
	out = append(out, constant(3, 0)...)
	out = append(out, 20*m, 60*m, 110*m, 150*m)
	out = append(out, constant(30, 180*m)...) // 18 m/s
	out = append(out, 120*m, 70*m, 20*m)
	return append(out, constant(3, 0)...)
}

func TestNormalOperationSourceCombinations(t *testing.T) {
	// Exact sources and the CAN odometer quantised from the same truth.
	// Expected: delta-source-only combinations reproduce the truth exactly;
	// combinations with CAN stay within one CAN step (100 m) of the truth.
	tests := []struct {
		name            string
		gps, can, pulse bool
		tol             telemetry.Millimetres
	}{
		{"gps only", true, false, false, 0},
		{"pulse only", false, false, true, 0},
		{"can only", false, true, false, 100 * m},
		{"gps+can", true, true, false, 100 * m},
		{"gps+pulse", true, false, true, 0},
		{"can+pulse", false, true, true, 100 * m},
		{"all three", true, true, true, 100 * m},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := newEngine(t)
			spec := tripSpec{epochs: drive(), interval: 10 * time.Second, odometer: 81_234_567_890,
				gps: tc.gps, can: tc.can, pulse: tc.pulse}
			samples, truth := spec.build(newStream("CAR-1", 11))
			rs := runAll(e, samples)
			got := lastTrip(t, rs, "CAR-1")
			if tc.can && !tc.gps && !tc.pulse {
				// CAN alone cannot see travel before its first reading.
				truth -= spec.epochs[0]
			}
			near(t, "trip", got, truth, tc.tol)
			for _, ev := range events(rs) {
				if ev.Outcome != OutcomeAccepted && ev.Outcome != OutcomeBuffered {
					t.Fatalf("unexpected outcome %s for seq %d", ev.Outcome, ev.Sequence)
				}
			}
		})
	}
}

func TestCANOnlyUsesExpectedValueAndBounds(t *testing.T) {
	st := newStream("CAN-1", 1)
	var ss []telemetry.Sample
	for _, v := range []telemetry.Millimetres{1000 * km, 1000 * km, 1000*km + 100*m, 1000*km + 300*m} {
		st.tick(10 * time.Second)
		ss = append(ss, st.can(v))
	}
	rs := runAll(newEngine(t), ss)
	if got := lastTrip(t, rs, "CAN-1"); got != 300*m {
		t.Fatalf("trip %d, want 300 m (difference of readings)", got)
	}
	es := estimates(rs, "CAN-1")
	if es[0].State != StateProvisional || es[len(es)-1].State != StateBounded {
		t.Fatalf("states %s..%s, want provisional then bounded", es[0].State, es[len(es)-1].State)
	}
	requireDiag(t, rs, CodeCANBaseline)
}

func TestGPSUnavailableIsNotZeroMovement(t *testing.T) {
	// GPS drops out for two epochs; the pulse counter keeps measuring.
	st := newStream("CAR-2", 2)
	var ss []telemetry.Sample
	for i := 0; i < 6; i++ {
		st.tick(10 * time.Second)
		if i == 2 || i == 3 {
			ss = append(ss, st.gpsNone())
		} else {
			ss = append(ss, st.gps(150*m))
		}
		ss = append(ss, st.pulse(150*m))
	}
	rs := runAll(newEngine(t), ss)
	if got := lastTrip(t, rs, "CAR-2"); got != 900*m {
		t.Fatalf("trip %d, want 900 m", got)
	}
	requireDiag(t, rs, CodeSourceUnavailable)

	// GPS alone: an outage leaves the estimate unchanged and says so,
	// rather than asserting zero movement.
	st = newStream("CAR-3", 3)
	ss = []telemetry.Sample{st.tick(10 * time.Second).gps(100 * m), st.tick(10 * time.Second).gpsNone(), st.tick(10 * time.Second).gps(100 * m)}
	rs = runAll(newEngine(t), ss)
	es := estimates(rs, "CAR-3")
	if es[1].Confidence == ConfidenceHigh || !hasDiag(rs, CodeInsufficientEvidence) {
		t.Fatalf("outage epoch: confidence %s, want insufficient evidence reported", es[1].Confidence)
	}
}

func TestGPSOutageIsInterpolatedNotZeroed(t *testing.T) {
	// GPS only: 100 m, 120 m, [no fix], [no fix], 160 m, 180 m.
	// The two missing epochs are interpolated from 12 m/s and 16 m/s.
	st := newStream("CAR-I", 1)
	ss := []telemetry.Sample{
		st.tick(10 * time.Second).gps(100 * m), st.tick(10 * time.Second).gps(120 * m),
		st.tick(10 * time.Second).gpsNone(), st.tick(10 * time.Second).gpsNone(),
		st.tick(10 * time.Second).gps(160 * m), st.tick(10 * time.Second).gps(180 * m),
	}
	rs := runAll(newEngine(t), ss)
	requireDiag(t, rs, CodeGapInterpolated)
	if got := lastTrip(t, rs, "CAR-I"); got != 100*m+120*m+280*m+160*m+180*m {
		t.Fatalf("trip %d, want 840 m (outage interpolated as 2 x 140 m)", got)
	}
	var interp telemetry.Millimetres
	for _, e := range estimates(rs, "CAR-I") {
		interp += e.InterpolatedMM
	}
	if interp != 280*m {
		t.Fatalf("interpolated %d, want 280 m reported separately", interp)
	}

	// Beyond MaxInterpolation nothing is guessed, and that is reported.
	st = newStream("CAR-J", 2)
	ss = []telemetry.Sample{st.tick(10 * time.Second).gps(100 * m)}
	for i := 0; i < 12; i++ { // two minutes without a fix
		ss = append(ss, st.tick(10*time.Second).gpsNone())
	}
	ss = append(ss, st.tick(10*time.Second).gps(100*m))
	rs = runAll(newEngine(t), ss)
	requireDiag(t, rs, CodeGapNotInterpolated)
	if got := lastTrip(t, rs, "CAR-J"); got != 200*m {
		t.Fatalf("trip %d, want 200 m (long outage not interpolated)", got)
	}
}

func TestPulseOutageFoldsIntoNextReadingWithoutDoubleCounting(t *testing.T) {
	st := newStream("CAR-4", 4)
	var ss []telemetry.Sample
	for i := 0; i < 5; i++ {
		st.tick(10 * time.Second)
		ss = append(ss, st.gps(100*m))
		switch i {
		case 2:
			ss = append(ss, st.pulseNone())
		case 3:
			ss = append(ss, st.pulse(200*m)) // pulses kept counting during the outage
		default:
			ss = append(ss, st.pulse(100*m))
		}
	}
	rs := runAll(newEngine(t), ss)
	if got := lastTrip(t, rs, "CAR-4"); got != 500*m {
		t.Fatalf("trip %d, want 500 m (no double counting)", got)
	}

	// Pulse only: the folded reading covers the outage, so nothing is
	// interpolated on top of it.
	st = newStream("CAR-4b", 4)
	ss = []telemetry.Sample{
		st.tick(10 * time.Second).pulse(100 * m), st.tick(10 * time.Second).pulseNone(),
		st.tick(10 * time.Second).pulse(200 * m), st.tick(10 * time.Second).pulse(100 * m),
	}
	rs = runAll(newEngine(t), ss)
	if hasDiag(rs, CodeGapInterpolated) || lastTrip(t, rs, "CAR-4b") != 400*m {
		t.Fatalf("trip %d, want 400 m without interpolation", lastTrip(t, rs, "CAR-4b"))
	}
}

func TestDegradedGPSWeighsLess(t *testing.T) {
	st := newStream("CAR-5", 5)
	ss := []telemetry.Sample{st.tick(10 * time.Second).gpsDegraded(150 * m), st.pulse(100 * m)}
	rs := runAll(newEngine(t), ss)
	got := lastTrip(t, rs, "CAR-5")
	if got <= 100*m || got >= 125*m {
		t.Fatalf("trip %d: want close to the good pulse reading (100 m), clearly below the midpoint", got)
	}
	es := estimates(rs, "CAR-5")
	for _, c := range es[0].Contributions {
		if c.Source == telemetry.SourceGPS && c.WeightPPM >= 500_000 {
			t.Fatalf("degraded GPS weight %d ppm, want < 50%%", c.WeightPPM)
		}
	}
}

func TestStationaryGPSDrift(t *testing.T) {
	st := newStream("CAR-6", 6)
	var ss []telemetry.Sample
	for i := 0; i < 10; i++ {
		st.tick(10 * time.Second)
		ss = append(ss, st.gps(telemetry.Millimetres(800+300*i)), st.pulse(0))
	}
	rs := runAll(newEngine(t), ss)
	if got := lastTrip(t, rs, "CAR-6"); got != 0 {
		t.Fatalf("parked vehicle trip %d, want 0", got)
	}
	requireDiag(t, rs, CodeGPSStationaryDrift)

	// GPS alone at rest: sub-deadband jitter is not accumulated.
	st = newStream("CAR-7", 7)
	ss = nil
	for i := 0; i < 10; i++ {
		ss = append(ss, st.tick(10*time.Second).gps(1500))
	}
	rs = runAll(newEngine(t), ss)
	if got := lastTrip(t, rs, "CAR-7"); got != 0 {
		t.Fatalf("GPS-only parked trip %d, want 0", got)
	}
	requireDiag(t, rs, CodeGPSBelowDeadband)
}

func TestCANQuantisationDoesNotMeanStationary(t *testing.T) {
	st := newStream("CAR-8", 8)
	ss := []telemetry.Sample{
		st.tick(10 * time.Second).gps(10 * m), st.can(5000*km + 0),
		st.tick(10 * time.Second).gps(30 * m), st.can(5000*km + 0),
		st.tick(10 * time.Second).gps(30 * m), st.can(5000*km + 0),
	}
	rs := runAll(newEngine(t), ss)
	if got := lastTrip(t, rs, "CAR-8"); got != 70*m {
		t.Fatalf("trip %d, want 70 m (unchanged CAN within one step must not zero the GPS travel)", got)
	}
	requireDiag(t, rs, CodeCANUnchanged)
}

func TestCANIncrementNotAttributedToLatestInterval(t *testing.T) {
	// GPS says 40 m per epoch; CAN changes once by 100 m after 3 epochs.
	// The increment reflects travel over all three epochs, so no
	// correction is needed: GPS already accounts for 120 m.
	st := newStream("CAR-9", 9)
	ss := []telemetry.Sample{
		st.tick(5 * time.Second).gps(40 * m), st.can(700*km + 0),
		st.tick(5 * time.Second).gps(40 * m), st.can(700*km + 0),
		st.tick(5 * time.Second).gps(40 * m), st.can(700*km + 0),
		st.tick(5 * time.Second).gps(40 * m), st.can(700*km + 100*m),
	}
	rs := runAll(newEngine(t), ss)
	if got := lastTrip(t, rs, "CAR-9"); got != 160*m {
		t.Fatalf("trip %d, want 160 m", got)
	}
	for _, est := range estimates(rs, "CAR-9") {
		if est.CorrectionMM != 0 {
			t.Fatalf("unexpected CAN correction %d at %s", est.CorrectionMM, est.To)
		}
	}
}

func TestCANBoundsCorrectOverestimatingSource(t *testing.T) {
	// GPS overestimates by 10%; CAN keeps the trip within one step of it.
	st := newStream("CAR-10", 10)
	odo := 300 * km
	var ss []telemetry.Sample
	for i := 0; i < 100; i++ {
		st.tick(10 * time.Second)
		odo += 150 * m
		ss = append(ss, st.gps(165*m), st.can(odo/(100*m)*(100*m)))
	}
	rs := runAll(newEngine(t), ss)
	// A biased source pushes the estimate to the edge of the CAN interval.
	// The interval (c-base-R, c-base+R) contains the truth, so the worst-case
	// error is just under 2R, plus the first epoch's 15 m before the baseline.
	near(t, "trip", lastTrip(t, rs, "CAR-10"), 100*150*m, 2*100*m+15*m)
	requireDiag(t, rs, CodeCANReconciled)
}

func TestCANTemporaryOutage(t *testing.T) {
	st := newStream("CAR-11", 11)
	odo := 50 * km
	var ss []telemetry.Sample
	for i := 0; i < 20; i++ {
		st.tick(10 * time.Second)
		odo += 100 * m
		ss = append(ss, st.pulse(100*m))
		if i >= 5 && i < 12 {
			ss = append(ss, st.canNone())
		} else {
			ss = append(ss, st.can(odo))
		}
	}
	rs := runAll(newEngine(t), ss)
	near(t, "trip", lastTrip(t, rs, "CAR-11"), 2000*m, 100*m)
	requireDiag(t, rs, CodeCANUnavailable)
}

func TestPulseMiscalibrationIsLearnedFromCAN(t *testing.T) {
	// The pulse counter over-reads by 3%. Over a 60 km drive the engine
	// calibrates it against CAN; afterwards, during a CAN outage, the
	// calibrated pulse distance is accurate.
	st := newStream("CAR-12", 12)
	spec := tripSpec{epochs: constant(300, 200*m), interval: 10 * time.Second, odometer: 12_345_678_901,
		can: true, pulse: true, pulseGain: 1_030_000}
	ss, _ := spec.build(st)
	// 10 km more with CAN unavailable.
	for i := 0; i < 50; i++ {
		st.tick(10 * time.Second)
		ss = append(ss, st.pulse(206*m), st.canNone())
	}
	rs := runAll(newEngine(t), ss)
	requireDiag(t, rs, CodeCalibrationUpdated)
	es := estimates(rs, "CAR-12")
	// Within 2R from CAN, plus the uncalibrated first epoch (+6 m), plus the
	// calibrated 10 km outage (calibration uncertainty 2R/60 km ≈ 0.3%: 33 m).
	near(t, "trip", es[len(es)-1].TripMM, 300*200*m+10*km, 2*100*m+6*m+33*m)

	// Without the calibration (nominal factor) the outage alone would add 300 m.
	uncal := runAll(newEngine(t, func(c *Config) { c.CalibrationMinSpan = 10_000 * km }), ss)
	if uncalErr, calErr := lastTrip(t, uncal, "CAR-12")-70*km, es[len(es)-1].TripMM-70*km; uncalErr <= calErr {
		t.Fatalf("calibration did not help: error %d mm with, %d mm without", calErr, uncalErr)
	}
}

func TestPulseCalibrationIsConservative(t *testing.T) {
	// One absurd pulse reading must neither calibrate nor count.
	st := newStream("CAR-13", 13)
	spec := tripSpec{epochs: constant(20, 150*m), interval: 10 * time.Second, odometer: 77 * km, can: true, pulse: true}
	ss, truth := spec.build(st)
	st.tick(10 * time.Second)
	ss = append(ss, st.pulse(5*km), st.can((77*km+truth+150*m)/(100*m)*(100*m)))
	rs := runAll(newEngine(t), ss)
	requireDiag(t, rs, CodeMeasurementImplausible)
	if hasDiag(rs, CodeCalibrationUpdated) {
		t.Fatal("calibration changed from a single spurious reading")
	}
	near(t, "trip", lastTrip(t, rs, "CAR-13"), truth, 150*m)
}

func TestSpuriousPulsesWithinLimitsAreOutweighedAndBounded(t *testing.T) {
	// A few extra pulses (plausible speed) disagree with GPS; fusion keeps
	// the estimate between them and CAN keeps the trip bounded.
	st := newStream("CAR-14", 14)
	odo := 10 * km
	var ss []telemetry.Sample
	for i := 0; i < 30; i++ {
		st.tick(10 * time.Second)
		odo += 100 * m
		p := 100 * m
		if i%10 == 5 {
			p = 400 * m // spurious burst
		}
		ss = append(ss, st.gps(100*m), st.pulseDegraded(p), st.can(odo))
	}
	rs := runAll(newEngine(t), ss)
	requireDiag(t, rs, CodeSensorDisagreement)
	near(t, "trip", lastTrip(t, rs, "CAR-14"), 3000*m, 100*m)
}

func TestConflictingSensorsAreReported(t *testing.T) {
	st := newStream("CAR-15", 15)
	ss := []telemetry.Sample{st.tick(10 * time.Second).gps(200 * m), st.pulse(20 * m)}
	rs := runAll(newEngine(t), ss)
	requireDiag(t, rs, CodeSensorDisagreement)
	es := estimates(rs, "CAR-15")
	if es[0].Confidence != ConfidenceLow {
		t.Fatalf("confidence %s, want low on unresolved disagreement", es[0].Confidence)
	}
}

func TestBrokenPulseBecomesSuspect(t *testing.T) {
	// The pulse counter stops counting (reports 0 while moving). Against CAN
	// it is marked suspect and excluded; GPS carries the estimate.
	st := newStream("CAR-16", 16)
	odo := 900 * km
	var ss []telemetry.Sample
	for i := 0; i < 60; i++ {
		st.tick(10 * time.Second)
		odo += 120 * m
		ss = append(ss, st.gps(120*m), st.pulse(0), st.can(odo/(100*m)*(100*m)))
	}
	rs := runAll(newEngine(t), ss)
	requireDiag(t, rs, CodeSourceSuspect)
	requireDiag(t, rs, CodeSourceExcluded)
	near(t, "trip", lastTrip(t, rs, "CAR-16"), 60*120*m, 2*100*m)
}

func TestCANAnomalies(t *testing.T) {
	t.Run("decrease ignored", func(t *testing.T) {
		st := newStream("C1", 1)
		ss := []telemetry.Sample{st.tick(10 * time.Second).can(100 * km), st.tick(10 * time.Second).can(99 * km), st.tick(10 * time.Second).can(100*km + 100*m)}
		rs := runAll(newEngine(t), ss)
		requireDiag(t, rs, CodeCANDecreased)
		if got := lastTrip(t, rs, "C1"); got != 100*m {
			t.Fatalf("trip %d", got)
		}
	})
	t.Run("single jump rejected", func(t *testing.T) {
		st := newStream("C2", 2)
		ss := []telemetry.Sample{st.tick(10 * time.Second).can(100 * km), st.tick(10 * time.Second).can(150 * km), st.tick(10 * time.Second).can(100*km + 200*m)}
		rs := runAll(newEngine(t), ss)
		requireDiag(t, rs, CodeCANJumpRejected)
		if got := lastTrip(t, rs, "C2"); got != 200*m {
			t.Fatalf("trip %d, want 200 m", got)
		}
	})
	t.Run("confirmed jump rebaselines without credit", func(t *testing.T) {
		st := newStream("C3", 3)
		ss := []telemetry.Sample{st.tick(10 * time.Second).can(100 * km), st.tick(10 * time.Second).can(150 * km), st.tick(10 * time.Second).can(150*km + 200*m)}
		rs := runAll(newEngine(t), ss)
		requireDiag(t, rs, CodeCANRebaselined)
		if got := lastTrip(t, rs, "C3"); got != 0 {
			t.Fatalf("trip %d, want 0 (jump not credited, no travel after rebaseline)", got)
		}
	})
	t.Run("frozen odometer marked stale", func(t *testing.T) {
		st := newStream("C4", 4)
		var ss []telemetry.Sample
		for i := 0; i < 10; i++ {
			ss = append(ss, st.tick(10*time.Second).gps(100*m), st.pulse(100*m), st.can(400*km))
		}
		rs := runAll(newEngine(t), ss)
		requireDiag(t, rs, CodeCANStale)
		if got := lastTrip(t, rs, "C4"); got != 1000*m {
			t.Fatalf("trip %d, want 1000 m (frozen CAN must not cap the estimate)", got)
		}
	})
}
