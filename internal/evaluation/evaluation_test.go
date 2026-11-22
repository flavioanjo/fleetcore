package evaluation

import (
	"reflect"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/fusion"
	"github.com/flavioanjo/fleetcore/internal/simulator"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// Acceptance thresholds. Each is derived from the sensor model or from a
// property of the algorithm, not fitted to observed results; see
// docs/fusion.md ("Accuracy evaluation") for the reasoning and the observed
// values, which are well inside these limits.
const (
	// With CAN, the trip is kept inside the odometer interval
	// (c-base-R, c-base+R), which contains the truth: |error| < 2R.
	maxErrWithCAN = 2 * 100 * telemetry.Metre
	// GPS random errors average out over hundreds of km; what remains is
	// interpolation over outages and the stationary deadband: 0.5% mean, 1% worst.
	gpsOnlyMeanRelPPM, gpsOnlyMaxRelPPM = 5_000, 10_000
	// A pulse counter alone cannot be calibrated: the error is the
	// simulator's per-vehicle calibration error (up to 3%) plus quantisation.
	pulseOnlyMaxRelPPM = 35_000
	// GPS + pulse: pulse calibrated against GPS; GPS-limited, as GPS only.
	gpsPulseMeanRelPPM = 5_000
	// Uncertainty indicator: at least 90% of estimates within it.
	minCoveragePPM = 900_000
)

func scenarios(t *testing.T, combo []telemetry.Source, seeds []uint64, perturb bool) []Result {
	t.Helper()
	var out []Result
	for _, seed := range seeds {
		cfg := simulator.DefaultConfig()
		cfg.Seed, cfg.Vehicles, cfg.Samples, cfg.Interval, cfg.Sources = seed, 5, 2000, 10*time.Second, combo
		sc := Scenario{Name: "eval", Sim: cfg}
		if perturb {
			sc.Perturb = DefaultPerturbation(seed)
		}
		r, err := Run(sc, fusion.DefaultConfig())
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	return out
}

func TestAccuracyBySourceCombination(t *testing.T) {
	seeds := []uint64{1, 2, 3, 4, 5}
	var table []Result
	for _, combo := range Combos() {
		hasCAN := false
		for _, s := range combo {
			hasCAN = hasCAN || s == telemetry.SourceCAN
		}
		for _, perturb := range []bool{false, true} {
			for _, r := range scenarios(t, combo, seeds, perturb) {
				if perturb {
					r.Scenario += " perturbed"
				}
				table = append(table, r)
				name := r.Sources + " " + r.Scenario
				if hasCAN && r.Fused.MaxAbs >= maxErrWithCAN {
					t.Errorf("%s: max error %d mm, limit %d", name, r.Fused.MaxAbs, maxErrWithCAN)
				}
				if r.Coverage < minCoveragePPM {
					t.Errorf("%s: uncertainty coverage %d ppm, want >= %d", name, r.Coverage, minCoveragePPM)
				}
				switch r.Sources {
				case "gps":
					if r.Fused.MeanRelPPM > gpsOnlyMeanRelPPM || r.Fused.MaxRelPPM > gpsOnlyMaxRelPPM {
						t.Errorf("%s: relative error mean %d / max %d ppm", name, r.Fused.MeanRelPPM, r.Fused.MaxRelPPM)
					}
				case "pulse":
					if r.Fused.MaxRelPPM > pulseOnlyMaxRelPPM {
						t.Errorf("%s: relative error max %d ppm", name, r.Fused.MaxRelPPM)
					}
				case "gps+pulse":
					if r.Fused.MeanRelPPM > gpsPulseMeanRelPPM {
						t.Errorf("%s: relative error mean %d ppm", name, r.Fused.MeanRelPPM)
					}
				}
				// Fusion must beat the unweighted baseline whenever it has
				// two delta sources or a delta source to combine with CAN.
				if len(combo) >= 2 && r.Fused.MeanAbs >= r.Naive.MeanAbs {
					t.Errorf("%s: fused mean error %d mm not better than naive %d mm", name, r.Fused.MeanAbs, r.Naive.MeanAbs)
				}
				if perturb && r.Outcomes[fusion.OutcomeRejected] != 0 {
					t.Errorf("%s: %d events rejected; delivery problems alone must not cause rejections", name, r.Outcomes[fusion.OutcomeRejected])
				}
			}
		}
	}
	t.Logf("\n%s", FormatTable(table))
}

func TestLongTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("long simulation")
	}
	for _, combo := range [][]telemetry.Source{{telemetry.SourceGPS}, {telemetry.SourceGPS, telemetry.SourceCAN, telemetry.SourcePulse}} {
		cfg := simulator.DefaultConfig()
		cfg.Seed, cfg.Vehicles, cfg.Samples, cfg.Interval, cfg.Sources = 99, 2, 8640, 10*time.Second, combo // 24 h
		r, err := Run(Scenario{Name: "24h", Sim: cfg, Perturb: DefaultPerturbation(99)}, fusion.DefaultConfig())
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%s: true %d km, fused max error %d m (%d ppm), naive max error %d m",
			r.Sources, r.MeanTrue/telemetry.Kilometre, r.Fused.MaxAbs/telemetry.Metre, r.Fused.MaxRelPPM, r.Naive.MaxAbs/telemetry.Metre)
		if len(combo) == 3 && r.Fused.MaxAbs >= maxErrWithCAN {
			t.Errorf("24 h all sources: max error %d mm", r.Fused.MaxAbs)
		}
		if len(combo) == 1 && r.Fused.MaxRelPPM > gpsOnlyMaxRelPPM {
			t.Errorf("24 h GPS only: max relative error %d ppm", r.Fused.MaxRelPPM)
		}
	}
}

func TestEvaluationIsDeterministic(t *testing.T) {
	cfg := simulator.DefaultConfig()
	cfg.Vehicles, cfg.Samples = 3, 300
	sc := Scenario{Name: "d", Sim: cfg, Perturb: DefaultPerturbation(5)}
	a, err := Run(sc, fusion.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	b, _ := Run(sc, fusion.DefaultConfig())
	if !reflect.DeepEqual(a, b) {
		t.Fatal("evaluation is not reproducible")
	}
}

func TestPerturbIsDeterministicAndConservesEvents(t *testing.T) {
	cfg := simulator.DefaultConfig()
	cfg.Samples = 500
	var ss []telemetry.Sample
	if err := simulator.Run(cfg, func(s telemetry.Sample) error { ss = append(ss, s); return nil }); err != nil {
		t.Fatal(err)
	}
	p := Perturbation{Seed: 1, SwapPPM: 50_000, DuplicatePPM: 50_000, LatePPM: 50_000}
	a, b := Perturb(ss, p), Perturb(ss, p)
	if !reflect.DeepEqual(a, b) {
		t.Fatal("perturbation not deterministic")
	}
	seen := map[telemetry.EventID]int{}
	for _, s := range a {
		seen[s.EventID]++
	}
	for _, s := range ss {
		if seen[s.EventID] == 0 {
			t.Fatalf("event %d lost without DropPPM", s.EventID)
		}
	}
	if len(a) <= len(ss) {
		t.Fatal("no duplicates were added")
	}
}
