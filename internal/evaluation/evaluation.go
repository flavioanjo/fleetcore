// Package evaluation measures the fusion engine against the simulator's
// ground truth and against simple baseline estimators.
//
// This package is test and tooling code: it is the only non-test package
// that may use simulator.RunWithGroundTruth. Production code must not
// import it.
package evaluation

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"time"

	"github.com/flavioanjo/fleetcore/internal/fusion"
	"github.com/flavioanjo/fleetcore/internal/simulator"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// Perturbation describes delivery problems applied to the simulated stream.
// Probabilities are per event, in parts per million. Zero means none.
type Perturbation struct {
	Seed         uint64
	SwapPPM      int64         // swap with the next event (reordering within the window)
	DuplicatePPM int64         // deliver a second copy a few events later
	DropPPM      int64         // never deliver (a sequence gap)
	LatePPM      int64         // deliver after LateBy of device time (beyond the window)
	LateBy       time.Duration // defaults to 2 minutes
}

// DefaultPerturbation is a demanding but realistic delivery profile: 2%
// swapped, 1% duplicated, 0.5% dropped and 0.2% delivered two minutes late.
func DefaultPerturbation(seed uint64) Perturbation {
	return Perturbation{Seed: seed, SwapPPM: 20_000, DuplicatePPM: 10_000, DropPPM: 5_000, LatePPM: 2_000}
}

// Scenario is one evaluation run.
type Scenario struct {
	Name    string
	Sim     simulator.Config
	Perturb Perturbation
}

// Errors summarises signed errors (estimate minus truth) in millimetres.
type Errors struct {
	MeanAbs telemetry.Millimetres `json:"mean_abs_mm"`
	MaxAbs  telemetry.Millimetres `json:"max_abs_mm"`
	// MeanRelPPM is the mean of |error| / true distance, in ppm.
	MeanRelPPM int64 `json:"mean_rel_ppm"`
	MaxRelPPM  int64 `json:"max_rel_ppm"`
}

// Result of one scenario.
type Result struct {
	Scenario  string                `json:"scenario"`
	Sources   string                `json:"sources"`
	Devices   int                   `json:"devices"`
	MeanTrue  telemetry.Millimetres `json:"mean_true_mm"`
	Fused     Errors                `json:"fused"`     // final trip error per device
	Naive     Errors                `json:"naive"`     // unweighted-average baseline
	CANDiff   *Errors               `json:"can_diff"`  // last minus first CAN reading, when CAN is enabled
	TrackMax  telemetry.Millimetres `json:"track_max"` // largest |trip - truth| at any estimate
	Coverage  int64                 `json:"uncertainty_coverage_ppm"`
	EpochGood telemetry.Millimetres `json:"epoch_err_good_mm"`     // mean |measured delta - true delta|, epochs with only good inputs
	EpochDegr telemetry.Millimetres `json:"epoch_err_degraded_mm"` // same, epochs with a degraded input
	Outcomes  map[fusion.Outcome]int
	Ambiguous int // estimates with insufficient evidence or unresolved disagreement
}

type key struct {
	dev telemetry.DeviceID
	at  int64 // device time, Unix ms
}

// Run executes one scenario with the given engine configuration.
func Run(sc Scenario, cfg fusion.Config) (Result, error) {
	var samples []telemetry.Sample
	truthAt := map[key]telemetry.Millimetres{}    // cumulative true distance at device time
	truthDelta := map[key]telemetry.Millimetres{} // true distance in the round ending then
	total := map[telemetry.DeviceID]telemetry.Millimetres{}
	err := simulator.RunWithGroundTruth(sc.Sim,
		func(s telemetry.Sample) error { samples = append(samples, s); return nil },
		func(tr simulator.Truth) {
			total[tr.DeviceID] += tr.DistanceMM
			k := key{tr.DeviceID, tr.DeviceTime.UnixMilli()}
			truthAt[k], truthDelta[k] = total[tr.DeviceID], tr.DistanceMM
		})
	if err != nil {
		return Result{}, err
	}
	degraded := map[key]bool{}
	for _, s := range samples {
		if s.Quality == telemetry.QualityDegraded {
			degraded[key{s.DeviceID, s.DeviceTime.UnixMilli()}] = true
		}
	}
	delivered := Perturb(samples, sc.Perturb)

	eng, err := fusion.New(cfg)
	if err != nil {
		return Result{}, err
	}
	res := Result{Scenario: sc.Name, Sources: sourceNames(sc.Sim.Sources), Devices: len(total), Outcomes: map[fusion.Outcome]int{}}
	// Every estimator is compared with the truth from the first instant it
	// observed: the fused trip from its first estimate, the baselines from
	// their first reading. (A CAN-only estimate cannot know the travel
	// before the first odometer reading, and should not be blamed for it.)
	type anchor struct {
		trip, truth telemetry.Millimetres
	}
	first := map[telemetry.DeviceID]anchor{}
	final := map[telemetry.DeviceID]telemetry.Millimetres{}
	var covered, estimatesN int64
	var goodSum, goodN, degrSum, degrN int64
	handle := func(rs []fusion.Report) {
		for _, r := range rs {
			if r.Type == fusion.ReportEvent {
				res.Outcomes[r.Event.Outcome]++
				continue
			}
			est := r.Estimate
			k := key{est.DeviceID, est.To.UnixMilli()}
			truth, ok := truthAt[k]
			if !ok {
				continue
			}
			a, seen := first[est.DeviceID]
			if !seen {
				// The first estimate's own increment is compared too: the
				// anchor is the state before it.
				a = anchor{est.TripMM - est.DeltaMM, truth - truthDelta[k]}
				if len(est.Contributions) == 0 { // e.g. a CAN baseline: nothing measured yet
					a = anchor{est.TripMM, truth}
				}
				first[est.DeviceID] = a
			}
			final[est.DeviceID] = est.TripMM - a.trip
			errMM := absMM((est.TripMM - a.trip) - (truth - a.truth))
			res.TrackMax = max(res.TrackMax, errMM)
			estimatesN++
			if errMM <= est.UncertaintyMM {
				covered++
			}
			for _, d := range est.Diagnostics {
				if d.Code == fusion.CodeInsufficientEvidence || d.Code == fusion.CodeSensorDisagreement {
					res.Ambiguous++
					break
				}
			}
			if len(est.Contributions) > 0 {
				e := int64(absMM(est.DeltaMM - est.CorrectionMM - truthDelta[k]))
				if degraded[k] {
					degrSum, degrN = degrSum+e, degrN+1
				} else {
					goodSum, goodN = goodSum+e, goodN+1
				}
			}
		}
	}
	for _, s := range delivered {
		handle(eng.Process(s))
	}
	handle(eng.Flush())

	naive, naiveStart := naiveEstimate(delivered)
	canDiff, canStart, hasCAN := canDifference(delivered)
	since := func(dev telemetry.DeviceID, start time.Time) telemetry.Millimetres {
		return total[dev] - truthAt[key{dev, start.UnixMilli()}]
	}
	var fusedE, naiveE, canE []errSample
	var trueSum telemetry.Millimetres
	for _, dev := range sortedDevices(total) {
		tr := total[dev] - first[dev].truth
		trueSum += tr
		fusedE = append(fusedE, errSample{final[dev] - tr, tr})
		ntr := since(dev, naiveStart[dev])
		naiveE = append(naiveE, errSample{naive[dev] - ntr, ntr})
		if hasCAN {
			ctr := since(dev, canStart[dev])
			canE = append(canE, errSample{canDiff[dev] - ctr, ctr})
		}
	}
	res.MeanTrue = trueSum / telemetry.Millimetres(max(1, len(total)))
	res.Fused, res.Naive = summarise(fusedE), summarise(naiveE)
	if hasCAN {
		e := summarise(canE)
		res.CANDiff = &e
	}
	if estimatesN > 0 {
		res.Coverage = covered * 1_000_000 / estimatesN
	}
	if goodN > 0 {
		res.EpochGood = telemetry.Millimetres(goodSum / goodN)
	}
	if degrN > 0 {
		res.EpochDegr = telemetry.Millimetres(degrSum / degrN)
	}
	return res, nil
}

// naiveEstimate is the baseline: per (device, device time), the unweighted
// mean of whatever GPS and pulse deltas are present (nominal calibration,
// quality ignored), summed in arrival order with no duplicate, ordering or
// session handling. Devices without delta sources use the CAN difference.
// It also returns, per device, the instant from which it measures (the
// device time just before its first epoch, i.e. the truth anchor).
func naiveEstimate(ss []telemetry.Sample) (map[telemetry.DeviceID]telemetry.Millimetres, map[telemetry.DeviceID]time.Time) {
	type acc struct{ sum, n telemetry.Millimetres }
	epochs := map[key]*acc{}
	var order []key
	hasDelta := map[telemetry.DeviceID]bool{}
	start := map[telemetry.DeviceID]time.Time{}
	for _, s := range ss {
		d, ok := s.Distance.Get()
		if !ok || s.Source == telemetry.SourceCAN {
			continue
		}
		if !hasDelta[s.DeviceID] {
			start[s.DeviceID] = time.Time{} // measures from the simulation start
		}
		hasDelta[s.DeviceID] = true
		k := key{s.DeviceID, s.DeviceTime.UnixMilli()}
		a := epochs[k]
		if a == nil {
			a = &acc{}
			epochs[k] = a
			order = append(order, k)
		}
		a.sum += d.Value
		a.n++
	}
	out := map[telemetry.DeviceID]telemetry.Millimetres{}
	for _, k := range order {
		out[k.dev] += epochs[k].sum / epochs[k].n
	}
	canDiff, canStart, _ := canDifference(ss)
	for dev, v := range canDiff {
		if !hasDelta[dev] {
			out[dev], start[dev] = v, canStart[dev]
		}
	}
	return out, start
}

// canDifference is the CAN-only baseline: highest minus first CAN reading,
// measured from the device time of that first reading.
func canDifference(ss []telemetry.Sample) (map[telemetry.DeviceID]telemetry.Millimetres, map[telemetry.DeviceID]time.Time, bool) {
	first := map[telemetry.DeviceID]telemetry.Millimetres{}
	last := map[telemetry.DeviceID]telemetry.Millimetres{}
	start := map[telemetry.DeviceID]time.Time{}
	seen := false
	for _, s := range ss {
		d, ok := s.Distance.Get()
		if !ok || s.Source != telemetry.SourceCAN {
			continue
		}
		seen = true
		if _, ok := first[s.DeviceID]; !ok {
			first[s.DeviceID], start[s.DeviceID] = d.Value, s.DeviceTime
		}
		last[s.DeviceID] = max(last[s.DeviceID], d.Value)
	}
	out := map[telemetry.DeviceID]telemetry.Millimetres{}
	for dev := range first {
		out[dev] = last[dev] - first[dev]
	}
	return out, start, seen
}

// Perturb applies p to a copy of ss. It is deterministic for a given Seed.
func Perturb(ss []telemetry.Sample, p Perturbation) []telemetry.Sample {
	if p.LateBy == 0 {
		p.LateBy = 2 * time.Minute
	}
	r := rand.New(rand.NewPCG(p.Seed, 0x9e3779b97f4a7c15))
	roll := func(ppm int64) bool { return ppm > 0 && r.Int64N(1_000_000) < ppm }

	type item struct {
		s     telemetry.Sample
		after time.Time // deliver once the stream reaches this device time
	}
	out := make([]telemetry.Sample, 0, len(ss))
	var held []item
	for i := 0; i < len(ss); i++ {
		s := ss[i]
		// Release held (late or duplicate) events whose time has come.
		keep := held[:0]
		for _, h := range held {
			if !s.DeviceTime.Before(h.after) {
				out = append(out, h.s)
			} else {
				keep = append(keep, h)
			}
		}
		held = keep
		switch {
		case roll(p.DropPPM):
			continue
		case roll(p.LatePPM):
			held = append(held, item{s, s.DeviceTime.Add(p.LateBy)})
			continue
		case roll(p.SwapPPM) && i+1 < len(ss):
			out = append(out, ss[i+1], s)
			i++
			continue
		}
		out = append(out, s)
		if roll(p.DuplicatePPM) {
			held = append(held, item{s, s.DeviceTime.Add(time.Duration(1+r.IntN(20)) * time.Second)})
		}
	}
	for _, h := range held {
		out = append(out, h.s)
	}
	return out
}

type errSample struct{ err, truth telemetry.Millimetres }

func summarise(es []errSample) Errors {
	var e Errors
	if len(es) == 0 {
		return e
	}
	var sumAbs, sumRel int64
	for _, x := range es {
		a := absMM(x.err)
		sumAbs += int64(a)
		e.MaxAbs = max(e.MaxAbs, a)
		if x.truth > 0 {
			rel := int64(a) * 1_000_000 / int64(x.truth)
			sumRel += rel
			e.MaxRelPPM = max(e.MaxRelPPM, rel)
		}
	}
	e.MeanAbs = telemetry.Millimetres(sumAbs / int64(len(es)))
	e.MeanRelPPM = sumRel / int64(len(es))
	return e
}

func absMM(v telemetry.Millimetres) telemetry.Millimetres {
	if v < 0 {
		return -v
	}
	return v
}

func sortedDevices(m map[telemetry.DeviceID]telemetry.Millimetres) []telemetry.DeviceID {
	out := make([]telemetry.DeviceID, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func sourceNames(src []telemetry.Source) string {
	s := ""
	for i, x := range src {
		if i > 0 {
			s += "+"
		}
		s += x.String()
	}
	return s
}

// Combos lists every non-empty combination of the three sources.
func Combos() [][]telemetry.Source {
	g, c, p := telemetry.SourceGPS, telemetry.SourceCAN, telemetry.SourcePulse
	return [][]telemetry.Source{{g}, {c}, {p}, {g, c}, {g, p}, {c, p}, {g, c, p}}
}

// FormatTable renders results as a Markdown table (distances in metres).
func FormatTable(rs []Result) string {
	out := "| scenario | sources | devices | mean true km | fused mean / max err m | fused mean rel % | naive mean / max err m | naive mean rel % | CAN-diff mean err m | track max m | U coverage % | epoch err good / degraded m | ambiguous | rejected | reconcile | dup |\n" +
		"|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n"
	for _, r := range rs {
		can := "—"
		if r.CANDiff != nil {
			can = fmt.Sprintf("%.1f", mm2m(r.CANDiff.MeanAbs))
		}
		out += fmt.Sprintf("| %s | %s | %d | %.1f | %.1f / %.1f | %.3f | %.1f / %.1f | %.3f | %s | %.1f | %.1f | %.2f / %.2f | %d | %d | %d | %d |\n",
			r.Scenario, r.Sources, r.Devices, mm2m(r.MeanTrue)/1000,
			mm2m(r.Fused.MeanAbs), mm2m(r.Fused.MaxAbs), float64(r.Fused.MeanRelPPM)/1e4,
			mm2m(r.Naive.MeanAbs), mm2m(r.Naive.MaxAbs), float64(r.Naive.MeanRelPPM)/1e4,
			can, mm2m(r.TrackMax), float64(r.Coverage)/1e4, mm2m(r.EpochGood), mm2m(r.EpochDegr),
			r.Ambiguous, r.Outcomes[fusion.OutcomeRejected], r.Outcomes[fusion.OutcomeRequiresReconciliation], r.Outcomes[fusion.OutcomeDuplicate])
	}
	return out
}

func mm2m(v telemetry.Millimetres) float64 { return float64(v) / 1000 }
