package fusion

import (
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

const (
	m  = telemetry.Metre
	km = telemetry.Kilometre
)

var t0 = time.Date(2026, time.May, 4, 6, 0, 0, 0, time.UTC)

// stream builds hand-made telemetry for one device session.
type stream struct {
	dev  telemetry.DeviceID
	boot telemetry.BootID
	seq  telemetry.Sequence
	now  time.Time
}

func newStream(dev telemetry.DeviceID, boot telemetry.BootID) *stream {
	return &stream{dev: dev, boot: boot, now: t0}
}

// tick advances the device clock.
func (st *stream) tick(d time.Duration) *stream { st.now = st.now.Add(d); return st }

func (st *stream) sample(src telemetry.Source, q telemetry.Quality, mm telemetry.Millimetres, present bool) telemetry.Sample {
	st.seq++
	s := telemetry.Sample{
		DeviceID: st.dev, EventID: telemetry.EventID(uint64(st.boot)<<32 | uint64(st.seq)),
		BootID: st.boot, Sequence: st.seq, DeviceTime: st.now, Source: src, Quality: q,
	}
	if present {
		s.Distance = telemetry.Some(telemetry.Distance{Kind: src.DistanceKind(), Value: mm})
	}
	return s
}

func (st *stream) gps(mm telemetry.Millimetres) telemetry.Sample {
	return st.sample(telemetry.SourceGPS, telemetry.QualityGood, mm, true)
}
func (st *stream) gpsDegraded(mm telemetry.Millimetres) telemetry.Sample {
	return st.sample(telemetry.SourceGPS, telemetry.QualityDegraded, mm, true)
}
func (st *stream) gpsNone() telemetry.Sample {
	return st.sample(telemetry.SourceGPS, telemetry.QualityUnavailable, 0, false)
}
func (st *stream) pulse(mm telemetry.Millimetres) telemetry.Sample {
	return st.sample(telemetry.SourcePulse, telemetry.QualityGood, mm, true)
}
func (st *stream) pulseDegraded(mm telemetry.Millimetres) telemetry.Sample {
	return st.sample(telemetry.SourcePulse, telemetry.QualityDegraded, mm, true)
}
func (st *stream) pulseNone() telemetry.Sample {
	return st.sample(telemetry.SourcePulse, telemetry.QualityUnavailable, 0, false)
}
func (st *stream) can(mm telemetry.Millimetres) telemetry.Sample {
	return st.sample(telemetry.SourceCAN, telemetry.QualityGood, mm, true)
}
func (st *stream) canNone() telemetry.Sample {
	return st.sample(telemetry.SourceCAN, telemetry.QualityUnavailable, 0, false)
}

func newEngine(t *testing.T, mutate ...func(*Config)) *Engine {
	t.Helper()
	cfg := DefaultConfig()
	for _, f := range mutate {
		f(&cfg)
	}
	e, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// runAll processes the samples, flushes, and returns every report.
func runAll(e *Engine, samples []telemetry.Sample) []Report {
	var out []Report
	for _, s := range samples {
		out = append(out, e.Process(s)...)
	}
	return append(out, e.Flush()...)
}

func estimates(rs []Report, dev telemetry.DeviceID) []Estimate {
	var out []Estimate
	for _, r := range rs {
		if r.Type == ReportEstimate && r.Estimate.DeviceID == dev {
			out = append(out, *r.Estimate)
		}
	}
	return out
}

func lastTrip(t *testing.T, rs []Report, dev telemetry.DeviceID) telemetry.Millimetres {
	t.Helper()
	es := estimates(rs, dev)
	if len(es) == 0 {
		t.Fatalf("no estimates for %s", dev)
	}
	return es[len(es)-1].TripMM
}

func events(rs []Report) []EventReport {
	var out []EventReport
	for _, r := range rs {
		if r.Type == ReportEvent {
			out = append(out, *r.Event)
		}
	}
	return out
}

// finalOutcomes maps each (boot, seq) to its last reported outcome.
func finalOutcomes(rs []Report) map[[2]uint64]EventReport {
	m := map[[2]uint64]EventReport{}
	for _, ev := range events(rs) {
		m[[2]uint64{uint64(ev.BootID), uint64(ev.Sequence)}] = ev
	}
	return m
}

func hasDiag(rs []Report, code Code) bool {
	for _, r := range rs {
		var ds []Diagnostic
		if r.Event != nil {
			ds = r.Event.Diagnostics
		} else {
			ds = r.Estimate.Diagnostics
		}
		for _, d := range ds {
			if d.Code == code {
				return true
			}
		}
	}
	return false
}

func requireDiag(t *testing.T, rs []Report, code Code) {
	t.Helper()
	if !hasDiag(rs, code) {
		t.Fatalf("expected diagnostic %q", code)
	}
}

func near(t *testing.T, what string, got, want, tol telemetry.Millimetres) {
	t.Helper()
	if d := got - want; d > tol || d < -tol {
		t.Fatalf("%s = %d mm, want %d ± %d mm (off by %d)", what, got, want, tol, d)
	}
}

// trip simulates a vehicle with known per-epoch true distances and returns
// samples for the chosen sources. CAN is the floor-quantised true odometer.
type tripSpec struct {
	epochs    []telemetry.Millimetres // true distance per epoch
	interval  time.Duration
	odometer  telemetry.Millimetres // true odometer at start
	gps       bool
	can       bool
	pulse     bool
	pulseGain int64 // pulse reports true*gain/1e6 (0 = 1e6)
}

func (ts tripSpec) build(st *stream) (samples []telemetry.Sample, truth telemetry.Millimetres) {
	odo := ts.odometer
	gain := ts.pulseGain
	if gain == 0 {
		gain = ppm
	}
	for _, d := range ts.epochs {
		st.tick(ts.interval)
		odo += d
		truth += d
		if ts.gps {
			samples = append(samples, st.gps(d))
		}
		if ts.can {
			samples = append(samples, st.can(odo/(100*m)*(100*m)))
		}
		if ts.pulse {
			samples = append(samples, st.pulse(telemetry.Millimetres(mulDiv(int64(d), gain, ppm))))
		}
	}
	return samples, truth
}

func constant(n int, d telemetry.Millimetres) []telemetry.Millimetres {
	out := make([]telemetry.Millimetres, n)
	for i := range out {
		out[i] = d
	}
	return out
}
