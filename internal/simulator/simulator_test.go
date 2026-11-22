package simulator

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

var update = flag.Bool("update", false, "rewrite golden files")

func collect(t *testing.T, cfg Config) []telemetry.Sample {
	t.Helper()
	var out []telemetry.Sample
	if err := Run(cfg, func(s telemetry.Sample) error { out = append(out, s); return nil }); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return out
}

func byVehicle(samples []telemetry.Sample) map[telemetry.DeviceID][]telemetry.Sample {
	m := map[telemetry.DeviceID][]telemetry.Sample{}
	for _, s := range samples {
		m[s.DeviceID] = append(m[s.DeviceID], s)
	}
	return m
}

func TestSameSeedIsReproducible(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Samples = 200
	a, b := collect(t, cfg), collect(t, cfg)
	if !reflect.DeepEqual(a, b) {
		t.Fatal("two runs with the same config differ")
	}
}

func TestDifferentSeedsDiffer(t *testing.T) {
	cfg := DefaultConfig()
	a := collect(t, cfg)
	cfg.Seed++
	if reflect.DeepEqual(a, collect(t, cfg)) {
		t.Fatal("different seeds produced identical output")
	}
}

func TestMultipleVehicles(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Vehicles, cfg.Samples = 25, 40
	samples := collect(t, cfg)

	if want := cfg.Vehicles * cfg.Samples * len(cfg.Sources); len(samples) != want {
		t.Fatalf("got %d samples, want %d", len(samples), want)
	}
	groups := byVehicle(samples)
	if len(groups) != cfg.Vehicles {
		t.Fatalf("got %d distinct devices, want %d", len(groups), cfg.Vehicles)
	}
	events := map[telemetry.EventID]bool{}
	boots := map[telemetry.BootID]telemetry.DeviceID{}
	for id, vs := range groups {
		if len(vs) != cfg.Samples*len(cfg.Sources) {
			t.Errorf("%s: %d samples", id, len(vs))
		}
		for i, s := range vs {
			if s.BootID != vs[0].BootID {
				t.Fatalf("%s: boot id changed mid-run", id)
			}
			if i > 0 {
				if s.Sequence != vs[i-1].Sequence+1 {
					t.Fatalf("%s: sequence %d follows %d", id, s.Sequence, vs[i-1].Sequence)
				}
				if s.DeviceTime.Before(vs[i-1].DeviceTime) {
					t.Fatalf("%s: device time went backwards", id)
				}
			}
			if events[s.EventID] {
				t.Fatalf("duplicate event id %d", s.EventID)
			}
			events[s.EventID] = true
		}
		if other, dup := boots[vs[0].BootID]; dup {
			t.Errorf("boot id %d shared by %s and %s", vs[0].BootID, id, other)
		}
		boots[vs[0].BootID] = id
	}
}

func TestEmittedSamplesAreValidAndOrdered(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Vehicles, cfg.Samples = 4, 50
	samples := collect(t, cfg)
	n := len(cfg.Sources)
	for i, s := range samples {
		if err := s.Validate(); err != nil {
			t.Fatalf("sample %d invalid: %v", i, err)
		}
		// Order is round-major, then vehicle, then source.
		rest := i % (cfg.Vehicles * n)
		vehicle, src := rest/n, rest%n
		if s.Source != cfg.Sources[src] {
			t.Fatalf("sample %d: source %s, want %s", i, s.Source, cfg.Sources[src])
		}
		if want := telemetry.DeviceID(fmt.Sprintf("%s%05d", DeviceIDPrefix, vehicle+1)); s.DeviceID != want {
			t.Fatalf("sample %d: device %s, want %s", i, s.DeviceID, want)
		}
	}
}

// TestVehiclesAreIndependent guards against vehicles sharing a random stream.
// (Subtler early-output correlation between neighbouring PCG seeds, seen
// during development, is prevented by the splitmix64 mixing in newRand.)
func TestVehiclesAreIndependent(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Vehicles, cfg.Samples, cfg.Interval = 50, 10, 5*time.Second
	cfg.Sources = []telemetry.Source{telemetry.SourceGPS}
	// Single readings may legitimately coincide (vehicles starting from rest
	// accelerate identically), but whole sequences must not.
	traces := map[string]bool{}
	for _, vs := range byVehicle(collect(t, cfg)) {
		var key []byte
		for _, s := range vs {
			key, _ = json.Marshal(append([]any{string(key)}, s.Distance, s.Quality))
		}
		traces[string(key)] = true
	}
	if len(traces) != cfg.Vehicles {
		t.Fatalf("%d vehicles produced only %d distinct GPS traces", cfg.Vehicles, len(traces))
	}
}

func TestGroundTruthDoesNotChangeSamples(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Vehicles, cfg.Samples = 4, 300
	plain := collect(t, cfg)
	var withTruth []telemetry.Sample
	var truths []Truth
	err := RunWithGroundTruth(cfg,
		func(s telemetry.Sample) error { withTruth = append(withTruth, s); return nil },
		func(tr Truth) { truths = append(truths, tr) })
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(plain, withTruth) {
		t.Fatal("observing ground truth changed the emitted samples")
	}
	if len(truths) != cfg.Vehicles*cfg.Samples {
		t.Fatalf("got %d truth records, want %d", len(truths), cfg.Vehicles*cfg.Samples)
	}
	// Truth must agree with the CAN odometer (which floors to 100 m) and be
	// internally consistent.
	odo := map[telemetry.DeviceID]telemetry.Millimetres{}
	for _, tr := range truths {
		if prev, ok := odo[tr.DeviceID]; ok && tr.OdometerMM != prev+tr.DistanceMM {
			t.Fatalf("%s round %d: odometer %d != %d + %d", tr.DeviceID, tr.Round, tr.OdometerMM, prev, tr.DistanceMM)
		}
		odo[tr.DeviceID] = tr.OdometerMM
	}
	i := 0
	for _, s := range plain {
		if s.Source != telemetry.SourceCAN {
			continue
		}
		for truths[i].DeviceID != s.DeviceID || !truths[i].DeviceTime.Equal(s.DeviceTime) {
			i++
		}
		if d, ok := s.Distance.Get(); ok && d.Value != truths[i].OdometerMM/canResolution*canResolution {
			t.Fatalf("CAN %d does not match quantised truth %d", d.Value, truths[i].OdometerMM)
		}
	}
}

func TestAddingVehiclesKeepsExistingStreams(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Vehicles, cfg.Samples = 2, 30
	small := byVehicle(collect(t, cfg))
	cfg.Vehicles = 6
	large := byVehicle(collect(t, cfg))
	for id, vs := range small {
		if !reflect.DeepEqual(vs, large[id]) {
			t.Fatalf("%s changed when more vehicles were added", id)
		}
	}
}

func TestDisablingASourceKeepsOtherSources(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Samples = 30
	all := collect(t, cfg)
	cfg.Sources = []telemetry.Source{telemetry.SourceCAN}
	canOnly := collect(t, cfg)

	var want []telemetry.Distance
	for _, s := range all {
		if s.Source == telemetry.SourceCAN {
			d, _ := s.Distance.Get()
			want = append(want, d)
		}
	}
	var got []telemetry.Distance
	for _, s := range canOnly {
		d, _ := s.Distance.Get()
		got = append(got, d)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("CAN readings changed when GPS and pulse were disabled")
	}
}

// TestSensorsArePlausible runs a long simulation and checks the sources
// describe the same motion within their error models.
func TestSensorsArePlausible(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Vehicles, cfg.Samples, cfg.Interval = 5, 2_000, 10*time.Second
	const maxSpeed = 33_000 + cruiseJitter // mm/s

	for id, vs := range byVehicle(collect(t, cfg)) {
		var firstCAN, lastCAN telemetry.Millimetres = -1, -1
		var lastCANTime time.Time
		var pulseSum, gpsSum telemetry.Millimetres
		for _, s := range vs {
			d, ok := s.Distance.Get()
			if !ok {
				continue
			}
			switch s.Source {
			case telemetry.SourceCAN:
				if d.Value%canResolution != 0 {
					t.Fatalf("%s: CAN value %d not on %d mm grid", id, d.Value, canResolution)
				}
				if lastCAN >= 0 {
					if d.Value < lastCAN {
						t.Fatalf("%s: odometer decreased %d -> %d", id, lastCAN, d.Value)
					}
					limit := telemetry.Millimetres(maxSpeed*s.DeviceTime.Sub(lastCANTime).Milliseconds()/1000) + canResolution
					if d.Value-lastCAN > limit {
						t.Fatalf("%s: odometer jumped %d mm in %s", id, d.Value-lastCAN, s.DeviceTime.Sub(lastCANTime))
					}
				} else {
					firstCAN = d.Value
				}
				lastCAN, lastCANTime = d.Value, s.DeviceTime
			case telemetry.SourcePulse:
				pulseSum += d.Value
			case telemetry.SourceGPS:
				gpsSum += d.Value
			}
		}
		odo := lastCAN - firstCAN
		if odo < 100*telemetry.Kilometre/10 {
			t.Fatalf("%s: only %d mm travelled in %s, movement model looks stuck", id, odo, time.Duration(cfg.Samples)*cfg.Interval)
		}
		within := func(name string, v telemetry.Millimetres, tol float64) {
			if r := float64(v) / float64(odo); r < 1-tol || r > 1+tol {
				t.Errorf("%s: %s total %d vs odometer %d (ratio %.3f)", id, name, v, odo, r)
			}
		}
		within("pulse", pulseSum, 0.05) // ±3% calibration + quantisation
		within("gps", gpsSum, 0.08)     // noise, drift and lost intervals
	}
}

func TestConfigValidation(t *testing.T) {
	tests := map[string]func(*Config){
		"zero vehicles":      func(c *Config) { c.Vehicles = 0 },
		"too many vehicles":  func(c *Config) { c.Vehicles = MaxVehicles + 1 },
		"zero samples":       func(c *Config) { c.Samples = 0 },
		"too many samples":   func(c *Config) { c.Samples = MaxSamples + 1 },
		"interval too short": func(c *Config) { c.Interval = 50 * time.Millisecond },
		"interval too long":  func(c *Config) { c.Interval = 2 * time.Hour },
		"interval step":      func(c *Config) { c.Interval = 1050 * time.Millisecond },
		"start not utc":      func(c *Config) { c.Start = c.Start.In(time.FixedZone("X", 3600)) },
		"start zero":         func(c *Config) { c.Start = time.Time{} },
		"start sub-ms":       func(c *Config) { c.Start = c.Start.Add(time.Microsecond) },
		"run past max":       func(c *Config) { c.Start = telemetry.MaxTimestamp.Add(-time.Minute) },
		"no sources":         func(c *Config) { c.Sources = nil },
		"duplicate source":   func(c *Config) { c.Sources = []telemetry.Source{telemetry.SourceGPS, telemetry.SourceGPS} },
		"invalid source":     func(c *Config) { c.Sources = []telemetry.Source{0} },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := DefaultConfig()
			mutate(&cfg)
			if err := Run(cfg, func(telemetry.Sample) error { return nil }); err == nil {
				t.Fatal("Run accepted invalid config")
			}
		})
	}
	edges := map[string]func(*Config){
		"min interval": func(c *Config) { c.Interval = MinInterval },
		"max interval": func(c *Config) { c.Interval = MaxInterval },
		"one vehicle":  func(c *Config) { c.Vehicles, c.Samples = 1, 1 },
	}
	for name, mutate := range edges {
		t.Run(name, func(t *testing.T) {
			cfg := DefaultConfig()
			mutate(&cfg)
			collect(t, cfg)
		})
	}
}

func TestEmitErrorStopsRun(t *testing.T) {
	stop := errors.New("stop")
	calls := 0
	err := Run(DefaultConfig(), func(telemetry.Sample) error {
		calls++
		if calls == 4 {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) || calls != 4 {
		t.Fatalf("Run = %v after %d calls, want stop after 4", err, calls)
	}
}

// TestGoldenOutput pins the exact output for one config so that accidental
// changes to the model (or to the random number generator) are noticed.
// Regenerate with: go test ./internal/simulator -run Golden -update
func TestGoldenOutput(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 42
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, s := range collect(t, cfg) {
		if err := enc.Encode(s); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join("testdata", "golden_seed42.jsonl")
	if *update {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run with -update to create it)", err)
	}
	if !bytes.Equal(bytes.ReplaceAll(want, []byte("\r\n"), []byte("\n")), buf.Bytes()) {
		t.Fatalf("output differs from %s; if intended, rerun with -update", path)
	}
}
