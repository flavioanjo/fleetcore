package fusion

import (
	"encoding/json"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

func TestMultipleDevicesAreIndependent(t *testing.T) {
	a := gpsPulseDrive(newStream("A", 1), 20)
	b := tripSpec{epochs: drive(), interval: 10 * time.Second, odometer: 5 * km, can: true, pulse: true}
	bs, _ := b.build(newStream("B", 2))

	aloneA := estimates(runAll(newEngine(t), a), "A")
	aloneB := estimates(runAll(newEngine(t), bs), "B")

	var mixed []telemetry.Sample
	for i := 0; i < len(a) || i < len(bs); i++ {
		if i < len(bs) {
			mixed = append(mixed, bs[i])
		}
		if i < len(a) {
			mixed = append(mixed, a[i])
		}
	}
	rs := runAll(newEngine(t), mixed)
	if !reflect.DeepEqual(estimates(rs, "A"), aloneA) || !reflect.DeepEqual(estimates(rs, "B"), aloneB) {
		t.Fatal("interleaving devices changed their estimates")
	}
}

func TestDeterministicOutput(t *testing.T) {
	ss := gpsPulseDrive(newStream("D", 1), 30)
	ss[5], ss[7] = ss[7], ss[5]
	ss = append(ss, ss[1])
	enc := func() string {
		b, err := json.Marshal(runAll(newEngine(t), ss))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	if enc() != enc() {
		t.Fatal("same input produced different reports")
	}
}

func TestVehicleAtRest(t *testing.T) {
	st := newStream("P", 1)
	var ss []telemetry.Sample
	for i := 0; i < 360; i++ { // one hour parked
		st.tick(10 * time.Second)
		ss = append(ss, st.gps(telemetry.Millimetres(i%7)*300), st.pulse(0), st.can(123_400*m))
	}
	rs := runAll(newEngine(t), ss)
	if got := lastTrip(t, rs, "P"); got != 0 {
		t.Fatalf("parked trip %d, want 0", got)
	}
}

// Persistence is not implemented: a restarted engine starts from nothing.
// The documented behaviour is that it re-establishes baselines and counts
// only what it sees, never re-crediting earlier travel.
func TestRestartLosesInMemoryState(t *testing.T) {
	spec := tripSpec{epochs: constant(60, 150*m), interval: 10 * time.Second, odometer: 20 * km, gps: true, can: true, pulse: true}
	ss, _ := spec.build(newStream("R", 1))
	first, second := ss[:90], ss[90:]

	runAll(newEngine(t), first) // the "crashed" engine
	rs := runAll(newEngine(t), second)
	requireDiag(t, rs, CodeSessionStarted)
	requireDiag(t, rs, CodeCANBaseline)
	near(t, "trip after restart", lastTrip(t, rs, "R"), 30*150*m, 200*m)
}

func TestBoundedMemory(t *testing.T) {
	e := newEngine(t, func(c *Config) {
		c.MaxBufferedPerSession = 16
		c.MaxTrackedGaps = 8
		c.RecentEvents = 32
		c.MaxClosedSessions = 4
	})
	cfg := e.cfg
	// A hostile stream: huge sequence jumps, reversed order, many reboots.
	for boot := telemetry.BootID(1); boot <= 20; boot++ {
		st := newStream("M", boot)
		st.now = t0.Add(time.Duration(boot) * time.Hour)
		var ss []telemetry.Sample
		for i := 0; i < 400; i++ {
			s := st.tick(time.Second).gps(10 * m)
			s.Sequence = telemetry.Sequence(i * 3) // every other two numbers missing
			s.EventID = telemetry.EventID(uint64(boot)<<32 | uint64(s.Sequence))
			ss = append(ss, s)
		}
		for i := len(ss) - 1; i >= 0; i-- {
			e.Process(ss[i])
			d := e.devices["M"]
			if n := len(d.cur.buf); n > cfg.MaxBufferedPerSession {
				t.Fatalf("buffer holds %d events, limit %d", n, cfg.MaxBufferedPerSession)
			}
			if len(d.cur.gaps) > cfg.MaxTrackedGaps || len(d.cur.recent) > cfg.RecentEvents {
				t.Fatalf("tracking grew beyond limits: gaps %d, recent %d", len(d.cur.gaps), len(d.cur.recent))
			}
			if len(d.closed) > cfg.MaxClosedSessions {
				t.Fatalf("closed sessions %d", len(d.closed))
			}
		}
	}
	e.Flush()
	if s := e.Stats(); s.Buffered != 0 || s.Devices != 1 {
		t.Fatalf("after flush: %+v", s)
	}
}

func TestDeviceCapacityAndInvalidInput(t *testing.T) {
	e := newEngine(t, func(c *Config) { c.MaxDevices = 1 })
	a := newStream("A", 1).tick(time.Second).gps(m)
	b := newStream("B", 1).tick(time.Second).gps(m)
	e.Process(a)
	rs := e.Process(b)
	if ev := rs[0].Event; ev.Outcome != OutcomeRejected || ev.Reason != ReasonDeviceCapacity {
		t.Fatalf("got %s/%s", ev.Outcome, ev.Reason)
	}
	bad := a
	bad.Source = 0
	rs = e.Process(bad)
	if ev := rs[0].Event; ev.Outcome != OutcomeRejected || ev.Reason != ReasonInvalidSample {
		t.Fatalf("got %s/%s", ev.Outcome, ev.Reason)
	}
	if _, err := json.Marshal(rs); err != nil {
		t.Fatalf("report for invalid input does not marshal: %v", err)
	}
}

func TestConfigValidation(t *testing.T) {
	if err := DefaultConfig().Validate(); err != nil {
		t.Fatal(err)
	}
	bad := []func(*Config){
		func(c *Config) { c.ReorderWindow = 0 },
		func(c *Config) { c.MaxBufferedPerSession = 0 },
		func(c *Config) { c.CANResolution = -1 },
		func(c *Config) { c.CalibrationMaxPPM = c.CalibrationMinPPM },
		func(c *Config) { c.GPS.GoodAbs = 0 },
		func(c *Config) { c.MaxSpeed = 2 * km },
		func(c *Config) { c.DisagreementFloor = -1 },
	}
	for i, f := range bad {
		c := DefaultConfig()
		f(&c)
		if _, err := New(c); err == nil {
			t.Errorf("case %d: invalid config accepted", i)
		}
	}
}

// TestNoGroundTruthDependency guards the architecture: production fusion
// code must not import the simulator (and so cannot see ground truth).
func TestNoGroundTruthDependency(t *testing.T) {
	files, _ := filepath.Glob("*.go")
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		af, err := parser.ParseFile(token.NewFileSet(), f, src, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range af.Imports {
			if strings.Contains(imp.Path.Value, "internal/simulator") {
				t.Fatalf("%s imports %s", f, imp.Path.Value)
			}
		}
	}
}

func TestIntegerHelpers(t *testing.T) {
	if got := mulDiv(1<<62, 1<<62, 1<<61); got != 1<<63-1 {
		t.Errorf("mulDiv saturation = %d", got)
	}
	if got := mulDiv(7, 3, 2); got != 11 { // 10.5 rounds up
		t.Errorf("mulDiv rounding = %d", got)
	}
	x, v := combine(100, 1, 200, 1)
	if x != 150 || v != 1 {
		t.Errorf("combine equal = %d, %d", x, v)
	}
	x, _ = combine(100, 1, 200, 3) // weight 3:1 towards the first
	if x != 125 {
		t.Errorf("combine weighted = %d", x)
	}
	x, _ = combine(int64(400*km), 1<<60, int64(500*km), 1<<60)
	if x != int64(450*km) {
		t.Errorf("combine large = %d", x)
	}
	for _, n := range []int64{0, 1, 2, 3, 4, 15, 16, 17, 1 << 40, 1<<62 - 1} {
		r := isqrt(n)
		if r*r > n || (r+1)*(r+1) <= n {
			t.Errorf("isqrt(%d) = %d", n, r)
		}
	}
}
