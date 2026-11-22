package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/flavioanjo/fleetcore/internal/simulator"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

func simJSONL(t *testing.T, vehicles, samples int) string {
	t.Helper()
	cfg := simulator.DefaultConfig()
	cfg.Vehicles, cfg.Samples = vehicles, samples
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	if err := simulator.Run(cfg, func(s telemetry.Sample) error { return enc.Encode(s) }); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

func runCLI(t *testing.T, input string, args ...string) (lines []map[string]any, stderr string, code int) {
	t.Helper()
	var out, errOut bytes.Buffer
	code = run(args, strings.NewReader(input), &out, &errOut)
	sc := bufio.NewScanner(&out)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("output is not JSON Lines: %v: %s", err, sc.Text())
		}
		lines = append(lines, m)
	}
	return lines, errOut.String(), code
}

func count(lines []map[string]any, typ string) int {
	n := 0
	for _, l := range lines {
		if l["type"] == typ {
			n++
		}
	}
	return n
}

func TestFusesSimulatorOutput(t *testing.T) {
	lines, stderr, code := runCLI(t, simJSONL(t, 3, 50), "-summary")
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if count(lines, "estimate") != 3*50 {
		t.Fatalf("%d estimates, want one per vehicle and round (150)", count(lines, "estimate"))
	}
	if count(lines, "summary") != 3 {
		t.Fatalf("%d summaries, want 3", count(lines, "summary"))
	}
	for _, l := range lines {
		if l["type"] == "summary" && l["trip_mm"].(float64) <= 0 {
			t.Fatalf("summary without distance: %v", l)
		}
	}
}

func TestEventModes(t *testing.T) {
	in := simJSONL(t, 1, 20)
	all, _, _ := runCLI(t, in, "-events", "all")
	none, _, _ := runCLI(t, in, "-events", "none")
	final := 0
	for _, l := range all {
		if ev, ok := l["event"].(map[string]any); ok && ev["outcome"] != "buffered" {
			final++
		}
	}
	// Every sample gets one final outcome; the first ones are also reported
	// as buffered while the session start is established.
	if final != 60 || count(none, "event") != 0 {
		t.Fatalf("final outcomes with -events all: %d (want 60); with none: %d events", final, count(none, "event"))
	}
}

func TestAcceptsUTF8BOM(t *testing.T) {
	_, stderr, code := runCLI(t, "\xef\xbb\xbf"+simJSONL(t, 1, 3))
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
}

func TestInputErrorsAreReportedNotDropped(t *testing.T) {
	in := simJSONL(t, 1, 5)
	parts := strings.SplitAfter(in, "\n")
	bad := parts[0] + "{not json\n" + strings.Join(parts[1:], "") + `{"device_id":"X","event_id":0}` + "\n"
	lines, stderr, code := runCLI(t, bad, "-events", "issues")
	if code != 1 || !strings.Contains(stderr, "1 input line") {
		t.Fatalf("exit %d, stderr %q: want exit 1 reporting the bad line", code, stderr)
	}
	found := false
	for _, l := range lines {
		if l["type"] == "input_error" && l["line"].(float64) == 2 {
			found = true
		}
	}
	if !found {
		t.Fatal("input_error for line 2 missing")
	}
	// The decodable but invalid sample is reported as a rejected event.
	rejected := false
	for _, l := range lines {
		if ev, ok := l["event"].(map[string]any); ok && ev["reason"] == "invalid_sample" {
			rejected = true
		}
	}
	if !rejected {
		t.Fatal("invalid sample not reported")
	}
	if _, _, code := runCLI(t, bad, "-strict"); code != 1 {
		t.Fatalf("-strict exit %d", code)
	}
}

func TestInputFileAndUsage(t *testing.T) {
	p := filepath.Join(t.TempDir(), "in.jsonl")
	if err := os.WriteFile(p, []byte(simJSONL(t, 1, 3)), 0o600); err != nil {
		t.Fatal(err)
	}
	if lines, stderr, code := runCLI(t, "", "-in", p); code != 0 || count(lines, "estimate") != 3 {
		t.Fatalf("file input: exit %d, %d estimates, %s", code, count(lines, "estimate"), stderr)
	}
	for _, args := range [][]string{{"-events", "some"}, {"-window", "0s"}, {"extra"}} {
		if _, _, code := runCLI(t, "", args...); code != 2 {
			t.Errorf("%v: exit %d, want 2", args, code)
		}
	}
	if _, _, code := runCLI(t, "", "-in", filepath.Join(t.TempDir(), "missing")); code != 1 {
		t.Errorf("missing file: exit %d, want 1", code)
	}
}

func TestDeterministicOutput(t *testing.T) {
	in := simJSONL(t, 4, 30)
	var a, b bytes.Buffer
	run([]string{"-events", "all", "-summary"}, strings.NewReader(in), &a, &bytes.Buffer{})
	run([]string{"-events", "all", "-summary"}, strings.NewReader(in), &b, &bytes.Buffer{})
	if a.String() != b.String() {
		t.Fatal("output differs between identical runs")
	}
}
