package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

func runCLI(t *testing.T, args ...string) (string, string, int) {
	t.Helper()
	var out, errOut bytes.Buffer
	code := run(args, &out, &errOut)
	return out.String(), errOut.String(), code
}

func TestCLIProducesValidJSONLines(t *testing.T) {
	out, stderr, code := runCLI(t, "-seed", "7", "-vehicles", "2", "-samples", "3", "-interval", "5s")
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	sc := bufio.NewScanner(strings.NewReader(out))
	lines := 0
	for sc.Scan() {
		var s telemetry.Sample
		if err := json.Unmarshal(sc.Bytes(), &s); err != nil {
			t.Fatalf("line %d: %v: %s", lines+1, err, sc.Text())
		}
		if err := s.Validate(); err != nil {
			t.Fatalf("line %d invalid: %v", lines+1, err)
		}
		lines++
	}
	if want := 2 * 3 * 3; lines != want {
		t.Fatalf("got %d lines, want %d", lines, want)
	}
}

func TestCLIIsReproducible(t *testing.T) {
	args := []string{"-seed", "99", "-vehicles", "3", "-samples", "4"}
	a, _, _ := runCLI(t, args...)
	b, _, _ := runCLI(t, args...)
	if a != b || a == "" {
		t.Fatal("same flags produced different output")
	}
	c, _, _ := runCLI(t, "-seed", "100", "-vehicles", "3", "-samples", "4")
	if a == c {
		t.Fatal("different seeds produced the same output")
	}
}

func TestCLISourcesFlag(t *testing.T) {
	out, stderr, code := runCLI(t, "-sources", "pulse,gps", "-vehicles", "1", "-samples", "2")
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	want := []string{"pulse", "gps", "pulse", "gps"}
	if len(lines) != len(want) {
		t.Fatalf("got %d lines, want %d", len(lines), len(want))
	}
	for i, l := range lines {
		if !strings.Contains(l, `"source":"`+want[i]+`"`) {
			t.Errorf("line %d: %s, want source %s", i, l, want[i])
		}
	}
}

func TestCLIUsageErrors(t *testing.T) {
	cases := [][]string{
		{"-vehicles", "0"},
		{"-samples", "-1"},
		{"-interval", "150ms"},
		{"-start", "yesterday"},
		{"-sources", "gps,radar"},
		{"-sources", "gps,gps"},
		{"-nope"},
		{"extra-arg"},
	}
	for _, args := range cases {
		out, stderr, code := runCLI(t, args...)
		if code != 2 {
			t.Errorf("%v: exit %d, want 2", args, code)
		}
		if out != "" {
			t.Errorf("%v: wrote to stdout on error", args)
		}
		if stderr == "" {
			t.Errorf("%v: no error message", args)
		}
	}
	if _, _, code := runCLI(t, "-h"); code != 0 {
		t.Errorf("-h: exit %d, want 0", code)
	}
}
