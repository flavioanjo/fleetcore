package main

import (
	"bytes"
	"strings"
	"testing"
)

// Every narrated scenario runs to completion (each one checks its own
// outcome and fails the run otherwise).
func TestAllScenarios(t *testing.T) {
	if testing.Short() {
		t.Skip("runs every simulated scenario")
	}
	var out bytes.Buffer
	if code := run([]string{"-seed", "3"}, &out); code != 0 {
		t.Fatalf("exit %d:\n%s", code, out.String())
	}
	for _, want := range []string{"actuator driven 1 time(s)", "position_unavailable", "door open, latch released",
		"server stored 1 PANIC row(s)", "alert acknowledged", "retries never repeat execution", "vehicle_moving",
		"forged authority signature: not executed", "provably not executed"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output lacks %q", want)
		}
	}
}
