package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestEvalPrintsTable(t *testing.T) {
	var out, errOut bytes.Buffer
	code := run([]string{"-seeds", "1", "-vehicles", "2", "-samples", "200", "-perturb"}, &out, &errOut)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, errOut.String())
	}
	if rows := strings.Count(out.String(), "\n"); rows != 2+7*2 {
		t.Fatalf("%d table lines, want header + 14 rows:\n%s", rows, out.String())
	}
	if code := run([]string{"-seeds", "x"}, &out, &errOut); code != 2 {
		t.Fatalf("bad seeds: exit %d", code)
	}
}

// -h and -help print usage and succeed, like the other FleetCore commands;
// unknown flags and stray arguments remain usage errors.
func TestEvalHelpAndUsageErrors(t *testing.T) {
	for _, args := range [][]string{{"-h"}, {"-help"}} {
		var out, errOut bytes.Buffer
		if code := run(args, &out, &errOut); code != 0 {
			t.Fatalf("%v: exit %d, want 0", args, code)
		}
		if !strings.Contains(errOut.String(), "usage: fleet-eval") || out.Len() != 0 {
			t.Fatalf("%v: stdout %q stderr %q", args, out.String(), errOut.String())
		}
	}
	for _, args := range [][]string{{"-nope"}, {"extra"}} {
		var out, errOut bytes.Buffer
		if code := run(args, &out, &errOut); code != 2 {
			t.Fatalf("%v: exit %d, want 2", args, code)
		}
	}
}
