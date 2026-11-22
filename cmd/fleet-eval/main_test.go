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
