package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestKeygenRequestListShow(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	var out, errb bytes.Buffer
	if code := run(ctx, []string{"keygen", "-dir", dir, "-devices", "TRK-1,TRK-2"}, &out, &errb); code != 0 {
		t.Fatalf("keygen: %d %s", code, errb.String())
	}
	if fi, err := os.Stat(filepath.Join(dir, "keys.json")); err != nil || fi.Size() == 0 {
		t.Fatal("no key file")
	}
	if strings.Contains(out.String(), "secret_hex") {
		t.Fatal("keygen printed key material")
	}
	out.Reset()
	args := []string{"request", "-data", dir, "-principal", "ops:alice", "-roles", "security_operator", "-device", "TRK-1",
		"-type", "BLOCK", "-reason", "test", "-request-id", "r1"}
	if code := run(ctx, args, &out, &errb); code != 0 {
		t.Fatalf("request: %d %s", code, errb.String())
	}
	id := strings.Fields(out.String())[1]
	out.Reset()
	if run(ctx, args, &out, &errb) != 0 || !strings.Contains(out.String(), "already exists") || !strings.Contains(out.String(), id) {
		t.Fatalf("repeated request: %s", out.String())
	}
	errb.Reset()
	if code := run(ctx, []string{"request", "-data", dir, "-principal", "ops:eve", "-roles", "fleet_viewer", "-device", "TRK-1",
		"-type", "UNBLOCK", "-reason", "x"}, &out, &errb); code != 1 || !strings.Contains(errb.String(), "not authorized") {
		t.Fatalf("unauthorized: %d %s", code, errb.String())
	}
	out.Reset()
	if run(ctx, []string{"show", "-data", dir, "-id", id}, &out, &errb) != 0 || !strings.Contains(out.String(), "requested") {
		t.Fatalf("show: %s", out.String())
	}
	out.Reset()
	if run(ctx, []string{"list", "-data", dir}, &out, &errb) != 0 || strings.Count(out.String(), "\n") != 1 {
		t.Fatalf("list: %q", out.String())
	}
}
