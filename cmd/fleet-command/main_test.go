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
	args := []string{"request", "-data", dir, "-unverified-demo-identity", "-principal", "ops:alice", "-roles", "security_operator", "-device", "TRK-1",
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
	if code := run(ctx, []string{"request", "-data", dir, "-unverified-demo-identity", "-principal", "ops:eve", "-roles", "fleet_viewer", "-device", "TRK-1",
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

// Identities typed on the command line are never presented as trusted:
// request and ack refuse to run without the explicit demo acknowledgement
// (nothing is created), and every identity used is labelled unverified.
func TestIdentityFlagsAreDemoOnly(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	var out, errb bytes.Buffer
	if code := run(ctx, []string{"keygen", "-dir", dir, "-devices", "TRK-1"}, &out, &errb); code != 0 {
		t.Fatalf("keygen: %d %s", code, errb.String())
	}
	req := []string{"request", "-data", dir, "-principal", "ops:alice", "-roles", "security_operator",
		"-device", "TRK-1", "-type", "BLOCK", "-reason", "test", "-request-id", "r1"}
	out.Reset()
	errb.Reset()
	if code := run(ctx, req, &out, &errb); code != 2 || !strings.Contains(errb.String(), "not authenticated") {
		t.Fatalf("request without the demo flag: %d %s", code, errb.String())
	}
	out.Reset()
	if run(ctx, []string{"list", "-data", dir}, &out, &errb) != 0 || out.Len() != 0 {
		t.Fatalf("a refused request created a command: %q", out.String())
	}
	out.Reset()
	errb.Reset()
	if code := run(ctx, append(req, "-unverified-demo-identity"), &out, &errb); code != 0 {
		t.Fatalf("demo request: %d %s", code, errb.String())
	}
	if !strings.Contains(errb.String(), "UNVERIFIED") || !strings.Contains(out.String(), "unverified demo identity") {
		t.Fatalf("identity not labelled unverified: out=%q err=%q", out.String(), errb.String())
	}
	errb.Reset()
	if code := run(ctx, []string{"ack", "-data", dir, "-device", "TRK-1", "-uid", "1", "-operator", "ops:dave"}, &out, &errb); code != 2 ||
		!strings.Contains(errb.String(), "not authenticated") {
		t.Fatalf("ack without the demo flag: %d %s", code, errb.String())
	}
	out.Reset()
	if code := run(ctx, []string{"alerts", "-data", dir}, &out, &errb); code != 0 ||
		!strings.Contains(out.String(), "no person was notified") {
		t.Fatalf("alerts output does not qualify \"notified\": %d %q", code, out.String())
	}
}
