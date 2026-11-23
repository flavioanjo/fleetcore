package main

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/security"
)

func TestServerStartsAndStops(t *testing.T) {
	dir := t.TempDir()
	s, _ := security.GenerateSecret()
	security.WriteKeyFile(filepath.Join(dir, "keys.json"), []security.DeviceKey{{DeviceID: "TRK-1", KeyID: 1, Secret: s, State: security.KeyActive}})
	a, _ := security.NewAuthority(1)
	security.WriteAuthorityFile(filepath.Join(dir, "authority.json"), a)
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan []string, 1)
	var log bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, []string{"-listen", "127.0.0.1:0,127.0.0.1:0", "-data", dir, "-keys", filepath.Join(dir, "keys.json"),
			"-authority", filepath.Join(dir, "authority.json"), "-stats-interval", "0"}, &log, ready)
	}()
	select {
	case addrs := <-ready:
		if len(addrs) != 2 {
			t.Fatalf("gateways %v", addrs)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("not ready: %s", log.String())
	}
	cancel()
	if code := <-done; code != 0 {
		t.Fatalf("exit %d: %s", code, log.String())
	}
	// Missing key file: usage error, without echoing anything secret.
	if code := run(context.Background(), []string{"-data", dir, "-keys", filepath.Join(dir, "none.json")}, &log, nil); code != 2 {
		t.Fatalf("missing keys: exit %d", code)
	}
}
