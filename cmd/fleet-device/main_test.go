package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/flavioanjo/fleetcore/internal/security"
)

func TestDeviceRunsAndKeepsItsQueue(t *testing.T) {
	dir := t.TempDir()
	s, _ := security.GenerateSecret()
	security.WriteKeyFile(filepath.Join(dir, "keys.json"), []security.DeviceKey{{DeviceID: "TRK-1", KeyID: 1, Secret: s, State: security.KeyActive}})
	a, _ := security.NewAuthority(1)
	pub, _ := security.WriteAuthorityFile(filepath.Join(dir, "authority.json"), a)
	os.WriteFile(filepath.Join(dir, "authority.pub"), []byte(pub), 0o644)
	var log bytes.Buffer
	args := []string{"-id", "TRK-1", "-keys", filepath.Join(dir, "keys.json"), "-authority-pub", filepath.Join(dir, "authority.pub"),
		"-gateway", "127.0.0.1:9", "-db", filepath.Join(dir, "d.db"), "-panic-after", "100ms", "-duration", "700ms", "-telemetry", "200ms"}
	if code := run(context.Background(), args, &log); code != 0 {
		t.Fatalf("exit %d: %s", code, log.String())
	}
	if !strings.Contains(log.String(), "PANIC button pressed") || !strings.Contains(log.String(), "pending events stay queued") {
		t.Fatalf("log: %s", log.String())
	}
	if strings.Contains(log.String(), "secret") && strings.Contains(log.String(), "hex") {
		t.Fatal("key material in log")
	}
}
