package main

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/eventstore"
	"github.com/flavioanjo/fleetcore/internal/resultstore"
	"github.com/flavioanjo/fleetcore/internal/simulator"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestHelpAndUsageErrors(t *testing.T) {
	var out syncBuffer
	if code := run(context.Background(), []string{"-h"}, &out); code != 0 || !strings.Contains(out.String(), "usage: fleet-processor") {
		t.Fatalf("-h: exit %d, %q", code, out.String())
	}
	for _, args := range [][]string{{"-nope"}, {"extra"}, {"-log-level", "loud"}, {"-log-format", "xml"}, {"-tick", "0s"}} {
		var stderr syncBuffer
		if code := run(context.Background(), args, &stderr); code != 2 {
			t.Errorf("%v: exit %d, want 2", args, code)
		}
	}
}

// TestProcessThenVerifyAfterRestart stores simulator events, runs the
// processor until they are processed, stops it with a cancellation (as a
// signal would), and runs it again with -verify.
func TestProcessThenVerifyAfterRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	eventsPath, resultsPath := filepath.Join(dir, "events.db"), filepath.Join(dir, "results.db")
	st, err := eventstore.Open(ctx, eventsPath, eventstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := simulator.DefaultConfig()
	cfg.Vehicles, cfg.Samples = 2, 40
	n := 0
	if err := simulator.Run(cfg, func(s telemetry.Sample) error {
		ev, err := eventstore.NewEvent(s)
		if err == nil {
			_, err = st.Append(ctx, ev)
		}
		n++
		return err
	}); err != nil {
		t.Fatal(err)
	}
	st.Close()

	args := []string{"-events", eventsPath, "-results", resultsPath, "-hold", "20ms", "-poll", "10ms", "-log-format", "text"}
	runCtx, cancel := context.WithCancel(ctx)
	var logs syncBuffer
	done := make(chan int, 1)
	go func() { done <- run(runCtx, args, &logs) }()

	deadline := time.Now().Add(20 * time.Second)
	for processed(t, resultsPath) < int64(n) {
		if time.Now().After(deadline) {
			t.Fatalf("processed %d of %d:\n%s", processed(t, resultsPath), n, logs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	if code := <-done; code != 0 {
		t.Fatalf("exit %d:\n%s", code, logs.String())
	}
	if !strings.Contains(logs.String(), "processor stopped") {
		t.Fatalf("no clean stop logged:\n%s", logs.String())
	}

	var verifyLogs syncBuffer
	if code := run(ctx, append(args, "-verify"), &verifyLogs); code != 0 || !strings.Contains(verifyLogs.String(), "journal verified") {
		t.Fatalf("-verify: exit %d:\n%s", code, verifyLogs.String())
	}
}

func processed(t *testing.T, path string) int64 {
	t.Helper()
	rs, err := resultstore.Open(context.Background(), path, resultstore.Options{})
	if err != nil {
		return 0 // not created yet
	}
	defer rs.Close()
	c, err := rs.Count(context.Background())
	if err != nil {
		return 0
	}
	return c.EventSteps
}
