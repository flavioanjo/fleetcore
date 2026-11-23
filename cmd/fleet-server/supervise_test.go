package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// waitUntilCancelled is a well-behaved service.
func waitUntilCancelled(stopped *atomic.Int32) func(context.Context) error {
	return func(ctx context.Context) error {
		<-ctx.Done()
		stopped.Add(1)
		return nil
	}
}

func superviseWithin(t *testing.T, ctx context.Context, grace time.Duration, svcs []service) (error, bool, string) {
	t.Helper()
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	type out struct {
		err error
		ok  bool
	}
	done := make(chan out, 1)
	go func() { e, ok := supervise(ctx, log, grace, svcs); done <- out{e, ok} }()
	select {
	case o := <-done:
		return o.err, o.ok, buf.String()
	case <-time.After(10 * time.Second):
		t.Fatal("supervise did not return")
		return nil, false, ""
	}
}

func TestSuperviseOrderlyStop(t *testing.T) {
	var stopped atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{}, 3)
	svc := func(ctx context.Context) error { started <- struct{}{}; return waitUntilCancelled(&stopped)(ctx) }
	go func() {
		for i := 0; i < 3; i++ {
			<-started
		}
		cancel()
	}()
	err, ok, _ := superviseWithin(t, ctx, time.Minute, []service{{"a", svc}, {"b", svc}, {"c", svc}})
	if err != nil || !ok || stopped.Load() != 3 {
		t.Fatalf("err=%v allStopped=%v stopped=%d", err, ok, stopped.Load())
	}
}

func TestSuperviseFailureStopsEveryService(t *testing.T) {
	injected := errors.New("store unavailable")
	cases := map[string]func(context.Context) error{
		"error":       func(context.Context) error { return injected },
		"nil return":  func(context.Context) error { return nil },
		"panic":       func(context.Context) error { panic("boom") },
		"error later": func(ctx context.Context) error { time.Sleep(time.Millisecond); return injected },
	}
	for name, failing := range cases {
		t.Run(name, func(t *testing.T) {
			var stopped atomic.Int32
			err, ok, log := superviseWithin(t, context.Background(), time.Minute, []service{
				{"gateway gw-1", waitUntilCancelled(&stopped)},
				{"dispatcher", failing},
				{"alerting", waitUntilCancelled(&stopped)},
			})
			var se *ServiceError
			if !errors.As(err, &se) || se.Service != "dispatcher" {
				t.Fatalf("failure %v", err)
			}
			if !ok || stopped.Load() != 2 {
				t.Fatalf("allStopped=%v, %d of 2 other services stopped", ok, stopped.Load())
			}
			if !strings.Contains(log, "essential service failed") || !strings.Contains(log, "service=dispatcher") {
				t.Fatalf("failure not logged: %s", log)
			}
			switch name {
			case "nil return":
				if !errors.Is(err, errStoppedUnexpectedly) {
					t.Fatalf("unexpected stop reported as %v", err)
				}
			case "panic":
				if !strings.Contains(err.Error(), "panic: boom") {
					t.Fatalf("panic reported as %v", err)
				}
			default:
				if !errors.Is(err, injected) {
					t.Fatalf("cause lost: %v", err)
				}
			}
		})
	}
}

// A service that ignores the stop request is reported after the grace
// period, and supervise says so, so that the caller does not close the
// stores under it (no deadlock either).
func TestSuperviseReportsServicesThatDoNotStop(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err, ok, log := superviseWithin(t, ctx, 50*time.Millisecond, []service{
		{"stuck", func(context.Context) error { <-release; return nil }},
		{"fine", func(ctx context.Context) error { <-ctx.Done(); return nil }},
	})
	if ok || err == nil || !strings.Contains(err.Error(), "stuck") || strings.Contains(err.Error(), "fine") {
		t.Fatalf("err=%v allStopped=%v", err, ok)
	}
	if !strings.Contains(log, "did not stop within the grace period") {
		t.Fatalf("not logged: %s", log)
	}
}

// A service that fails while stopping makes the stop a failure too.
func TestSuperviseErrorWhileStopping(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err, ok, _ := superviseWithin(t, ctx, time.Minute, []service{
		{"gateway gw-1", func(ctx context.Context) error { <-ctx.Done(); return errors.New("drain failed") }},
	})
	var se *ServiceError
	if !ok || !errors.As(err, &se) || se.Service != "gateway gw-1" {
		t.Fatalf("err=%v ok=%v", err, ok)
	}
}
