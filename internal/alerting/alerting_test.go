package alerting

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/eventstore"
	"github.com/flavioanjo/fleetcore/internal/reliable"
)

var t0 = time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type funcSink func(context.Context, Alert) error

func (f funcSink) Notify(ctx context.Context, a Alert) error { return f(ctx, a) }

type fixture struct {
	dir    string
	events *eventstore.Store
}

func newFixture(t *testing.T, panics int) *fixture {
	t.Helper()
	dir := t.TempDir()
	ev, err := eventstore.Open(context.Background(), filepath.Join(dir, "events.db"), eventstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ev.Close() })
	var evs []eventstore.ReliableEvent
	for u := uint64(1); u <= uint64(panics); u++ {
		up, err := reliable.NewEvent(u, t0, reliable.Panic{Trigger: reliable.PanicButton})
		if err != nil {
			t.Fatal(err)
		}
		re, err := eventstore.NewReliableEvent("DEV-1", up, 1, 1, t0, "gw-a")
		if err != nil {
			t.Fatal(err)
		}
		evs = append(evs, re)
	}
	if len(evs) > 0 {
		if _, err := ev.AppendReliable(context.Background(), evs); err != nil {
			t.Fatal(err)
		}
	}
	return &fixture{dir: dir, events: ev}
}

func (f *fixture) open(t *testing.T, sink Sink, mutate func(*Config)) *Consumer {
	t.Helper()
	cfg := Config{Path: filepath.Join(f.dir, "alerts.db"), Events: f.events, Sink: sink, Logger: quiet(),
		Interval: time.Millisecond, Now: func() time.Time { return t0 }}
	if mutate != nil {
		mutate(&cfg)
	}
	c, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func runWithin(t *testing.T, ctx context.Context, c *Consumer) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return")
		return nil
	}
}

// A stop request arriving while the sink is being notified does not
// interrupt the step: the accepted notification is recorded, the store
// closes cleanly and its files are released.
func TestStopDuringNotificationCompletesTheStep(t *testing.T) {
	f := newFixture(t, 3)
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	calls := 0
	c := f.open(t, funcSink(func(ctx context.Context, a Alert) error {
		calls++
		stop() // SIGTERM while notifying
		return ctx.Err()
	}), nil)
	if err := runWithin(t, ctx, c); err != nil {
		t.Fatalf("orderly stop reported %v", err)
	}
	if calls != 3 {
		t.Fatalf("%d notifications, want 3 (the step in progress completes)", calls)
	}
	l, err := c.List(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range l {
		if a.State != StateNotified {
			t.Fatalf("alert %s/%d left %s by an orderly stop", a.DeviceID, a.UID, a.State)
		}
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS == "windows" {
		if err := os.Remove(filepath.Join(f.dir, "alerts.db")); err != nil {
			t.Fatalf("alerts.db still open: %v", err)
		}
	}
}

func TestRunFailsAfterConsecutiveFailures(t *testing.T) {
	f := newFixture(t, 1)
	c := f.open(t, funcSink(func(context.Context, Alert) error { return nil }), func(c *Config) { c.MaxConsecutiveFailures = 3 })
	c.Close() // every step now fails
	if err := runWithin(t, context.Background(), c); !errors.Is(err, ErrFailed) {
		t.Fatalf("Run returned %v, want ErrFailed", err)
	}
}

// A failing sink is not a failed step: the alert stays open, is retried
// with backoff, and Run keeps going.
func TestSinkFailureIsRetriedNotFatal(t *testing.T) {
	f := newFixture(t, 1)
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	calls := 0
	c := f.open(t, funcSink(func(context.Context, Alert) error {
		if calls++; calls >= 5 {
			stop()
		}
		return errors.New("pager unreachable")
	}), func(c *Config) {
		c.MaxConsecutiveFailures = 2
		c.InitialBackoff, c.MaxBackoff = time.Millisecond, time.Millisecond
		now := t0
		c.Now = func() time.Time { now = now.Add(time.Second); return now } // only Run's goroutine reads the clock
	})
	defer c.Close()
	if err := runWithin(t, ctx, c); err != nil {
		t.Fatalf("sink failures stopped alert processing: %v", err)
	}
	l, _ := c.List(context.Background(), 10)
	if len(l) != 1 || l[0].State != StateOpen {
		t.Fatalf("alerts %+v", l)
	}
}
