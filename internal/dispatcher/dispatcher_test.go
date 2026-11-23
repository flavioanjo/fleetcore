package dispatcher

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/commandstore"
	"github.com/flavioanjo/fleetcore/internal/eventstore"
	"github.com/flavioanjo/fleetcore/internal/reliable"
	"github.com/flavioanjo/fleetcore/internal/security"
)

type fixture struct {
	cmds   *commandstore.Store
	events *eventstore.Store
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	dir := t.TempDir()
	auth, err := security.NewAuthority(1)
	if err != nil {
		t.Fatal(err)
	}
	cmds, err := commandstore.Open(context.Background(), filepath.Join(dir, "commands.db"), auth, commandstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	events, err := eventstore.Open(context.Background(), filepath.Join(dir, "events.db"), eventstore.Options{})
	if err != nil {
		cmds.Close()
		t.Fatal(err)
	}
	f := &fixture{cmds: cmds, events: events}
	t.Cleanup(func() {
		f.cmds.Close()
		f.events.Close()
	})
	return f
}

func (f *fixture) command(t *testing.T, id string) reliable.CommandID {
	t.Helper()
	c, _, err := f.cmds.Create(context.Background(), commandstore.Request{RequestID: id,
		Principal: commandstore.Principal{ID: "test", Roles: []string{"fleet_viewer"}},
		DeviceID:  "DEV-1", Type: reliable.CommandRequestPosition, Args: reliable.PositionArgs(time.Minute), TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	return c.ID
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type funcTransport func(reliable.CommandDelivery) error

func (f funcTransport) SendCommand(d reliable.CommandDelivery) error { return f(d) }

// runWithin runs d.Run and fails the test if it does not return in time.
func runWithin(t *testing.T, ctx context.Context, d *Dispatcher, limit time.Duration) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()
	select {
	case err := <-done:
		return err
	case <-time.After(limit):
		t.Fatalf("Run did not return within %v", limit)
		return nil
	}
}

// Regression: a stop request used to cancel the context of the delivery in
// progress, between the write-ahead BeginAttempt ("sending") and
// FinishAttempt. The attempt stayed "sending", which after a restart makes
// the command's outcome unknown (needs attention) although the datagram
// had been handed to the network and the shutdown was orderly. The
// transport below requests the stop deterministically while it sends.
func TestStopDuringDeliveryFinishesTheAttempt(t *testing.T) {
	f := newFixture(t)
	id := f.command(t, "r1")
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	sent := 0
	tr := funcTransport(func(reliable.CommandDelivery) error {
		sent++
		stop() // SIGTERM arrives while the datagram is being sent
		return nil
	})
	d, err := New(Config{Commands: f.cmds, Events: f.events, Transports: []Named{{"gw", tr}}, Interval: time.Millisecond, Logger: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	if err := runWithin(t, ctx, d, 10*time.Second); err != nil {
		t.Fatalf("orderly stop reported %v", err)
	}
	if sent != 1 {
		t.Fatalf("%d sends", sent)
	}
	at, err := f.cmds.Attempts(context.Background(), id)
	if err != nil || len(at) != 1 {
		t.Fatalf("attempts %+v, %v", at, err)
	}
	if at[0].Outcome != string(commandstore.AttemptSent) {
		t.Fatalf("attempt left %q by an orderly stop, want %q", at[0].Outcome, commandstore.AttemptSent)
	}
}

// A dispatcher whose store keeps failing must stop and report it, instead
// of looping while command delivery has silently stopped.
func TestRunFailsAfterConsecutiveFailures(t *testing.T) {
	f := newFixture(t)
	d, err := New(Config{Commands: f.cmds, Events: f.events, Transports: []Named{{"gw", funcTransport(func(reliable.CommandDelivery) error { return nil })}},
		Interval: time.Millisecond, MaxConsecutiveFailures: 3, Logger: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	f.cmds.Close() // every store operation now fails
	err = runWithin(t, context.Background(), d, 10*time.Second)
	if !errors.Is(err, ErrFailed) {
		t.Fatalf("Run returned %v, want ErrFailed", err)
	}
}

// Failures below the threshold do not stop the dispatcher; a success
// resets the count.
func TestRunToleratesTransientFailures(t *testing.T) {
	f := newFixture(t)
	f.command(t, "r1")
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	var failNext, iterations int
	d, err := New(Config{Commands: f.cmds, Events: f.events, Transports: []Named{{"gw", funcTransport(func(reliable.CommandDelivery) error { return nil })}},
		Interval: time.Millisecond, MaxConsecutiveFailures: 3, Logger: quiet(),
		InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	// Two failing iterations, one success, two failing, ... never three in
	// a row.
	d.beforeIteration = func() error {
		iterations++
		if iterations >= 30 {
			stop()
			return nil
		}
		if failNext = (failNext + 1) % 3; failNext != 0 {
			return errors.New("injected transient failure")
		}
		return nil
	}
	if err := runWithin(t, ctx, d, 10*time.Second); err != nil {
		t.Fatalf("transient failures stopped the dispatcher: %v", err)
	}
}
