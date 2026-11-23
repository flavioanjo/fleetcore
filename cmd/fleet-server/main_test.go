package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/commandstore"
	"github.com/flavioanjo/fleetcore/internal/eventstore"
	"github.com/flavioanjo/fleetcore/internal/reliable"
	"github.com/flavioanjo/fleetcore/internal/security"
)

// syncBuffer is a log sink safe for the server's concurrent writers.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}
func (s *syncBuffer) String() string { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

// provision writes a key file and an authority into dir.
func provision(t *testing.T, dir string) *security.Authority {
	t.Helper()
	s, err := security.GenerateSecret()
	if err != nil {
		t.Fatal(err)
	}
	if err := security.WriteKeyFile(filepath.Join(dir, "keys.json"), []security.DeviceKey{{DeviceID: "TRK-1", KeyID: 1, Secret: s, State: security.KeyActive}}); err != nil {
		t.Fatal(err)
	}
	a, err := security.NewAuthority(1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := security.WriteAuthorityFile(filepath.Join(dir, "authority.json"), a); err != nil {
		t.Fatal(err)
	}
	return a
}

func serverArgs(dir string, extra ...string) []string {
	return append([]string{"-listen", "127.0.0.1:0,127.0.0.1:0", "-data", dir, "-keys", filepath.Join(dir, "keys.json"),
		"-authority", filepath.Join(dir, "authority.json"), "-stats-interval", "0", "-interval", "5ms"}, extra...)
}

type server struct {
	log  *syncBuffer
	done chan int
}

// start runs the server and waits until it is ready.
func start(t *testing.T, ctx context.Context, args []string) *server {
	t.Helper()
	s := &server{log: &syncBuffer{}, done: make(chan int, 1)}
	ready := make(chan []string, 1)
	go func() { s.done <- run(ctx, args, s.log, ready) }()
	select {
	case addrs := <-ready:
		if len(addrs) != 2 {
			t.Fatalf("gateways %v", addrs)
		}
	case code := <-s.done:
		t.Fatalf("exited %d before ready: %s", code, s.log.String())
	case <-time.After(10 * time.Second):
		t.Fatalf("not ready: %s", s.log.String())
	}
	return s
}

func (s *server) wait(t *testing.T) int {
	t.Helper()
	select {
	case code := <-s.done:
		return code
	case <-time.After(30 * time.Second):
		t.Fatalf("server did not stop: %s", s.log.String())
		return -1
	}
}

// assertReleased checks that every database file of dir was closed: no
// connection still holds a read snapshot (a WAL TRUNCATE checkpoint
// succeeds without waiting, on every platform), and on Windows the files
// can be deleted.
func assertReleased(t *testing.T, dir string) {
	t.Helper()
	for _, f := range []string{"events.db", "commands.db", "alerts.db"} {
		db, err := sql.Open("sqlite", filepath.Join(dir, f)+"?_pragma=busy_timeout(0)")
		if err != nil {
			t.Fatal(err)
		}
		// A write adds WAL frames, which a TRUNCATE checkpoint cannot reset
		// while any other connection holds an older snapshot. (The probe
		// table only exists in these throwaway test databases.)
		var busy, frames, done int
		_, err = db.Exec(`CREATE TABLE IF NOT EXISTS zz_probe (x); INSERT INTO zz_probe VALUES (1)`)
		if err == nil {
			err = db.QueryRow(`PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &frames, &done)
		}
		db.Close()
		if err != nil || busy != 0 {
			t.Fatalf("%s: a connection still holds it after the server stopped (checkpoint busy=%d, err=%v)", f, busy, err)
		}
	}
	if runtime.GOOS != "windows" {
		return
	}
	for _, f := range []string{"events.db", "commands.db", "alerts.db"} {
		if err := os.Remove(filepath.Join(dir, f)); err != nil {
			t.Fatalf("%s still open after the server stopped: %v", f, err)
		}
	}
}

func TestServerStartsAndStops(t *testing.T) {
	dir := t.TempDir()
	provision(t, dir)
	ctx, cancel := context.WithCancel(context.Background())
	s := start(t, ctx, serverArgs(dir))
	cancel()
	if code := s.wait(t); code != 0 {
		t.Fatalf("exit %d: %s", code, s.log.String())
	}
	if !strings.Contains(s.log.String(), "status=ok") {
		t.Fatalf("no final status: %s", s.log.String())
	}
	assertReleased(t, dir)
	// Missing key file: usage error, without echoing anything secret.
	var log bytes.Buffer
	if code := run(context.Background(), []string{"-data", dir, "-keys", filepath.Join(dir, "none.json")}, &log, nil); code != 2 {
		t.Fatalf("missing keys: exit %d", code)
	}
}

// injectFailure makes the named service fail (returning err, or panicking
// if err is nil) once trigger is closed, while every other service runs
// normally.
func injectFailure(t *testing.T, target string, trigger <-chan struct{}, err error) {
	t.Helper()
	wrapService = func(name string, run func(context.Context) error) func(context.Context) error {
		if name != target {
			return run
		}
		return func(ctx context.Context) error {
			inner, cancel := context.WithCancel(ctx)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- run(inner) }()
			select {
			case <-trigger:
				cancel()
				<-done // the real service has released what it held
				if err == nil {
					panic("injected panic in " + name)
				}
				return err
			case e := <-done:
				return e
			}
		}
	}
	t.Cleanup(func() { wrapService = nil })
}

// An essential service failing while the others are active stops the whole
// server with status 1, names the failed service, stops every other
// service and only then closes the stores.
func TestServerStopsWhenAnEssentialServiceFails(t *testing.T) {
	for _, tc := range []struct {
		service string
		err     error
	}{
		{"dispatcher", fmt.Errorf("injected: %w", errors.New("command store unavailable"))},
		{"gateway gw-2", errors.New("injected: socket failure")},
		{"alerting", nil}, // panic
		{"gateway gw-1", nil},
	} {
		t.Run(tc.service, func(t *testing.T) {
			dir := t.TempDir()
			provision(t, dir)
			trigger := make(chan struct{})
			injectFailure(t, tc.service, trigger, tc.err)
			s := start(t, context.Background(), serverArgs(dir))
			close(trigger) // every service is running now
			if code := s.wait(t); code != 1 {
				t.Fatalf("exit %d after %s failed: %s", code, tc.service, s.log.String())
			}
			log := s.log.String()
			quoted := func(v string) string {
				if strings.Contains(v, " ") {
					return `"` + v + `"`
				}
				return v
			}
			for _, want := range []string{"essential service failed", "service=" + quoted(tc.service), "status=failed"} {
				if !strings.Contains(log, want) {
					t.Fatalf("log lacks %q: %s", want, log)
				}
			}
			if n := strings.Count(log, "gateway stopped"); n != 2 {
				t.Fatalf("%d gateways stopped, want 2: %s", n, log)
			}
			for _, other := range []string{"dispatcher", "alerting", "gateway gw-1", "gateway gw-2"} {
				if other != tc.service && !strings.Contains(log, `msg="service stopped" service=`+quoted(other)) {
					t.Fatalf("%s was not stopped: %s", other, log)
				}
			}
			assertReleased(t, dir)
		})
	}
}

// seed fills dir with work for every service: critical events for the
// alerting consumer and commands for the dispatcher.
func seed(t *testing.T, dir string, auth *security.Authority) {
	t.Helper()
	ctx := context.Background()
	ev, err := eventstore.Open(ctx, filepath.Join(dir, "events.db"), eventstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	var evs []eventstore.ReliableEvent
	now := time.Now()
	for u := uint64(1); u <= 400; u++ {
		up, err := reliable.NewEvent(u, now, reliable.Panic{Trigger: reliable.PanicButton})
		if err != nil {
			t.Fatal(err)
		}
		re, err := eventstore.NewReliableEvent("TRK-1", up, 1, 1, now, "gw-1")
		if err != nil {
			t.Fatal(err)
		}
		evs = append(evs, re)
	}
	if _, err := ev.AppendReliable(ctx, evs); err != nil {
		t.Fatal(err)
	}
	if err := ev.Close(); err != nil {
		t.Fatal(err)
	}
	cs, err := commandstore.Open(ctx, filepath.Join(dir, "commands.db"), auth, commandstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 60; i++ {
		if _, _, err := cs.Create(ctx, commandstore.Request{RequestID: fmt.Sprint(i),
			Principal: commandstore.Principal{ID: "test", Roles: []string{"fleet_viewer"}},
			DeviceID:  fmt.Sprintf("TRK-%d", i%7+1), Type: reliable.CommandRequestPosition,
			Args: reliable.PositionArgs(time.Minute), TTL: 5 * time.Minute}); err != nil {
			t.Fatal(err)
		}
	}
	if err := cs.Close(); err != nil {
		t.Fatal(err)
	}
}

// Repeated start and stop, with the stop landing at varied points of alert
// processing and command dispatch: every run exits 0, leaves no delivery
// attempt half-recorded, and releases every SQLite handle.
func TestServerRepeatedStartStopReleasesStores(t *testing.T) {
	runs := 40
	if testing.Short() {
		runs = 6
	}
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < runs; i++ {
		dir := t.TempDir()
		auth := provision(t, dir)
		// Alternate loaded runs (the stop lands in deliveries and
		// notifications) with idle ones (it lands in the polling queries,
		// where the driver's cancellation defect lived).
		if i%2 == 0 {
			seed(t, dir, auth)
		}
		ctx, cancel := context.WithCancel(context.Background())
		s := start(t, ctx, serverArgs(dir))
		// Vary where the stop lands (not a synchronization: every outcome
		// asserted below must hold wherever it lands).
		time.Sleep(time.Duration(rng.Intn(40)) * time.Millisecond)
		cancel()
		if code := s.wait(t); code != 0 {
			t.Fatalf("run %d: exit %d: %s", i, code, s.log.String())
		}
		cs, err := commandstore.Open(context.Background(), filepath.Join(dir, "commands.db"), auth, commandstore.Options{})
		if err != nil {
			t.Fatal(err)
		}
		l, err := cs.List(context.Background(), commandstore.Filter{})
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range l {
			at, err := cs.Attempts(context.Background(), c.ID)
			if err != nil {
				t.Fatal(err)
			}
			for _, a := range at {
				if a.Outcome == "sending" {
					t.Fatalf("run %d: attempt %d of %s left half-recorded by an orderly stop", i, a.N, c.ID)
				}
			}
		}
		if err := cs.Close(); err != nil {
			t.Fatal(err)
		}
		assertReleased(t, dir)
	}
}
