package main

import (
	"bytes"
	"context"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/eventstore"
	"github.com/flavioanjo/fleetcore/internal/protocol"
	"github.com/flavioanjo/fleetcore/internal/simulator"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// syncBuffer is a goroutine-safe log sink.
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

func TestUsageErrors(t *testing.T) {
	for _, args := range [][]string{
		{"-nope"},
		{"extra"},
		{"-log-level", "loud"},
		{"-log-format", "xml"},
		{"-max-datagram", "4", "-db", filepath.Join(t.TempDir(), "x.db")},
	} {
		var stderr syncBuffer
		if code := run(context.Background(), args, &stderr, nil); code != 2 {
			t.Errorf("%v: exit %d, want 2 (%s)", args, code, stderr.String())
		}
	}
}

// TestRestartKeepsDeduplicating runs the binary's entry point twice over the
// same database and replays the same packets into the second run.
func TestRestartKeepsDeduplicating(t *testing.T) {
	db := filepath.Join(t.TempDir(), "events.db")
	cfg := simulator.DefaultConfig()
	cfg.Vehicles, cfg.Samples = 2, 3
	var pkts [][]byte
	if err := simulator.Run(cfg, func(s telemetry.Sample) error {
		p, err := protocol.EncodeSample(s)
		pkts = append(pkts, p)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	for round := 1; round <= 2; round++ {
		var logs syncBuffer
		ctx, cancel := context.WithCancel(context.Background())
		ready := make(chan string, 1)
		done := make(chan int, 1)
		go func() {
			done <- run(ctx, []string{"-listen", "127.0.0.1:0", "-db", db, "-log-format", "text", "-batch-delay", "1ms"}, &logs, ready)
		}()
		var addr string
		select {
		case addr = <-ready:
		case code := <-done:
			t.Fatalf("round %d exited early with %d: %s", round, code, logs.String())
		}
		conn, err := net.Dial("udp", addr)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range pkts {
			conn.Write(p)
		}
		conn.Close()
		time.Sleep(300 * time.Millisecond) // let the pipeline store them
		cancel()
		if code := <-done; code != 0 {
			t.Fatalf("round %d exit %d: %s", round, code, logs.String())
		}
		out := logs.String()
		want := "inserted=18 duplicates=0"
		if round == 2 {
			want = "inserted=0 duplicates=18"
		}
		if !strings.Contains(out, "gateway stopped") || !strings.Contains(out, want) {
			t.Fatalf("round %d: final stats do not contain %q:\n%s", round, want, out)
		}
	}

	st, err := eventstore.Open(context.Background(), db, eventstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	recs, err := st.ReadAfter(context.Background(), 0, 100)
	if err != nil || len(recs) != 18 {
		t.Fatalf("%d stored events (%v), want 18", len(recs), err)
	}
}
