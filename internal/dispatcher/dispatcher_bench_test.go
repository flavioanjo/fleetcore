package dispatcher

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/commandstore"
	"github.com/flavioanjo/fleetcore/internal/eventstore"
	"github.com/flavioanjo/fleetcore/internal/reliable"
	"github.com/flavioanjo/fleetcore/internal/security"
)

type nullTransport struct{ n int }

func (t *nullTransport) SendCommand(reliable.CommandDelivery) error { t.n++; return nil }

// BenchmarkCommandDelivery measures the server-side cost of one command
// from creation (authorization, Ed25519 signature, durable record) through
// promotion and one durably recorded delivery attempt.
func BenchmarkCommandDelivery(b *testing.B) {
	dir := b.TempDir()
	auth, _ := security.NewAuthority(1)
	cmds, err := commandstore.Open(context.Background(), filepath.Join(dir, "commands.db"), auth, commandstore.Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer cmds.Close()
	events, _ := eventstore.Open(context.Background(), filepath.Join(dir, "events.db"), eventstore.Options{})
	defer events.Close()
	tr := &nullTransport{}
	// A long backoff: unacknowledged commands are not retried during the run,
	// so exactly one delivery per command is measured.
	d, _ := New(Config{Commands: cmds, Events: events, Transports: []Named{{"null", tr}}, Batch: 1 << 20,
		InitialBackoff: time.Hour, MaxBackoff: time.Hour})
	p := commandstore.Principal{ID: "bench", Roles: []string{"fleet_viewer"}}
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := cmds.Create(ctx, commandstore.Request{RequestID: fmt.Sprint(i), Principal: p,
			DeviceID: fmt.Sprintf("DEV-%d", i%100), Type: reliable.CommandRequestPosition,
			Args: reliable.PositionArgs(time.Minute), TTL: time.Minute}); err != nil {
			b.Fatal(err)
		}
		if i%64 == 63 || i == b.N-1 {
			if _, err := d.Step(ctx); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()
	if tr.n != b.N {
		b.Fatalf("%d deliveries for %d commands", tr.n, b.N)
	}
}
