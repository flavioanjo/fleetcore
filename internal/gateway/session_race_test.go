package gateway

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/eventstore"
	"github.com/flavioanjo/fleetcore/internal/reliable"
	"github.com/flavioanjo/fleetcore/internal/security"
)

// The cross-gateway interleaving, forced deterministically through the
// BeforeCommit hook (no sleeps):
//
//  1. gateway A authenticates and admits an event frame of session N;
//  2. before A's insert transaction, gateway B durably records session N+1
//     for the same device;
//  3. A then commits the event of session N.
//
// The documented invariant (docs/protocol-v2.md §Sessions) is that a frame
// of a generation lower than the device's durable high-water mark is
// rejected by every gateway. The insert transaction therefore re-checks the
// session atomically: the event of session N is not stored, A answers
// nothing (stale_session), the high-water mark stays at N+1 and A does not
// route commands to the device's old session.
func TestReliableSessionAdvancedBetweenAdmitAndCommit(t *testing.T) {
	f := newRelFixture(t)
	ctx := context.Background()
	dev := newTestDevice(t, f.ks, "DEV-1", 4000)
	var b *Reliable
	interleaved := false
	a := f.core(t, "gw-a", &ReliableHooks{BeforeCommit: func() error {
		if interleaved {
			return nil
		}
		interleaved = true
		// Step 2: the device has rebooted and is heard by gateway B.
		saved := *dev
		dev.gen, dev.ctr = dev.gen+1, 0
		hb, _ := reliable.Heartbeat{DeviceTime: rt0}.Encode()
		b.HandleBatch(ctx, []Inbound{dev.in(dev.frame(t, reliable.TypeHeartbeat, hb))})
		*dev = saved
		return nil
	}})
	b = f.core(t, "gw-b", nil)

	// Session 1 is known to both gateways' store before the race.
	hb, _ := reliable.Heartbeat{DeviceTime: rt0}.Encode()
	a.HandleBatch(ctx, []Inbound{dev.in(dev.frame(t, reliable.TypeHeartbeat, hb))})

	// Step 1 and 3: an event of session 1 admitted by A.
	pkt, _ := dev.event(t, 10, reliable.Panic{Trigger: reliable.PanicButton})
	out := a.HandleBatch(ctx, []Inbound{dev.in(pkt)})
	if !interleaved {
		t.Fatal("the interleaving was not exercised")
	}
	if st, err := f.store.DeviceSession(ctx, "DEV-1"); err != nil || st.SessionGen != 2 {
		t.Fatalf("high-water mark %+v, %v (must stay at the newer session)", st, err)
	}
	if _, err := f.store.GetReliable(ctx, "DEV-1", 10); !errors.Is(err, eventstore.ErrNotFound) {
		t.Fatalf("event of session 1 stored after session 2 was recorded: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("%d answers to a frame whose session became stale", len(out))
	}
	if a.Stats().RejectedBy(reliable.CodeStaleSession) != 1 {
		t.Fatalf("stale_session rejections %d", a.Stats().RejectedBy(reliable.CodeStaleSession))
	}

	// Gateway A must not seal commands for the old session.
	auth, _ := security.NewAuthority(1)
	cmd := reliable.Command{ID: reliable.CommandID{1}, DeviceID: "DEV-1", Type: reliable.CommandBlock,
		IssuedAt: rt0, ExpiresAt: rt0.Add(time.Minute), Principal: "ops"}
	d := reliable.CommandDelivery{Command: cmd, Attempt: 1, AttemptSentAt: rt0, AttemptNotAfter: rt0.Add(30 * time.Second),
		AuthorityKeyID: 1, Signature: auth.Sign(cmd.Statement())}
	if _, err := a.CommandPacket(d); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("gateway A sealed a delivery for a superseded session: %v", err)
	}

	// The device retransmits the still-pending event in its new session:
	// stored once and acknowledged, through either gateway.
	dev.gen, dev.ctr = 2, 100
	again, fp := dev.event(t, 10, reliable.Panic{Trigger: reliable.PanicButton})
	out = a.HandleBatch(ctx, []Inbound{dev.in(again)})
	if len(out) != 1 {
		t.Fatalf("%d answers to the retransmission", len(out))
	}
	if ack := dev.openAck(t, out[0]); ack.Status != reliable.AckStored || ack.Fingerprint != fp {
		t.Fatalf("retransmission ack %+v", ack)
	}
	rec, err := f.store.GetReliable(ctx, "DEV-1", 10)
	if err != nil || rec.SessionGen != 2 {
		t.Fatalf("stored %+v, %v", rec, err)
	}
}
