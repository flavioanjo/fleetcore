package gateway

import (
	"context"
	"errors"
	"net/netip"
	"path/filepath"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/eventstore"
	"github.com/flavioanjo/fleetcore/internal/reliable"
	"github.com/flavioanjo/fleetcore/internal/security"
)

var rt0 = time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)

// testDevice builds v2 frames the way a device does.
type testDevice struct {
	id    string
	key   security.Secret
	keyID uint32
	gen   uint64
	ctr   uint64
	addr  netip.AddrPort
}

func newTestDevice(t testing.TB, ks *security.MemoryKeyStore, id string, port uint16) *testDevice {
	k, err := security.GenerateSecret()
	if err != nil {
		t.Fatal(err)
	}
	if err := ks.Put(security.DeviceKey{DeviceID: id, KeyID: 1, Secret: k}); err != nil {
		t.Fatal(err)
	}
	return &testDevice{id: id, key: k, keyID: 1, gen: 1, addr: netip.AddrPortFrom(netip.MustParseAddr("192.0.2.1"), port)}
}

func (d *testDevice) frame(t testing.TB, typ reliable.MessageType, payload []byte) []byte {
	t.Helper()
	d.ctr++
	pkt, err := reliable.Seal(reliable.Header{Type: typ, KeyID: d.keyID, SessionGen: d.gen, Counter: d.ctr, DeviceID: d.id}, payload, &d.key)
	if err != nil {
		t.Fatal(err)
	}
	return pkt
}

func (d *testDevice) event(t testing.TB, uid uint64, b reliable.Body) ([]byte, reliable.Fingerprint) {
	t.Helper()
	up, err := reliable.NewEvent(uid, rt0, b)
	if err != nil {
		t.Fatal(err)
	}
	p, _ := up.Encode()
	return d.frame(t, reliable.TypeEventUpload, p), reliable.EventFingerprint(d.id, p)
}

func (d *testDevice) in(pkt []byte) Inbound { return Inbound{Data: pkt, From: d.addr, At: rt0} }

// openAck authenticates a downlink frame as the device would.
func (d *testDevice) openAck(t testing.TB, o Outbound) reliable.EventAck {
	t.Helper()
	f, err := reliable.OpenWithKey(o.Data, security.Downlink, d.id, d.keyID, &d.key)
	if err != nil {
		t.Fatalf("device cannot authenticate the ack: %v", err)
	}
	if f.Type != reliable.TypeEventAck || f.SessionGen != d.gen {
		t.Fatalf("ack type %v session %d", f.Type, f.SessionGen)
	}
	if o.To != d.addr {
		t.Fatalf("ack sent to %v, device at %v", o.To, d.addr)
	}
	a, err := reliable.DecodeEventAck(f.Payload)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

type relFixture struct {
	ks    *security.MemoryKeyStore
	store *eventstore.Store
	path  string
}

func newRelFixture(t *testing.T) *relFixture {
	path := filepath.Join(t.TempDir(), "events.db")
	st, err := eventstore.Open(context.Background(), path, eventstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return &relFixture{ks: security.NewMemoryKeyStore(), store: st, path: path}
}

func (f *relFixture) core(t *testing.T, id string, hooks *ReliableHooks) *Reliable {
	r, err := NewReliable(ReliableConfig{Keys: f.ks, Store: f.store, GatewayID: id, Hooks: hooks, RetryBackoff: time.Millisecond},
		func() time.Time { return rt0 }, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestReliableAcksOnlyAfterCommit(t *testing.T) {
	f := newRelFixture(t)
	gw := f.core(t, "gw-a", nil)
	dev := newTestDevice(t, f.ks, "DEV-1", 4000)
	pkt, fp := dev.event(t, 7, reliable.Panic{Trigger: reliable.PanicButton})
	out := gw.HandleBatch(context.Background(), []Inbound{dev.in(pkt)})
	if len(out) != 1 {
		t.Fatalf("%d answers", len(out))
	}
	a := dev.openAck(t, out[0])
	if a.UID != 7 || a.Fingerprint != fp || a.Status != reliable.AckStored || a.Position == 0 {
		t.Fatalf("ack %+v", a)
	}
	rec, err := f.store.GetReliable(context.Background(), "DEV-1", 7)
	if err != nil || rec.Position != a.Position || rec.Fingerprint != fp || rec.Gateway != "gw-a" {
		t.Fatalf("acked event not stored as acked: %+v %v", rec, err)
	}
	// Retransmission (new counter, same content) is acknowledged as duplicate.
	again, _ := dev.event(t, 7, reliable.Panic{Trigger: reliable.PanicButton})
	if a := dev.openAck(t, gw.HandleBatch(context.Background(), []Inbound{dev.in(again)})[0]); a.Status != reliable.AckDuplicate || a.Position != rec.Position {
		t.Fatalf("retransmission ack %+v", a)
	}
	if gw.Stats().Critical.Load() != 2 {
		t.Fatalf("critical counter %d", gw.Stats().Critical.Load())
	}
}

func TestReliableRejectsForgedReplayedAndStale(t *testing.T) {
	f := newRelFixture(t)
	a, b := f.core(t, "gw-a", nil), f.core(t, "gw-b", nil)
	dev := newTestDevice(t, f.ks, "DEV-1", 4000)
	other := newTestDevice(t, f.ks, "DEV-2", 4001)
	ctx := context.Background()

	// Forged: tampered byte → no answer, nothing stored.
	pkt, _ := dev.event(t, 1, reliable.Diagnostic{Code: 1})
	bad := append([]byte(nil), pkt...)
	bad[len(bad)-40] ^= 1
	if out := a.HandleBatch(ctx, []Inbound{dev.in(bad)}); len(out) != 0 || a.Stats().RejectedBy(reliable.CodeBadMAC) != 1 {
		t.Fatalf("forged frame answered (%d) or not counted", len(out))
	}
	// A frame for DEV-1 keyed by DEV-2's key.
	impostor := &testDevice{id: "DEV-1", key: other.key, keyID: 1, gen: 1, addr: other.addr}
	ip, _ := impostor.event(t, 2, reliable.Panic{Trigger: reliable.PanicButton})
	if out := a.HandleBatch(ctx, []Inbound{impostor.in(ip)}); len(out) != 0 {
		t.Fatal("frame keyed by another device answered")
	}
	if _, err := f.store.GetReliable(ctx, "DEV-1", 2); !errors.Is(err, eventstore.ErrNotFound) {
		t.Fatal("impostor event stored")
	}

	// Exact replay of an accepted frame (same counter) is dropped.
	a.HandleBatch(ctx, []Inbound{dev.in(pkt)})
	if out := a.HandleBatch(ctx, []Inbound{dev.in(pkt)}); len(out) != 0 || a.Stats().RejectedBy(reliable.CodeReplay) != 1 {
		t.Fatal("replayed frame accepted")
	}

	// The device reboots (session 2) through gateway B; session 1 frames
	// are then stale on both gateways.
	old, _ := dev.event(t, 3, reliable.Diagnostic{Code: 1})
	dev.gen, dev.ctr = 2, 0
	fresh, _ := dev.event(t, 4, reliable.Diagnostic{Code: 1})
	if out := b.HandleBatch(ctx, []Inbound{dev.in(fresh)}); len(out) != 1 {
		t.Fatal("new session not accepted")
	}
	dev.gen = 1
	for name, gw := range map[string]*Reliable{"gw-a": a, "gw-b": b} {
		if out := gw.HandleBatch(ctx, []Inbound{dev.in(old)}); len(out) != 0 {
			t.Fatalf("%s accepted a frame of an older session", name)
		}
	}
	if a.Stats().RejectedBy(reliable.CodeStaleSession) != 1 || b.Stats().RejectedBy(reliable.CodeStaleSession) != 1 {
		t.Fatal("stale sessions not counted")
	}
}

func TestReliableInvalidEventGetsSignedNack(t *testing.T) {
	f := newRelFixture(t)
	gw := f.core(t, "gw-a", nil)
	dev := newTestDevice(t, f.ks, "DEV-1", 4000)
	// Telemetry sample whose EventID differs from the UID: authenticated but invalid.
	up, _ := reliable.NewEvent(9, rt0, reliable.Diagnostic{Code: 1})
	up.Type = reliable.EventBlockState // body does not match the type
	p, _ := up.Encode()
	out := gw.HandleBatch(context.Background(), []Inbound{dev.in(dev.frame(t, reliable.TypeEventUpload, p))})
	a := dev.openAck(t, out[0])
	if a.Status != reliable.AckInvalid || a.UID != 9 || a.Fingerprint != reliable.EventFingerprint("DEV-1", p) {
		t.Fatalf("nack %+v", a)
	}
	if _, err := f.store.GetReliable(context.Background(), "DEV-1", 9); !errors.Is(err, eventstore.ErrNotFound) {
		t.Fatal("invalid event stored")
	}
}

func TestReliableStorageFailureNeverAcksSuccess(t *testing.T) {
	f := newRelFixture(t)
	failing := f.core(t, "gw-a", &ReliableHooks{BeforeCommit: func() error { return errors.New("disk full") }})
	dev := newTestDevice(t, f.ks, "DEV-1", 4000)
	pkt, _ := dev.event(t, 1, reliable.Panic{Trigger: reliable.PanicButton})
	out := failing.HandleBatch(context.Background(), []Inbound{dev.in(pkt)})
	if a := dev.openAck(t, out[0]); a.Status != reliable.AckRetry || a.Status.Success() {
		t.Fatalf("ack on failed commit: %+v", a)
	}
	if _, err := f.store.GetReliable(context.Background(), "DEV-1", 1); !errors.Is(err, eventstore.ErrNotFound) {
		t.Fatal("event stored despite the failure")
	}
	if failing.Stats().StoreRetries.Load() != 3 {
		t.Fatalf("retries %d", failing.Stats().StoreRetries.Load())
	}
}

// Crash after commit, before ACK: the retransmission reaches the other
// gateway, which acknowledges the committed event as a duplicate.
func TestReliableCrashAfterCommitRecoveredByOtherGateway(t *testing.T) {
	f := newRelFixture(t)
	crashing := f.core(t, "gw-a", &ReliableHooks{AfterCommit: func() bool { return false }})
	other := f.core(t, "gw-b", nil)
	dev := newTestDevice(t, f.ks, "DEV-1", 4000)
	pkt, _ := dev.event(t, 1, reliable.Panic{Trigger: reliable.PanicButton})
	if out := crashing.HandleBatch(context.Background(), []Inbound{dev.in(pkt)}); len(out) != 0 {
		t.Fatal("ack sent despite the simulated crash")
	}
	rec, err := f.store.GetReliable(context.Background(), "DEV-1", 1)
	if err != nil {
		t.Fatal("event not committed before the crash")
	}
	again, _ := dev.event(t, 1, reliable.Panic{Trigger: reliable.PanicButton})
	a := dev.openAck(t, other.HandleBatch(context.Background(), []Inbound{dev.in(again)})[0])
	if a.Status != reliable.AckDuplicate || a.Position != rec.Position {
		t.Fatalf("ack after crash %+v", a)
	}
	n, _ := f.store.CountReliable(context.Background())
	if n[reliable.EventPanic] != 1 {
		t.Fatalf("%d panic rows", n[reliable.EventPanic])
	}
}

func TestReliableCommandPacketUsesCurrentSession(t *testing.T) {
	f := newRelFixture(t)
	gw := f.core(t, "gw-a", nil)
	dev := newTestDevice(t, f.ks, "DEV-1", 4000)
	auth, _ := security.NewAuthority(1)
	cmd := reliable.Command{ID: reliable.CommandID{1}, DeviceID: "DEV-1", Type: reliable.CommandBlock,
		IssuedAt: rt0, ExpiresAt: rt0.Add(time.Minute), Principal: "ops"}
	d := reliable.CommandDelivery{Command: cmd, Attempt: 1, AttemptSentAt: rt0, AttemptNotAfter: rt0.Add(30 * time.Second),
		AuthorityKeyID: 1, Signature: auth.Sign(cmd.Statement())}
	if _, err := gw.CommandPacket(d); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("before any frame: %v", err)
	}
	dev.gen = 3
	hb, _ := reliable.Heartbeat{DeviceTime: rt0}.Encode()
	gw.HandleBatch(context.Background(), []Inbound{dev.in(dev.frame(t, reliable.TypeHeartbeat, hb))})
	o, err := gw.CommandPacket(d)
	if err != nil {
		t.Fatal(err)
	}
	fr, err := reliable.OpenWithKey(o.Data, security.Downlink, "DEV-1", 1, &dev.key)
	if err != nil || fr.SessionGen != 3 || fr.Type != reliable.TypeCommandDelivery || o.To != dev.addr {
		t.Fatalf("delivery %+v to %v: %v", fr.Header, o.To, err)
	}
	// The delivery is useless to another device: it does not authenticate
	// under any other key.
	other := newTestDevice(t, f.ks, "DEV-2", 4001)
	if _, err := reliable.OpenWithKey(o.Data, security.Downlink, "DEV-1", 1, &other.key); !errors.Is(err, reliable.ErrBadMAC) {
		t.Fatalf("delivery under another key: %v", err)
	}
}
