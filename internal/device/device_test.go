package device_test

import (
	"context"
	"crypto/ed25519"
	"errors"
	"io"
	"log/slog"
	mrand "math/rand"
	"net/netip"
	"path/filepath"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/device"
	"github.com/flavioanjo/fleetcore/internal/eventstore"
	"github.com/flavioanjo/fleetcore/internal/gateway"
	"github.com/flavioanjo/fleetcore/internal/reliable"
	"github.com/flavioanjo/fleetcore/internal/security"
	"github.com/flavioanjo/fleetcore/internal/simhw"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

var t0 = time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)

type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

type capture struct {
	pkts    [][]byte
	offline bool
}

func (c *capture) Send(p []byte) error {
	if c.offline {
		return errors.New("offline")
	}
	c.pkts = append(c.pkts, append([]byte(nil), p...))
	return nil
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type rig struct {
	t      *testing.T
	dir    string
	clk    *clock
	veh    *simhw.Vehicle
	keys   *security.MemoryKeyStore
	key    security.Secret
	auth   *security.Authority
	events *eventstore.Store
	gw     *gateway.Reliable
	net    *capture
	dev    *device.Device
	cfg    func(*device.Config)
}

func newRig(t *testing.T, mutate func(*device.Config)) *rig {
	r := &rig{t: t, dir: t.TempDir(), clk: &clock{t: t0}, net: &capture{}, keys: security.NewMemoryKeyStore(), cfg: mutate}
	r.veh = simhw.New(r.clk.now)
	var err error
	if r.key, err = security.GenerateSecret(); err != nil {
		t.Fatal(err)
	}
	r.keys.Put(security.DeviceKey{DeviceID: "DEV-1", KeyID: 1, Secret: r.key})
	if r.auth, err = security.NewAuthority(1); err != nil {
		t.Fatal(err)
	}
	if r.events, err = eventstore.Open(context.Background(), filepath.Join(r.dir, "events.db"), eventstore.Options{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.events.Close() })
	if r.gw, err = gateway.NewReliable(gateway.ReliableConfig{Keys: r.keys, Store: r.events, GatewayID: "gw-a"}, r.clk.now, quiet()); err != nil {
		t.Fatal(err)
	}
	r.start()
	return r
}

// start opens and boots the device process (after a crash or reboot).
func (r *rig) start() {
	r.t.Helper()
	if r.dev != nil {
		r.dev.Close()
	}
	cfg := device.Config{DeviceID: "DEV-1", KeyID: 1, Key: r.key,
		Authority: security.NewVerifier(map[uint32]ed25519.PublicKey{1: r.auth.Public()}),
		StorePath: filepath.Join(r.dir, "device.db"), Transport: r.net, Hardware: r.veh,
		Now: r.clk.now, Rand: mrand.New(mrand.NewSource(1)), Logger: quiet()}
	if r.cfg != nil {
		r.cfg(&cfg)
	}
	d, err := device.Open(context.Background(), cfg)
	if err != nil {
		r.t.Fatal(err)
	}
	if err := d.Boot(context.Background()); err != nil {
		r.t.Fatal(err)
	}
	r.dev = d
	r.t.Cleanup(func() { d.Close() })
}

var devAddr = netip.MustParseAddrPort("192.0.2.10:5000")

// exchange: device transmits what is due; the gateway processes it; its
// answers are delivered back unless dropAcks.
func (r *rig) exchange(dropAcks bool) []gateway.Outbound {
	r.t.Helper()
	if _, err := r.dev.Tick(context.Background()); err != nil {
		r.t.Fatal(err)
	}
	var in []gateway.Inbound
	for _, p := range r.net.pkts {
		in = append(in, gateway.Inbound{Data: p, From: devAddr, At: r.clk.now()})
	}
	r.net.pkts = nil
	out := r.gw.HandleBatch(context.Background(), in)
	if !dropAcks {
		for _, o := range out {
			r.dev.HandleDownlink(context.Background(), o.Data) //nolint:errcheck
		}
	}
	return out
}

func (r *rig) state(uid uint64) string {
	s, _, ok := r.dev.EventState(context.Background(), uid)
	if !ok {
		return "absent"
	}
	return s
}

func (r *rig) panicEvent() uint64 {
	r.t.Helper()
	uid, err := r.dev.Panic(context.Background())
	if err != nil {
		r.t.Fatal(err)
	}
	return uid
}

func TestEventPendingUntilValidAck(t *testing.T) {
	r := newRig(t, nil)
	ctx := context.Background()
	r.exchange(false) // boot state reports
	uid := r.panicEvent()

	// Lost ACK: still pending.
	out := r.exchange(true)
	if r.state(uid) != "pending" {
		t.Fatalf("state after lost ack: %s", r.state(uid))
	}
	// A forged ACK (wrong key) is rejected.
	var fake security.Secret
	fake[0] = 1
	a, _ := reliable.EventAck{UID: uid, Status: reliable.AckStored, Position: 1}.Encode()
	forged, _ := reliable.Seal(reliable.Header{Type: reliable.TypeEventAck, KeyID: 1, SessionGen: r.dev.SessionGen(), Counter: 1, DeviceID: "DEV-1"}, a, &fake)
	if err := r.dev.HandleDownlink(ctx, forged); !errors.Is(err, reliable.ErrBadMAC) || r.state(uid) != "pending" {
		t.Fatalf("forged ack: %v, state %s", err, r.state(uid))
	}
	// An authentic ACK for the same UID but other content does not complete it.
	wrong, _ := reliable.EventAck{UID: uid, Fingerprint: reliable.Fingerprint{1}, Status: reliable.AckStored, Position: 1}.Encode()
	wp, _ := reliable.Seal(reliable.Header{Type: reliable.TypeEventAck, KeyID: 1, SessionGen: r.dev.SessionGen(), Counter: 2, DeviceID: "DEV-1"}, wrong, &r.key)
	r.dev.HandleDownlink(ctx, wp)
	if r.state(uid) != "pending" || r.dev.Metrics().AcksUnmatched != 1 {
		t.Fatalf("mismatched ack completed the event: %s", r.state(uid))
	}
	// Reboot: the ACK of the previous session (delivered late) is stale.
	r.start()
	if err := r.dev.HandleDownlink(ctx, out[len(out)-1].Data); !errors.Is(err, reliable.ErrStaleSession) || r.state(uid) != "pending" {
		t.Fatalf("old-session ack: %v, state %s", err, r.state(uid))
	}
	// Retransmission after reboot → duplicate at the server → acked.
	r.exchange(false)
	if r.state(uid) != "acked" {
		t.Fatalf("after retransmission: %s", r.state(uid))
	}
	n, _ := r.events.CountReliable(ctx)
	if n[reliable.EventPanic] != 1 {
		t.Fatalf("%d panic rows at the server", n[reliable.EventPanic])
	}
	if r.gw.Stats().Duplicates.Load() == 0 {
		t.Fatal("retransmission not classified as duplicate")
	}
}

func TestCrashBeforePersistenceDoesNotQueue(t *testing.T) {
	fail := true
	r := newRig(t, func(c *device.Config) {
		c.Hooks = &device.Hooks{BeforeEnqueueCommit: func() error {
			if fail {
				return errors.New("power loss")
			}
			return nil
		}}
	})
	if _, err := r.dev.Panic(context.Background()); err == nil {
		t.Fatal("enqueue succeeded despite the crash")
	}
	fail = false
	r.start()
	q, _ := r.dev.Queue(context.Background())
	if q.Pending[reliable.ClassEmergency] != 0 {
		t.Fatal("event queued although its transaction never committed")
	}
	// After persistence, a crash does not lose it.
	uid := r.panicEvent()
	r.start()
	if r.state(uid) != "pending" {
		t.Fatalf("queued event after crash: %s", r.state(uid))
	}
}

func TestUIDsAndSessionsNeverRepeat(t *testing.T) {
	r := newRig(t, func(c *device.Config) { c.InitialSequence = ^telemetry.Sequence(0) - 1 })
	ctx := context.Background()
	seen := map[uint64]bool{}
	gens := map[uint64]bool{}
	for boot := 0; boot < 4; boot++ {
		if gens[r.dev.SessionGen()] {
			t.Fatal("session generation reused")
		}
		gens[r.dev.SessionGen()] = true
		for i := 0; i < 4; i++ { // crosses the v1 sequence wrap each boot
			s := telemetry.Sample{Source: telemetry.SourceGPS, Quality: telemetry.QualityGood,
				Distance: telemetry.Some(telemetry.Distance{Kind: telemetry.DistanceDelta, Value: 100})}
			uid, err := r.dev.EnqueueTelemetry(ctx, s)
			if err != nil {
				t.Fatal(err)
			}
			if seen[uid] {
				t.Fatalf("uid %d reused", uid)
			}
			seen[uid] = true
		}
		r.clk.advance(time.Second)
		r.start()
	}
	for i := 0; i < 6; i++ {
		r.exchange(false)
		r.clk.advance(20 * time.Second)
	}
	// All 16 samples are stored once in the v1 table, with identities
	// unique across the reboots and the sequence wrap.
	recs, _ := r.events.ReadAfter(ctx, 0, 100)
	if len(recs) != 16 {
		t.Fatalf("%d v1 rows, want 16", len(recs))
	}
	keys := map[eventstore.Key]bool{}
	for _, rec := range recs {
		k := eventstore.Key{DeviceID: rec.Sample.DeviceID, BootID: rec.Sample.BootID, EventID: rec.Sample.EventID}
		if keys[k] {
			t.Fatalf("v1 identity %s repeated", k)
		}
		keys[k] = true
	}
}

func TestCapacityReservesRoomForCriticalEvents(t *testing.T) {
	r := newRig(t, func(c *device.Config) { c.Capacity, c.CriticalReserve = 10, 4 })
	ctx := context.Background()
	q, _ := r.dev.Queue(ctx)
	boot := q.PendingTotal() // boot state reports
	for i := boot; i < 6; i++ {
		if _, err := r.dev.Enqueue(ctx, func(uint64, time.Time) (reliable.Body, error) { return reliable.Diagnostic{Code: 1}, nil }); err != nil {
			t.Fatalf("routine %d: %v", i, err)
		}
	}
	if _, err := r.dev.Enqueue(ctx, func(uint64, time.Time) (reliable.Body, error) { return reliable.Diagnostic{Code: 1}, nil }); !errors.Is(err, device.ErrQueueFull) {
		t.Fatalf("routine beyond its share: %v", err)
	}
	for i := 0; i < 4; i++ {
		r.panicEvent() // the reserve
	}
	if _, err := r.dev.Panic(ctx); !errors.Is(err, device.ErrQueueFull) {
		t.Fatalf("panic beyond capacity: %v", err)
	}
	q, _ = r.dev.Queue(ctx)
	if q.PendingTotal() != 10 || r.dev.Metrics().QueueFull != 2 {
		t.Fatalf("queue %+v, full %d (nothing may be evicted)", q, r.dev.Metrics().QueueFull)
	}
}

func TestPriorityWithoutStarvation(t *testing.T) {
	r := newRig(t, func(c *device.Config) { c.MaxSendsPerTick, c.RoutineShare = 4, 4 })
	ctx := context.Background()
	r.exchange(false)
	var routine []uint64
	for i := 0; i < 8; i++ {
		u, _ := r.dev.Enqueue(ctx, func(uint64, time.Time) (reliable.Body, error) { return reliable.Diagnostic{Code: 1}, nil })
		routine = append(routine, u)
	}
	var panics []uint64
	for i := 0; i < 8; i++ {
		panics = append(panics, r.panicEvent())
	}
	r.exchange(false) // 4 slots: 3 critical, 1 routine
	acked := func(us []uint64) int {
		n := 0
		for _, u := range us {
			if r.state(u) == "acked" {
				n++
			}
		}
		return n
	}
	if acked(panics) != 3 || acked(routine) != 1 {
		t.Fatalf("first tick: %d panics, %d routine acked", acked(panics), acked(routine))
	}
	if r.state(panics[0]) != "acked" || r.state(routine[0]) != "acked" {
		t.Fatal("not in order within the class")
	}
	r.exchange(false)
	if acked(panics) != 6 || acked(routine) != 2 {
		t.Fatalf("second tick: %d panics, %d routine", acked(panics), acked(routine))
	}
}

func TestBackoffIsBoundedAndCriticalNeverExpires(t *testing.T) {
	r := newRig(t, func(c *device.Config) {
		c.InitialBackoff, c.MaxBackoff, c.CriticalMaxBackoff, c.RoutineMaxAge = time.Second, 8*time.Second, 4*time.Second, time.Hour
	})
	ctx := context.Background()
	r.exchange(false)
	p := r.panicEvent()
	d, _ := r.dev.Enqueue(ctx, func(uint64, time.Time) (reliable.Body, error) { return reliable.Diagnostic{Code: 1}, nil })
	// ACKs never arrive for an hour (lost on the way back).
	for elapsed := time.Duration(0); elapsed < time.Hour; elapsed += time.Second {
		r.exchange(true)
		r.clk.advance(time.Second)
	}
	_, attempts, _ := r.dev.EventState(ctx, p)
	// Backoff capped at 4s ±20%, ticks every second: one attempt every 4-5 s
	// after the ramp, so 720-900 in an hour. Bounded rate; never gives up.
	if attempts < 700 || attempts > 950 {
		t.Fatalf("critical attempts over 1h: %d", attempts)
	}
	_, dAttempts, _ := r.dev.EventState(ctx, d)
	if dAttempts > 3600/7+10 { // routine cap 8s ±20%
		t.Fatalf("routine attempts over 1h: %d", dAttempts)
	}
	if r.state(p) != "pending" {
		t.Fatalf("critical event state %s", r.state(p))
	}
	r.exchange(false) // the hour has passed: routine expires, critical is delivered
	if r.state(d) != "expired" || r.dev.Metrics().Expired != 1 {
		t.Fatalf("routine event %s after its max age", r.state(d))
	}
	for i := 0; i < 6 && r.state(p) != "acked"; i++ { // its next attempt is at most ~4.8s away
		r.clk.advance(time.Second)
		r.exchange(false)
	}
	if r.state(p) != "acked" {
		t.Fatalf("critical event %s once ACKs get through (critical events never expire)", r.state(p))
	}
}

// Regression: the v1 telemetry sequence used to advance inside the body
// builder, before the capacity check, the encoding and the commit, so
// every rejected enqueue left a gap in the transmitted sequence. Only a
// durably queued sample may consume a sequence number.
func TestTelemetrySequenceAdvancesOnlyOnDurableEnqueue(t *testing.T) {
	failCommit := false
	r := newRig(t, func(c *device.Config) {
		c.Capacity, c.CriticalReserve = 5, 1 // routine limit 4; boot queues 2 state reports
		c.InitialSequence = ^telemetry.Sequence(0) - 1
		c.Hooks = &device.Hooks{BeforeEnqueueCommit: func() error {
			if failCommit {
				return errors.New("storage failure")
			}
			return nil
		}}
	})
	ctx := context.Background()
	good := func() telemetry.Sample {
		return telemetry.Sample{Source: telemetry.SourceGPS, Quality: telemetry.QualityGood,
			Distance: telemetry.Some(telemetry.Distance{Kind: telemetry.DistanceDelta, Value: 100})}
	}
	mustQueue := func(what string) {
		t.Helper()
		if _, err := r.dev.EnqueueTelemetry(ctx, good()); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	mustQueue("first sample")
	bad := good()
	bad.Source = telemetry.Source(250) // invalid body: rejected by the encoder
	if _, err := r.dev.EnqueueTelemetry(ctx, bad); err == nil {
		t.Fatal("invalid sample queued")
	}
	mustQueue("after an invalid body")
	if _, err := r.dev.EnqueueTelemetry(ctx, good()); !errors.Is(err, device.ErrQueueFull) {
		t.Fatalf("full queue: %v", err)
	}
	r.exchange(false) // drains the queue
	failCommit = true
	if _, err := r.dev.EnqueueTelemetry(ctx, good()); err == nil {
		t.Fatal("enqueue succeeded although its commit failed")
	}
	failCommit = false
	mustQueue("retry after the failed commit") // crosses the 2^32 wrap
	mustQueue("after the wrap")
	r.clk.advance(time.Second)
	r.exchange(false)

	recs, err := r.events.ReadAfter(ctx, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	max := ^telemetry.Sequence(0)
	want := []telemetry.Sequence{max - 1, max, 0, 1}
	if len(recs) != len(want) {
		t.Fatalf("%d samples stored, want %d", len(recs), len(want))
	}
	for i, rec := range recs {
		if rec.Sample.Sequence != want[i] {
			t.Fatalf("sample %d has sequence %d, want %d (a rejected enqueue left a gap)", i, rec.Sample.Sequence, want[i])
		}
	}
}
