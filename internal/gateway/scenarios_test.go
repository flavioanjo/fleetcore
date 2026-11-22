package gateway

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"net"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/eventstore"
	"github.com/flavioanjo/fleetcore/internal/protocol"
	"github.com/flavioanjo/fleetcore/internal/sessions"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

func openSQLite(t testing.TB) *eventstore.Store {
	t.Helper()
	st, err := eventstore.Open(context.Background(), filepath.Join(t.TempDir(), "events.db"), eventstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

// reseal recomputes the CRC-32C of a modified packet so that it reaches the
// checks behind the checksum.
func reseal(p []byte) []byte {
	end := len(p) - protocol.ChecksumSize
	binary.BigEndian.PutUint32(p[end:], crc32.Checksum(p[:end], crc32.MakeTable(crc32.Castagnoli)))
	return p
}

func canSample(dev string, boot telemetry.BootID, eid telemetry.EventID, seq telemetry.Sequence, mm telemetry.Millimetres) telemetry.Sample {
	return telemetry.Sample{
		DeviceID: telemetry.DeviceID(dev), EventID: eid, BootID: boot, Sequence: seq,
		DeviceTime: clock.Add(-time.Hour + time.Duration(seq)*time.Second),
		Source:     telemetry.SourceCAN, Quality: telemetry.QualityGood,
		Distance: telemetry.Some(telemetry.Distance{Kind: telemetry.DistanceCumulative, Value: mm}),
	}
}

func TestRejectsMalformedPackets(t *testing.T) {
	r := start(t, baseConfig(), newMemStore(), sessions.New(sessions.Config{}))
	valid := encode(t, canSample("mal-1", 1, 1, 1, 1000))
	mutate := func(f func(p []byte) []byte) []byte { return f(append([]byte(nil), valid...)) }

	var bad [][]byte
	// Every truncation (non-empty) of a valid packet.
	for n := 1; n < len(valid); n++ {
		bad = append(bad, valid[:n])
	}
	// Every single-bit flip, in every byte (header, payload and CRC).
	for i := 0; i < len(valid); i++ {
		for bit := 0; bit < 8; bit++ {
			bad = append(bad, mutate(func(p []byte) []byte { p[i] ^= 1 << bit; return p }))
		}
	}
	want := map[string]uint64{}
	named := []struct {
		reason string
		pkt    []byte
	}{
		{"length_mismatch", append(append([]byte(nil), valid...), 0)},
		{"reserved_flags", mutate(func(p []byte) []byte { p[4] = 0x80; return reseal(p) })},
		{"unsupported_version", mutate(func(p []byte) []byte { p[2] = 2; return reseal(p) })},
		{"unknown_type", mutate(func(p []byte) []byte { p[3] = 0x7F; return reseal(p) })},
		{"malformed_payload", mutate(func(p []byte) []byte { p[protocol.HeaderSize+26] |= 0x02; return reseal(p) })},
		{"invalid_sample", mutate(func(p []byte) []byte { p[protocol.HeaderSize+24] = 0; return reseal(p) })},
		{"oversize", make([]byte, protocol.MaxPacketSize+1)},
	}
	for _, n := range named {
		bad = append(bad, n.pkt)
		want[n.reason]++
	}

	r.send(t, bad...)
	st := waitSettled(t, r.g, uint64(len(bad)))
	if st.Inserted != 0 || st.Rejected() != uint64(len(bad)) {
		t.Fatalf("%d malformed datagrams: %+v", len(bad), st)
	}
	for reason, n := range want {
		if st.RejectedBy[reason] < n {
			t.Errorf("RejectedBy[%s] = %d, want >= %d", reason, st.RejectedBy[reason], n)
		}
	}
	r.send(t, valid)
	if st := waitSettled(t, r.g, uint64(len(bad)+1)); st.Inserted != 1 {
		t.Fatalf("valid packet after garbage: %+v", st)
	}
}

// TestRebootAndOutOfOrderOverUDP: a device whose EventIDs repeat after a
// reboot, with reordering and late events from the previous boot.
func TestRebootAndOutOfOrderOverUDP(t *testing.T) {
	ctx := context.Background()
	store := openSQLite(t)
	reg := sessions.New(sessions.Config{})
	cfg := baseConfig()
	cfg.Workers = 1 // keep send order so diagnostics are deterministic
	r := start(t, cfg, store, reg)
	const bootA, bootB = 0x9F00AA01, 0x00000002

	// Boot A: 1, 2, 3, 5 (4 delayed).
	for _, seq := range []telemetry.Sequence{1, 2, 3, 5} {
		r.send(t, encode(t, canSample("rb-1", bootA, telemetry.EventID(seq), seq, telemetry.Millimetres(seq)*1000)))
	}
	waitSettled(t, r.g, 4)
	// Reboot: boot B reuses EventIDs 1..3 with new measurements.
	for _, seq := range []telemetry.Sequence{1, 2, 3} {
		r.send(t, encode(t, canSample("rb-1", bootB, telemetry.EventID(seq), seq, 50_000+telemetry.Millimetres(seq))))
	}
	waitSettled(t, r.g, 7)
	// Late event 4 of boot A, after the transition, and a retransmission of
	// boot A's event 2.
	r.send(t, encode(t, canSample("rb-1", bootA, 4, 4, 4000)), encode(t, canSample("rb-1", bootA, 2, 2, 2000)))
	st := waitSettled(t, r.g, 9)

	if st.Inserted != 8 || st.Duplicates != 1 || st.Conflicts != 0 {
		t.Fatalf("stats %+v", st)
	}
	if st.EventIDReused != 3 {
		t.Fatalf("EventIDReused = %d, want 3", st.EventIDReused)
	}
	for kind, n := range map[string]uint64{"new_session": 2, "sequence_gap": 1, "earlier_observed_session": 1, "out_of_order": 1} {
		if st.Diagnostics[kind] != n {
			t.Errorf("diagnostics[%s] = %d, want %d (%v)", kind, st.Diagnostics[kind], n, st.Diagnostics)
		}
	}
	// Both boots are fully stored; nothing was dropped for being late.
	for _, boot := range []telemetry.BootID{bootA, bootB} {
		for seq := telemetry.EventID(1); seq <= 3; seq++ {
			if _, err := store.Get(ctx, eventstore.Key{DeviceID: "rb-1", BootID: boot, EventID: seq}); err != nil {
				t.Fatalf("boot %x event %d: %v", boot, seq, err)
			}
		}
	}
	if sess := reg.Sessions("rb-1"); len(sess) != 2 {
		t.Fatalf("%d sessions tracked", len(sess))
	} else {
		for _, s := range sess {
			if s.BootID == bootA && (s.Missing != 0 || s.Received != 5) {
				t.Fatalf("boot A session %+v", s)
			}
		}
	}
}

// TestConcurrentDevicesOverUDP sends from many devices, each with its own
// socket, at once, and then checks that the stored history reconstructs the
// same session state as the live registry.
func TestConcurrentDevicesOverUDP(t *testing.T) {
	ctx := context.Background()
	store := openSQLite(t)
	reg := sessions.New(sessions.Config{})
	cfg := baseConfig()
	cfg.ReadBuffer = 4 << 20
	cfg.BatchSize = 64
	r := start(t, cfg, store, reg)

	const devices, perDevice = 16, 120
	var wg sync.WaitGroup
	errs := make(chan error, devices)
	for d := 0; d < devices; d++ {
		wg.Add(1)
		go func(d int) {
			defer wg.Done()
			conn, err := net.Dial("udp", r.g.Addr().String())
			if err != nil {
				errs <- err
				return
			}
			defer conn.Close()
			dev := fmt.Sprintf("multi-%02d", d)
			for i := 1; i <= perDevice; i++ {
				p, err := protocol.EncodeSample(canSample(dev, telemetry.BootID(d+1), telemetry.EventID(i), telemetry.Sequence(i), telemetry.Millimetres(i)))
				if err != nil {
					errs <- err
					return
				}
				if _, err := conn.Write(p); err != nil {
					errs <- err
					return
				}
				if i%20 == 0 {
					time.Sleep(time.Millisecond)
				}
			}
		}(d)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	st := waitSettled(t, r.g, devices*perDevice)
	if st.Inserted != devices*perDevice {
		t.Fatalf("stats %+v", st)
	}
	if err := r.stop(t); err != nil {
		t.Fatal(err)
	}

	// Reconstruct session state by replaying the store in position order.
	replayed := sessions.New(sessions.Config{})
	var after int64
	for {
		recs, err := store.ReadAfter(ctx, after, 500)
		if err != nil {
			t.Fatal(err)
		}
		if len(recs) == 0 {
			break
		}
		for _, rec := range recs {
			if _, err := replayed.Observe(rec.Sample); err != nil {
				t.Fatal(err)
			}
			after = rec.Position
		}
	}
	for d := 0; d < devices; d++ {
		dev := telemetry.DeviceID(fmt.Sprintf("multi-%02d", d))
		live, rebuilt := reg.Sessions(dev), replayed.Sessions(dev)
		if !reflect.DeepEqual(live, rebuilt) {
			t.Fatalf("%s: live %+v != replayed %+v", dev, live, rebuilt)
		}
		if live[0].Received != perDevice {
			t.Fatalf("%s: %+v", dev, live[0])
		}
	}
}

func TestDedupCacheIsBounded(t *testing.T) {
	c := newDedupCache(3)
	fp := eventstore.Fingerprint{1}
	for i := 1; i <= 10; i++ {
		c.add(eventstore.Key{DeviceID: "d", BootID: 1, EventID: telemetry.EventID(i)}, fp)
	}
	if len(c.items) != 3 || c.lru.Len() != 3 {
		t.Fatalf("cache holds %d/%d entries, want 3", len(c.items), c.lru.Len())
	}
	if c.isStoredDuplicate(eventstore.Key{DeviceID: "d", BootID: 1, EventID: 1}, fp) {
		t.Fatal("evicted entry still answers")
	}
	if !c.isStoredDuplicate(eventstore.Key{DeviceID: "d", BootID: 1, EventID: 10}, fp) {
		t.Fatal("recent entry missing")
	}
	if c.isStoredDuplicate(eventstore.Key{DeviceID: "d", BootID: 2, EventID: 10}, fp) {
		t.Fatal("cache ignores BootID")
	}
	off := newDedupCache(-1)
	off.add(eventstore.Key{DeviceID: "d", BootID: 1, EventID: 1}, fp)
	if len(off.items) != 0 {
		t.Fatal("disabled cache stores entries")
	}
}

func TestBurstLimiter(t *testing.T) {
	l := newBurstLimiter(time.Second, 2)
	ok := 0
	for i := 0; i < 10; i++ {
		if a, _ := l.allow(clock); a {
			ok++
		}
	}
	if ok != 2 {
		t.Fatalf("allowed %d in one window, want 2", ok)
	}
	a, suppressed := l.allow(clock.Add(time.Second))
	if !a || suppressed != 8 {
		t.Fatalf("next window: allow=%v suppressed=%d, want true, 8", a, suppressed)
	}
}
