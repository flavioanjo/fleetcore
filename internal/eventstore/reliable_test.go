package eventstore

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/reliable"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

func relEvent(t testing.TB, dev string, uid uint64, b reliable.Body) ReliableEvent {
	t.Helper()
	up, err := reliable.NewEvent(uid, t0.Add(time.Duration(uid)*time.Second), b)
	if err != nil {
		t.Fatal(err)
	}
	re, err := NewReliableEvent(dev, up, 1, 3, t0.Add(time.Hour), "gw-a")
	if err != nil {
		t.Fatal(err)
	}
	return re
}

func telemetryBody(dev string, uid uint64, boot telemetry.BootID, mm telemetry.Millimetres) reliable.Telemetry {
	s := sample(dev, telemetry.EventID(uid), boot, telemetry.Sequence(uid), mm)
	s.DeviceTime = t0.Add(time.Duration(uid) * time.Second)
	s.IngestedAt = telemetry.None[time.Time]()
	return reliable.Telemetry{Sample: s}
}

func appendRel(t testing.TB, st *Store, evs ...ReliableEvent) []ReliableResult {
	t.Helper()
	res, err := st.AppendReliable(context.Background(), evs)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func TestReliableClassification(t *testing.T) {
	st := openStore(t, tempDB(t))
	ctx := context.Background()
	panicEv := relEvent(t, "dev-1", 10, reliable.Panic{Trigger: reliable.PanicButton})
	res := appendRel(t, st, panicEv, panicEv) // retransmission inside one batch
	if res[0].Outcome != Inserted || res[1].Outcome != Duplicate || res[0].Position != res[1].Position {
		t.Fatalf("batch %+v", res)
	}
	// Retransmission in a later session (different envelope) is a duplicate.
	again := panicEv
	again.SessionGen, again.Gateway = 4, "gw-b"
	if r := appendRel(t, st, again)[0]; r.Outcome != Duplicate {
		t.Fatalf("retransmission %v", r.Outcome)
	}
	// Same UID, other content: conflict, preserved, stored event unchanged.
	other := relEvent(t, "dev-1", 10, reliable.Diagnostic{Code: 1})
	if r := appendRel(t, st, other, other)[1]; r.Outcome != Conflict || r.ConflictReason != ConflictUIDReused || r.Existing != panicEv.Fingerprint {
		t.Fatalf("conflict %+v", r)
	}
	cs, _ := st.ReliableConflicts(ctx, "dev-1", 10)
	if len(cs) != 1 || cs[0].Receipts != 2 || cs[0].Fingerprint != other.Fingerprint {
		t.Fatalf("conflicts %+v", cs)
	}
	got, err := st.GetReliable(ctx, "dev-1", 10)
	if err != nil || got.Fingerprint != panicEv.Fingerprint || got.Upload.Type != reliable.EventPanic ||
		got.Class != reliable.ClassEmergency || got.SessionGen != 3 || got.Gateway != "gw-a" {
		t.Fatalf("stored %+v %v", got, err)
	}
	// Same UID on another device is another identity.
	if r := appendRel(t, st, relEvent(t, "dev-2", 10, reliable.Diagnostic{Code: 1}))[0]; r.Outcome != Inserted {
		t.Fatalf("other device %v", r.Outcome)
	}
	// Stored rows are immutable.
	if _, err := st.w.Exec(`UPDATE reliable_events SET payload = payload`); err == nil {
		t.Fatal("reliable event updated")
	}
}

func TestReliableTelemetryIsStoredOnceInV1Table(t *testing.T) {
	st := openStore(t, tempDB(t))
	ctx := context.Background()
	tel := relEvent(t, "dev-1", 5, telemetryBody("dev-1", 5, 2, 100))
	r := appendRel(t, st, tel)[0]
	if r.Outcome != Inserted || r.EventPosition == 0 {
		t.Fatalf("telemetry %+v", r)
	}
	recs, _ := st.ReadAfter(ctx, 0, 10)
	if len(recs) != 1 || recs[0].Position != r.EventPosition || recs[0].Sample.EventID != 5 {
		t.Fatalf("v1 rows %+v", recs)
	}
	// A retransmission neither adds a v1 row nor a reliable row.
	appendRel(t, st, tel)
	if recs, _ := st.ReadAfter(ctx, 0, 10); len(recs) != 1 {
		t.Fatalf("v1 rows after retransmission %d", len(recs))
	}
	// The same sample delivered earlier over v1 is linked, not duplicated.
	s := telemetryBody("dev-1", 6, 2, 100).Sample
	s.IngestedAt = telemetry.Some(t0.Add(time.Hour))
	v1 := mustAppend(t, st, event(t, s), Inserted)
	r6 := appendRel(t, st, relEvent(t, "dev-1", 6, telemetryBody("dev-1", 6, 2, 100)))[0]
	if r6.Outcome != Inserted || r6.EventPosition != v1.Position {
		t.Fatalf("link to v1 row %+v (v1 position %d)", r6, v1.Position)
	}
	// A v1 row with the same identity but other content is a conflict.
	s7 := telemetryBody("dev-1", 7, 2, 100).Sample
	s7.IngestedAt = telemetry.Some(t0.Add(time.Hour))
	mustAppend(t, st, event(t, s7), Inserted)
	r7 := appendRel(t, st, relEvent(t, "dev-1", 7, telemetryBody("dev-1", 7, 2, 999)))[0]
	if r7.Outcome != Conflict || r7.ConflictReason != ConflictTelemetryIdentity || r7.Position != 0 {
		t.Fatalf("telemetry identity conflict %+v", r7)
	}
	if _, err := st.GetReliable(ctx, "dev-1", 7); !errors.Is(err, ErrNotFound) {
		t.Fatalf("conflicting telemetry stored: %v", err)
	}
	if recs, _ := st.ReadAfter(ctx, 0, 10); len(recs) != 3 {
		t.Fatalf("v1 rows %d, want 3", len(recs))
	}
}

func TestReliableBatchIsAtomic(t *testing.T) {
	st := openStore(t, tempDB(t))
	good := relEvent(t, "dev-1", 1, reliable.Diagnostic{Code: 1})
	bad := relEvent(t, "dev-1", 2, reliable.Diagnostic{Code: 2})
	bad.Payload = append([]byte(nil), bad.Payload...)
	bad.Payload[len(bad.Payload)-1] ^= 1 // altered after validation
	if _, err := st.AppendReliable(context.Background(), []ReliableEvent{good, bad}); err == nil {
		t.Fatal("altered event accepted")
	}
	if _, err := st.GetReliable(context.Background(), "dev-1", 1); !errors.Is(err, ErrNotFound) {
		t.Fatal("part of a failed batch was stored")
	}
}

func TestReadReliableAfterFiltersAndPages(t *testing.T) {
	st := openStore(t, tempDB(t))
	var evs []ReliableEvent
	for uid := uint64(1); uid <= 10; uid++ {
		var b reliable.Body = reliable.Diagnostic{Code: 1}
		if uid%3 == 0 {
			b = reliable.Panic{Trigger: reliable.PanicButton}
		}
		evs = append(evs, relEvent(t, "dev-1", uid, b))
	}
	appendRel(t, st, evs...)
	ctx := context.Background()
	page, _ := st.ReadReliableAfter(ctx, 0, 4)
	if len(page) != 4 || page[3].Upload.UID != 4 {
		t.Fatalf("page %d", len(page))
	}
	rest, _ := st.ReadReliableAfter(ctx, page[3].Position, 100)
	if len(rest) != 6 {
		t.Fatalf("rest %d", len(rest))
	}
	panics, _ := st.ReadReliableAfter(ctx, 0, 100, reliable.EventPanic)
	if len(panics) != 3 {
		t.Fatalf("panics %d", len(panics))
	}
	for _, p := range panics {
		if b, err := p.Body(); err != nil || b.EventType() != reliable.EventPanic {
			t.Fatalf("panic body %v", err)
		}
	}
	n, _ := st.CountReliable(ctx)
	if n[reliable.EventPanic] != 3 || n[reliable.EventDiagnostic] != 7 {
		t.Fatalf("counts %v", n)
	}
}

func TestDeviceSessionsAreMonotonic(t *testing.T) {
	path := tempDB(t)
	a, b := openStore(t, path), openStore(t, path) // two gateways, one database
	ctx := context.Background()
	if _, err := a.DeviceSession(ctx, "dev-1"); !errors.Is(err, ErrNotFound) {
		t.Fatal("session before any frame")
	}
	if _, err := a.ObserveSession(ctx, "dev-1", 1, 5, t0, "gw-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ObserveSession(ctx, "dev-1", 1, 5, t0.Add(time.Second), "gw-b"); err != nil {
		t.Fatal(err)
	}
	if st, err := b.ObserveSession(ctx, "dev-1", 1, 4, t0, "gw-b"); !errors.Is(err, ErrStaleSession) || st.SessionGen != 5 {
		t.Fatalf("older session through the other gateway: %+v %v", st, err)
	}
	if _, err := a.ObserveSession(ctx, "dev-1", 2, 5, t0, "gw-a"); !errors.Is(err, ErrSessionKeyMismatch) {
		t.Fatalf("same generation, other key: %v", err)
	}
	if st, err := a.ObserveSession(ctx, "dev-1", 2, 6, t0.Add(2*time.Second), "gw-a"); err != nil || st.KeyID != 2 || st.SessionGen != 6 {
		t.Fatalf("new session with rotated key: %+v %v", st, err)
	}
	st, _ := b.DeviceSession(ctx, "dev-1")
	if st.SessionGen != 6 || st.KeyID != 2 || st.Gateway != "gw-a" {
		t.Fatalf("session %+v", st)
	}
	// After reopening (restart) the high-water mark is still enforced.
	a.Close()
	c := openStore(t, path)
	if _, err := c.ObserveSession(ctx, "dev-1", 1, 5, t0, "gw-a"); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("old session after restart: %v", err)
	}
}

// Two stores on one file (two gateways) racing to store the same
// retransmitted events: each identity is inserted exactly once.
func TestReliableConcurrentGatewaysDeduplicate(t *testing.T) {
	path := tempDB(t)
	stores := []*Store{openStore(t, path), openStore(t, path)}
	var evs []ReliableEvent
	for uid := uint64(1); uid <= 50; uid++ {
		evs = append(evs, relEvent(t, "dev-1", uid, telemetryBody("dev-1", uid, 1, 10)))
	}
	var mu sync.Mutex
	inserted := map[uint64]int{}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(st *Store) {
			defer wg.Done()
			for _, ev := range evs {
				r, err := st.AppendReliable(context.Background(), []ReliableEvent{ev})
				if err != nil {
					t.Error(err)
					return
				}
				if r[0].Outcome == Inserted {
					mu.Lock()
					inserted[ev.Upload.UID]++
					mu.Unlock()
				} else if r[0].Outcome != Duplicate {
					t.Errorf("outcome %v", r[0].Outcome)
				}
			}
		}(stores[i%2])
	}
	wg.Wait()
	for uid := uint64(1); uid <= 50; uid++ {
		if inserted[uid] != 1 {
			t.Fatalf("uid %d inserted %d times", uid, inserted[uid])
		}
	}
	recs, _ := stores[0].ReadAfter(context.Background(), 0, 1000)
	if len(recs) != 50 {
		t.Fatalf("%d v1 rows for 50 telemetry events", len(recs))
	}
}

// A database created by the previous release (schema 1, with data) is
// upgraded in place; version-1 data and behaviour are unchanged.
func TestUpgradeFromSchema1(t *testing.T) {
	path := tempDB(t)
	saved := migrations
	migrations = saved[:1]
	st := openStore(t, path)
	mustAppend(t, st, event(t, sample("dev-1", 1, 7, 1, 100)), Inserted)
	st.Close()
	migrations = saved

	st2 := openStore(t, path)
	if v := schemaVersion(t, st2); v != SchemaVersion() || SchemaVersion() < 2 {
		t.Fatalf("schema %d", v)
	}
	mustAppend(t, st2, event(t, sample("dev-1", 1, 7, 1, 100)), Duplicate)
	if r := appendRel(t, st2, relEvent(t, "dev-1", 1, reliable.Diagnostic{Code: 1}))[0]; r.Outcome != Inserted {
		t.Fatal("reliable tables unusable after upgrade")
	}

}
