package eventstore

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

var t0 = time.Date(2026, 3, 14, 9, 30, 0, 0, time.UTC)

func sample(dev string, eid telemetry.EventID, boot telemetry.BootID, seq telemetry.Sequence, mm telemetry.Millimetres) telemetry.Sample {
	return telemetry.Sample{
		DeviceID:   telemetry.DeviceID(dev),
		EventID:    eid,
		BootID:     boot,
		Sequence:   seq,
		DeviceTime: t0.Add(time.Duration(seq) * time.Second),
		IngestedAt: telemetry.Some(t0.Add(time.Duration(seq)*time.Second + 250*time.Millisecond)),
		Source:     telemetry.SourceGPS,
		Quality:    telemetry.QualityGood,
		Distance:   telemetry.Some(telemetry.Distance{Kind: telemetry.DistanceDelta, Value: mm}),
	}
}

func event(t testing.TB, s telemetry.Sample) Event {
	t.Helper()
	ev, err := NewEvent(s)
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	return ev
}

func openStore(t testing.TB, path string) *Store {
	t.Helper()
	s, err := Open(context.Background(), path, Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func tempDB(t testing.TB) string { return filepath.Join(t.TempDir(), "events.db") }

func mustAppend(t testing.TB, s *Store, ev Event, want Outcome) Result {
	t.Helper()
	res, err := s.Append(context.Background(), ev)
	if err != nil {
		t.Fatalf("Append %s: %v", ev.Key(), err)
	}
	if res.Outcome != want {
		t.Fatalf("Append %s: outcome %s, want %s", ev.Key(), res.Outcome, want)
	}
	return res
}

func TestNewEventRequiresIngestionTime(t *testing.T) {
	s := sample("dev-1", 1, 7, 1, 100)
	s.IngestedAt = telemetry.None[time.Time]()
	if _, err := NewEvent(s); !errors.Is(err, ErrNoIngestionTime) {
		t.Fatalf("got %v, want ErrNoIngestionTime", err)
	}
	s = sample("dev-1", 0, 7, 1, 100)
	if _, err := NewEvent(s); err == nil {
		t.Fatal("NewEvent accepted an invalid sample")
	}
}

func TestFingerprintExcludesIngestionTime(t *testing.T) {
	a := sample("dev-1", 1, 7, 1, 100)
	b := a
	b.IngestedAt = telemetry.Some(t0.Add(time.Hour))
	if event(t, a).Fingerprint != event(t, b).Fingerprint {
		t.Fatal("fingerprint depends on IngestedAt")
	}
	c := a
	c.Distance = telemetry.Some(telemetry.Distance{Kind: telemetry.DistanceDelta, Value: 101})
	if event(t, a).Fingerprint == event(t, c).Fingerprint {
		t.Fatal("fingerprint ignores distance")
	}
}

func TestRetransmissionIsIdempotent(t *testing.T) {
	st := openStore(t, tempDB(t))
	orig := sample("dev-1", 42, 7, 1, 100)
	first := mustAppend(t, st, event(t, orig), Inserted)

	retx := orig
	retx.IngestedAt = telemetry.Some(t0.Add(time.Hour)) // received again, later
	dup := mustAppend(t, st, event(t, retx), Duplicate)
	if dup.Position != first.Position {
		t.Fatalf("duplicate position %d, want %d", dup.Position, first.Position)
	}
	rec, err := st.Get(context.Background(), Key{"dev-1", 7, 42})
	if err != nil {
		t.Fatal(err)
	}
	want, _ := orig.IngestedAt.Get()
	if ing, _ := rec.Sample.IngestedAt.Get(); !ing.Equal(want) {
		t.Fatalf("ingested_at overwritten: %v", ing)
	}
	all, _ := st.ReadAfter(context.Background(), 0, 100)
	if len(all) != 1 {
		t.Fatalf("%d stored events, want 1", len(all))
	}
}

func TestConflictingPayloadIsKeptSeparately(t *testing.T) {
	st := openStore(t, tempDB(t))
	ctx := context.Background()
	orig := sample("dev-1", 42, 7, 1, 100)
	mustAppend(t, st, event(t, orig), Inserted)

	bad := sample("dev-1", 42, 7, 1, 999) // same identity, different payload
	res := mustAppend(t, st, event(t, bad), Conflict)
	if res.Existing != event(t, orig).Fingerprint {
		t.Fatal("conflict result does not report the stored fingerprint")
	}
	bad.IngestedAt = telemetry.Some(t0.Add(time.Minute))
	mustAppend(t, st, event(t, bad), Conflict) // conflicting retransmission

	rec, err := st.Get(ctx, Key{"dev-1", 7, 42})
	if err != nil {
		t.Fatal(err)
	}
	if d, _ := rec.Sample.Distance.Get(); d.Value != 100 {
		t.Fatalf("stored event overwritten: distance %d", d.Value)
	}
	cs, err := st.Conflicts(ctx, Key{"dev-1", 7, 42})
	if err != nil {
		t.Fatal(err)
	}
	if len(cs) != 1 || cs[0].Count != 2 || cs[0].Fingerprint != event(t, bad).Fingerprint {
		t.Fatalf("conflicts = %+v, want one entry with 2 receipts", cs)
	}
	if !cs[0].LastSeenAt.Equal(t0.Add(time.Minute)) || cs[0].FirstSeenAt.After(cs[0].LastSeenAt) {
		t.Fatalf("conflict times %v..%v", cs[0].FirstSeenAt, cs[0].LastSeenAt)
	}
}

func TestIdentityScopes(t *testing.T) {
	st := openStore(t, tempDB(t))

	// Same EventID on different devices: distinct identities.
	mustAppend(t, st, event(t, sample("dev-a", 5, 7, 1, 100)), Inserted)
	mustAppend(t, st, event(t, sample("dev-b", 5, 7, 1, 100)), Inserted)

	// Identical measurement, different EventIDs: distinct events.
	mustAppend(t, st, event(t, sample("dev-a", 6, 7, 2, 100)), Inserted)
	mustAppend(t, st, event(t, sample("dev-a", 7, 7, 2, 100)), Inserted)
}

// TestRebootReusingEventIDs models the reference simulator, whose EventID
// embeds the per-boot sequence number and therefore repeats after a reboot.
func TestRebootReusingEventIDs(t *testing.T) {
	ctx := context.Background()
	st := openStore(t, tempDB(t))
	const bootA, bootB = 0xA1B2C3D4, 0x00000001 // opaque; B is numerically smaller
	for seq := telemetry.Sequence(1); seq <= 3; seq++ {
		res := mustAppend(t, st, event(t, sample("dev-1", telemetry.EventID(seq), bootA, seq, 100)), Inserted)
		if len(res.EventIDAlsoInBoots) != 0 {
			t.Fatalf("first boot reported reuse: %v", res.EventIDAlsoInBoots)
		}
	}
	// After the reboot the same EventIDs carry new measurements: stored, not
	// conflicts, and the domain-contract violation is reported.
	for seq := telemetry.Sequence(1); seq <= 3; seq++ {
		res := mustAppend(t, st, event(t, sample("dev-1", telemetry.EventID(seq), bootB, seq, 200)), Inserted)
		if len(res.EventIDAlsoInBoots) != 1 || res.EventIDAlsoInBoots[0] != bootA {
			t.Fatalf("seq %d: EventIDAlsoInBoots = %v, want [%d]", seq, res.EventIDAlsoInBoots, bootA)
		}
	}
	// Late retransmission from the first boot, after the transition: duplicate.
	mustAppend(t, st, event(t, sample("dev-1", 2, bootA, 2, 100)), Duplicate)
	// Retransmission from the second boot: duplicate.
	mustAppend(t, st, event(t, sample("dev-1", 2, bootB, 2, 200)), Duplicate)
	// Conflicting payloads are scoped to their own boot.
	mustAppend(t, st, event(t, sample("dev-1", 2, bootA, 2, 999)), Conflict)
	if cs, _ := st.Conflicts(ctx, Key{"dev-1", bootB, 2}); len(cs) != 0 {
		t.Fatalf("conflict leaked into the other boot: %+v", cs)
	}
	if cs, _ := st.Conflicts(ctx, Key{"dev-1", bootA, 2}); len(cs) != 1 {
		t.Fatalf("conflicts for boot A: %d, want 1", len(cs))
	}

	recs, err := st.ByEventID(ctx, "dev-1", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 || recs[0].Sample.BootID != bootA || recs[1].Sample.BootID != bootB {
		t.Fatalf("ByEventID: %+v", recs)
	}
	if all, _ := st.ReadAfter(ctx, 0, 100); len(all) != 6 {
		t.Fatalf("%d stored events, want 6", len(all))
	}
}

// TestBootIDReuse documents the unavoidable limitation: a device that reuses
// both a BootID and an EventID within it produces the same identity.
func TestBootIDReuse(t *testing.T) {
	ctx := context.Background()
	st := openStore(t, tempDB(t))
	first := sample("dev-1", 1, 42, 1, 100)
	mustAppend(t, st, event(t, first), Inserted)

	// Second power cycle that happened to pick BootID 42 again, with a
	// different measurement: kept as a conflict, never overwriting.
	again := sample("dev-1", 1, 42, 1, 300)
	again.DeviceTime = first.DeviceTime.Add(24 * time.Hour)
	again.IngestedAt = telemetry.Some(again.DeviceTime.Add(time.Second))
	mustAppend(t, st, event(t, again), Conflict)
	rec, _ := st.Get(ctx, Key{"dev-1", 42, 1})
	if d, _ := rec.Sample.Distance.Get(); d.Value != 100 {
		t.Fatalf("stored event overwritten: %d", d.Value)
	}
	cs, _ := st.Conflicts(ctx, Key{"dev-1", 42, 1})
	if len(cs) != 1 {
		t.Fatalf("conflicts: %d, want 1", len(cs))
	}
	// The conflicting payload is fully recoverable from the conflict table.
	if got := event(t, again).Packet; string(cs[0].Packet) != string(got) {
		t.Fatal("conflict packet differs from the received payload")
	}
}

func TestBatchClassifiesWithinBatch(t *testing.T) {
	st := openStore(t, tempDB(t))
	a := sample("dev-1", 1, 7, 1, 100)
	b := sample("dev-1", 1, 7, 1, 200)
	res, err := st.AppendBatch(context.Background(), []Event{event(t, a), event(t, a), event(t, b), event(t, sample("dev-1", 2, 7, 2, 1))})
	if err != nil {
		t.Fatal(err)
	}
	want := []Outcome{Inserted, Duplicate, Conflict, Inserted}
	for i, r := range res {
		if r.Outcome != want[i] {
			t.Fatalf("result %d: %s, want %s", i, r.Outcome, want[i])
		}
	}
	if res[3].Position <= res[0].Position {
		t.Fatal("positions do not increase")
	}
}

func TestBatchIsAtomic(t *testing.T) {
	st := openStore(t, tempDB(t))
	good := event(t, sample("dev-1", 1, 7, 1, 100))
	tampered := event(t, sample("dev-1", 2, 7, 2, 100))
	tampered.Sample.Sequence = 99 // no longer matches the packet
	if _, err := st.AppendBatch(context.Background(), []Event{good, tampered}); err == nil {
		t.Fatal("tampered event accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := st.AppendBatch(ctx, []Event{good}); err == nil {
		t.Fatal("append with cancelled context succeeded")
	}
	if all, _ := st.ReadAfter(context.Background(), 0, 10); len(all) != 0 {
		t.Fatalf("%d events written by failed batches", len(all))
	}
}

func TestRecoveryAfterRestart(t *testing.T) {
	path := tempDB(t)
	ctx := context.Background()
	st, err := Open(ctx, path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 10; i++ {
		mustAppend(t, st, event(t, sample("dev-1", telemetry.EventID(i), 7, telemetry.Sequence(i), 10)), Inserted)
	}
	mustAppend(t, st, event(t, sample("dev-1", 3, 7, 3, 11)), Conflict)
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := st.Append(ctx, event(t, sample("dev-1", 99, 7, 99, 1))); !errors.Is(err, ErrClosed) {
		t.Fatalf("append after close: %v", err)
	}

	st2 := openStore(t, path)
	// Deduplication does not depend on in-memory state.
	mustAppend(t, st2, event(t, sample("dev-1", 5, 7, 5, 10)), Duplicate)
	mustAppend(t, st2, event(t, sample("dev-1", 5, 7, 5, 12)), Conflict)
	// The device rebooted while the gateway was down; its reused EventIDs
	// are new identities, and the cross-boot reuse is detected from durable
	// state alone.
	res := mustAppend(t, st2, event(t, sample("dev-1", 5, 8, 5, 10)), Inserted)
	if len(res.EventIDAlsoInBoots) != 1 || res.EventIDAlsoInBoots[0] != 7 {
		t.Fatalf("EventIDAlsoInBoots after restart = %v", res.EventIDAlsoInBoots)
	}
	all, err := st2.ReadAfter(ctx, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 11 {
		t.Fatalf("%d events after restart, want 11", len(all))
	}
	page, _ := st2.ReadAfter(ctx, all[4].Position, 3)
	if len(page) != 3 || page[0].Position != all[5].Position {
		t.Fatal("ReadAfter paging is wrong")
	}
	cs, _ := st2.Conflicts(ctx, Key{"dev-1", 7, 3})
	if len(cs) != 1 {
		t.Fatalf("conflicts after restart: %d", len(cs))
	}
}

func TestStoredEventsAreImmutable(t *testing.T) {
	st := openStore(t, tempDB(t))
	mustAppend(t, st, event(t, sample("dev-1", 1, 7, 1, 100)), Inserted)
	if _, err := st.w.Exec(`UPDATE events SET distance_mm = 5`); err == nil {
		t.Fatal("UPDATE of a stored event succeeded")
	}
}

func TestReaderIsReadOnly(t *testing.T) {
	st := openStore(t, tempDB(t))
	if _, err := st.r.Exec(`DELETE FROM events`); err == nil {
		t.Fatal("read pool accepted a write")
	}
}

func TestSchemaVersioning(t *testing.T) {
	path := tempDB(t)
	st := openStore(t, path)
	var mode string
	if err := st.r.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil || mode != "wal" {
		t.Fatalf("journal_mode = %q, %v", mode, err)
	}
	var v int
	if err := st.r.QueryRow(`SELECT max(version) FROM schema_migrations`).Scan(&v); err != nil || v != SchemaVersion() {
		t.Fatalf("schema version %d, %v", v, err)
	}
	// Reopening is a no-op migration.
	st2 := openStore(t, path)
	st2.Close()
}

func TestConcurrentWritersAcrossConnections(t *testing.T) {
	path := tempDB(t)
	// Two independent Stores on one file model two processes.
	stores := []*Store{openStore(t, path), openStore(t, path)}
	const writers, perWriter, unique = 8, 50, 100
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	counts := make([][3]int, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			st := stores[w%2]
			for i := 0; i < perWriter; i++ {
				id := (w*perWriter + i) % unique // heavy overlap between writers
				ev, err := NewEvent(sample("dev-1", telemetry.EventID(id+1), 7, telemetry.Sequence(id), 10))
				if err != nil {
					errs <- err
					return
				}
				res, err := st.Append(context.Background(), ev)
				if err != nil {
					errs <- fmt.Errorf("writer %d: %w", w, err)
					return
				}
				counts[w][res.Outcome-1]++
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var ins, dup, conf int
	for _, c := range counts {
		ins, dup, conf = ins+c[0], dup+c[1], conf+c[2]
	}
	if ins != unique || dup != writers*perWriter-unique || conf != 0 {
		t.Fatalf("inserted %d duplicate %d conflict %d", ins, dup, conf)
	}
}
