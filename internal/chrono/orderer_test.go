package chrono

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

var t0 = time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

// feeder builds items with increasing positions.
type feeder struct {
	o   *Orderer
	pos int64
}

func newFeeder(cfg Config) *feeder { return &feeder{o: New(cfg)} }

func smp(dev string, boot telemetry.BootID, seq telemetry.Sequence, devSec float64) telemetry.Sample {
	return telemetry.Sample{
		DeviceID: telemetry.DeviceID(dev), BootID: boot, Sequence: seq,
		EventID:    telemetry.EventID(uint64(boot)<<32|uint64(seq)) + 1,
		DeviceTime: t0.Add(time.Duration(devSec * float64(time.Second))),
		Source:     telemetry.SourceGPS, Quality: telemetry.QualityUnavailable,
	}
}

// add adds samples, all seen at processing time seenSec.
func (f *feeder) add(seenSec float64, ss ...telemetry.Sample) {
	for _, s := range ss {
		f.pos++
		f.o.Add(Item{Position: f.pos, Sample: s, SeenAt: t0.Add(time.Duration(seenSec * float64(time.Second)))})
	}
}

func (f *feeder) release(atSec float64) []Released {
	return f.o.Release(t0.Add(time.Duration(atSec*float64(time.Second))), false)
}

type got struct {
	boot  telemetry.BootID
	seq   telemetry.Sequence
	flags Flag
}

func summary(rs []Released) []got {
	var out []got
	for _, r := range rs {
		out = append(out, got{r.Sample.BootID, r.Sample.Sequence, r.Flags})
	}
	return out
}

func expect(t *testing.T, rs []Released, want ...got) {
	t.Helper()
	if fmt.Sprint(summary(rs)) != fmt.Sprint(want) {
		t.Fatalf("released %v, want %v", summary(rs), want)
	}
}

func TestHoldDelaysRelease(t *testing.T) {
	f := newFeeder(Config{Hold: 2 * time.Second})
	f.add(0, smp("d", 1, 1, 0))
	if rs := f.release(1.999); len(rs) != 0 {
		t.Fatalf("released before the hold expired: %v", summary(rs))
	}
	expect(t, f.release(2), got{1, 1, 0})
	if f.o.Held() != 0 {
		t.Fatal("held count not decremented")
	}
}

func TestSessionOrderIsSequenceNotArrival(t *testing.T) {
	f := newFeeder(Config{})
	f.add(0, smp("d", 1, 3, 3), smp("d", 1, 1, 1), smp("d", 1, 2, 2))
	expect(t, f.release(10), got{1, 1, 0}, got{1, 2, 0}, got{1, 3, 0})
}

// A regressing clock is reported, and the events keep sequence order.
func TestClockRegressionKeepsSequenceOrder(t *testing.T) {
	f := newFeeder(Config{})
	f.add(0, smp("d", 1, 1, 100), smp("d", 1, 2, 40), smp("d", 1, 3, 41))
	expect(t, f.release(10), got{1, 1, 0}, got{1, 2, FlagClockRegression}, got{1, 3, FlagClockRegression})
}

func TestWrapAroundKeepsGenerationOrder(t *testing.T) {
	f := newFeeder(Config{})
	max := telemetry.Sequence(math.MaxUint32)
	f.add(0, smp("d", 1, 1, 4), smp("d", 1, max, 2), smp("d", 1, 0, 3), smp("d", 1, max-1, 1))
	rs := f.release(10)
	expect(t, rs, got{1, max - 1, 0}, got{1, max, 0}, got{1, 0, 0}, got{1, 1, 0})
	for i := 1; i < len(rs); i++ {
		if rs[i].ExtSequence != rs[i-1].ExtSequence+1 {
			t.Fatalf("extended sequence not contiguous: %d after %d", rs[i].ExtSequence, rs[i-1].ExtSequence)
		}
	}
}

func TestLateAndReusedAreReleasedAndFlagged(t *testing.T) {
	f := newFeeder(Config{})
	f.add(0, smp("d", 1, 5, 5), smp("d", 1, 6, 6))
	expect(t, f.release(10), got{1, 5, 0}, got{1, 6, 0})
	reuse := smp("d", 1, 6, 6)
	reuse.EventID += 1000 // a distinct stored event with the same slot
	f.add(11, smp("d", 1, 4, 4), reuse)
	expect(t, f.release(20), got{1, 4, FlagLate}, got{1, 6, FlagSequenceReused})
}

func TestSessionsOrderedByDeviceTime(t *testing.T) {
	f := newFeeder(Config{SessionTolerance: time.Second})
	// Boot 900 (numerically larger) was generated earlier but ingested later.
	f.add(0, smp("d", 5, 1, 100), smp("d", 5, 2, 110))
	f.add(1, smp("d", 900, 1, 50), smp("d", 900, 2, 60))
	expect(t, f.release(10), got{900, 1, 0}, got{900, 2, 0}, got{5, 1, 0}, got{5, 2, 0})
}

func TestUnclearChronologyFallsBackToIngestionOrder(t *testing.T) {
	f := newFeeder(Config{SessionTolerance: time.Second})
	f.add(0, smp("d", 2, 1, 100.5)) // ingested first
	f.add(0, smp("d", 1, 1, 100.0)) // earlier, but by less than the tolerance
	expect(t, f.release(10), got{2, 1, FlagChronologyAmbiguous}, got{1, 1, 0})
}

// The tie-break must not depend on map iteration order: three sessions
// with chained, overlapping device times must always give the same order.
func TestSessionChoiceIsIterationIndependent(t *testing.T) {
	var first string
	for run := 0; run < 50; run++ {
		f := newFeeder(Config{SessionTolerance: time.Second})
		f.add(0, smp("d", 3, 1, 1.2)) // position 1
		f.add(0, smp("d", 2, 1, 0.6)) // position 2
		f.add(0, smp("d", 1, 1, 0.0)) // position 3
		s := fmt.Sprint(summary(f.release(10)))
		if run == 0 {
			first = s
		} else if s != first {
			t.Fatalf("run %d: %s, first run %s", run, s, first)
		}
	}
	// Heads below 0.0 s + 1 s are boots 1 and 2, so ingestion order picks
	// boot 2 (position 2). Next, boot 3 (1.2 s) is outside boot 1's
	// tolerance, so device time puts boot 1 first.
	if want := fmt.Sprint([]got{{2, 1, FlagChronologyAmbiguous}, {1, 1, 0}, {3, 1, 0}}); first != want {
		t.Fatalf("order %s, want %s", first, want)
	}
}

func TestDevicesInIngestionOrder(t *testing.T) {
	f := newFeeder(Config{})
	f.add(0, smp("b", 1, 1, 9999)) // device clocks are not compared
	f.add(0, smp("a", 1, 1, 1))
	rs := f.release(10)
	if rs[0].Sample.DeviceID != "b" || rs[1].Sample.DeviceID != "a" {
		t.Fatalf("devices released as %s, %s; want ingestion order b, a", rs[0].Sample.DeviceID, rs[1].Sample.DeviceID)
	}
}

func TestAmbiguousSequenceIsReleasedInIngestionOrder(t *testing.T) {
	f := newFeeder(Config{})
	f.add(0, smp("d", 1, 10, 10))
	f.add(0, smp("d", 1, 10+SerialLimit, 11), smp("d", 1, 11, 12))
	rs := f.release(10)
	expect(t, rs, got{1, 10, 0}, got{1, 10 + SerialLimit, FlagSequenceAmbiguous}, got{1, 11, 0})
	if rs[1].ExtSequence != 0 {
		t.Fatal("ambiguous event got an extended sequence")
	}
}

func TestMaxHeldForcesRelease(t *testing.T) {
	f := newFeeder(Config{MaxHeld: 3, Hold: time.Hour})
	for i := 1; i <= 5; i++ {
		f.add(0, smp("d", 1, telemetry.Sequence(i), float64(i)))
	}
	rs := f.release(0)
	expect(t, rs, got{1, 1, FlagForced}, got{1, 2, FlagForced})
	if f.o.Held() != 3 {
		t.Fatalf("held %d, want 3", f.o.Held())
	}
}

func TestMinHeldPosition(t *testing.T) {
	f := newFeeder(Config{})
	if _, ok := f.o.MinHeldPosition(); ok {
		t.Fatal("empty orderer reports a held position")
	}
	f.add(0, smp("d", 1, 2, 2), smp("d", 1, 1, 1)) // positions 1, 2
	f.add(5, smp("e", 1, 1, 1))                    // position 3, held longer
	f.release(2)
	if p, ok := f.o.MinHeldPosition(); !ok || p != 3 {
		t.Fatalf("min held %d %v, want 3", p, ok)
	}
	f.release(10)
	if _, ok := f.o.MinHeldPosition(); ok {
		t.Fatal("min held after releasing everything")
	}
}

// After a restart, restoring the released history reproduces the flags an
// uninterrupted orderer would compute.
func TestRestoreContinuesFlags(t *testing.T) {
	a := newFeeder(Config{})
	a.add(0, smp("d", 1, 1, 1), smp("d", 1, 2, 2), smp("d", 1, 3, 3))
	done := a.release(10)

	b := newFeeder(Config{})
	b.pos = a.pos
	for _, r := range done {
		b.o.Restore(r)
	}
	reuse := smp("d", 1, 3, 3)
	reuse.EventID += 77
	b.add(11, smp("d", 1, 0, 0), reuse, smp("d", 1, 4, 0.5))
	expect(t, b.release(20), got{1, 0, FlagLate}, got{1, 3, FlagSequenceReused}, got{1, 4, FlagClockRegression})
}

func TestSessionStateIsBounded(t *testing.T) {
	f := newFeeder(Config{MaxSessions: 10})
	for b := 1; b <= 100; b++ {
		f.add(float64(b), smp("d", telemetry.BootID(b), 1, float64(b)))
		f.release(float64(b) + 10)
	}
	if n := f.o.lru.Len(); n > 10 {
		t.Fatalf("%d sessions remembered, want <= 10", n)
	}
	if f.o.SessionsEvicted() != 90 {
		t.Fatalf("SessionsEvicted = %d, want 90", f.o.SessionsEvicted())
	}
}

func TestFlagNames(t *testing.T) {
	if got := (FlagLate | FlagForced).Names(); fmt.Sprint(got) != "[late forced_release]" {
		t.Fatalf("names %v", got)
	}
}
