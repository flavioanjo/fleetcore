package fusion

import (
	"encoding/json"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/chrono"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// wrapTrip builds n epochs of GPS + pulse + CAN readings starting at the
// given sequence number (the stream increments before each sample).
func wrapTrip(firstSeq telemetry.Sequence, n int) []telemetry.Sample {
	st := newStream("W", 7)
	st.seq = firstSeq - 1
	var out []telemetry.Sample
	can := 10_000 * telemetry.Kilometre
	for i := 0; i < n; i++ {
		st.tick(10 * time.Second)
		can += 150 * m
		out = append(out, st.gps(150*m), st.pulse(150*m), st.can(can/(100*m)*(100*m)))
	}
	return out
}

func tripsOf(rs []Report) []telemetry.Millimetres {
	var out []telemetry.Millimetres
	for _, e := range estimates(rs, "W") {
		out = append(out, e.TripMM)
	}
	return out
}

func equalTrips(t *testing.T, got, want []telemetry.Millimetres) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%d estimates, want %d", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("estimate %d: trip %d, want %d", i, got[i], want[i])
		}
	}
}

// A stream that wraps from 2^32-1 to 0 must be processed exactly like the
// same stream far from the wrap point.
func TestSequenceWrapIsContinuous(t *testing.T) {
	ref := runAll(newEngine(t), wrapTrip(1000, 20))
	wrapped := wrapTrip(^telemetry.Sequence(0)-29, 20) // crosses the wrap in epoch 10
	rs := runAll(newEngine(t), wrapped)

	equalTrips(t, tripsOf(rs), tripsOf(ref))
	if hasDiag(rs, CodeSequenceGap) || hasDiag(rs, CodeSequenceAmbiguous) {
		t.Fatal("wrap produced a spurious gap or ambiguity")
	}
	for _, ev := range finalOutcomes(rs) {
		if ev.Outcome != OutcomeAccepted {
			t.Fatalf("seq %d: %s/%s", ev.Sequence, ev.Outcome, ev.Reason)
		}
	}
	// Reports keep the 32-bit wire values.
	es := estimates(rs, "W")
	if es[9].LastSeq != ^telemetry.Sequence(0) || es[10].FirstSeq != 0 {
		t.Fatalf("epochs around the wrap: last %d, first %d", es[9].LastSeq, es[10].FirstSeq)
	}
}

func TestWrapWithReorderingAndDuplicates(t *testing.T) {
	ref := runAll(newEngine(t), wrapTrip(1000, 20))
	in := wrapTrip(^telemetry.Sequence(0)-29, 20)
	var shuffled []telemetry.Sample
	for i := 0; i+1 < len(in); i += 2 { // swap neighbours, including across the wrap
		shuffled = append(shuffled, in[i+1], in[i])
	}
	if len(in)%2 == 1 {
		shuffled = append(shuffled, in[len(in)-1])
	}
	// Retransmit the events on both sides of the wrap.
	dupBefore, dupAfter := in[29], in[30] // sequence 2^32-1 and 0
	if dupBefore.Sequence != ^telemetry.Sequence(0) || dupAfter.Sequence != 0 {
		t.Fatalf("test setup: %d %d", dupBefore.Sequence, dupAfter.Sequence)
	}
	shuffled = append(shuffled[:40:40], append([]telemetry.Sample{dupBefore, dupAfter}, shuffled[40:]...)...)

	rs := runAll(newEngine(t), shuffled)
	equalTrips(t, tripsOf(rs), tripsOf(ref))
	dups := 0
	for _, ev := range events(rs) {
		if ev.Outcome == OutcomeDuplicate {
			dups++
		}
	}
	if dups != 2 {
		t.Fatalf("%d duplicates, want 2", dups)
	}
}

func TestGapAcrossWrapIsDeclared(t *testing.T) {
	in := wrapTrip(^telemetry.Sequence(0)-29, 20)
	var kept []telemetry.Sample
	for _, s := range in {
		if s.Sequence != 0 && s.Sequence != 1 { // lose the first two after the wrap
			kept = append(kept, s)
		}
	}
	rs := runAll(newEngine(t), kept)
	var found bool
	for _, r := range rs {
		if r.Type != ReportEvent {
			continue
		}
		for _, d := range r.Event.Diagnostics {
			if d.Code == CodeSequenceGap {
				if d.Seq == nil || *d.Seq != 0 || d.Value == nil || *d.Value != 2 {
					t.Fatalf("gap diagnostic %+v, want seq 0, 2 missing", d)
				}
				found = true
			}
		}
	}
	if !found {
		t.Fatal("no gap declared across the wrap")
	}
}

func TestLateEventAcrossWrap(t *testing.T) {
	in := wrapTrip(^telemetry.Sequence(0)-29, 20)
	var late telemetry.Sample
	var kept []telemetry.Sample
	for _, s := range in {
		if s.Sequence == ^telemetry.Sequence(0)-1 {
			late = s // held back until long after its window
			continue
		}
		kept = append(kept, s)
	}
	e := newEngine(t)
	var rs []Report
	for _, s := range kept {
		rs = append(rs, e.Process(s)...)
	}
	rs = append(rs, e.Process(late)...)
	got := events(rs)[len(events(rs))-1]
	if got.Sequence != late.Sequence || got.Outcome != OutcomeRequiresReconciliation || got.Reason != ReasonLateAfterGap {
		t.Fatalf("late pre-wrap event: seq %d %s/%s, want requires_reconciliation/late_after_gap", got.Sequence, got.Outcome, got.Reason)
	}
}

func TestAmbiguousSequenceIsRejected(t *testing.T) {
	st := newStream("W", 7)
	var in []telemetry.Sample
	for i := 0; i < 5; i++ {
		in = append(in, st.tick(10*time.Second).gps(100*m))
	}
	base := in[len(in)-1].Sequence
	far := st.tick(10 * time.Second).gps(100 * m)
	far.Sequence = base + serialLimit // exactly at the ambiguity bound
	back := far
	back.Sequence = base - serialLimit
	back.EventID++
	justInside := far
	justInside.Sequence = base + serialLimit - 1
	justInside.EventID += 2

	rs := runAll(newEngine(t), append(in, far, back, justInside))
	evs := finalOutcomes(rs)
	for _, s := range []telemetry.Sample{far, back} {
		ev := evs[[2]uint64{uint64(s.BootID), uint64(s.Sequence)}]
		if ev.Outcome != OutcomeRejected || ev.Reason != ReasonSequenceAmbiguous {
			t.Fatalf("seq %d: %s/%s, want rejected/sequence_ambiguous", s.Sequence, ev.Outcome, ev.Reason)
		}
	}
	requireDiag(t, rs, CodeSequenceAmbiguous)
	// Just inside the bound the number is placed (far ahead, so it opens a gap).
	ev := evs[[2]uint64{uint64(justInside.BootID), uint64(justInside.Sequence)}]
	if ev.Outcome != OutcomeAccepted {
		t.Fatalf("seq just inside the bound: %s/%s", ev.Outcome, ev.Reason)
	}
}

func TestReplayAcrossWrapIsDeterministic(t *testing.T) {
	in := wrapTrip(^telemetry.Sequence(0)-29, 20)
	a, _ := json.Marshal(runAll(newEngine(t), in))
	b, _ := json.Marshal(runAll(newEngine(t), in))
	if string(a) != string(b) {
		t.Fatal("two runs over a wrapping stream differ")
	}
}

func TestSeqExtender(t *testing.T) {
	var x seqExtender
	e0, _ := x.extend(^telemetry.Sequence(0))
	x.observe(e0)
	e1, ok := x.extend(0)
	if !ok || e1 != e0+1 {
		t.Fatalf("wrap: %d, %v; want %d", e1, ok, e0+1)
	}
	x.observe(e1)
	if b, ok := x.extend(^telemetry.Sequence(0) - 5); !ok || b != e0-5 {
		t.Fatalf("behind across the wrap: %d %v", b, ok)
	}
	if _, ok := x.extend(serialLimit); ok {
		t.Fatal("distance 2^30 accepted")
	}
	if e, ok := x.extend(serialLimit - 1); !ok || e != e1+serialLimit-1 {
		t.Fatalf("distance 2^30-1: %d %v", e, ok)
	}
	x.observe(e1 + 10)
	if e, _ := x.extend(5); e != e1+5 {
		t.Fatal("observe of a higher value moved a lower lookup")
	}
}

// The chronological processor (internal/chrono) and the engine must extend
// sequence numbers identically, or events ordered by one would be judged
// out of order by the other.
func TestExtenderParityWithChrono(t *testing.T) {
	var fx seqExtender
	var cx chrono.Extender
	rng := rand.New(rand.NewPCG(4, 2))
	seq := telemetry.Sequence(rng.Uint32())
	for i := 0; i < 200_000; i++ {
		switch rng.IntN(10) {
		case 0: // far jump, often ambiguous
			seq += telemetry.Sequence(rng.Uint32())
		case 1: // behind
			seq -= telemetry.Sequence(rng.IntN(5000))
		default:
			seq += telemetry.Sequence(rng.IntN(300))
		}
		fe, fok := fx.extend(seq)
		ce, cok := cx.Extend(seq)
		if fe != ce || fok != cok {
			t.Fatalf("step %d seq %d: fusion (%d, %v), chrono (%d, %v)", i, seq, fe, fok, ce, cok)
		}
		if fok {
			fx.observe(fe)
			cx.Observe(ce)
		}
	}
	if chrono.SerialLimit != serialLimit {
		t.Fatal("serial limits differ")
	}
}
