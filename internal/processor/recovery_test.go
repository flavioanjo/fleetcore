package processor

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/resultstore"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// Recovery of events that were read and held by the orderer, but not yet
// journaled, when the process stopped. The durable cursor then has
// low_position < high_position, and the events in (low, high] that are not
// journaled must be read again after a restart.

// fillerEvents stores and processes n events of an unrelated device, so the
// positions of the events under test start at n+1.
func (h *harness) fillerEvents(p *Processor, n int) {
	h.t.Helper()
	var ss []telemetry.Sample
	for i := 1; i <= n; i++ {
		ss = append(ss, sample("filler", 1, telemetry.Sequence(i), i, 10*m))
	}
	h.store(ss...)
	h.settle(p)
}

// tickCommit advances the clock past TickInterval (but not past the hold of
// events read now) and steps, so that the cursor, including held events, is
// committed with a tick step, as Run does every second.
func (h *harness) tickCommit(p *Processor) {
	h.t.Helper()
	h.clock.advance(1100 * time.Millisecond)
	step(h.t, p)
}

func durableState(t *testing.T, h *harness) (low, high, next int64) {
	t.Helper()
	if err := writeDB(h).QueryRow(`SELECT low_position, high_position, next_step FROM processor_state WHERE id = 1`).
		Scan(&low, &high, &next); err != nil {
		t.Fatal(err)
	}
	return low, high, next
}

// checkJournal verifies that every stored position is journaled exactly
// once and that each session's events are journaled in sequence order.
func checkJournal(t *testing.T, h *harness, stored int64) []resultstore.Entry {
	t.Helper()
	es := journal(t, h.results)
	seen := map[int64]int{}
	last := map[string]telemetry.Sequence{}
	var events []resultstore.Entry
	for _, e := range es {
		if e.Kind != resultstore.KindEvent {
			continue
		}
		events = append(events, e)
		seen[e.Position]++
		key := fmt.Sprintf("%s/%d", e.DeviceID, e.BootID)
		if prev, ok := last[key]; ok && e.Sequence <= prev {
			t.Errorf("session %s: seq %d journaled after %d", key, e.Sequence, prev)
		}
		last[key] = e.Sequence
	}
	for pos := int64(1); pos <= stored; pos++ {
		switch seen[pos] {
		case 1:
		case 0:
			t.Errorf("position %d was never journaled (lost)", pos)
		default:
			t.Errorf("position %d journaled %d times (duplicated)", pos, seen[pos])
		}
	}
	if int64(len(events)) != stored {
		t.Errorf("%d event steps journaled, want %d", len(events), stored)
	}
	return events
}

func positionsOf(es []resultstore.Entry, dev string) []int64 {
	var out []int64
	for _, e := range es {
		if string(e.DeviceID) == dev {
			out = append(out, e.Position)
		}
	}
	return out
}

func equal(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// The scenario of the review: 101–102 processed, 103–105 held and committed
// as held (low 102, high 105), the process stops before the hold expires,
// and restarts on the same databases with no new input.
func TestRestartRecoversHeldEventsWithoutNewInput(t *testing.T) {
	h := newHarness(t)
	p := h.processor()
	h.fillerEvents(p, 100)

	h.store(sample("r", 1, 1, 1000, 1*m), sample("r", 1, 2, 1010, 2*m)) // positions 101, 102
	h.settle(p)
	h.store(sample("r", 1, 3, 1020, 3*m), sample("r", 1, 4, 1030, 4*m), sample("r", 1, 5, 1040, 5*m)) // 103..105
	step(t, p)                                                                                        // read and held
	h.tickCommit(p)                                                                                   // a tick commits the cursor while 103..105 are still held
	if low, high, _ := durableState(t, h); low != 102 || high != 105 {
		t.Fatalf("durable cursor low=%d high=%d, want 102/105 (precondition)", low, high)
	}
	if got := positionsOf(journal(t, h.results), "r"); !equal(got, []int64{101, 102}) {
		t.Fatalf("journaled before stop %v, want [101 102]", got)
	}

	// Stop before the hold expires; restart on the same files.
	h.reopen()
	p2 := h.processor()
	step(t, p2) // no new input
	if p2.Stats().Held != 3 {
		t.Fatalf("held after restart %d, want 3 (103..105 re-read from the low watermark)", p2.Stats().Held)
	}
	h.clock.advance(3 * time.Second)
	step(t, p2)
	es := checkJournal(t, h, 105)
	if got := positionsOf(es, "r"); !equal(got, []int64{101, 102, 103, 104, 105}) {
		t.Fatalf("journal order of r %v, want 101..105", got)
	}
	if low, high, _ := durableState(t, h); low != 105 || high != 105 {
		t.Fatalf("durable cursor after release low=%d high=%d, want 105/105", low, high)
	}

	// New input afterwards: no loss, no duplicate, order kept.
	h.store(sample("r", 1, 7, 1060, 7*m), sample("r", 1, 6, 1050, 6*m)) // 106 (seq 7), 107 (seq 6)
	h.settle(p2)
	es = checkJournal(t, h, 107)
	if got := positionsOf(es, "r"); !equal(got, []int64{101, 102, 103, 104, 105, 107, 106}) {
		t.Fatalf("journal order of r %v, want 101..105 107 106 (sequence order)", got)
	}
	h.reopen()
	h.processor() // the whole journal replays and verifies
}

// Journaled positions interleaved with held ones: position 2 (device y) is
// released while positions 1 and 3 (device x) are held. After a restart 1
// and 3 must be read again and 2 must not be given to fusion a second time.
func TestRestartWithJournaledPositionsInterleavedWithHeld(t *testing.T) {
	h := newHarness(t)
	p := h.processor()
	h.store(sample("x", 1, 2, 20, 2*m), sample("y", 1, 1, 10, 1*m)) // positions 1, 2
	step(t, p)
	h.clock.advance(1500 * time.Millisecond)
	h.store(sample("x", 1, 1, 10, 1*m)) // position 3: x's new candidate, read later
	step(t, p)
	h.clock.advance(1000 * time.Millisecond)
	step(t, p) // y (2) released; x (3, then 1) still held; a tick commits
	if got := positionsOf(journal(t, h.results), "y"); !equal(got, []int64{2}) {
		t.Fatalf("y journaled %v, want [2] (precondition)", got)
	}
	if low, high, _ := durableState(t, h); low != 0 || high != 3 {
		t.Fatalf("durable cursor low=%d high=%d, want 0/3 (precondition)", low, high)
	}

	h.reopen()
	p2 := h.processor()
	h.settle(p2)
	es := checkJournal(t, h, 3)
	if got := positionsOf(es, "x"); !equal(got, []int64{3, 1}) {
		t.Fatalf("x journal order %v, want [3 1] (sequence 1 then 2)", got)
	}
	if low, high, _ := durableState(t, h); low != 3 || high != 3 {
		t.Fatalf("durable cursor low=%d high=%d, want 3/3", low, high)
	}
	h.reopen()
	h.processor()
}

// Two gateways writing sessions of the same device: boot 9 is stored first
// but generated later. The process restarts twice while all of them are held;
// the cross-session order by device time must survive both restarts.
func TestRepeatedRestartsWhileHeldAcrossGateways(t *testing.T) {
	h := newHarness(t)
	p := h.processor()
	h.store(sample("d", 9, 1, 100, 10*m), sample("d", 9, 2, 110, 10*m)) // gateway A
	h.store(sample("d", 7, 1, 0, 10*m), sample("d", 7, 2, 10, 10*m))    // gateway B
	step(t, p)
	h.tickCommit(p)
	if low, high, _ := durableState(t, h); low != 0 || high != 4 {
		t.Fatalf("durable cursor low=%d high=%d, want 0/4 (precondition)", low, high)
	}
	for restart := 0; restart < 2; restart++ {
		h.reopen()
		p = h.processor()
		step(t, p)
		h.tickCommit(p) // still within the (restarted) hold
		if p.Stats().Held != 4 {
			t.Fatalf("restart %d: held %d, want 4", restart+1, p.Stats().Held)
		}
	}
	h.reopen()
	p = h.processor()
	h.settle(p)
	es := checkJournal(t, h, 4)
	var boots []telemetry.BootID
	for _, e := range es {
		boots = append(boots, e.BootID)
	}
	if len(boots) != 4 || boots[0] != 7 || boots[1] != 7 || boots[2] != 9 || boots[3] != 9 {
		t.Fatalf("boot order %v, want 7 7 9 9", boots)
	}
	h.reopen()
	h.processor()
}

// A crash immediately before the commit that would journal the release:
// the commit fails, the in-memory release is lost, and a restart must
// release the same events once.
func TestCrashBeforeAndAfterReleaseCommit(t *testing.T) {
	h := newHarness(t)
	p := h.processor(func(c *Config) { c.CommitRetries = 1 })
	h.store(sample("c", 1, 1, 10, 1*m), sample("c", 1, 2, 20, 2*m))
	h.settle(p)
	h.store(sample("c", 1, 3, 30, 3*m), sample("c", 1, 4, 40, 4*m))
	step(t, p)
	h.tickCommit(p) // committed as held: low 2, high 4

	// Crash before commit: the store disappears under the processor.
	h.clock.advance(3 * time.Second)
	h.results.Close()
	if _, err := p.Step(context.Background()); !errors.Is(err, ErrFatal) {
		t.Fatalf("step with a failing commit: %v, want ErrFatal", err)
	}

	h.reopen()
	p2 := h.processor()
	h.settle(p2) // crash right after this commit:
	h.reopen()
	p3 := h.processor()
	h.settle(p3)
	es := checkJournal(t, h, 4)
	if got := positionsOf(es, "c"); !equal(got, []int64{1, 2, 3, 4}) {
		t.Fatalf("journal order %v", got)
	}
}

// Fencing: the old owner, still holding events in memory, must not journal
// them after a new owner took over and recovered them.
func TestFencedOwnerCannotJournalRecoveredHeldEvents(t *testing.T) {
	h := newHarness(t)
	p1 := h.processor()
	h.store(sample("f", 1, 1, 10, 1*m), sample("f", 1, 2, 20, 2*m))
	step(t, p1)
	h.tickCommit(p1)

	p2 := h.processor() // takes over on the same databases
	step(t, p2)
	h.clock.advance(3 * time.Second)
	if _, err := p1.Step(context.Background()); !errors.Is(err, resultstore.ErrNotOwner) {
		t.Fatalf("old owner step: %v, want ErrNotOwner", err)
	}
	step(t, p2)
	checkJournal(t, h, 2)
	h.reopen()
	h.processor()
}
