package security

import (
	"math"
	"math/rand/v2"
	"testing"
	"time"
)

// acceptWithin runs w.Accept(c) and fails the test if it does not return:
// the window must terminate for every counter, including math.MaxUint64.
func acceptWithin(t *testing.T, w *ReplayWindow, c uint64) bool {
	t.Helper()
	done := make(chan bool, 1)
	go func() { done <- w.Accept(c) }()
	select {
	case ok := <-done:
		return ok
	case <-time.After(5 * time.Second):
		t.Fatalf("Accept(%d) did not terminate (top %d)", c, w.top)
		return false
	}
}

// Regression: advancing the window to math.MaxUint64 by a small step used
// to loop forever, because the clearing loop's counter wrapped to zero.
func TestReplayWindowMaxCounterTerminates(t *testing.T) {
	var w ReplayWindow
	if !acceptWithin(t, &w, math.MaxUint64-1) {
		t.Fatal("MaxUint64-1 rejected on an empty window")
	}
	if !acceptWithin(t, &w, math.MaxUint64) {
		t.Fatal("MaxUint64 rejected after MaxUint64-1")
	}
	if w.Top() != math.MaxUint64 {
		t.Fatalf("top %d", w.Top())
	}
	for _, c := range []uint64{math.MaxUint64, math.MaxUint64 - 1} {
		if acceptWithin(t, &w, c) {
			t.Fatalf("replayed %d accepted", c)
		}
	}
	if !acceptWithin(t, &w, math.MaxUint64-5) {
		t.Fatal("fresh counter below the maximum rejected")
	}
	if acceptWithin(t, &w, math.MaxUint64-WindowSize) {
		t.Fatal("counter beyond the window accepted")
	}
	if !acceptWithin(t, &w, math.MaxUint64-(WindowSize-1)) {
		t.Fatal("oldest counter inside the window rejected")
	}
}

func TestReplayWindowBoundaries(t *testing.T) {
	cases := []struct {
		name  string
		seq   []uint64
		want  []bool
		top   uint64
		check map[uint64]bool // Check results after the sequence
	}{
		{"zero is never valid", []uint64{0, 1, 0}, []bool{false, true, false}, 1, map[uint64]bool{0: false, 2: true}},
		{"one", []uint64{1, 1}, []bool{true, false}, 1, map[uint64]bool{1: false, 2: true}},
		{"duplicates", []uint64{7, 7, 3, 3, 7}, []bool{true, false, true, false, false}, 7, nil},
		{"out of order", []uint64{10, 4, 9, 1, 2, 10, 4}, []bool{true, true, true, true, true, false, false}, 10,
			map[uint64]bool{3: true, 5: true, 11: true}},
		{"beyond the window", []uint64{2000, 2000 - WindowSize, 2000 - WindowSize + 1, 976},
			[]bool{true, false, true, false}, 2000, map[uint64]bool{977: false, 978: true, 976: false}},
		{"max minus one then max", []uint64{math.MaxUint64 - 1, math.MaxUint64, math.MaxUint64 - 1},
			[]bool{true, true, false}, math.MaxUint64, map[uint64]bool{math.MaxUint64 - 2: true}},
		{"max directly", []uint64{math.MaxUint64, math.MaxUint64, 1}, []bool{true, false, false}, math.MaxUint64,
			map[uint64]bool{math.MaxUint64 - (WindowSize - 1): true, math.MaxUint64 - WindowSize: false}},
		{"jump from 1 to max", []uint64{1, math.MaxUint64, 1}, []bool{true, true, false}, math.MaxUint64, nil},
		{"small steps up to max", []uint64{math.MaxUint64 - 3, math.MaxUint64 - 2, math.MaxUint64 - 1, math.MaxUint64},
			[]bool{true, true, true, true}, math.MaxUint64, map[uint64]bool{math.MaxUint64 - 4: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var w ReplayWindow
			for i, c := range tc.seq {
				if got := acceptWithin(t, &w, c); got != tc.want[i] {
					t.Fatalf("step %d: Accept(%d) = %v, want %v", i, c, got, tc.want[i])
				}
			}
			if w.Top() != tc.top {
				t.Fatalf("top %d, want %d", w.Top(), tc.top)
			}
			for c, want := range tc.check {
				if got := w.Check(c); got != want {
					t.Fatalf("Check(%d) = %v, want %v", c, got, want)
				}
			}
		})
	}
}

// The window agrees with a simple reference model (a set of accepted
// counters plus the highest one) on random walks near both ends of the
// counter space.
func TestReplayWindowMatchesModel(t *testing.T) {
	for _, base := range []uint64{1, 1 << 32, math.MaxUint64 - 5000} {
		r := rand.New(rand.NewPCG(base, 42))
		var w ReplayWindow
		seen := map[uint64]bool{}
		var top uint64
		for i := 0; i < 50000; i++ {
			var c uint64
			switch r.IntN(4) {
			case 0: // near the top, possibly a duplicate or slightly ahead
				c = top - uint64(r.IntN(40)) + uint64(r.IntN(50))
			case 1: // around the edge of the window
				c = top - WindowSize + uint64(r.IntN(8))
			default: // ahead, with small steps
				c = top + uint64(r.IntN(30))
			}
			if top == 0 {
				c = base + uint64(r.IntN(10))
			}
			if c < base || (top > 0 && c > top && c-top > 100) {
				continue // wrapped around: not part of this walk
			}
			want := c != 0 && !seen[c] && (c > top || top-c < WindowSize)
			if got := acceptWithin(t, &w, c); got != want {
				t.Fatalf("base %d step %d: Accept(%d) = %v, want %v (top %d)", base, i, c, got, want, top)
			}
			if want {
				seen[c] = true
				if c > top {
					top = c
				}
			}
			if w.Top() != top {
				t.Fatalf("top %d, model %d", w.Top(), top)
			}
		}
	}
}
