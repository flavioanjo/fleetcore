package gateway

import (
	"errors"
	"testing"
	"time"
)

// fakeClock drives awaitProgress deterministically: every sleep advances
// it, and the workload is a function of the elapsed time.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time                   { return c.t }
func (c *fakeClock) sleep(d time.Duration)            { c.t = c.t.Add(d) }
func (c *fakeClock) since(t0 time.Time) time.Duration { return c.t.Sub(t0) }

func TestAwaitProgressPolicy(t *testing.T) {
	t0 := time.Date(2026, 11, 23, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name    string
		work    func(el time.Duration) (done bool, progress uint64)
		want    error
		atLeast time.Duration
		atMost  time.Duration
	}{
		{"done at once", func(time.Duration) (bool, uint64) { return true, 0 }, nil, 0, 0},
		{"slow but steady beyond the former 10 s deadline", func(el time.Duration) (bool, uint64) {
			n := uint64(el / time.Second) // one event per second
			return n >= 30, n
		}, nil, 30 * time.Second, 31 * time.Second},
		{"progress stops", func(el time.Duration) (bool, uint64) {
			if el > 5*time.Second {
				el = 5 * time.Second
			}
			return false, uint64(el / time.Millisecond)
		}, errNoProgress, 15 * time.Second, 15100 * time.Millisecond},
		{"never any progress", func(time.Duration) (bool, uint64) { return false, 7 }, errNoProgress, 10 * time.Second, 10100 * time.Millisecond},
		{"progress forever", func(el time.Duration) (bool, uint64) { return false, uint64(el / time.Second) }, errSettleLimit, 120 * time.Second, 121 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &fakeClock{t: t0}
			err := awaitProgress(func() (bool, uint64) { return tc.work(c.since(t0)) }, settleStall, settleLimit, c.now, c.sleep)
			if !errors.Is(err, tc.want) || (tc.want == nil) != (err == nil) {
				t.Fatalf("err %v, want %v", err, tc.want)
			}
			if el := c.since(t0); el < tc.atLeast || el > tc.atMost {
				t.Fatalf("returned after %v, want within [%v, %v]", el, tc.atLeast, tc.atMost)
			}
		})
	}
}
