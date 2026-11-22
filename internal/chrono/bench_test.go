package chrono

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

func benchOrderer(b *testing.B, shuffle bool) {
	const devices = 100
	o := New(Config{Hold: time.Second})
	rng := rand.New(rand.NewPCG(1, 2))
	seqs := make([]telemetry.Sequence, devices)
	b.ReportAllocs()
	b.ResetTimer()
	now := t0
	for i := 0; i < b.N; i++ {
		d := i % devices
		seq := seqs[d]
		seqs[d]++
		if shuffle && rng.IntN(4) == 0 { // a quarter arrive one step early
			seq++
		}
		o.Add(Item{Position: int64(i + 1), SeenAt: now, Sample: telemetry.Sample{
			DeviceID: telemetry.DeviceID([]string{"d0", "d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9"}[d%10]) + telemetry.DeviceID(rune('a'+d/10)),
			BootID:   1, Sequence: seq, DeviceTime: t0.Add(time.Duration(seq) * time.Second),
		}})
		if i%256 == 255 {
			now = now.Add(2 * time.Second)
			o.Release(now, false)
		}
	}
	o.Release(now, true)
}

func BenchmarkOrdererInOrder(b *testing.B)   { benchOrderer(b, false) }
func BenchmarkOrdererReordered(b *testing.B) { benchOrderer(b, true) }
