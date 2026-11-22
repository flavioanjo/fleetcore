package fusion_test

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/fusion"
	"github.com/flavioanjo/fleetcore/internal/simulator"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

func simSamples(b *testing.B, vehicles, rounds int) []telemetry.Sample {
	b.Helper()
	cfg := simulator.DefaultConfig()
	cfg.Vehicles, cfg.Samples, cfg.Interval = vehicles, rounds, 10*time.Second
	ss := make([]telemetry.Sample, 0, vehicles*rounds*3)
	if err := simulator.Run(cfg, func(s telemetry.Sample) error { ss = append(ss, s); return nil }); err != nil {
		b.Fatal(err)
	}
	return ss
}

// BenchmarkProcess measures steady-state throughput on in-order traffic.
func BenchmarkProcess(b *testing.B) {
	for _, vehicles := range []int{1, 100} {
		b.Run(fmt.Sprintf("vehicles=%d", vehicles), func(b *testing.B) {
			ss := simSamples(b, vehicles, 30_000/vehicles)
			b.ReportAllocs()
			b.ResetTimer()
			n := 0
			for i := 0; i < b.N; i++ {
				e, _ := fusion.New(fusion.DefaultConfig())
				for _, s := range ss {
					e.Process(s)
				}
				e.Flush()
				n += len(ss)
			}
			b.ReportMetric(float64(n)/b.Elapsed().Seconds(), "samples/s")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(n), "ns/sample")
		})
	}
}

// BenchmarkMemoryPerDevice reports the retained heap per tracked device
// after a short in-order history (session established, buffers drained).
func BenchmarkMemoryPerDevice(b *testing.B) {
	const devices = 10_000
	ss := simSamples(b, devices, 20)
	for i := 0; i < b.N; i++ {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		e, _ := fusion.New(fusion.DefaultConfig())
		for _, s := range ss {
			e.Process(s)
		}
		runtime.GC()
		runtime.ReadMemStats(&after)
		b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc)/devices, "B/device")
		runtime.KeepAlive(e)
	}
}
