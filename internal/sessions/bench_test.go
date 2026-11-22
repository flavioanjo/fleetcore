package sessions

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

func BenchmarkObserveInOrder(b *testing.B) {
	r := New(Config{})
	s := smp("bench", 1, 0, 0, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Sequence = telemetry.Sequence(i)
		s.IngestedAt = telemetry.Some(t0.Add(time.Duration(i) * time.Millisecond))
		if _, err := r.Observe(s); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRegistryMemory reports retained heap per tracked device (one
// session each, default 1024-sample window) at several fleet sizes.
func BenchmarkRegistryMemory(b *testing.B) {
	for _, devices := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("devices=%d", devices), func(b *testing.B) {
			ids := make([]telemetry.DeviceID, devices)
			for i := range ids {
				ids[i] = telemetry.DeviceID(fmt.Sprintf("mem-%07d", i))
			}
			var perDevice float64
			for n := 0; n < b.N; n++ {
				runtime.GC()
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				r := New(Config{MaxDevices: devices})
				for i, id := range ids {
					s := smp(string(id), 1, 1, 0, 0)
					s.IngestedAt = telemetry.Some(t0.Add(time.Duration(i) * time.Microsecond))
					if _, err := r.Observe(s); err != nil {
						b.Fatal(err)
					}
				}
				runtime.GC()
				runtime.ReadMemStats(&after)
				perDevice = float64(after.HeapAlloc-before.HeapAlloc) / float64(devices)
				runtime.KeepAlive(r)
			}
			b.ReportMetric(perDevice, "B/device")
		})
	}
}
