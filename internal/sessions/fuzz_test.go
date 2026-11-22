package sessions

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// FuzzObserve feeds arbitrary sample streams to a small registry and checks
// that it never panics, stays within its bounds and keeps its counters
// consistent.
func FuzzObserve(f *testing.F) {
	f.Add([]byte{0, 1, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 9})
	f.Add([]byte{1, 2, 255, 255, 255, 255, 0, 0, 0, 0, 1, 2, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		cfg := Config{MaxDevices: 2, MaxSessionsPerDevice: 2, MaxRetiredPerDevice: 2, SequenceWindow: 64,
			SessionIdleTimeout: time.Minute, DeviceIdleTimeout: 2 * time.Minute, OverlapWindow: 10 * time.Second}
		r := New(cfg)
		ing := t0
		// Each record: device(1) boot(1) seq(4) device-time delta(2).
		for len(data) >= 8 {
			rec := data[:8]
			data = data[8:]
			ing = ing.Add(time.Duration(rec[7]) * time.Second)
			s := telemetry.Sample{
				DeviceID:   telemetry.DeviceID([]string{"a", "b", "c"}[rec[0]%3]),
				BootID:     telemetry.BootID(rec[1]%4 + 1),
				Sequence:   telemetry.Sequence(binary.BigEndian.Uint32(rec[2:6])),
				DeviceTime: t0.Add(time.Duration(int8(rec[6])) * time.Second),
				IngestedAt: telemetry.Some(ing),
			}
			o, err := r.Observe(s)
			if err != nil {
				t.Fatal(err)
			}
			st := o.Session
			if span := uint64(st.Highest-st.Lowest) + 1; st.Missing > span {
				t.Fatalf("missing %d exceeds span %d: %+v", st.Missing, span, st)
			}
			if len(o.Diagnostics) == 0 && st.Received == 0 {
				t.Fatalf("empty session: %+v", st)
			}
			stats := r.Stats()
			if stats.Devices > cfg.MaxDevices || stats.Sessions > cfg.MaxDevices*cfg.MaxSessionsPerDevice || stats.Sessions < 0 {
				t.Fatalf("bounds exceeded: %+v", stats)
			}
		}
		r.Expire(ing.Add(time.Hour))
		if st := r.Stats(); st.Devices != 0 || st.Sessions != 0 {
			t.Fatalf("expire left %+v", st)
		}
	})
}
