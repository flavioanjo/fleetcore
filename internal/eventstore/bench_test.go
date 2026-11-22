package eventstore

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

func benchEvents(b *testing.B, n int, boot telemetry.BootID) []Event {
	b.Helper()
	evs := make([]Event, n)
	for i := range evs {
		evs[i] = event(b, sample("bench-dev", telemetry.EventID(i+1), boot, telemetry.Sequence(i+1), 100))
	}
	return evs
}

// BenchmarkAppendBatch measures durable inserts (synchronous=FULL, WAL) by
// batch size. Each b.N iteration stores one new event.
func BenchmarkAppendBatch(b *testing.B) {
	for _, size := range []int{1, 64, 256} {
		b.Run(fmt.Sprintf("batch=%d", size), func(b *testing.B) {
			st := openStore(b, tempDB(b))
			evs := benchEvents(b, b.N, 1)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			begin := time.Now()
			for i := 0; i < len(evs); i += size {
				end := min(i+size, len(evs))
				if _, err := st.AppendBatch(ctx, evs[i:end]); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.N)/time.Since(begin).Seconds(), "events/s")
		})
	}
}

// BenchmarkAppendDuplicates measures retransmission handling against the
// database (no gateway cache), in batches of 256.
func BenchmarkAppendDuplicates(b *testing.B) {
	st := openStore(b, tempDB(b))
	ctx := context.Background()
	evs := benchEvents(b, 4096, 1)
	for i := 0; i < len(evs); i += 256 {
		if _, err := st.AppendBatch(ctx, evs[i:i+256]); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	begin := time.Now()
	for n := 0; n < b.N; n += 256 {
		off := n % len(evs)
		res, err := st.AppendBatch(ctx, evs[off:off+256])
		if err != nil || res[0].Outcome != Duplicate {
			b.Fatal(err, res[0].Outcome)
		}
	}
	b.ReportMetric(float64(b.N)/time.Since(begin).Seconds(), "events/s")
}
