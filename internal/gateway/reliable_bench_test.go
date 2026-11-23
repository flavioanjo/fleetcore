package gateway

import (
	"context"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/reliable"
)

// BenchmarkReliableIngest measures authenticated v2 ingestion end to end in
// one gateway core: authenticate, session/replay checks, durable commit of
// a 64-event batch (SQLite, synchronous=FULL) and signing of the ACKs. The
// reported ns/op is per event; ack_latency is the time from the batch
// entering Process to its ACKs being ready (commit included).
func BenchmarkReliableIngest(b *testing.B) {
	for _, batch := range []int{1, 64} {
		b.Run(map[int]string{1: "batch1", 64: "batch64"}[batch], func(b *testing.B) {
			fx := newRelFixtureB(b)
			gw := fx.coreB(b)
			dev := newTestDevice(b, fx.ks, "DEV-1", 4000)
			var uid uint64
			var latency time.Duration
			b.ResetTimer()
			for done := 0; done < b.N; done += batch {
				in := make([]Inbound, 0, batch)
				for j := 0; j < batch; j++ {
					uid++
					pkt, _ := dev.event(b, uid, reliable.Diagnostic{Code: 1, Value: int64(uid)})
					in = append(in, dev.in(pkt))
				}
				start := time.Now()
				if out := gw.HandleBatch(context.Background(), in); len(out) != batch {
					b.Fatalf("%d acks", len(out))
				}
				latency += time.Since(start)
			}
			batches := (b.N + batch - 1) / batch
			b.ReportMetric(float64(latency.Microseconds())/float64(batches), "ack_latency_us/batch")
		})
	}
}
