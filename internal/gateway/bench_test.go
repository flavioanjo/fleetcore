package gateway

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/eventstore"
	"github.com/flavioanjo/fleetcore/internal/protocol"
	"github.com/flavioanjo/fleetcore/internal/sessions"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// BenchmarkDecodeToEvent is the per-packet CPU cost of a decode worker:
// protocol decode, validation, canonical re-encoding and fingerprint.
func BenchmarkDecodeToEvent(b *testing.B) {
	pkt, err := protocol.EncodeSample(canSample("bench-dev", 1, 1, 1, 1000))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, err := protocol.DecodeSample(pkt)
		if err != nil {
			b.Fatal(err)
		}
		s.IngestedAt = telemetry.Some(clock)
		if _, err := eventstore.NewEvent(s); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDedupCache(b *testing.B) {
	c := newDedupCache(65536)
	fp := eventstore.Fingerprint{1}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		k := eventstore.Key{DeviceID: "d", BootID: 1, EventID: telemetry.EventID(i)}
		c.add(k, fp)
		c.isStoredDuplicate(k, fp)
	}
}

// BenchmarkPipelineUDP sends b.N distinct packets from 8 devices over
// loopback UDP to a gateway backed by SQLite (WAL, synchronous=FULL) and
// waits until every datagram read has been accounted for. It reports
// end-to-end stored events per second, datagrams lost before or at the
// gateway, and the heap in use at the end.
func BenchmarkPipelineUDP(b *testing.B) {
	const devices = 8
	pkts := make([][]byte, b.N)
	for i := range pkts {
		dev := fmt.Sprintf("pipe-%d", i%devices)
		seq := telemetry.Sequence(i/devices + 1)
		p, err := protocol.EncodeSample(canSample(dev, 1, telemetry.EventID(seq), seq, telemetry.Millimetres(i)))
		if err != nil {
			b.Fatal(err)
		}
		pkts[i] = p
	}
	store, err := eventstore.Open(context.Background(), b.TempDir()+"/events.db", eventstore.Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	cfg := baseConfig()
	cfg.ReadBuffer = 8 << 20
	cfg.QueueSize, cfg.StoreQueueSize = 16384, 16384
	cfg.BatchDelay = 5 * time.Millisecond
	g, err := New(cfg, store, sessions.New(sessions.Config{}))
	if err != nil {
		b.Fatal(err)
	}
	done := make(chan error, 1)
	ctx, cancel := contextWithCancel()
	go func() { done <- g.Run(ctx) }()
	conn, err := net.Dial("udp", g.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	b.ResetTimer()
	begin := time.Now()
	for i, p := range pkts {
		conn.Write(p)
		if i%256 == 255 {
			time.Sleep(100 * time.Microsecond) // modest pacing to stay inside socket buffers
		}
	}
	// Wait until the gateway is idle: everything read has been settled and
	// no new datagrams arrived for 200ms.
	var st Stats
	lastRecv, idleSince := uint64(0), time.Now()
	for {
		st = g.Stats()
		if st.Received != lastRecv {
			lastRecv, idleSince = st.Received, time.Now()
		}
		if settled(st) == st.Received && (st.Received == uint64(b.N) || time.Since(idleSince) > 200*time.Millisecond) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	elapsed := time.Since(begin)
	b.StopTimer()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	cancel()
	<-done

	b.ReportMetric(float64(st.Inserted)/elapsed.Seconds(), "stored/s")
	b.ReportMetric(float64(uint64(b.N)-st.Inserted), "lost")
	b.ReportMetric(float64(ms.HeapInuse)/(1<<20), "heapMiB")
}

func contextWithCancel() (ctx context.Context, cancel context.CancelFunc) {
	return context.WithCancel(context.Background())
}
