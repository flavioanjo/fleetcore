package protocol_test

import (
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/protocol"
	"github.com/flavioanjo/fleetcore/internal/simulator"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// TestSimulatorOutputRoundTrips encodes every sample of a simulator run and
// checks it decodes back unchanged (apart from IngestedAt, which is not on
// the wire).
func TestSimulatorOutputRoundTrips(t *testing.T) {
	cfg := simulator.DefaultConfig()
	cfg.Vehicles, cfg.Samples = 10, 100
	n := 0
	err := simulator.Run(cfg, func(in telemetry.Sample) error {
		pkt, err := protocol.EncodeSample(in)
		if err != nil {
			t.Fatalf("encode %+v: %v", in, err)
		}
		out, err := protocol.DecodeSample(pkt)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		in.IngestedAt = telemetry.None[time.Time]()
		if !out.DeviceTime.Equal(in.DeviceTime) {
			t.Fatalf("device time %v != %v", out.DeviceTime, in.DeviceTime)
		}
		out.DeviceTime, in.DeviceTime = time.Time{}, time.Time{}
		if out != in {
			t.Fatalf("mismatch\n in %+v\nout %+v", in, out)
		}
		n++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatal("no samples generated")
	}
}
