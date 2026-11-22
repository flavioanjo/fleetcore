package simulator

import (
	"time"

	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// Truth is the simulator's ground truth for one vehicle and one round: the
// motion the sensors observed, before any sensor error.
//
// Ground truth exists only for testing and evaluation. It is not part of
// telemetry.Sample, of the wire protocol or of fleet-sim's output, and
// production code (such as internal/fusion) must never depend on it.
type Truth struct {
	DeviceID telemetry.DeviceID
	Round    int
	// DeviceTime equals the DeviceTime of the samples emitted for this
	// vehicle and round.
	DeviceTime time.Time
	// DistanceMM is the true distance travelled during the round's interval.
	DistanceMM telemetry.Millimetres
	// OdometerMM is the true (unquantised) odometer at the end of the round.
	OdometerMM telemetry.Millimetres
}

// TruthFunc receives ground truth. It is called once per vehicle and round,
// immediately before that round's samples are emitted.
type TruthFunc func(Truth)

// RunWithGroundTruth behaves exactly like Run — same samples, same order —
// and additionally reports the ground truth of every round to truth.
//
// TESTING AND EVALUATION API ONLY. Observing the truth does not consume
// randomness, so the emitted samples are identical to Run's.
func RunWithGroundTruth(cfg Config, emit EmitFunc, truth TruthFunc) error {
	if truth == nil {
		truth = func(Truth) {}
	}
	return run(cfg, emit, truth)
}
