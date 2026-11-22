// Package fusion estimates the distance travelled by each vehicle from
// imperfect telemetry: GPS distance deltas, a quantised CAN odometer and a
// pulse counter of unknown calibration.
//
// The Engine accepts samples in any arrival order. Per device it
//
//   - orders events by sequence number within each boot session, using a
//     bounded reorder buffer (late, duplicate, conflicting and stale events
//     receive explicit outcomes instead of being applied or dropped
//     silently);
//   - groups events that share a device timestamp into epochs;
//   - fuses delta sources by inverse-variance weighting, keeps the trip
//     inside the interval implied by the CAN odometer, calibrates the pulse
//     counter over long reference spans, and interpolates short spans no
//     source covered;
//   - reports, for every epoch, an Estimate with uncertainty, contributing
//     sources and structured diagnostics.
//
// The engine is deterministic and uses integer arithmetic in millimetres.
// It has no I/O, goroutines or wall-clock dependency, and its memory per
// device is bounded by Config. It keeps no persistent state: a restarted
// process starts from nothing.
//
// The design, its assumptions and the evaluation are documented in
// docs/fusion.md.
package fusion
