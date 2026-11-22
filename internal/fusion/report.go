package fusion

import (
	"time"

	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// ReportType discriminates the two kinds of Report.
type ReportType string

// Report types.
const (
	ReportEvent    ReportType = "event"    // the disposition of one input sample
	ReportEstimate ReportType = "estimate" // a distance estimate for one epoch
)

// Report is one unit of engine output. Exactly one of Event and Estimate is
// set, according to Type. Reports are produced in a deterministic order.
type Report struct {
	Type     ReportType   `json:"type"`
	Event    *EventReport `json:"event,omitempty"`
	Estimate *Estimate    `json:"estimate,omitempty"`
}

// Outcome is the disposition of an input sample.
type Outcome string

// Outcomes. Every input sample receives exactly one final outcome; a
// buffered sample receives "buffered" first and a final outcome later.
const (
	// OutcomeAccepted: released in sequence order and given to the estimator.
	OutcomeAccepted Outcome = "accepted"
	// OutcomeBuffered: held in the reorder buffer, waiting for earlier
	// sequence numbers. A later report gives its final outcome.
	OutcomeBuffered Outcome = "buffered"
	// OutcomeDuplicate: an identical copy of an event already seen; ignored.
	OutcomeDuplicate Outcome = "duplicate"
	// OutcomeRejected: not applied and not recoverable by this engine;
	// Reason says why.
	OutcomeRejected Outcome = "rejected"
	// OutcomeRequiresReconciliation: a plausible event that arrived after
	// its position in the sequence was finalised. It was not applied; a
	// replay or reconciliation process must handle it.
	OutcomeRequiresReconciliation Outcome = "requires_reconciliation"
)

// Reason refines a non-accepted outcome.
type Reason string

// Reasons.
const (
	ReasonNone             Reason = ""
	ReasonInvalidSample    Reason = "invalid_sample"    // failed telemetry validation
	ReasonSequenceConflict Reason = "sequence_conflict" // same (boot, sequence) as a different event
	ReasonStaleSession     Reason = "stale_session"     // belongs to a closed or older session
	ReasonDeviceCapacity   Reason = "device_capacity"   // engine already tracks MaxDevices devices
	// ReasonSequenceExhausted is no longer produced: sequence numbers are
	// extended internally and wrap-around is supported. Kept so that stored
	// reports from earlier versions still decode to a named reason.
	ReasonSequenceExhausted Reason = "sequence_exhausted"
	ReasonSequenceAmbiguous Reason = "sequence_ambiguous" // too far from the session's position (serial arithmetic) to be placed
	ReasonLateAfterGap      Reason = "late_after_gap"     // its sequence slot was declared missing
	ReasonBeyondTracking    Reason = "beyond_tracking"    // older than duplicate/gap tracking; duplicate or late cannot be told apart
)

// EventReport describes what happened to one input sample.
type EventReport struct {
	DeviceID    telemetry.DeviceID `json:"device_id"`
	EventID     telemetry.EventID  `json:"event_id"`
	BootID      telemetry.BootID   `json:"boot_id"`
	Sequence    telemetry.Sequence `json:"seq"`
	Source      string             `json:"source"`
	DeviceTime  time.Time          `json:"device_time"`
	Outcome     Outcome            `json:"outcome"`
	Reason      Reason             `json:"reason,omitempty"`
	Diagnostics []Diagnostic       `json:"diagnostics,omitempty"`
}

// EstimateState says how firm an estimate is. No estimate is certain.
type EstimateState string

// Estimate states.
const (
	// StateProvisional: based on delta sources only since the last CAN
	// constraint; later CAN readings may revise the trip distance.
	StateProvisional EstimateState = "provisional"
	// StateBounded: the trip distance was just checked against (and, if
	// needed, moved inside) the interval implied by a CAN odometer reading.
	StateBounded EstimateState = "bounded"
)

// Confidence is a coarse summary of Estimate.Uncertainty.
type Confidence string

// Confidence levels.
const (
	ConfidenceNone   Confidence = "none"   // no measurement has contributed yet
	ConfidenceLow    Confidence = "low"    // uncertainty above 2% of the trip (or 20 m on short trips), or unresolved disagreement
	ConfidenceMedium Confidence = "medium" // uncertainty above 0.5% of the trip (or 5 m)
	ConfidenceHigh   Confidence = "high"
)

// Contribution is one delta source's input to an estimate.
type Contribution struct {
	Source telemetry.Source `json:"source"`
	// ReportedMM is the distance in the sample (pulse: nominal calibration).
	ReportedMM telemetry.Millimetres `json:"reported_mm"`
	// UsedMM is this source's view of the trip increment for the epoch
	// after calibration, span alignment and stationary handling.
	UsedMM telemetry.Millimetres `json:"used_mm"`
	// WeightPPM is the source's share of the fused increment, in ppm.
	WeightPPM int64 `json:"weight_ppm"`
}

// Estimate is the fused distance estimate for one epoch: the set of samples
// of one device session that share a device timestamp.
type Estimate struct {
	DeviceID telemetry.DeviceID `json:"device_id"`
	BootID   telemetry.BootID   `json:"boot_id"`
	FirstSeq telemetry.Sequence `json:"first_seq"`
	LastSeq  telemetry.Sequence `json:"last_seq"`
	// From is the previous epoch's device time in this session (absent for
	// the first epoch of a session); To is this epoch's device time.
	From telemetry.Optional[time.Time] `json:"from"`
	To   time.Time                     `json:"to"`
	// DeltaMM is the change of TripMM: the fused measured increment plus
	// InterpolatedMM plus CorrectionMM. It can be negative when a CAN
	// reading revises the trip downwards.
	DeltaMM telemetry.Millimetres `json:"delta_mm"`
	// InterpolatedMM is travel estimated (not measured) for an earlier span
	// that no source covered, from the speeds before and after it.
	InterpolatedMM telemetry.Millimetres `json:"interpolated_mm"`
	// CorrectionMM is the part of DeltaMM that comes from CAN
	// reconciliation rather than from this epoch's delta measurements. It
	// may represent travel over several earlier epochs.
	CorrectionMM telemetry.Millimetres `json:"correction_mm"`
	// TripMM is the estimated distance travelled since the engine first saw
	// the device. It survives device reboots.
	TripMM telemetry.Millimetres `json:"trip_mm"`
	// UncertaintyMM is an approximate 95% half-width of TripMM's error. It
	// is a model-based indicator, not a guarantee.
	UncertaintyMM telemetry.Millimetres `json:"uncertainty_mm"`
	Confidence    Confidence            `json:"confidence"`
	State         EstimateState         `json:"state"`
	Contributions []Contribution        `json:"contributions"`
	Diagnostics   []Diagnostic          `json:"diagnostics,omitempty"`
}

// Level is a diagnostic's severity.
type Level string

// Levels.
const (
	LevelInfo    Level = "info"
	LevelWarning Level = "warning"
)

// Code identifies a diagnostic. Codes are stable machine-readable strings.
type Code string

// Diagnostic codes.
const (
	// Input, ordering and sessions.
	CodeInvalidSample       Code = "invalid_sample"
	CodeStaleSession        Code = "stale_session_event"
	CodeOutOfOrderBuffered  Code = "out_of_order_buffered"
	CodeReleasedFromBuffer  Code = "released_from_buffer"
	CodeSequenceGap         Code = "sequence_gap"
	CodeIdleFlush           Code = "idle_flush"
	CodeSessionStarted      Code = "session_started"
	CodeSessionTransition   Code = "session_transition"
	CodeSessionOrderUnclear Code = "session_order_ambiguous"
	CodeSessionAdopted      Code = "session_adopted_despite_clock"
	CodeDelayedDelivery     Code = "delayed_delivery"
	CodeDeviceClockAhead    Code = "device_clock_ahead"
	CodeRepeatedTimestamp   Code = "repeated_timestamp"
	CodeClockRegression     Code = "clock_regression"
	CodeSequenceAmbiguous   Code = "sequence_ambiguous"

	// Measurements.
	CodeSourceUnavailable      Code = "source_unavailable"
	CodeSpanAmbiguous          Code = "span_ambiguous"
	CodeMeasurementImplausible Code = "measurement_implausible"
	CodeGPSStationaryDrift     Code = "gps_stationary_drift"
	CodeGPSBelowDeadband       Code = "gps_below_deadband"
	CodeSensorDisagreement     Code = "sensor_disagreement"
	CodeSourceSuspect          Code = "source_suspect"
	CodeSourceRecovered        Code = "source_recovered"
	CodeSourceExcluded         Code = "source_excluded"
	CodeInsufficientEvidence   Code = "insufficient_evidence"
	CodeGapInterpolated        Code = "gap_interpolated"
	CodeGapNotInterpolated     Code = "gap_not_interpolated"

	// CAN.
	CodeCANBaseline        Code = "can_baseline_established"
	CodeCANUnavailable     Code = "can_unavailable"
	CodeCANUnchanged       Code = "can_unchanged_quantization"
	CodeCANReconciled      Code = "can_reconciled"
	CodeCANDecreased       Code = "can_decreased"
	CodeCANJumpRejected    Code = "can_jump_rejected"
	CodeCANRebaselined     Code = "can_rebaselined"
	CodeCANStale           Code = "can_stale"
	CodeCalibrationUpdated Code = "pulse_calibration_updated"
	CodeCalibrationReject  Code = "pulse_calibration_rejected"
)

// Diagnostic explains one decision of the engine.
type Diagnostic struct {
	Code   Code             `json:"code"`
	Level  Level            `json:"level"`
	Source telemetry.Source `json:"source,omitempty"`
	// Seq is the sequence number the diagnostic refers to, when relevant.
	Seq *telemetry.Sequence `json:"seq,omitempty"`
	// Value carries a code-specific number (documented per code in
	// docs/fusion.md), e.g. a correction in mm or a calibration in ppm.
	Value   *int64 `json:"value,omitempty"`
	Message string `json:"message"`
}

func diag(code Code, level Level, msg string) Diagnostic {
	return Diagnostic{Code: code, Level: level, Message: msg}
}

func (d Diagnostic) withSource(s telemetry.Source) Diagnostic { d.Source = s; return d }
func (d Diagnostic) withSeq(s telemetry.Sequence) Diagnostic  { d.Seq = &s; return d }
func (d Diagnostic) withValue(v int64) Diagnostic             { d.Value = &v; return d }
