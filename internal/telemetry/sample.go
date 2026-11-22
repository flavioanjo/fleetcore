// Package telemetry defines the FleetCore telemetry domain model.
//
// A Sample is one measurement produced by one source of one device. The
// package defines the types, units and validation rules; it performs no I/O
// and no cross-sample processing (such as odometer fusion).
//
// # Invariants of a valid Sample
//
//   - DeviceID is 1..32 bytes of [A-Za-z0-9_-].
//   - EventID and BootID are non-zero.
//   - DeviceTime is within [MinTimestamp, MaxTimestamp), in UTC, with
//     millisecond resolution.
//   - IngestedAt, when present, is within [MinTimestamp, MaxTimestamp) and
//     DeviceTime is at most MaxClockSkew after it.
//   - Source and Quality are defined, non-reserved values.
//   - Distance is absent if and only if Quality is QualityUnavailable.
//   - A present Distance has the kind dictated by the source
//     (Source.DistanceKind) and a value in [0, MaxCumulativeDistance] for
//     cumulative readings or [0, MaxDeltaDistance] for deltas.
//
// Stream-level invariants (not checkable on a single sample, documented for
// consumers): within one (DeviceID, BootID), Sequence increases by one per
// sample, and EventID is unique per DeviceID.
package telemetry

import (
	"errors"
	"fmt"
	"time"
)

// Sample is a single telemetry measurement.
type Sample struct {
	DeviceID DeviceID `json:"device_id"`
	EventID  EventID  `json:"event_id"`
	BootID   BootID   `json:"boot_id"`
	Sequence Sequence `json:"seq"`
	// DeviceTime is when the device took the measurement, by its own clock.
	DeviceTime time.Time `json:"device_time"`
	// IngestedAt is when the platform received the sample. It is set by the
	// receiver, never by the device, and is therefore not part of the wire
	// protocol.
	IngestedAt Optional[time.Time] `json:"ingested_at"`
	Source     Source              `json:"source"`
	Quality    Quality             `json:"quality"`
	Distance   Optional[Distance]  `json:"distance"`
}

// Validation error reasons. Use errors.Is to test for them.
var (
	ErrMissing      = errors.New("missing required value")
	ErrOutOfRange   = errors.New("value out of range")
	ErrInvalid      = errors.New("invalid value")
	ErrInconsistent = errors.New("inconsistent with other fields")
)

// FieldError reports a validation failure on one field.
type FieldError struct {
	Field  string
	Reason error  // one of ErrMissing, ErrOutOfRange, ErrInvalid, ErrInconsistent
	Detail string // human-readable detail, may be empty
}

func (e *FieldError) Error() string {
	if e.Detail == "" {
		return fmt.Sprintf("telemetry: %s: %v", e.Field, e.Reason)
	}
	return fmt.Sprintf("telemetry: %s: %v: %s", e.Field, e.Reason, e.Detail)
}

// Unwrap returns the reason, so errors.Is(err, ErrOutOfRange) works.
func (e *FieldError) Unwrap() error { return e.Reason }

// Validate checks every invariant documented in the package comment. It
// returns nil for a valid sample, or an error joining one *FieldError per
// violated rule (inspect with errors.As / errors.Is).
func (s Sample) Validate() error {
	var errs []error
	fail := func(field string, reason error, format string, args ...any) {
		errs = append(errs, &FieldError{Field: field, Reason: reason, Detail: fmt.Sprintf(format, args...)})
	}

	if err := s.DeviceID.Validate(); err != nil {
		errs = append(errs, err)
	}
	if s.EventID == 0 {
		fail("event_id", ErrMissing, "zero is reserved")
	}
	if s.BootID == 0 {
		fail("boot_id", ErrMissing, "zero is reserved")
	}

	switch {
	case s.DeviceTime.IsZero():
		fail("device_time", ErrMissing, "")
	case !inTimeRange(s.DeviceTime):
		fail("device_time", ErrOutOfRange, "%s outside [%s, %s)", s.DeviceTime.Format(time.RFC3339Nano), MinTimestamp.Format(time.RFC3339), MaxTimestamp.Format(time.RFC3339))
	case s.DeviceTime.Location() != time.UTC:
		fail("device_time", ErrInvalid, "must be in UTC")
	case s.DeviceTime.Nanosecond()%int(TimestampResolution) != 0:
		fail("device_time", ErrInvalid, "must have millisecond resolution")
	}

	if ing, ok := s.IngestedAt.Get(); ok {
		switch {
		case !inTimeRange(ing):
			fail("ingested_at", ErrOutOfRange, "%s outside [%s, %s)", ing.Format(time.RFC3339Nano), MinTimestamp.Format(time.RFC3339), MaxTimestamp.Format(time.RFC3339))
		case !s.DeviceTime.IsZero() && s.DeviceTime.Sub(ing) > MaxClockSkew:
			fail("device_time", ErrInconsistent, "%s ahead of ingestion time (max %s)", s.DeviceTime.Sub(ing), MaxClockSkew)
		}
	}

	if !s.Source.Valid() {
		fail("source", ErrInvalid, "%s", s.Source)
	}
	if !s.Quality.Valid() {
		fail("quality", ErrInvalid, "%s", s.Quality)
	}

	d, present := s.Distance.Get()
	switch {
	case s.Quality == QualityUnavailable && present:
		fail("distance", ErrInconsistent, "must be absent when quality is unavailable")
	case s.Quality.Valid() && s.Quality != QualityUnavailable && !present:
		fail("distance", ErrMissing, "required when quality is %s", s.Quality)
	case present:
		if err := validateDistance(s.Source, d); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func validateDistance(src Source, d Distance) error {
	if !d.Kind.Valid() {
		return &FieldError{Field: "distance.kind", Reason: ErrInvalid, Detail: d.Kind.String()}
	}
	if src.Valid() && d.Kind != src.DistanceKind() {
		return &FieldError{Field: "distance.kind", Reason: ErrInconsistent,
			Detail: fmt.Sprintf("source %s reports %s distances, got %s", src, src.DistanceKind(), d.Kind)}
	}
	limit := MaxDeltaDistance
	if d.Kind == DistanceCumulative {
		limit = MaxCumulativeDistance
	}
	if d.Value < 0 || d.Value > limit {
		return &FieldError{Field: "distance.value_mm", Reason: ErrOutOfRange,
			Detail: fmt.Sprintf("%d outside [0, %d] for %s distance", d.Value, limit, d.Kind)}
	}
	return nil
}

func inTimeRange(t time.Time) bool {
	return !t.Before(MinTimestamp) && t.Before(MaxTimestamp)
}

// Validate checks the DeviceID format rules.
func (d DeviceID) Validate() error {
	if len(d) == 0 {
		return &FieldError{Field: "device_id", Reason: ErrMissing}
	}
	if len(d) > MaxDeviceIDLen {
		return &FieldError{Field: "device_id", Reason: ErrOutOfRange,
			Detail: fmt.Sprintf("%d bytes, max %d", len(d), MaxDeviceIDLen)}
	}
	for i := 0; i < len(d); i++ {
		c := d[i]
		ok := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_'
		if !ok {
			return &FieldError{Field: "device_id", Reason: ErrInvalid,
				Detail: fmt.Sprintf("byte %d (0x%02x) not in [A-Za-z0-9_-]", i, c)}
		}
	}
	return nil
}
