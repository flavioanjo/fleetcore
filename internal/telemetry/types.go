package telemetry

import (
	"fmt"
	"time"
)

// DeviceID identifies a telemetry device. It is 1 to MaxDeviceIDLen bytes
// of ASCII letters, digits, '-' or '_'.
type DeviceID string

// MaxDeviceIDLen is the maximum length of a DeviceID in bytes.
const MaxDeviceIDLen = 32

// EventID identifies a single sample. Zero is reserved and invalid.
type EventID uint64

// BootID identifies one power cycle (session) of a device. A device picks a
// new BootID every time it starts; sequence numbers restart with it. Zero is
// reserved and invalid.
type BootID uint32

// Sequence is the per-boot sample counter. Within one BootID, sequence
// numbers increase by one for every sample the device produces, across all
// measurement sources. It may wrap around after 2^32-1.
type Sequence uint32

// Millimetres is a distance in millimetres. All FleetCore distances use this
// unit so that values are exact integers.
type Millimetres int64

// Common distance helpers.
const (
	Metre     Millimetres = 1000
	Kilometre Millimetres = 1000 * Metre
)

// Distance limits enforced by validation.
const (
	// MaxCumulativeDistance bounds odometer readings (10 million km).
	MaxCumulativeDistance = 10_000_000 * Kilometre
	// MaxDeltaDistance bounds the distance a single delta sample may report
	// (500 km), which covers one hour at well above highway speed.
	MaxDeltaDistance = 500 * Kilometre
)

// Timestamp limits enforced by validation.
var (
	// MinTimestamp is the earliest accepted device or ingestion time.
	MinTimestamp = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	// MaxTimestamp is the latest accepted device or ingestion time.
	MaxTimestamp = time.Date(2100, time.January, 1, 0, 0, 0, 0, time.UTC)
)

// MaxClockSkew is how far a device timestamp may be ahead of its ingestion
// timestamp before the sample is considered inconsistent.
const MaxClockSkew = 5 * time.Minute

// TimestampResolution is the resolution of device timestamps.
const TimestampResolution = time.Millisecond

// Source is the measurement source that produced a sample.
type Source uint8

// Measurement sources. SourceUnspecified is never valid in a sample.
const (
	SourceUnspecified Source = 0
	SourceGPS         Source = 1 // distance travelled between GPS fixes
	SourceCAN         Source = 2 // vehicle odometer read from the CAN bus
	SourcePulse       Source = 3 // wheel/gearbox pulse counter
)

var sourceNames = map[Source]string{
	SourceGPS:   "gps",
	SourceCAN:   "can",
	SourcePulse: "pulse",
}

// Sources lists every valid source in canonical order.
func Sources() []Source {
	return []Source{SourceGPS, SourceCAN, SourcePulse}
}

// Valid reports whether s is a defined, non-reserved source.
func (s Source) Valid() bool {
	_, ok := sourceNames[s]
	return ok
}

// DistanceKind returns the only distance kind a source may report:
// GPS and pulse counters report deltas, the CAN odometer reports a
// cumulative value.
func (s Source) DistanceKind() DistanceKind {
	switch s {
	case SourceGPS, SourcePulse:
		return DistanceDelta
	case SourceCAN:
		return DistanceCumulative
	default:
		return DistanceUnspecified
	}
}

func (s Source) String() string {
	if n, ok := sourceNames[s]; ok {
		return n
	}
	return fmt.Sprintf("source(%d)", uint8(s))
}

// MarshalText implements encoding.TextMarshaler.
func (s Source) MarshalText() ([]byte, error) {
	if !s.Valid() {
		return nil, fmt.Errorf("telemetry: cannot marshal invalid source %d", uint8(s))
	}
	return []byte(s.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (s *Source) UnmarshalText(b []byte) error {
	v, err := ParseSource(string(b))
	if err != nil {
		return err
	}
	*s = v
	return nil
}

// ParseSource parses a source name ("gps", "can", "pulse").
func ParseSource(name string) (Source, error) {
	for k, v := range sourceNames {
		if v == name {
			return k, nil
		}
	}
	return SourceUnspecified, fmt.Errorf("telemetry: unknown source %q", name)
}

// DistanceKind says how a distance value is to be interpreted.
type DistanceKind uint8

// Distance kinds. DistanceUnspecified is never valid in a sample.
const (
	DistanceUnspecified DistanceKind = 0
	// DistanceCumulative is a total since the vehicle was built (odometer).
	DistanceCumulative DistanceKind = 1
	// DistanceDelta is the distance travelled since the previous sample of
	// the same source, within the same boot, that carried a distance.
	// Whether distance travelled during an unavailable period is folded
	// into the next delta or lost depends on the source.
	DistanceDelta DistanceKind = 2
)

var kindNames = map[DistanceKind]string{
	DistanceCumulative: "cumulative",
	DistanceDelta:      "delta",
}

// Valid reports whether k is a defined, non-reserved kind.
func (k DistanceKind) Valid() bool {
	_, ok := kindNames[k]
	return ok
}

func (k DistanceKind) String() string {
	if n, ok := kindNames[k]; ok {
		return n
	}
	return fmt.Sprintf("kind(%d)", uint8(k))
}

// MarshalText implements encoding.TextMarshaler.
func (k DistanceKind) MarshalText() ([]byte, error) {
	if !k.Valid() {
		return nil, fmt.Errorf("telemetry: cannot marshal invalid distance kind %d", uint8(k))
	}
	return []byte(k.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (k *DistanceKind) UnmarshalText(b []byte) error {
	for kk, v := range kindNames {
		if v == string(b) {
			*k = kk
			return nil
		}
	}
	return fmt.Errorf("telemetry: unknown distance kind %q", string(b))
}

// Quality describes how trustworthy a measurement is.
type Quality uint8

// Quality values. QualityUnspecified is never valid in a sample.
const (
	QualityUnspecified Quality = 0
	// QualityGood: the source reports the measurement as reliable.
	QualityGood Quality = 1
	// QualityDegraded: a value is present but its accuracy is reduced
	// (for example a GPS fix with few satellites).
	QualityDegraded Quality = 2
	// QualityUnavailable: the source produced no measurement. The sample's
	// Distance must be absent.
	QualityUnavailable Quality = 3
)

var qualityNames = map[Quality]string{
	QualityGood:        "good",
	QualityDegraded:    "degraded",
	QualityUnavailable: "unavailable",
}

// Valid reports whether q is a defined, non-reserved quality.
func (q Quality) Valid() bool {
	_, ok := qualityNames[q]
	return ok
}

func (q Quality) String() string {
	if n, ok := qualityNames[q]; ok {
		return n
	}
	return fmt.Sprintf("quality(%d)", uint8(q))
}

// MarshalText implements encoding.TextMarshaler.
func (q Quality) MarshalText() ([]byte, error) {
	if !q.Valid() {
		return nil, fmt.Errorf("telemetry: cannot marshal invalid quality %d", uint8(q))
	}
	return []byte(q.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (q *Quality) UnmarshalText(b []byte) error {
	for k, v := range qualityNames {
		if v == string(b) {
			*q = k
			return nil
		}
	}
	return fmt.Errorf("telemetry: unknown quality %q", string(b))
}

// Distance is a distance measurement with an explicit interpretation.
type Distance struct {
	Kind  DistanceKind `json:"kind"`
	Value Millimetres  `json:"value_mm"`
}
