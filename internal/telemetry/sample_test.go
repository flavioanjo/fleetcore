package telemetry

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, time.March, 14, 9, 30, 0, 0, time.UTC)

func validSample() Sample {
	return Sample{
		DeviceID:   "SIM-00001",
		EventID:    42,
		BootID:     7,
		Sequence:   3,
		DeviceTime: t0,
		IngestedAt: Some(t0.Add(800 * time.Millisecond)),
		Source:     SourceCAN,
		Quality:    QualityGood,
		Distance:   Some(Distance{Kind: DistanceCumulative, Value: 123_456 * Kilometre}),
	}
}

// fieldErr returns the FieldError for field inside err, or nil.
func fieldErr(err error, field string) *FieldError {
	var joined interface{ Unwrap() []error }
	list := []error{err}
	if errors.As(err, &joined) {
		list = joined.Unwrap()
	}
	for _, e := range list {
		var fe *FieldError
		if errors.As(e, &fe) && fe.Field == field {
			return fe
		}
	}
	return nil
}

func TestValidateAcceptsValidSamples(t *testing.T) {
	tests := map[string]func(*Sample){
		"can cumulative":           func(*Sample) {},
		"gps delta":                func(s *Sample) { s.Source = SourceGPS; s.Distance = Some(Distance{DistanceDelta, 1500}) },
		"pulse delta":              func(s *Sample) { s.Source = SourcePulse; s.Distance = Some(Distance{DistanceDelta, 0}) },
		"degraded":                 func(s *Sample) { s.Quality = QualityDegraded },
		"unavailable no distance":  func(s *Sample) { s.Quality = QualityUnavailable; s.Distance = None[Distance]() },
		"not yet ingested":         func(s *Sample) { s.IngestedAt = None[time.Time]() },
		"zero sequence":            func(s *Sample) { s.Sequence = 0 },
		"max sequence":             func(s *Sample) { s.Sequence = ^Sequence(0) },
		"zero cumulative":          func(s *Sample) { s.Distance = Some(Distance{DistanceCumulative, 0}) },
		"max cumulative":           func(s *Sample) { s.Distance = Some(Distance{DistanceCumulative, MaxCumulativeDistance}) },
		"max delta":                func(s *Sample) { s.Source = SourceGPS; s.Distance = Some(Distance{DistanceDelta, MaxDeltaDistance}) },
		"min timestamp":            func(s *Sample) { s.DeviceTime = MinTimestamp; s.IngestedAt = Some(MinTimestamp) },
		"last ms before max":       func(s *Sample) { s.DeviceTime = MaxTimestamp.Add(-time.Millisecond); s.IngestedAt = None[time.Time]() },
		"device ahead within skew": func(s *Sample) { s.IngestedAt = Some(t0.Add(-MaxClockSkew)) },
		"device id max length":     func(s *Sample) { s.DeviceID = DeviceID(strings.Repeat("A", MaxDeviceIDLen)) },
		"device id charset":        func(s *Sample) { s.DeviceID = "aZ09-_" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			s := validSample()
			mutate(&s)
			if err := s.Validate(); err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
		})
	}
}

func TestValidateRejectsInvalidSamples(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Sample)
		field  string
		reason error
	}{
		{"empty device id", func(s *Sample) { s.DeviceID = "" }, "device_id", ErrMissing},
		{"device id too long", func(s *Sample) { s.DeviceID = DeviceID(strings.Repeat("A", MaxDeviceIDLen+1)) }, "device_id", ErrOutOfRange},
		{"device id bad char", func(s *Sample) { s.DeviceID = "SIM 01" }, "device_id", ErrInvalid},
		{"device id non-ascii", func(s *Sample) { s.DeviceID = "SIMé" }, "device_id", ErrInvalid},
		{"zero event id", func(s *Sample) { s.EventID = 0 }, "event_id", ErrMissing},
		{"zero boot id", func(s *Sample) { s.BootID = 0 }, "boot_id", ErrMissing},
		{"missing device time", func(s *Sample) { s.DeviceTime = time.Time{}; s.IngestedAt = None[time.Time]() }, "device_time", ErrMissing},
		{"device time too early", func(s *Sample) { s.DeviceTime = MinTimestamp.Add(-time.Millisecond); s.IngestedAt = None[time.Time]() }, "device_time", ErrOutOfRange},
		{"device time at max", func(s *Sample) { s.DeviceTime = MaxTimestamp; s.IngestedAt = None[time.Time]() }, "device_time", ErrOutOfRange},
		{"device time not utc", func(s *Sample) { s.DeviceTime = t0.In(time.FixedZone("X", 3600)) }, "device_time", ErrInvalid},
		{"device time sub-ms", func(s *Sample) { s.DeviceTime = t0.Add(time.Microsecond) }, "device_time", ErrInvalid},
		{"ingested out of range", func(s *Sample) { s.IngestedAt = Some(MaxTimestamp) }, "ingested_at", ErrOutOfRange},
		{"device beyond skew", func(s *Sample) { s.IngestedAt = Some(t0.Add(-MaxClockSkew - time.Millisecond)) }, "device_time", ErrInconsistent},
		{"unspecified source", func(s *Sample) { s.Source = SourceUnspecified }, "source", ErrInvalid},
		{"reserved source", func(s *Sample) { s.Source = 9 }, "source", ErrInvalid},
		{"unspecified quality", func(s *Sample) { s.Quality = QualityUnspecified }, "quality", ErrInvalid},
		{"missing distance", func(s *Sample) { s.Distance = None[Distance]() }, "distance", ErrMissing},
		{"distance when unavailable", func(s *Sample) { s.Quality = QualityUnavailable }, "distance", ErrInconsistent},
		{"unspecified kind", func(s *Sample) { s.Distance = Some(Distance{DistanceUnspecified, 1}) }, "distance.kind", ErrInvalid},
		{"can reporting delta", func(s *Sample) { s.Distance = Some(Distance{DistanceDelta, 1}) }, "distance.kind", ErrInconsistent},
		{"gps reporting cumulative", func(s *Sample) { s.Source = SourceGPS }, "distance.kind", ErrInconsistent},
		{"negative cumulative", func(s *Sample) { s.Distance = Some(Distance{DistanceCumulative, -1}) }, "distance.value_mm", ErrOutOfRange},
		{"cumulative over max", func(s *Sample) { s.Distance = Some(Distance{DistanceCumulative, MaxCumulativeDistance + 1}) }, "distance.value_mm", ErrOutOfRange},
		{"negative delta", func(s *Sample) { s.Source = SourcePulse; s.Distance = Some(Distance{DistanceDelta, -1}) }, "distance.value_mm", ErrOutOfRange},
		{"delta over max", func(s *Sample) {
			s.Source = SourceGPS
			s.Distance = Some(Distance{DistanceDelta, MaxDeltaDistance + 1})
		}, "distance.value_mm", ErrOutOfRange},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := validSample()
			tc.mutate(&s)
			err := s.Validate()
			if err == nil {
				t.Fatal("Validate() = nil, want error")
			}
			fe := fieldErr(err, tc.field)
			if fe == nil {
				t.Fatalf("Validate() = %v, want error on field %q", err, tc.field)
			}
			if !errors.Is(fe, tc.reason) {
				t.Fatalf("field %q reason = %v, want %v", tc.field, fe.Reason, tc.reason)
			}
		})
	}
}

func TestValidateReportsAllViolations(t *testing.T) {
	err := Sample{}.Validate()
	for _, f := range []string{"device_id", "event_id", "boot_id", "device_time", "source", "quality"} {
		if fieldErr(err, f) == nil {
			t.Errorf("zero Sample: no error for field %q (got %v)", f, err)
		}
	}
}

func TestSourceDistanceKind(t *testing.T) {
	want := map[Source]DistanceKind{
		SourceGPS:         DistanceDelta,
		SourceCAN:         DistanceCumulative,
		SourcePulse:       DistanceDelta,
		SourceUnspecified: DistanceUnspecified,
	}
	for src, k := range want {
		if got := src.DistanceKind(); got != k {
			t.Errorf("%s.DistanceKind() = %s, want %s", src, got, k)
		}
	}
}

func TestJSONRoundTrip(t *testing.T) {
	cases := []Sample{validSample(), func() Sample {
		s := validSample()
		s.Source = SourceGPS
		s.Quality = QualityUnavailable
		s.Distance = None[Distance]()
		s.IngestedAt = None[time.Time]()
		return s
	}()}
	for _, in := range cases {
		b, err := json.Marshal(in)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		var out Sample
		if err := json.Unmarshal(b, &out); err != nil {
			t.Fatalf("Unmarshal(%s): %v", b, err)
		}
		if !samplesEqual(in, out) {
			t.Fatalf("round trip mismatch:\n in: %+v\nout: %+v\njson: %s", in, out, b)
		}
	}
}

func TestJSONShape(t *testing.T) {
	s := validSample()
	s.Distance = None[Distance]()
	s.Quality = QualityUnavailable
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	for _, want := range []string{`"source":"can"`, `"quality":"unavailable"`, `"distance":null`, `"device_time":"2026-03-14T09:30:00Z"`} {
		if !strings.Contains(got, want) {
			t.Errorf("JSON %s missing %s", got, want)
		}
	}
}

func TestParseSource(t *testing.T) {
	for _, src := range Sources() {
		got, err := ParseSource(src.String())
		if err != nil || got != src {
			t.Errorf("ParseSource(%q) = %v, %v", src.String(), got, err)
		}
	}
	if _, err := ParseSource("radar"); err == nil {
		t.Error("ParseSource(radar) succeeded")
	}
}

func TestOptional(t *testing.T) {
	var zero Optional[int]
	if zero.IsPresent() {
		t.Error("zero Optional is present")
	}
	if v, ok := Some(0).Get(); !ok || v != 0 {
		t.Error("Some(0) must be present with value 0")
	}
}

func samplesEqual(a, b Sample) bool {
	ai, aok := a.IngestedAt.Get()
	bi, bok := b.IngestedAt.Get()
	if aok != bok || (aok && !ai.Equal(bi)) {
		return false
	}
	a.IngestedAt, b.IngestedAt = None[time.Time](), None[time.Time]()
	if !a.DeviceTime.Equal(b.DeviceTime) {
		return false
	}
	a.DeviceTime, b.DeviceTime = time.Time{}, time.Time{}
	return a == b
}
