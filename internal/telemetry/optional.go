package telemetry

import (
	"bytes"
	"encoding/json"
)

// Optional holds a value that may be absent.
//
// FleetCore never uses a zero value to mean "not measured": a distance of
// 0 mm is a real measurement (the vehicle did not move), which is different
// from "the sensor produced no reading". Optional makes that distinction
// explicit in the type system.
//
// The zero value of Optional is an absent value.
type Optional[T any] struct {
	value   T
	present bool
}

// Some returns an Optional holding v.
func Some[T any](v T) Optional[T] {
	return Optional[T]{value: v, present: true}
}

// None returns an absent Optional.
func None[T any]() Optional[T] {
	return Optional[T]{}
}

// Get returns the held value and whether it is present. When the value is
// absent the returned T is the zero value and must not be used.
func (o Optional[T]) Get() (T, bool) {
	return o.value, o.present
}

// IsPresent reports whether a value is held.
func (o Optional[T]) IsPresent() bool {
	return o.present
}

// MarshalJSON encodes an absent value as JSON null.
func (o Optional[T]) MarshalJSON() ([]byte, error) {
	if !o.present {
		return []byte("null"), nil
	}
	return json.Marshal(o.value)
}

// UnmarshalJSON decodes JSON null as an absent value.
func (o *Optional[T]) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		*o = None[T]()
		return nil
	}
	var v T
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	*o = Some(v)
	return nil
}
