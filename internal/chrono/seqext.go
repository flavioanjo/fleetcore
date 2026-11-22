package chrono

import "github.com/flavioanjo/fleetcore/internal/telemetry"

// Sequence numbers are 32-bit and may wrap (telemetry.Sequence). Within a
// session the orderer places them in an extended 64-bit space by bounded
// serial arithmetic around the highest number seen, so a wrap is simply the
// next number. The extension is internal: samples keep their 32-bit values.
//
// internal/fusion applies the same rule (fusion/order.go). The two copies
// are kept separate so that fusion continues to depend only on
// internal/telemetry; TestExtenderParityWithChrono (in internal/fusion)
// pins them together.
const (
	// SerialLimit bounds the serial distance at which a 32-bit sequence
	// number can still be placed. At SerialLimit or more, in either
	// direction, the number is ambiguous.
	SerialLimit = 1 << 30
	extBase     = 1 << 32
)

// Extender maps one session's 32-bit sequence numbers to extended ones.
// The zero value is ready to use.
type Extender struct {
	high    uint64
	started bool
}

// Extend returns the extended position of s, or false if s is ambiguous. It
// does not move the reference; call Observe for numbers that are accepted.
func (x *Extender) Extend(s telemetry.Sequence) (uint64, bool) {
	if !x.started {
		return extBase + uint64(s), true
	}
	d := int64(int32(uint32(s) - uint32(x.high)))
	if d >= SerialLimit || d <= -SerialLimit {
		return 0, false
	}
	return uint64(int64(x.high) + d), true
}

// Observe records an accepted extended sequence number.
func (x *Extender) Observe(ext uint64) {
	if !x.started || ext > x.high {
		x.high, x.started = ext, true
	}
}
