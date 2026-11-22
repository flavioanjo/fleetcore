package reliable

import (
	"encoding/binary"
	"time"
)

// reader decodes big-endian fields with bounds checks. The first failure
// sticks; callers check err once at the end.
type reader struct {
	b   []byte
	off int
	err *Error
}

func (r *reader) need(n int) bool {
	if r.err != nil {
		return false
	}
	if n < 0 || len(r.b)-r.off < n {
		r.err = errf(CodeMalformedPayload, "need %d bytes at offset %d, have %d", n, r.off, len(r.b)-r.off)
		return false
	}
	return true
}

func (r *reader) u8() uint8 {
	if !r.need(1) {
		return 0
	}
	v := r.b[r.off]
	r.off++
	return v
}

func (r *reader) u16() uint16 {
	if !r.need(2) {
		return 0
	}
	v := binary.BigEndian.Uint16(r.b[r.off:])
	r.off += 2
	return v
}

func (r *reader) u32() uint32 {
	if !r.need(4) {
		return 0
	}
	v := binary.BigEndian.Uint32(r.b[r.off:])
	r.off += 4
	return v
}

func (r *reader) u64() uint64 {
	if !r.need(8) {
		return 0
	}
	v := binary.BigEndian.Uint64(r.b[r.off:])
	r.off += 8
	return v
}

func (r *reader) bytes(n int) []byte {
	if !r.need(n) {
		return nil
	}
	v := append([]byte(nil), r.b[r.off:r.off+n]...)
	r.off += n
	return v
}

func (r *reader) fixed(dst []byte) {
	if !r.need(len(dst)) {
		return
	}
	copy(dst, r.b[r.off:])
	r.off += len(dst)
}

// millis decodes a Unix-millisecond timestamp in the domain's range.
func (r *reader) millis(field string) time.Time {
	v := int64(r.u64())
	if r.err != nil {
		return time.Time{}
	}
	t := time.UnixMilli(v).UTC()
	if t.Before(MinTime) || !t.Before(MaxTime) {
		r.err = errf(CodeMalformedPayload, "%s out of range", field)
	}
	return t
}

// done fails on trailing bytes and returns the sticky error.
func (r *reader) done() error {
	if r.err == nil && r.off != len(r.b) {
		r.err = errf(CodeLengthMismatch, "%d trailing bytes", len(r.b)-r.off)
	}
	if r.err == nil {
		return nil
	}
	return r.err
}

// fail records a semantic error (if none is recorded yet).
func (r *reader) fail(c Code, format string, args ...any) {
	if r.err == nil {
		r.err = errf(c, format, args...)
	}
}

// Timestamp range of every v2 time field: the domain range of
// internal/telemetry, at millisecond resolution.
var (
	MinTime = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	MaxTime = time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
)

func putMillis(b []byte, t time.Time) []byte {
	return binary.BigEndian.AppendUint64(b, uint64(t.UnixMilli()))
}

// canonicalTime truncates to the wire resolution.
func canonicalTime(t time.Time) time.Time { return time.UnixMilli(t.UnixMilli()).UTC() }

func inRange(t time.Time) bool { return !t.Before(MinTime) && t.Before(MaxTime) }
