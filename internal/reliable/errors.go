package reliable

import (
	"errors"
	"fmt"
)

// Code classifies why a v2 packet or message was rejected. Codes are
// diagnostic: a receiver never answers a packet that failed before
// authentication (it could be forged), and answers authenticated but
// invalid events with a signed negative ACK.
type Code uint8

// Rejection codes, in the order the decoder checks them.
const (
	CodeTruncated          Code = iota + 1 // shorter than its framing requires
	CodeTooLarge                           // packet, payload or field exceeds its maximum
	CodeBadMagic                           // not a FleetCore packet
	CodeUnsupportedVersion                 // not version 2
	CodeLengthMismatch                     // declared length disagrees with the packet
	CodeReservedFlags                      // reserved flag bits set
	CodeUnknownType                        // message type not defined in version 2
	CodeBadDeviceID                        // device id length or alphabet invalid
	CodeBadHeader                          // zero key id, session or counter
	CodeUnknownKey                         // no key for (device, key id)
	CodeRevokedKey                         // key version revoked
	CodeBadMAC                             // authentication tag does not verify
	CodeWrongDirection                     // message type not valid in this direction
	CodeStaleSession                       // session older than the device's latest
	CodeReplay                             // counter already seen or too old
	CodeMalformedPayload                   // payload structure invalid
	CodeInvalidEvent                       // event body violates domain rules
	CodeInvalidCommand                     // command violates domain rules
	CodeWrongDevice                        // message for/from another device
)

var codeNames = map[Code]string{
	CodeTruncated:          "truncated",
	CodeTooLarge:           "too_large",
	CodeBadMagic:           "bad_magic",
	CodeUnsupportedVersion: "unsupported_version",
	CodeLengthMismatch:     "length_mismatch",
	CodeReservedFlags:      "reserved_flags",
	CodeUnknownType:        "unknown_type",
	CodeBadDeviceID:        "bad_device_id",
	CodeBadHeader:          "bad_header",
	CodeUnknownKey:         "unknown_key",
	CodeRevokedKey:         "revoked_key",
	CodeBadMAC:             "bad_mac",
	CodeWrongDirection:     "wrong_direction",
	CodeStaleSession:       "stale_session",
	CodeReplay:             "replay",
	CodeMalformedPayload:   "malformed_payload",
	CodeInvalidEvent:       "invalid_event",
	CodeInvalidCommand:     "invalid_command",
	CodeWrongDevice:        "wrong_device",
}

// Codes lists every code, for metrics.
func Codes() []Code {
	out := make([]Code, 0, len(codeNames))
	for c := CodeTruncated; c <= CodeWrongDevice; c++ {
		out = append(out, c)
	}
	return out
}

func (c Code) String() string {
	if n, ok := codeNames[c]; ok {
		return n
	}
	return fmt.Sprintf("code(%d)", uint8(c))
}

// Error is returned by every decoder in this package. errors.Is matches on
// Code against the sentinels below.
type Error struct {
	Code   Code
	Detail string
	Err    error
}

func (e *Error) Error() string {
	s := "reliable: " + e.Code.String()
	if e.Detail != "" {
		s += ": " + e.Detail
	}
	if e.Err != nil {
		s += ": " + e.Err.Error()
	}
	return s
}

// Is matches *Error values with the same Code.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Code == e.Code
}

// Unwrap returns the cause.
func (e *Error) Unwrap() error { return e.Err }

// Sentinels for errors.Is.
var (
	ErrTruncated          = &Error{Code: CodeTruncated}
	ErrTooLarge           = &Error{Code: CodeTooLarge}
	ErrBadMagic           = &Error{Code: CodeBadMagic}
	ErrUnsupportedVersion = &Error{Code: CodeUnsupportedVersion}
	ErrLengthMismatch     = &Error{Code: CodeLengthMismatch}
	ErrReservedFlags      = &Error{Code: CodeReservedFlags}
	ErrUnknownType        = &Error{Code: CodeUnknownType}
	ErrBadDeviceID        = &Error{Code: CodeBadDeviceID}
	ErrBadHeader          = &Error{Code: CodeBadHeader}
	ErrUnknownKey         = &Error{Code: CodeUnknownKey}
	ErrRevokedKey         = &Error{Code: CodeRevokedKey}
	ErrBadMAC             = &Error{Code: CodeBadMAC}
	ErrWrongDirection     = &Error{Code: CodeWrongDirection}
	ErrStaleSession       = &Error{Code: CodeStaleSession}
	ErrReplay             = &Error{Code: CodeReplay}
	ErrMalformedPayload   = &Error{Code: CodeMalformedPayload}
	ErrInvalidEvent       = &Error{Code: CodeInvalidEvent}
	ErrInvalidCommand     = &Error{Code: CodeInvalidCommand}
	ErrWrongDevice        = &Error{Code: CodeWrongDevice}
)

func errf(c Code, format string, args ...any) *Error {
	return &Error{Code: c, Detail: fmt.Sprintf(format, args...)}
}

// CodeOf returns the code of a reliable error, or 0.
func CodeOf(err error) Code {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return 0
}

// ErrDeviceUnreachable is returned by a command transport that cannot
// address the device: it has not heard an authenticated frame of the
// device's current session, so it knows neither its address nor the
// session to seal the delivery for. Dispatchers try another transport.
var ErrDeviceUnreachable = errors.New("reliable: device not reachable through this transport")
