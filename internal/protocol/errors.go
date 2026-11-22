package protocol

import "fmt"

// ErrorCode classifies protocol errors.
type ErrorCode uint8

// Error codes.
const (
	CodeTruncated          ErrorCode = iota + 1 // packet shorter than its framing requires
	CodeTooLarge                                // packet or payload exceeds the maximum size
	CodeBadMagic                                // packet does not start with the FleetCore magic
	CodeUnsupportedVersion                      // protocol version not supported
	CodeUnknownMessageType                      // message type not defined for this version
	CodeReservedFlags                           // reserved flag bits are set
	CodeLengthMismatch                          // declared length disagrees with actual length
	CodeChecksumMismatch                        // integrity check failed
	CodeMalformedPayload                        // payload structure is invalid
	CodeInvalidSample                           // payload decodes but violates domain rules
)

var codeNames = map[ErrorCode]string{
	CodeTruncated:          "truncated packet",
	CodeTooLarge:           "packet too large",
	CodeBadMagic:           "bad magic",
	CodeUnsupportedVersion: "unsupported protocol version",
	CodeUnknownMessageType: "unknown message type",
	CodeReservedFlags:      "reserved flags set",
	CodeLengthMismatch:     "length mismatch",
	CodeChecksumMismatch:   "checksum mismatch",
	CodeMalformedPayload:   "malformed payload",
	CodeInvalidSample:      "invalid sample",
}

func (c ErrorCode) String() string {
	if n, ok := codeNames[c]; ok {
		return n
	}
	return fmt.Sprintf("error code %d", uint8(c))
}

// Error is the error type returned by every function in this package.
//
// Compare against the exported sentinels with errors.Is, which matches on
// Code only:
//
//	if errors.Is(err, protocol.ErrChecksumMismatch) { ... }
type Error struct {
	Code   ErrorCode
	Detail string
	// Err is the underlying cause, if any (for CodeInvalidSample it is the
	// telemetry validation error).
	Err error
}

func (e *Error) Error() string {
	s := "protocol: " + e.Code.String()
	if e.Detail != "" {
		s += ": " + e.Detail
	}
	if e.Err != nil {
		s += ": " + e.Err.Error()
	}
	return s
}

// Is reports whether target is an *Error with the same Code.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Code == e.Code
}

// Unwrap returns the underlying cause.
func (e *Error) Unwrap() error { return e.Err }

// Sentinel errors for use with errors.Is.
var (
	ErrTruncated          = &Error{Code: CodeTruncated}
	ErrTooLarge           = &Error{Code: CodeTooLarge}
	ErrBadMagic           = &Error{Code: CodeBadMagic}
	ErrUnsupportedVersion = &Error{Code: CodeUnsupportedVersion}
	ErrUnknownMessageType = &Error{Code: CodeUnknownMessageType}
	ErrReservedFlags      = &Error{Code: CodeReservedFlags}
	ErrLengthMismatch     = &Error{Code: CodeLengthMismatch}
	ErrChecksumMismatch   = &Error{Code: CodeChecksumMismatch}
	ErrMalformedPayload   = &Error{Code: CodeMalformedPayload}
	ErrInvalidSample      = &Error{Code: CodeInvalidSample}
)

func newError(code ErrorCode, format string, args ...any) *Error {
	return &Error{Code: code, Detail: fmt.Sprintf(format, args...)}
}
