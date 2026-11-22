package gateway

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/flavioanjo/fleetcore/internal/protocol"
	"github.com/flavioanjo/fleetcore/internal/sessions"
)

// reason is why a datagram was rejected before reaching the store.
type reason uint8

const (
	rejOversize     reason = iota // exceeds MaxDatagramSize
	rejTruncated                  // protocol.CodeTruncated
	rejTooLarge                   // protocol.CodeTooLarge
	rejBadMagic                   // protocol.CodeBadMagic
	rejVersion                    // protocol.CodeUnsupportedVersion
	rejType                       // protocol.CodeUnknownMessageType
	rejFlags                      // protocol.CodeReservedFlags
	rejLength                     // protocol.CodeLengthMismatch
	rejChecksum                   // protocol.CodeChecksumMismatch
	rejMalformed                  // protocol.CodeMalformedPayload (and unknown errors)
	rejInvalid                    // protocol.CodeInvalidSample
	rejInconsistent               // valid on the wire, invalid with its ingestion time (clock skew)
	numReasons
)

var reasonNames = [numReasons]string{
	"oversize", "truncated", "too_large", "bad_magic", "unsupported_version", "unknown_type",
	"reserved_flags", "length_mismatch", "checksum_mismatch", "malformed_payload", "invalid_sample",
	"inconsistent_with_ingestion_time",
}

func (r reason) String() string { return reasonNames[r] }

func rejectReason(c protocol.ErrorCode) reason {
	switch c {
	case protocol.CodeTruncated:
		return rejTruncated
	case protocol.CodeTooLarge:
		return rejTooLarge
	case protocol.CodeBadMagic:
		return rejBadMagic
	case protocol.CodeUnsupportedVersion:
		return rejVersion
	case protocol.CodeUnknownMessageType:
		return rejType
	case protocol.CodeReservedFlags:
		return rejFlags
	case protocol.CodeLengthMismatch:
		return rejLength
	case protocol.CodeChecksumMismatch:
		return rejChecksum
	case protocol.CodeInvalidSample:
		return rejInvalid
	default:
		return rejMalformed
	}
}

type counters struct {
	received, queueFull                    atomic.Uint64
	rejected                               [numReasons]atomic.Uint64
	inserted, duplicates, duplicatesCached atomic.Uint64
	conflicts, storeFailed, storeRetries   atomic.Uint64
	shutdownDropped, eventIDReused         atomic.Uint64
	diagMu                                 sync.Mutex
	diagnostics                            map[sessions.Kind]uint64
}

func (c *counters) countDiagnostic(k sessions.Kind) {
	c.diagMu.Lock()
	if c.diagnostics == nil {
		c.diagnostics = make(map[sessions.Kind]uint64)
	}
	c.diagnostics[k]++
	c.diagMu.Unlock()
}

// Stats is a snapshot of gateway counters. Every datagram read is counted
// in Received and ends in exactly one of: a Rejected reason, QueueFull,
// Inserted, Duplicates, Conflicts, StoreFailed or ShutdownDropped (or is
// still in flight).
type Stats struct {
	Received  uint64 // datagrams read from the socket
	QueueFull uint64 // dropped because the decode queue was full
	// RejectedBy counts datagrams rejected before storage, by reason.
	RejectedBy       map[string]uint64
	Inserted         uint64 // newly stored events
	Duplicates       uint64 // retransmissions of stored events (incl. cached)
	DuplicatesCached uint64 // duplicates answered by the in-memory cache
	Conflicts        uint64 // identity reused with a different payload
	StoreFailed      uint64 // events dropped after exhausting store retries
	StoreRetries     uint64 // retried store transactions
	ShutdownDropped  uint64 // events abandoned at the drain deadline
	// EventIDReused counts inserted events whose EventID was already stored
	// for the device under another BootID (a domain-contract violation).
	EventIDReused uint64
	// Diagnostics counts provisional session diagnostics by kind.
	Diagnostics map[string]uint64
	At          time.Time
}

// Rejected is the total of RejectedBy.
func (s Stats) Rejected() uint64 {
	var n uint64
	for _, v := range s.RejectedBy {
		n += v
	}
	return n
}

func (c *counters) snapshot() Stats {
	s := Stats{
		Received:         c.received.Load(),
		QueueFull:        c.queueFull.Load(),
		RejectedBy:       map[string]uint64{},
		Inserted:         c.inserted.Load(),
		Duplicates:       c.duplicates.Load(),
		DuplicatesCached: c.duplicatesCached.Load(),
		Conflicts:        c.conflicts.Load(),
		StoreFailed:      c.storeFailed.Load(),
		StoreRetries:     c.storeRetries.Load(),
		ShutdownDropped:  c.shutdownDropped.Load(),
		EventIDReused:    c.eventIDReused.Load(),
		Diagnostics:      map[string]uint64{},
		At:               time.Now(),
	}
	for r := reason(0); r < numReasons; r++ {
		if v := c.rejected[r].Load(); v > 0 {
			s.RejectedBy[r.String()] = v
		}
	}
	c.diagMu.Lock()
	for k, v := range c.diagnostics {
		s.Diagnostics[k.String()] = v
	}
	c.diagMu.Unlock()
	return s
}

// logLimiter allows one event per interval (used to rate-limit warnings
// that a flood could otherwise turn into a log flood).
type logLimiter struct {
	every time.Duration
	last  time.Time
}

func newLogLimiter(every time.Duration) *logLimiter { return &logLimiter{every: every} }

// allow is not safe for concurrent use; each goroutine owns its limiter.
func (l *logLimiter) allow(now time.Time) bool {
	if l.last.IsZero() || now.Sub(l.last) >= l.every {
		l.last = now
		return true
	}
	return false
}

// burstLimiter allows up to max events per window and reports, once, how
// many were suppressed in earlier windows. Not safe for concurrent use.
type burstLimiter struct {
	window     time.Duration
	max        int
	start      time.Time
	n          int
	suppressed uint64
}

func newBurstLimiter(window time.Duration, max int) *burstLimiter {
	return &burstLimiter{window: window, max: max}
}

func (l *burstLimiter) allow(now time.Time) (ok bool, reportSuppressed uint64) {
	if l.start.IsZero() || now.Sub(l.start) >= l.window {
		l.start, l.n = now, 0
		reportSuppressed, l.suppressed = l.suppressed, 0
	}
	if l.n < l.max {
		l.n++
		return true, reportSuppressed
	}
	l.suppressed++
	return false, reportSuppressed
}
