package fusion

import (
	"encoding/binary"
	"hash/fnv"
	"time"

	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// Sequence numbers are 32-bit on the wire and may wrap around after
// 2^32-1 (telemetry.Sequence). Inside a session the engine orders events in
// an extended 64-bit space instead, so a wrap is just the next number. The
// extension is internal: reports and diagnostics carry the original 32-bit
// values.
//
// The mapping uses bounded serial arithmetic around the highest sequence
// number seen in the session: a 32-bit value is placed at the nearest
// extended position, provided that position is closer than serialLimit.
// Values at serial distance serialLimit or more (in either direction) are
// ambiguous, because they could be far in the past or far in the future,
// and are rejected (ReasonSequenceAmbiguous) instead of being guessed.
const (
	serialLimit = 1 << 30
	// extBase places the first sequence number of a session far enough from
	// zero that serial distances down to -serialLimit never underflow.
	extBase = 1 << 32
)

// seqExtender maps one session's 32-bit sequence numbers to extended ones.
type seqExtender struct {
	high    uint64 // highest extended sequence number observed
	started bool
}

// extend returns the extended position of s, or false if s is ambiguous.
// It does not change the reference; call observe for accepted numbers.
func (x *seqExtender) extend(s telemetry.Sequence) (uint64, bool) {
	if !x.started {
		return extBase + uint64(s), true
	}
	d := int64(int32(uint32(s) - uint32(x.high))) // serial distance in [-2^31, 2^31)
	if d >= serialLimit || d <= -serialLimit {
		return 0, false
	}
	return uint64(int64(x.high) + d), true
}

func (x *seqExtender) observe(ext uint64) {
	if !x.started || ext > x.high {
		x.high, x.started = ext, true
	}
}

// session orders the events of one device boot by extended sequence number.
//
// Invariants:
//   - every buffered event has ext >= next once started;
//   - len(buf) <= MaxBufferedPerSession after release returns;
//   - events are released strictly in increasing extended sequence order;
//   - an extended sequence number below next is never released again.
//
// Before the first release ("not started") the session does not know its
// first sequence number: it buffers until the reorder window or the buffer
// capacity forces a start at the lowest buffered sequence number.
type session struct {
	boot    telemetry.BootID
	ext     seqExtender
	started bool
	start   uint64    // first released extended sequence number
	next    uint64    // next extended sequence number to release
	maxTime time.Time // latest device time seen in this session
	buf     map[uint64]telemetry.Sample
	bufHigh int // buffer high-water mark since the map was last reallocated

	gaps    []seqRange // ring of declared-missing ranges
	gapHead int

	// Fingerprints of the most recently released events, as a ring. It is
	// searched only for events below next (duplicates or late arrivals),
	// so a linear scan of a compact slice beats a map in memory.
	recent     []recentEvent
	recentHead int
}

type recentEvent struct {
	ext uint64
	fp  uint64
}

// seqRange is an inclusive range of extended sequence numbers.
type seqRange struct{ from, to uint64 }

func (r seqRange) contains(e uint64) bool { return e >= r.from && e <= r.to }

// fromSeq and toSeq are the range bounds as wire sequence numbers.
func (r seqRange) fromSeq() telemetry.Sequence { return telemetry.Sequence(r.from) }
func (r seqRange) toSeq() telemetry.Sequence   { return telemetry.Sequence(r.to) }
func (r seqRange) count() int64                { return int64(r.to-r.from) + 1 }

// released is an event leaving the buffer, with the gap declared just
// before it (if any).
type released struct {
	sample telemetry.Sample
	gap    *seqRange
}

func newSession(boot telemetry.BootID, cfg *Config) *session {
	return &session{boot: boot, buf: make(map[uint64]telemetry.Sample)}
}

// fingerprint identifies an event's content, for telling exact duplicates
// from conflicting events that reuse a sequence number.
func fingerprint(s telemetry.Sample) uint64 {
	h := fnv.New64a()
	var b [8]byte
	put := func(v uint64) { binary.BigEndian.PutUint64(b[:], v); h.Write(b[:]) }
	put(uint64(s.EventID))
	put(uint64(s.DeviceTime.UnixMilli()))
	put(uint64(s.Source)<<8 | uint64(s.Quality))
	if d, ok := s.Distance.Get(); ok {
		put(uint64(d.Kind))
		put(uint64(d.Value))
	} else {
		put(^uint64(0))
	}
	return h.Sum64()
}

// classify decides whether s can enter the buffer. It returns
// OutcomeAccepted, with the extended sequence number, when it can (the final
// outcome is decided on release).
func (ss *session) classify(s telemetry.Sample) (Outcome, Reason, uint64) {
	e, ok := ss.ext.extend(s.Sequence)
	if !ok {
		return OutcomeRejected, ReasonSequenceAmbiguous, 0
	}
	fp := fingerprint(s)
	if b, ok := ss.buf[e]; ok {
		if fingerprint(b) == fp {
			return OutcomeDuplicate, ReasonNone, e
		}
		return OutcomeRejected, ReasonSequenceConflict, e
	}
	if !ss.started || e >= ss.next {
		return OutcomeAccepted, ReasonNone, e
	}
	for _, r := range ss.recent {
		if r.ext == e {
			if r.fp == fp {
				return OutcomeDuplicate, ReasonNone, e
			}
			return OutcomeRejected, ReasonSequenceConflict, e
		}
	}
	if e < ss.start {
		return OutcomeRequiresReconciliation, ReasonLateAfterGap, e // before the session's chosen start
	}
	for _, g := range ss.gaps {
		if g.contains(e) {
			return OutcomeRequiresReconciliation, ReasonLateAfterGap, e
		}
	}
	return OutcomeRequiresReconciliation, ReasonBeyondTracking, e
}

func (ss *session) insert(s telemetry.Sample, e uint64) {
	ss.buf[e] = s
	ss.ext.observe(e)
	ss.bufHigh = max(ss.bufHigh, len(ss.buf))
	if s.DeviceTime.After(ss.maxTime) {
		ss.maxTime = s.DeviceTime
	}
}

func (ss *session) minBuffered() uint64 {
	first := true
	var m uint64
	for k := range ss.buf {
		if first || k < m {
			m, first = k, false
		}
	}
	return m
}

// release returns the events that may now be processed, in extended
// sequence order. With force, everything buffered is released (end of input
// or session end), declaring gaps as needed.
func (ss *session) release(cfg *Config, force bool) []released {
	var out []released
	var pendingGap *seqRange
	for len(ss.buf) > 0 {
		if _, ok := ss.buf[ss.next]; !ss.started || !ok {
			m := ss.minBuffered()
			waited := ss.maxTime.Sub(ss.buf[m].DeviceTime)
			if !force && len(ss.buf) <= cfg.MaxBufferedPerSession && waited < cfg.ReorderWindow {
				break
			}
			if ss.started {
				g := seqRange{ss.next, m - 1}
				ss.recordGap(g, cfg)
				pendingGap = &g
			} else {
				ss.start = m
			}
			ss.started, ss.next = true, m
		}
		s := ss.buf[ss.next]
		delete(ss.buf, ss.next)
		ss.remember(ss.next, s, cfg)
		out = append(out, released{sample: s, gap: pendingGap})
		pendingGap = nil
		ss.next++
	}
	if len(ss.buf) == 0 && ss.bufHigh > 16 {
		// Go maps never shrink; drop the buckets left by a reordering burst.
		ss.buf, ss.bufHigh = make(map[uint64]telemetry.Sample), 0
	}
	return out
}

func (ss *session) recordGap(g seqRange, cfg *Config) {
	if len(ss.gaps) < cfg.MaxTrackedGaps {
		ss.gaps = append(ss.gaps, g)
		return
	}
	ss.gaps[ss.gapHead] = g
	ss.gapHead = (ss.gapHead + 1) % cfg.MaxTrackedGaps
}

func (ss *session) remember(e uint64, s telemetry.Sample, cfg *Config) {
	r := recentEvent{e, fingerprint(s)}
	if len(ss.recent) < cfg.RecentEvents {
		ss.recent = append(ss.recent, r)
		return
	}
	ss.recent[ss.recentHead] = r
	ss.recentHead = (ss.recentHead + 1) % cfg.RecentEvents
}
