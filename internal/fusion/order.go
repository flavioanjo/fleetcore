package fusion

import (
	"encoding/binary"
	"hash/fnv"
	"time"

	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// session orders the events of one device boot by sequence number.
//
// Invariants:
//   - every buffered event has seq >= next once started;
//   - len(buf) <= MaxBufferedPerSession after release returns;
//   - events are released strictly in increasing sequence order;
//   - a sequence number below next is never released again.
//
// Before the first release ("not started") the session does not know its
// first sequence number: it buffers until the reorder window or the buffer
// capacity forces a start at the lowest buffered sequence number.
type session struct {
	boot      telemetry.BootID
	started   bool
	start     telemetry.Sequence // first released sequence number
	next      telemetry.Sequence
	exhausted bool      // the maximum sequence number was released; wrap-around is unsupported
	maxTime   time.Time // latest device time seen in this session
	buf       map[telemetry.Sequence]telemetry.Sample
	bufHigh   int // buffer high-water mark since the map was last reallocated

	gaps    []seqRange // ring of declared-missing ranges
	gapHead int

	// Fingerprints of the most recently released events, as a ring. It is
	// searched only for events below next (duplicates or late arrivals),
	// so a linear scan of a compact slice beats a map in memory.
	recent     []recentEvent
	recentHead int
}

type recentEvent struct {
	seq telemetry.Sequence
	fp  uint64
}

type seqRange struct{ from, to telemetry.Sequence }

func (r seqRange) contains(s telemetry.Sequence) bool { return s >= r.from && s <= r.to }

// released is an event leaving the buffer, with the gap declared just
// before it (if any).
type released struct {
	sample telemetry.Sample
	gap    *seqRange
}

func newSession(boot telemetry.BootID, cfg *Config) *session {
	return &session{boot: boot, buf: make(map[telemetry.Sequence]telemetry.Sample)}
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
// OutcomeAccepted when it can (the final outcome is decided on release).
func (ss *session) classify(s telemetry.Sample) (Outcome, Reason) {
	fp := fingerprint(s)
	if b, ok := ss.buf[s.Sequence]; ok {
		if fingerprint(b) == fp {
			return OutcomeDuplicate, ReasonNone
		}
		return OutcomeRejected, ReasonSequenceConflict
	}
	if !ss.started || (s.Sequence >= ss.next && !ss.exhausted) {
		return OutcomeAccepted, ReasonNone
	}
	for _, r := range ss.recent {
		if r.seq == s.Sequence {
			if r.fp == fp {
				return OutcomeDuplicate, ReasonNone
			}
			return OutcomeRejected, ReasonSequenceConflict
		}
	}
	if ss.exhausted {
		return OutcomeRejected, ReasonSequenceExhausted
	}
	if s.Sequence < ss.start {
		return OutcomeRequiresReconciliation, ReasonLateAfterGap // before the session's chosen start
	}
	for _, g := range ss.gaps {
		if g.contains(s.Sequence) {
			return OutcomeRequiresReconciliation, ReasonLateAfterGap
		}
	}
	return OutcomeRequiresReconciliation, ReasonBeyondTracking
}

func (ss *session) insert(s telemetry.Sample) {
	ss.buf[s.Sequence] = s
	ss.bufHigh = max(ss.bufHigh, len(ss.buf))
	if s.DeviceTime.After(ss.maxTime) {
		ss.maxTime = s.DeviceTime
	}
}

func (ss *session) minBuffered() telemetry.Sequence {
	first := true
	var m telemetry.Sequence
	for k := range ss.buf {
		if first || k < m {
			m, first = k, false
		}
	}
	return m
}

// release returns the events that may now be processed, in sequence order.
// With force, everything buffered is released (end of input or session
// end), declaring gaps as needed.
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
		ss.remember(s, cfg)
		out = append(out, released{sample: s, gap: pendingGap})
		pendingGap = nil
		if ss.next == ^telemetry.Sequence(0) {
			ss.exhausted = true // nothing can follow without wrapping
			break
		}
		ss.next++
	}
	if len(ss.buf) == 0 && ss.bufHigh > 16 {
		// Go maps never shrink; drop the buckets left by a reordering burst.
		ss.buf, ss.bufHigh = make(map[telemetry.Sequence]telemetry.Sample), 0
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

func (ss *session) remember(s telemetry.Sample, cfg *Config) {
	r := recentEvent{s.Sequence, fingerprint(s)}
	if len(ss.recent) < cfg.RecentEvents {
		ss.recent = append(ss.recent, r)
		return
	}
	ss.recent[ss.recentHead] = r
	ss.recentHead = (ss.recentHead + 1) % cfg.RecentEvents
}
