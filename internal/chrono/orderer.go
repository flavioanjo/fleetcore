// Package chrono orders stored telemetry events into the chronological
// stream that is fed to the fusion engine.
//
// # Ordering rule (ADR-14 in docs/architecture.md)
//
//   - Within a session (DeviceID + BootID): extended sequence order. The
//     device assigns sequence numbers in generation order; its clock may
//     jump. A regressing device time is reported (FlagClockRegression) and
//     never used to reorder events of the same session.
//   - Between sessions of one device: device time, when it is evidence. The
//     session whose next event has the earliest device time goes first,
//     unless another session's next event is within Config.SessionTolerance
//     of it. Then the device times do not establish an order, and the
//     central ingestion order (store position) decides, reported as
//     FlagChronologyAmbiguous.
//   - Between devices: central ingestion order. Device clocks of different
//     devices are not comparable, and fusion processes devices
//     independently, so no cross-device chronology is claimed.
//
// The store position is a deterministic tie-breaker, not evidence of when
// an event happened.
//
// # Holding
//
// Events arrive in ingestion order, not in generation order. The orderer
// holds each device's next candidate for Config.Hold of processing time
// after it was read, so that an earlier event arriving shortly afterwards
// can still go first. Events that arrive after a later event of the same
// session was released are released anyway, flagged FlagLate; nothing is
// dropped. Memory is bounded by Config.MaxHeld: beyond it the oldest
// candidates are released early (FlagForced).
//
// The orderer is deterministic: the same sequence of Add and Release calls,
// with the same arguments, produces the same output. It has no clock of
// its own and performs no I/O.
package chrono

import (
	"container/heap"
	"container/list"
	"fmt"
	"time"

	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// Config tunes an Orderer. Zero fields select the defaults.
type Config struct {
	// Hold is how long, in processing time, a device's next event is held
	// after it was read before it may be released. Default 2s.
	Hold time.Duration
	// SessionTolerance: device times of two sessions closer than this do not
	// establish their order. Default 1s.
	SessionTolerance time.Duration
	// MaxHeld bounds the number of held events. Default 65536.
	MaxHeld int
	// MaxSessions bounds the number of sessions whose ordering state is
	// remembered after their events are released. Default 100000.
	MaxSessions int
}

func (c Config) withDefaults() Config {
	if c.Hold <= 0 {
		c.Hold = 2 * time.Second
	}
	if c.SessionTolerance <= 0 {
		c.SessionTolerance = time.Second
	}
	if c.MaxHeld <= 0 {
		c.MaxHeld = 65536
	}
	if c.MaxSessions <= 0 {
		c.MaxSessions = 100_000
	}
	return c
}

// Item is a stored event entering the orderer.
type Item struct {
	// Position is the event's store position: the central, persistent
	// ingestion order shared by every gateway.
	Position int64
	Sample   telemetry.Sample
	// SeenAt is the processing time at which the event was read.
	SeenAt time.Time
}

// Flag annotates a released event. Flags are diagnostics; none of them
// causes an event to be withheld.
type Flag uint16

// Flags.
const (
	// FlagLate: a later event of the same session was already released.
	FlagLate Flag = 1 << iota
	// FlagClockRegression: the device time is earlier than that of an
	// earlier-sequence event of the same session already released.
	FlagClockRegression
	// FlagSequenceAmbiguous: the sequence number is SerialLimit or more
	// away from the session's position and cannot be placed; the event is
	// released in ingestion order.
	FlagSequenceAmbiguous
	// FlagSequenceReused: another event with the same session and sequence
	// number was released before (a distinct EventID; see eventstore).
	FlagSequenceReused
	// FlagChronologyAmbiguous: chosen between sessions of the same device
	// whose device times were within SessionTolerance; ingestion order
	// decided.
	FlagChronologyAmbiguous
	// FlagForced: released before its hold expired, because MaxHeld was
	// reached.
	FlagForced
)

var flagNames = []struct {
	f    Flag
	name string
}{
	{FlagLate, "late"},
	{FlagClockRegression, "clock_regression"},
	{FlagSequenceAmbiguous, "sequence_ambiguous"},
	{FlagSequenceReused, "sequence_reused"},
	{FlagChronologyAmbiguous, "chronology_ambiguous"},
	{FlagForced, "forced_release"},
}

// Names returns the names of the set flags, in a fixed order.
func (f Flag) Names() []string {
	var out []string
	for _, n := range flagNames {
		if f&n.f != 0 {
			out = append(out, n.name)
		}
	}
	return out
}

func (f Flag) String() string { return fmt.Sprint(f.Names()) }

// Released is an event leaving the orderer, in chronological order.
type Released struct {
	Item
	// ExtSequence is the internal extended sequence number (0 when the
	// sequence was ambiguous). It is never sent on the wire.
	ExtSequence uint64
	Flags       Flag
}

// Orderer holds and orders events. It is not safe for concurrent use.
type Orderer struct {
	cfg     Config
	devices map[telemetry.DeviceID]*device
	lru     *list.List // of *session, front = most recently used
	ready   candidateHeap
	held    int
	seq     uint64 // version counter for lazy heap entries
	evicted uint64

	// FIFO of held entries in Add (position) order, for MinHeldPosition.
	fifo []*entry
}

type sessionKey struct {
	dev  telemetry.DeviceID
	boot telemetry.BootID
}

type session struct {
	key     sessionKey
	ext     Extender
	held    entryHeap // by (ext, position)
	sent    bool      // an event of this session has been released
	lastExt uint64    // highest extended sequence released
	maxTime time.Time // latest device time released
	elem    *list.Element
}

type device struct {
	id        telemetry.DeviceID
	sessions  map[telemetry.BootID]*session
	ambiguous []*entry // unplaceable sequence numbers, in position order
	version   uint64
}

type entry struct {
	Item
	ext       uint64
	ambiguous bool
	done      bool
}

// New returns an empty orderer.
func New(cfg Config) *Orderer {
	return &Orderer{cfg: cfg.withDefaults(), devices: map[telemetry.DeviceID]*device{}, lru: list.New()}
}

// Held returns the number of events currently held.
func (o *Orderer) Held() int { return o.held }

// SessionsEvicted counts sessions whose ordering state was forgotten
// because MaxSessions was exceeded. A later event of such a session starts
// its ordering state afresh: lateness and reuse relative to events released
// before the eviction are not detected.
func (o *Orderer) SessionsEvicted() uint64 { return o.evicted }

// MinHeldPosition returns the lowest position currently held, or false.
// Every event read with a lower position has been released.
func (o *Orderer) MinHeldPosition() (int64, bool) {
	for len(o.fifo) > 0 && o.fifo[0].done {
		o.fifo[0] = nil
		o.fifo = o.fifo[1:]
	}
	if len(o.fifo) == 0 {
		return 0, false
	}
	return o.fifo[0].Position, true
}

// Add holds an event. Items must be added in increasing Position order.
func (o *Orderer) Add(it Item) {
	d := o.device(it.Sample.DeviceID)
	s := o.session(d, it.Sample.BootID)
	e := &entry{Item: it}
	if ext, ok := s.ext.Extend(it.Sample.Sequence); ok {
		s.ext.Observe(ext)
		e.ext = ext
		heap.Push(&s.held, e)
	} else {
		e.ambiguous = true
		d.ambiguous = append(d.ambiguous, e)
	}
	o.fifo = append(o.fifo, e)
	o.held++
	o.touch(s)
	o.schedule(d)
}

// Restore records an event that was released in an earlier run (replayed
// from a journal) without holding it, so that later lateness, reuse and
// clock-regression flags are computed as if the orderer had never
// restarted.
func (o *Orderer) Restore(r Released) {
	d := o.device(r.Sample.DeviceID)
	s := o.session(d, r.Sample.BootID)
	if r.Flags&FlagSequenceAmbiguous == 0 {
		s.ext.Observe(r.ExtSequence)
		o.noteReleased(s, r.ExtSequence, r.Sample.DeviceTime)
	}
	o.touch(s)
	o.evict()
}

// Release returns, in chronological order, every event whose hold has
// expired at now. With force, everything held is released.
func (o *Orderer) Release(now time.Time, force bool) []Released {
	var out []Released
	for o.ready.Len() > 0 {
		c := o.ready[0]
		d := o.devices[c.dev]
		if d == nil || d.version != c.version {
			heap.Pop(&o.ready) // stale entry
			continue
		}
		e, s, flags := o.candidate(d)
		over := o.held > o.cfg.MaxHeld
		if !force && !over && now.Before(e.SeenAt.Add(o.cfg.Hold)) {
			break
		}
		heap.Pop(&o.ready)
		if over && !force && now.Before(e.SeenAt.Add(o.cfg.Hold)) {
			flags |= FlagForced
		}
		out = append(out, o.release(d, s, e, flags))
		o.schedule(d)
	}
	o.evict()
	return out
}

// candidate picks a device's next event. Among the session heads, the
// earliest device time m is found; the heads with device time below
// m + SessionTolerance are the sessions device time cannot order, and of
// those the lowest position (central ingestion order) wins. The rule
// depends only on the set of heads, never on iteration order. An
// unplaceable (ambiguous) event goes first if it was ingested before the
// chosen head.
func (o *Orderer) candidate(d *device) (*entry, *session, Flag) {
	var minTime time.Time
	first := true
	for _, s := range d.sessions {
		if s.held.Len() > 0 && (first || s.held[0].Sample.DeviceTime.Before(minTime)) {
			minTime, first = s.held[0].Sample.DeviceTime, false
		}
	}
	var best *session
	var flags Flag
	if !first {
		limit := minTime.Add(o.cfg.SessionTolerance)
		tied := 0
		for _, s := range d.sessions {
			if s.held.Len() == 0 || !s.held[0].Sample.DeviceTime.Before(limit) {
				continue
			}
			tied++
			if best == nil || s.held[0].Position < best.held[0].Position {
				best = s
			}
		}
		if tied > 1 {
			flags |= FlagChronologyAmbiguous
		}
	}
	if len(d.ambiguous) > 0 && (best == nil || d.ambiguous[0].Position < best.held[0].Position) {
		return d.ambiguous[0], nil, FlagSequenceAmbiguous
	}
	return best.held[0], best, flags
}

func (o *Orderer) release(d *device, s *session, e *entry, flags Flag) Released {
	if s == nil {
		d.ambiguous[0] = nil
		d.ambiguous = d.ambiguous[1:]
	} else {
		heap.Pop(&s.held)
		switch {
		case s.sent && e.ext == s.lastExt:
			flags |= FlagSequenceReused
		case s.sent && e.ext < s.lastExt:
			flags |= FlagLate
		case s.sent && e.Sample.DeviceTime.Before(s.maxTime):
			flags |= FlagClockRegression
		}
		o.noteReleased(s, e.ext, e.Sample.DeviceTime)
	}
	e.done = true
	o.held--
	return Released{Item: e.Item, ExtSequence: e.ext, Flags: flags}
}

func (o *Orderer) noteReleased(s *session, ext uint64, t time.Time) {
	if !s.sent || ext > s.lastExt {
		s.lastExt = ext
	}
	if !s.sent || t.After(s.maxTime) {
		s.maxTime = t
	}
	s.sent = true
}

func (o *Orderer) device(id telemetry.DeviceID) *device {
	d := o.devices[id]
	if d == nil {
		d = &device{id: id, sessions: map[telemetry.BootID]*session{}}
		o.devices[id] = d
	}
	return d
}

func (o *Orderer) session(d *device, boot telemetry.BootID) *session {
	s := d.sessions[boot]
	if s == nil {
		s = &session{key: sessionKey{d.id, boot}}
		s.elem = o.lru.PushFront(s)
		d.sessions[boot] = s
	}
	return s
}

func (o *Orderer) touch(s *session) { o.lru.MoveToFront(s.elem) }

// schedule (re)queues a device with its current candidate.
func (o *Orderer) schedule(d *device) {
	o.seq++
	d.version = o.seq
	var pos int64 = -1
	for _, s := range d.sessions {
		if s.held.Len() > 0 && (pos < 0 || s.held[0].Position < pos) {
			pos = s.held[0].Position
		}
	}
	if len(d.ambiguous) > 0 && (pos < 0 || d.ambiguous[0].Position < pos) {
		pos = d.ambiguous[0].Position
	}
	if pos >= 0 {
		heap.Push(&o.ready, candidateRef{pos: pos, dev: d.id, version: d.version})
	}
}

// evict forgets the least recently used sessions without held events,
// beyond MaxSessions. Sessions with held events are never evicted (their
// number is bounded by MaxHeld).
func (o *Orderer) evict() {
	for o.lru.Len() > o.cfg.MaxSessions {
		s := o.lru.Back().Value.(*session)
		if s.held.Len() > 0 {
			return
		}
		o.lru.Remove(s.elem)
		o.evicted++
		d := o.devices[s.key.dev]
		delete(d.sessions, s.key.boot)
		if len(d.sessions) == 0 && len(d.ambiguous) == 0 {
			delete(o.devices, d.id)
		}
	}
}

func absDur(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// candidateRef queues a device by the lowest position it holds, so devices
// are served in central ingestion order. Stale refs (older versions) are
// skipped.
type candidateRef struct {
	pos     int64
	dev     telemetry.DeviceID
	version uint64
}

type candidateHeap []candidateRef

func (h candidateHeap) Len() int { return len(h) }
func (h candidateHeap) Less(i, j int) bool {
	if h[i].pos != h[j].pos {
		return h[i].pos < h[j].pos
	}
	return h[i].version < h[j].version
}
func (h candidateHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *candidateHeap) Push(x any)   { *h = append(*h, x.(candidateRef)) }
func (h *candidateHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

type entryHeap []*entry

func (h entryHeap) Len() int { return len(h) }
func (h entryHeap) Less(i, j int) bool {
	if h[i].ext != h[j].ext {
		return h[i].ext < h[j].ext
	}
	return h[i].Position < h[j].Position
}
func (h entryHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *entryHeap) Push(x any)   { *h = append(*h, x.(*entry)) }
func (h *entryHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	return x
}
