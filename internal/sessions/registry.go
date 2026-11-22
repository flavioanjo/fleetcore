// Package sessions tracks device sessions (boots) and reports provisional
// ordering diagnostics: sequence gaps, out-of-order and reused sequence
// numbers, new and retired sessions, and ambiguous session chronology.
//
// # Scope
//
// The registry is advisory. It never decides whether telemetry is kept:
// event identity and deduplication are owned by the event store, and the
// pipeline feeds the registry only events the store has newly inserted (so
// retransmissions never look like reordering). A late event — even one from
// a session that has long expired — is stored normally; the registry only
// describes it.
//
// Registry state is in memory and bounded; after a restart it starts empty
// and its first diagnostics for each device are correspondingly limited.
// Diagnostics are therefore labelled provisional, unlike stored events.
//
// # Chronology
//
// BootID is an opaque identifier, not a counter, and device clocks may reset
// on boot, so neither reliably orders sessions. The registry orders sessions
// only by when it first observed them (ingestion time) and reports
// [ChronologyAmbiguous] whenever the evidence disagrees or is insufficient,
// instead of inventing an order.
//
// # Sequence numbers
//
// Within a session, sequence numbers are compared with serial-number
// arithmetic (RFC 1982 style, 32-bit), so wraparound after 2^32-1 is handled.
// A jump of 2^31 or more is indistinguishable from going backwards.
package sessions

import (
	"container/list"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// Config bounds the registry. Zero fields select the defaults.
type Config struct {
	// MaxDevices is the number of devices tracked; the least recently
	// active device is forgotten when it is exceeded. Default 100000.
	MaxDevices int
	// MaxSessionsPerDevice is the number of sessions tracked per device;
	// the least recently active one is retired when it is exceeded.
	// Default 4.
	MaxSessionsPerDevice int
	// MaxRetiredPerDevice is how many retired BootIDs are remembered per
	// device, so late events from them are recognized. Default 16.
	MaxRetiredPerDevice int
	// SequenceWindow is how many of the most recent sequence numbers per
	// session are remembered individually (rounded up to a multiple of 64).
	// Older late samples cannot be checked for reuse. Default 1024.
	SequenceWindow int
	// SessionIdleTimeout retires a session with no activity for this long
	// (measured in ingestion time). Default 1h.
	SessionIdleTimeout time.Duration
	// DeviceIdleTimeout forgets a device with no activity for this long.
	// Default 24h; raised to SessionIdleTimeout if smaller.
	DeviceIdleTimeout time.Duration
	// OverlapWindow: when a new session appears while another session of
	// the same device was active this recently, their order is ambiguous.
	// Default 1m.
	OverlapWindow time.Duration
}

func (c Config) withDefaults() Config {
	if c.MaxDevices <= 0 {
		c.MaxDevices = 100_000
	}
	if c.MaxSessionsPerDevice <= 0 {
		c.MaxSessionsPerDevice = 4
	}
	if c.MaxRetiredPerDevice <= 0 {
		c.MaxRetiredPerDevice = 16
	}
	if c.SequenceWindow <= 0 {
		c.SequenceWindow = 1024
	}
	c.SequenceWindow = (c.SequenceWindow + 63) / 64 * 64
	if c.SessionIdleTimeout <= 0 {
		c.SessionIdleTimeout = time.Hour
	}
	if c.DeviceIdleTimeout <= 0 {
		c.DeviceIdleTimeout = 24 * time.Hour
	}
	if c.DeviceIdleTimeout < c.SessionIdleTimeout {
		c.DeviceIdleTimeout = c.SessionIdleTimeout
	}
	if c.OverlapWindow <= 0 {
		c.OverlapWindow = time.Minute
	}
	return c
}

// Kind classifies a diagnostic.
type Kind uint8

// Diagnostic kinds.
const (
	// NewSession: first sample of a BootID the registry does not track or
	// remember for this device.
	NewSession Kind = iota + 1
	// RetiredSession: sample for a BootID whose session was retired (idle
	// or evicted). The event is a delayed event from an older session; the
	// session is tracked again.
	RetiredSession
	// EarlierObservedSession: sample for a tracked session other than the
	// device's most recently observed one (an older session's delayed
	// tail, or interleaved sessions).
	EarlierObservedSession
	// ChronologyAmbiguous: the order of two sessions cannot be established
	// (see Diagnostic.OtherBoot and Detail).
	ChronologyAmbiguous
	// SequenceGap: the sequence number skipped ahead; Diagnostic.Count
	// sequence numbers are missing (so far).
	SequenceGap
	// OutOfOrder: sequence number behind the session's highest one, not
	// seen before (it may fill an earlier gap).
	OutOfOrder
	// SequenceReuse: the session already had an event with this sequence
	// number (a distinct EventID with the same BootID and Sequence).
	SequenceReuse
	// SequenceUnverifiable: sequence number older than the tracking window;
	// whether it fills a gap or reuses a number cannot be determined.
	SequenceUnverifiable
)

var kindNames = map[Kind]string{
	NewSession:             "new_session",
	RetiredSession:         "retired_session",
	EarlierObservedSession: "earlier_observed_session",
	ChronologyAmbiguous:    "chronology_ambiguous",
	SequenceGap:            "sequence_gap",
	OutOfOrder:             "out_of_order",
	SequenceReuse:          "sequence_reuse",
	SequenceUnverifiable:   "sequence_unverifiable",
}

func (k Kind) String() string {
	if n, ok := kindNames[k]; ok {
		return n
	}
	return fmt.Sprintf("kind(%d)", uint8(k))
}

// Diagnostic is one provisional finding about a sample.
type Diagnostic struct {
	Kind Kind
	// Count is the number of missing sequence numbers for SequenceGap.
	Count uint32
	// Expected is the next sequence number the session expected
	// (SequenceGap, OutOfOrder).
	Expected telemetry.Sequence
	// OtherBoot is the other session involved (ChronologyAmbiguous,
	// EarlierObservedSession: the device's most recently observed session;
	// NewSession: the previously most recently observed session, if any).
	OtherBoot telemetry.BootID
	Detail    string
}

// SessionState is a snapshot of one tracked session.
type SessionState struct {
	DeviceID telemetry.DeviceID
	BootID   telemetry.BootID
	// FirstSeen and LastSeen are ingestion times of the first and most
	// recent samples.
	FirstSeen, LastSeen time.Time
	// MinDeviceTime and MaxDeviceTime bound the device timestamps seen.
	MinDeviceTime, MaxDeviceTime time.Time
	// Lowest and Highest bound the sequence numbers seen (serial order).
	Lowest, Highest telemetry.Sequence
	// Received counts samples with distinct sequence numbers.
	Received uint64
	// Missing is the number of sequence numbers in [Lowest, Highest] not
	// received.
	Missing uint64
	// Gaps, OutOfOrder and Reused count diagnostics of those kinds.
	Gaps, OutOfOrder, Reused uint64
	// Reopened is true for a session that was retired and then received a
	// late sample.
	Reopened bool
}

// Observation is the registry's view of one sample.
type Observation struct {
	Session     SessionState // after applying the sample
	Diagnostics []Diagnostic
}

// Stats summarizes registry occupancy.
type Stats struct {
	Devices, Sessions int
	DevicesEvicted    uint64 // forgotten because MaxDevices was exceeded
	DevicesExpired    uint64 // forgotten after DeviceIdleTimeout
	SessionsRetired   uint64 // idle or evicted sessions
}

// Registry tracks sessions for many devices. It is safe for concurrent use.
type Registry struct {
	cfg Config

	mu      sync.Mutex
	devices map[telemetry.DeviceID]*list.Element // of *device
	lru     *list.List                           // front = most recently active
	stats   Stats
}

type device struct {
	id       telemetry.DeviceID
	lastSeen time.Time
	sessions map[telemetry.BootID]*session
	latest   telemetry.BootID   // most recently first-observed session
	retired  []telemetry.BootID // ring of retired BootIDs, oldest first
}

type session struct {
	st   SessionState
	seen []uint64 // bitmap over sequence numbers (Highest-window, Highest]
}

// New returns an empty registry.
func New(cfg Config) *Registry {
	return &Registry{
		cfg:     cfg.withDefaults(),
		devices: make(map[telemetry.DeviceID]*list.Element),
		lru:     list.New(),
	}
}

// Observe records a sample, which must carry IngestedAt, and returns the
// diagnostics it produced. Samples need not arrive in any order.
func (r *Registry) Observe(s telemetry.Sample) (Observation, error) {
	now, ok := s.IngestedAt.Get()
	if !ok {
		return Observation{}, fmt.Errorf("sessions: sample %s/%d has no ingestion time", s.DeviceID, s.EventID)
	}
	if s.BootID == 0 || s.DeviceID == "" {
		return Observation{}, fmt.Errorf("sessions: sample without device or boot id")
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	d := r.device(s.DeviceID, now)
	r.retireIdle(d, now)

	var diags []Diagnostic
	ss, tracked := d.sessions[s.BootID]
	if !tracked {
		kind := NewSession
		if d.forget(s.BootID) {
			kind = RetiredSession
		}
		prev := d.latest
		diags = append(diags, Diagnostic{Kind: kind, OtherBoot: prev})
		if kind == NewSession {
			// A reopened session's order relative to the others was
			// already assessed when they were first observed.
			diags = append(diags, r.chronology(d, s, now)...)
		}
		ss = &session{
			st: SessionState{
				DeviceID: s.DeviceID, BootID: s.BootID,
				FirstSeen: now, LastSeen: now,
				MinDeviceTime: s.DeviceTime, MaxDeviceTime: s.DeviceTime,
				Lowest: s.Sequence, Highest: s.Sequence,
				Received: 1, Reopened: kind == RetiredSession,
			},
			seen: make([]uint64, r.cfg.SequenceWindow/64),
		}
		ss.mark(s.Sequence)
		if kind == NewSession {
			d.latest = s.BootID
		} else if _, ok := d.sessions[d.latest]; !ok {
			d.latest = s.BootID
		}
		d.sessions[s.BootID] = ss
		r.stats.Sessions++
		r.enforceSessionLimit(d, s.BootID)
	} else {
		if d.latest != s.BootID {
			diags = append(diags, Diagnostic{Kind: EarlierObservedSession, OtherBoot: d.latest})
		}
		diags = append(diags, ss.observe(s, now, r.cfg.SequenceWindow)...)
	}
	d.lastSeen = now
	return Observation{Session: ss.st, Diagnostics: diags}, nil
}

// chronology reports why the order of a newly observed session relative to
// the device's other sessions cannot be trusted.
func (r *Registry) chronology(d *device, s telemetry.Sample, now time.Time) []Diagnostic {
	var out []Diagnostic
	for boot, o := range d.sessions {
		switch {
		case now.Sub(o.st.LastSeen) < r.cfg.OverlapWindow:
			out = append(out, Diagnostic{Kind: ChronologyAmbiguous, OtherBoot: boot,
				Detail: fmt.Sprintf("session %d was active %s before this session was first observed", boot, now.Sub(o.st.LastSeen))})
		case !s.DeviceTime.After(o.st.MaxDeviceTime):
			out = append(out, Diagnostic{Kind: ChronologyAmbiguous, OtherBoot: boot,
				Detail: fmt.Sprintf("observed after session %d but device time %s is not after its latest %s",
					boot, s.DeviceTime.Format(time.RFC3339Nano), o.st.MaxDeviceTime.Format(time.RFC3339Nano))})
		}
	}
	// Map iteration order is random; report deterministically. (BootIDs are
	// sorted only for stable output, not to imply chronology.)
	sort.Slice(out, func(i, j int) bool { return out[i].OtherBoot < out[j].OtherBoot })
	return out
}

func (ss *session) observe(s telemetry.Sample, now time.Time, window int) []Diagnostic {
	st := &ss.st
	if now.After(st.LastSeen) {
		st.LastSeen = now
	}
	if s.DeviceTime.Before(st.MinDeviceTime) {
		st.MinDeviceTime = s.DeviceTime
	}
	if s.DeviceTime.After(st.MaxDeviceTime) {
		st.MaxDeviceTime = s.DeviceTime
	}

	ahead := int32(s.Sequence - st.Highest) // serial-number distance
	switch {
	case ahead > 0:
		var diags []Diagnostic
		if ahead > 1 {
			st.Gaps++
			diags = append(diags, Diagnostic{Kind: SequenceGap, Count: uint32(ahead - 1), Expected: st.Highest + 1})
		}
		ss.advance(uint32(ahead), window)
		st.Highest = s.Sequence
		st.Received++
		ss.mark(s.Sequence)
		ss.recount()
		return diags
	case uint32(-ahead) < uint32(window): // within the tracked window (incl. ahead == 0)
		if ss.marked(s.Sequence) {
			st.Reused++
			return []Diagnostic{{Kind: SequenceReuse}}
		}
		st.OutOfOrder++
		st.Received++
		ss.mark(s.Sequence)
		if int32(s.Sequence-st.Lowest) < 0 {
			st.Lowest = s.Sequence
		}
		ss.recount()
		return []Diagnostic{{Kind: OutOfOrder, Expected: st.Highest + 1}}
	default:
		st.OutOfOrder++
		st.Received++ // counted as a fill: it cannot be proven a reuse
		if int32(s.Sequence-st.Lowest) < 0 {
			st.Lowest = s.Sequence
		}
		ss.recount()
		return []Diagnostic{{Kind: SequenceUnverifiable, Detail: fmt.Sprintf("sequence %d is older than the %d-sample tracking window", s.Sequence, window)}}
	}
}

func (ss *session) recount() {
	span := uint64(ss.st.Highest-ss.st.Lowest) + 1
	if ss.st.Received >= span {
		ss.st.Missing = 0
	} else {
		ss.st.Missing = span - ss.st.Received
	}
}

// The bitmap covers the window of sequence numbers ending at Highest; bit
// (seq mod window) is set when seq was received.
func (ss *session) bit(seq telemetry.Sequence) (word int, mask uint64) {
	i := int(uint32(seq) % uint32(len(ss.seen)*64))
	return i / 64, 1 << (i % 64)
}

func (ss *session) mark(seq telemetry.Sequence) {
	w, m := ss.bit(seq)
	ss.seen[w] |= m
}

func (ss *session) marked(seq telemetry.Sequence) bool {
	w, m := ss.bit(seq)
	return ss.seen[w]&m != 0
}

// advance slides the window forward by n, clearing the slots it reuses.
func (ss *session) advance(n uint32, window int) {
	if n >= uint32(window) {
		clear(ss.seen)
		return
	}
	for i := uint32(1); i <= n; i++ {
		w, m := ss.bit(ss.st.Highest + telemetry.Sequence(i))
		ss.seen[w] &^= m
	}
}

// device returns the device's state, creating it and evicting the least
// recently active device if needed, and marks it most recently active.
func (r *Registry) device(id telemetry.DeviceID, now time.Time) *device {
	if el, ok := r.devices[id]; ok {
		r.lru.MoveToFront(el)
		return el.Value.(*device)
	}
	for r.lru.Len() >= r.cfg.MaxDevices {
		r.removeDevice(r.lru.Back())
		r.stats.DevicesEvicted++
	}
	d := &device{id: id, lastSeen: now, sessions: make(map[telemetry.BootID]*session)}
	r.devices[id] = r.lru.PushFront(d)
	return d
}

func (r *Registry) removeDevice(el *list.Element) {
	d := r.lru.Remove(el).(*device)
	delete(r.devices, d.id)
	r.stats.Sessions -= len(d.sessions)
}

// retireIdle retires the device's sessions idle for SessionIdleTimeout.
func (r *Registry) retireIdle(d *device, now time.Time) {
	for boot, ss := range d.sessions {
		if now.Sub(ss.st.LastSeen) >= r.cfg.SessionIdleTimeout {
			r.retire(d, boot)
		}
	}
}

// enforceSessionLimit retires least recently active sessions, never keep.
func (r *Registry) enforceSessionLimit(d *device, keep telemetry.BootID) {
	for len(d.sessions) > r.cfg.MaxSessionsPerDevice {
		var oldest telemetry.BootID
		var oldestSeen time.Time
		for boot, ss := range d.sessions {
			if boot == keep {
				continue
			}
			if oldest == 0 || ss.st.LastSeen.Before(oldestSeen) || ss.st.LastSeen.Equal(oldestSeen) && boot < oldest {
				oldest, oldestSeen = boot, ss.st.LastSeen
			}
		}
		r.retire(d, oldest)
	}
}

func (r *Registry) retire(d *device, boot telemetry.BootID) {
	delete(d.sessions, boot)
	r.stats.Sessions--
	r.stats.SessionsRetired++
	d.retired = append(d.retired, boot)
	if over := len(d.retired) - r.cfg.MaxRetiredPerDevice; over > 0 {
		d.retired = append(d.retired[:0], d.retired[over:]...)
	}
}

// forget removes boot from the retired ring and reports whether it was there.
func (d *device) forget(boot telemetry.BootID) bool {
	for i, b := range d.retired {
		if b == boot {
			d.retired = append(d.retired[:i], d.retired[i+1:]...)
			return true
		}
	}
	return false
}

// Expire forgets devices idle for DeviceIdleTimeout as of now and returns
// how many were removed. Call it periodically; its cost is proportional to
// the number of devices removed.
func (r *Registry) Expire(now time.Time) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for el := r.lru.Back(); el != nil; el = r.lru.Back() {
		if now.Sub(el.Value.(*device).lastSeen) < r.cfg.DeviceIdleTimeout {
			break
		}
		r.removeDevice(el)
		r.stats.DevicesExpired++
		n++
	}
	return n
}

// Sessions returns snapshots of a device's tracked sessions, most recently
// active first.
func (r *Registry) Sessions(id telemetry.DeviceID) []SessionState {
	r.mu.Lock()
	defer r.mu.Unlock()
	el, ok := r.devices[id]
	if !ok {
		return nil
	}
	d := el.Value.(*device)
	out := make([]SessionState, 0, len(d.sessions))
	for _, ss := range d.sessions {
		out = append(out, ss.st)
	}
	for i := 1; i < len(out); i++ { // insertion sort; at most MaxSessionsPerDevice
		for j := i; j > 0 && out[j].LastSeen.After(out[j-1].LastSeen); j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// Stats returns registry occupancy counters.
func (r *Registry) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.stats
	s.Devices = r.lru.Len()
	return s
}
