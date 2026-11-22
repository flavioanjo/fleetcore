package fusion

import (
	"fmt"
	"sort"

	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// Engine fuses telemetry streams of many devices. It is deterministic:
// the reports depend only on the configuration and the sequence of samples
// passed to Process, never on wall-clock time or map iteration order.
//
// An Engine is not safe for concurrent use; feed it from one goroutine, or
// shard devices across several engines.
type Engine struct {
	cfg     Config
	devices map[telemetry.DeviceID]*device
	stats   Stats
}

// Stats counts engine activity. All counters are cumulative except
// Devices and Buffered.
type Stats struct {
	Devices   int             // devices currently tracked
	Buffered  int             // events currently held in reorder buffers
	Outcomes  map[Outcome]int // final outcomes (buffered counts arrivals into the buffer)
	Estimates int
}

type device struct {
	id      telemetry.DeviceID
	cur     *session
	closed  []telemetry.BootID // ring of previous sessions
	closedN int
	cand    telemetry.BootID // unknown "older" BootID seen consecutively
	candN   int
	est     *estimator
}

// New returns an engine, or an error if cfg is invalid.
func New(cfg Config) (*Engine, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Engine{cfg: cfg, devices: map[telemetry.DeviceID]*device{}, stats: Stats{Outcomes: map[Outcome]int{}}}, nil
}

// Stats returns a snapshot of the engine counters.
func (e *Engine) Stats() Stats {
	s := e.stats
	s.Outcomes = make(map[Outcome]int, len(e.stats.Outcomes))
	for k, v := range e.stats.Outcomes {
		s.Outcomes[k] = v
	}
	s.Devices = len(e.devices)
	s.Buffered = 0
	for _, d := range e.devices {
		if d.cur != nil {
			s.Buffered += len(d.cur.buf)
		}
	}
	return s
}

// Process ingests one sample and returns the reports it caused: the
// sample's own event report, reports for previously buffered events it
// released, and estimates for epochs that closed.
func (e *Engine) Process(s telemetry.Sample) []Report {
	var out reports
	if err := s.Validate(); err != nil {
		out.event(e, s, OutcomeRejected, ReasonInvalidSample,
			diag(CodeInvalidSample, LevelWarning, err.Error()))
		return out.list
	}
	d := e.devices[s.DeviceID]
	if d == nil {
		if len(e.devices) >= e.cfg.MaxDevices {
			out.event(e, s, OutcomeRejected, ReasonDeviceCapacity)
			return out.list
		}
		d = &device{id: s.DeviceID, closed: make([]telemetry.BootID, 0, e.cfg.MaxClosedSessions), est: newEstimator(&e.cfg, s.DeviceID)}
		e.devices[s.DeviceID] = d
	}
	d.process(e, s, &out)
	return out.list
}

// Flush releases every buffered event of every device (declaring gaps for
// anything still missing) and closes all open epochs. Call it at the end of
// the input. Devices are flushed in DeviceID order.
func (e *Engine) Flush() []Report {
	ids := make([]telemetry.DeviceID, 0, len(e.devices))
	for id := range e.devices {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	var out reports
	for _, id := range ids {
		d := e.devices[id]
		if d.cur != nil {
			d.drain(e, &out, true)
		}
		out.estimates(e, d.est.endSession())
	}
	return out.list
}

type reports struct{ list []Report }

func (r *reports) event(e *Engine, s telemetry.Sample, o Outcome, reason Reason, diags ...Diagnostic) {
	e.stats.Outcomes[o]++
	r.list = append(r.list, Report{Type: ReportEvent, Event: &EventReport{
		DeviceID: s.DeviceID, EventID: s.EventID, BootID: s.BootID, Sequence: s.Sequence,
		Source: s.Source.String(), DeviceTime: s.DeviceTime, Outcome: o, Reason: reason, Diagnostics: diags,
	}})
}

func (r *reports) estimates(e *Engine, es []Estimate) {
	for i := range es {
		e.stats.Estimates++
		r.list = append(r.list, Report{Type: ReportEstimate, Estimate: &es[i]})
	}
}

func (d *device) process(e *Engine, s telemetry.Sample, out *reports) {
	cfg := &e.cfg
	var notes []Diagnostic
	if ing, ok := s.IngestedAt.Get(); ok {
		if lag := ing.Sub(s.DeviceTime); lag > cfg.DelayedDeliveryNotice {
			notes = append(notes, diag(CodeDelayedDelivery, LevelInfo,
				fmt.Sprintf("ingested %s after the device timestamp; ordering uses sequence numbers, not arrival", lag)).withValue(lag.Milliseconds()))
		} else if ahead := s.DeviceTime.Sub(ing); ahead > cfg.ClockAheadNotice {
			notes = append(notes, diag(CodeDeviceClockAhead, LevelWarning,
				fmt.Sprintf("device clock %s ahead of the ingestion clock", ahead)).withValue(ahead.Milliseconds()))
		}
	}

	switch {
	case d.cur == nil:
		d.begin(s.BootID)
		notes = append(notes, diag(CodeSessionStarted, LevelInfo, "first session seen for this device"))
	case s.BootID != d.cur.boot:
		if d.isClosed(s.BootID) {
			out.event(e, s, OutcomeRejected, ReasonStaleSession, append(notes,
				diag(CodeStaleSession, LevelWarning, "event from a session that already ended; not applied"))...)
			return
		}
		older := s.DeviceTime.Before(d.cur.maxTime.Add(-cfg.SessionSkewTolerance))
		if older {
			if d.cand == s.BootID {
				d.candN++
			} else {
				d.cand, d.candN = s.BootID, 1
			}
			if d.candN < cfg.AdoptAfter {
				out.event(e, s, OutcomeRejected, ReasonStaleSession, append(notes,
					diag(CodeSessionOrderUnclear, LevelWarning, fmt.Sprintf(
						"unknown boot %d is older than current boot %d by more than %s: a delayed earlier session or a device clock reset; not applied (%d/%d before adoption)",
						s.BootID, d.cur.boot, cfg.SessionSkewTolerance, d.candN, cfg.AdoptAfter)))...)
				return
			}
			notes = append(notes, diag(CodeSessionAdopted, LevelWarning,
				fmt.Sprintf("adopting boot %d after %d consecutive events despite its older clock", s.BootID, d.candN)))
		}
		prev := d.cur.boot
		d.drain(e, out, true)
		out.estimates(e, d.est.endSession())
		d.pushClosed(prev)
		d.begin(s.BootID)
		notes = append(notes, diag(CodeSessionTransition, LevelInfo,
			fmt.Sprintf("new boot %d replaces boot %d; trip distance kept, delta-source baselines restarted", s.BootID, prev)).withValue(int64(prev)))
	}
	if s.BootID == d.cur.boot {
		d.candN = 0
	}

	ss := d.cur
	outcome, reason := ss.classify(s)
	if outcome != OutcomeAccepted {
		out.event(e, s, outcome, reason, notes...)
		return
	}
	ss.insert(s)
	rel := ss.release(cfg, false)
	releasedNow := false
	for _, r := range rel {
		releasedNow = releasedNow || r.sample.Sequence == s.Sequence
	}
	self := &s.Sequence
	if !releasedNow {
		out.event(e, s, OutcomeBuffered, ReasonNone, append(notes,
			diag(CodeOutOfOrderBuffered, LevelInfo, "waiting for earlier sequence numbers or the reorder window"))...)
		self, notes = nil, nil
	}
	d.apply(e, rel, out, self, notes)
}

// drain releases everything buffered in the current session.
func (d *device) drain(e *Engine, out *reports, force bool) {
	d.apply(e, d.cur.release(&e.cfg, force), out, nil, nil)
	if force && len(d.cur.buf) != 0 {
		panic("fusion: forced release left events buffered") // invariant violation
	}
}

// apply feeds released events to the estimator. self is the sequence of
// the event being processed if it was released without waiting; selfNotes
// are its diagnostics. Every other released event waited in the buffer.
func (d *device) apply(e *Engine, rel []released, out *reports, self *telemetry.Sequence, selfNotes []Diagnostic) {
	for _, r := range rel {
		if r.gap != nil {
			out.estimates(e, d.est.gap(*r.gap))
		}
		var notes []Diagnostic
		if self != nil && r.sample.Sequence == *self {
			notes = selfNotes
		} else {
			notes = []Diagnostic{diag(CodeReleasedFromBuffer, LevelInfo, "released in sequence order")}
		}
		if r.gap != nil {
			notes = append(notes, diag(CodeSequenceGap, LevelWarning, "preceded by missing sequence numbers").
				withSeq(r.gap.from).withValue(int64(r.gap.to-r.gap.from)+1))
		}
		out.event(e, r.sample, OutcomeAccepted, ReasonNone, notes...)
		out.estimates(e, d.est.ingest(r.sample))
	}
}

func (d *device) begin(boot telemetry.BootID) {
	d.cur = newSession(boot, d.est.cfg)
	d.cand, d.candN = 0, 0
	d.est.startSession(boot)
}

func (d *device) isClosed(b telemetry.BootID) bool {
	for _, c := range d.closed {
		if c == b {
			return true
		}
	}
	return false
}

func (d *device) pushClosed(b telemetry.BootID) {
	limit := cap(d.closed)
	if len(d.closed) < limit {
		d.closed = append(d.closed, b)
		return
	}
	d.closed[d.closedN] = b
	d.closedN = (d.closedN + 1) % limit
}
