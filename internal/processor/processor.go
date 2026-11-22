// Package processor connects the event store to the fusion engine and
// persists the results:
//
//	event store ──ReadAfter──► chrono.Orderer ──► fusion.Engine ──► resultstore
//	(all gateways)            (hybrid order)     Process / Tick     (journal + outputs)
//
// # Determinism and recovery (ADR-15)
//
// Live ordering depends on when events arrive, so the processor records its
// decisions instead of re-deriving them: every event given to
// Engine.Process and every Engine.Tick is a journal step, committed in the
// same transaction as the outputs it produced. After a restart the journal
// is replayed into a fresh engine. The replay is verified step by step
// against the stored output hashes, so the recovered engine provably has
// the state the live engine had. Then processing continues.
//
// Events that were read but not yet released when the process stopped are
// not in the journal. They are read again from the event store (the cursor
// restarts at the low watermark, skipping journaled positions), so no stored
// event is lost or processed twice.
//
// The processor never calls Engine.Flush: an open epoch stays open across a
// restart exactly as it would have without one.
package processor

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/flavioanjo/fleetcore/internal/chrono"
	"github.com/flavioanjo/fleetcore/internal/eventstore"
	"github.com/flavioanjo/fleetcore/internal/fusion"
	"github.com/flavioanjo/fleetcore/internal/resultstore"
)

// Config configures a Processor. Zero fields select the defaults.
type Config struct {
	Fusion fusion.Config // default fusion.DefaultConfig()
	Chrono chrono.Config
	// PollInterval is the delay between processing cycles in Run. Default 50ms.
	PollInterval time.Duration
	// TickInterval is how often Engine.Tick is called with the processing
	// clock. Default 1s.
	TickInterval time.Duration
	// ReadBatch bounds the events read from the store per cycle. Default 4096.
	ReadBatch int
	// CommitRetries bounds retries of a failed commit before Run gives up.
	// Default 5.
	CommitRetries int
	// Rebuild rewrites stored outputs that do not match the replay (after an
	// intentional fusion change) instead of failing.
	Rebuild bool
	// Owner identifies this processor instance. Default random.
	Owner string
	// Now is the processing clock. Default time.Now.
	Now    func() time.Time
	Logger *slog.Logger
}

func (c Config) withDefaults() Config {
	if c.Fusion == (fusion.Config{}) {
		c.Fusion = fusion.DefaultConfig()
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 50 * time.Millisecond
	}
	if c.TickInterval <= 0 {
		c.TickInterval = time.Second
	}
	if c.ReadBatch <= 0 {
		c.ReadBatch = 4096
	}
	if c.CommitRetries <= 0 {
		c.CommitRetries = 5
	}
	if c.Owner == "" {
		c.Owner = resultstore.NewOwner()
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return c
}

// Errors.
var (
	// ErrReplayMismatch: replaying the journal produced different outputs
	// than were stored (a fusion change, or a different event store).
	ErrReplayMismatch = errors.New("processor: journal replay does not reproduce the stored outputs")
	// ErrWrongEventStore: a journaled event is missing from, or differs in,
	// the event store.
	ErrWrongEventStore = errors.New("processor: journaled event not found unchanged in the event store")
	// ErrFatal wraps errors after which the engine state is ahead of the
	// journal; the processor must be restarted (it recovers by replay).
	ErrFatal = errors.New("processor: unrecoverable error; restart to recover from the journal")
)

// Stats counts processor activity since it was created.
type Stats struct {
	Read          uint64 // events read from the store (excluding already journaled)
	Processed     uint64 // event steps journaled
	Ticks         uint64 // tick steps journaled
	Held          int    // events held in the orderer
	ReplaySteps   uint64 // journal steps replayed at recovery
	Rewritten     uint64 // steps whose outputs were rebuilt
	Flags         map[string]uint64
	LowPosition   int64
	HighPosition  int64
	Commits       uint64
	CommitRetries uint64
}

// Processor runs the chronological processing loop. It is not safe for
// concurrent use; Run drives it from one goroutine.
type Processor struct {
	cfg     Config
	events  *eventstore.Store
	results *resultstore.Store
	engine  *fusion.Engine
	order   *chrono.Orderer
	log     *slog.Logger

	recovered bool
	nextStep  int64
	low, high int64
	skip      map[int64]bool // journaled positions above low, not to be re-added
	lastTick  time.Time
	pending   []resultstore.Step
	stats     Stats
	snapshot  atomic.Pointer[Stats] // published copy of stats, safe to read concurrently
}

// New returns a processor reading from events and writing to results.
func New(cfg Config, events *eventstore.Store, results *resultstore.Store) (*Processor, error) {
	cfg = cfg.withDefaults()
	eng, err := fusion.New(cfg.Fusion)
	if err != nil {
		return nil, err
	}
	return &Processor{
		cfg: cfg, events: events, results: results, engine: eng,
		order: chrono.New(cfg.Chrono), log: cfg.Logger,
		stats: Stats{Flags: map[string]uint64{}},
	}, nil
}

// ConfigHash identifies a fusion configuration; it is stored with the
// journal so a changed configuration is detected before replay.
func ConfigHash(c fusion.Config) string {
	b, _ := json.Marshal(c)
	sum := sha256.Sum256(b)
	return fmt.Sprintf("%x", sum[:8])
}

// RecoveryStats describes what Recover did.
type RecoveryStats struct {
	Steps, Events, Ticks, Rewritten int64
	Duration                        time.Duration
}

// Recover claims the results database and replays the journal. It must be
// called once before Step; Run calls it.
func (p *Processor) Recover(ctx context.Context) (RecoveryStats, error) {
	begin := time.Now()
	var rs RecoveryStats
	st, err := p.results.Claim(ctx, p.cfg.Owner, ConfigHash(p.cfg.Fusion), p.cfg.Rebuild)
	if err != nil {
		return rs, err
	}
	var after int64
	for {
		entries, err := p.results.Journal(ctx, after, 1024)
		if err != nil {
			return rs, err
		}
		if len(entries) == 0 {
			break
		}
		if err := p.replay(ctx, entries, &rs); err != nil {
			return rs, err
		}
		after = entries[len(entries)-1].Step
	}
	if after+1 != st.NextStep {
		return rs, fmt.Errorf("processor: journal ends at step %d but state expects next step %d", after, st.NextStep)
	}
	p.nextStep, p.low, p.high = st.NextStep, st.LowPosition, st.HighPosition
	if p.skip, err = p.results.JournaledPositionsAbove(ctx, p.low); err != nil {
		return rs, err
	}
	p.recovered = true
	rs.Duration = time.Since(begin)
	p.stats.ReplaySteps = uint64(rs.Steps)
	p.stats.Rewritten = uint64(rs.Rewritten)
	p.stats.LowPosition, p.stats.HighPosition = p.low, p.high
	p.publish()
	p.log.Info("processor recovered", "steps", rs.Steps, "events", rs.Events, "ticks", rs.Ticks,
		"rewritten", rs.Rewritten, "low_position", p.low, "high_position", p.high, "duration", rs.Duration)
	return rs, nil
}

func (p *Processor) replay(ctx context.Context, entries []resultstore.Entry, rs *RecoveryStats) error {
	var positions []int64
	for _, e := range entries {
		if e.Kind == resultstore.KindEvent {
			positions = append(positions, e.Position)
		}
	}
	recs, err := p.events.ByPositions(ctx, positions)
	if err != nil {
		if errors.Is(err, eventstore.ErrNotFound) {
			return fmt.Errorf("%w: %v", ErrWrongEventStore, err)
		}
		return err
	}
	var rewrite []resultstore.Step
	next := 0
	for _, e := range entries {
		var reports []fusion.Report
		switch e.Kind {
		case resultstore.KindEvent:
			rec := recs[next]
			next++
			if rec.Fingerprint != e.Fingerprint || rec.Sample.DeviceID != e.DeviceID || rec.Sample.Sequence != e.Sequence {
				return fmt.Errorf("%w: step %d, position %d", ErrWrongEventStore, e.Step, e.Position)
			}
			reports = p.engine.Process(rec.Sample)
			p.order.Restore(chrono.Released{Item: chrono.Item{Position: rec.Position, Sample: rec.Sample},
				ExtSequence: e.ExtSequence, Flags: e.Flags})
			rs.Events++
		case resultstore.KindTick:
			reports = p.engine.Tick(e.TickAt)
			p.lastTick = e.TickAt
			rs.Ticks++
		default:
			return fmt.Errorf("processor: unknown journal kind %q at step %d", e.Kind, e.Step)
		}
		sum, _, err := resultstore.HashReports(reports)
		if err != nil {
			return err
		}
		if sum != e.OutputHash {
			if !p.cfg.Rebuild {
				return fmt.Errorf("%w at step %d (use rebuild after an intentional fusion change)", ErrReplayMismatch, e.Step)
			}
			e.OutputHash = sum
			rewrite = append(rewrite, resultstore.Step{Entry: e, Reports: reports})
		}
		rs.Steps++
	}
	if len(rewrite) > 0 {
		if err := p.results.Rewrite(ctx, p.cfg.Owner, rewrite); err != nil {
			return err
		}
		rs.Rewritten += int64(len(rewrite))
	}
	return nil
}

// Step runs one processing cycle: read new events, release the ones whose
// hold expired, feed them (and a tick, when due) to the engine, and commit.
// It returns the number of journal steps committed.
func (p *Processor) Step(ctx context.Context) (int, error) {
	if !p.recovered {
		return 0, errors.New("processor: Step before Recover")
	}
	now := p.cfg.Now().UTC()
	recs, err := p.events.ReadAfter(ctx, p.high, p.cfg.ReadBatch)
	if err != nil {
		return 0, err // nothing changed yet; the caller may retry
	}
	for _, r := range recs {
		p.high = r.Position
		if p.skip[r.Position] {
			delete(p.skip, r.Position)
			continue
		}
		p.order.Add(chrono.Item{Position: r.Position, Sample: r.Sample, SeenAt: now})
		p.stats.Read++
	}
	return p.process(ctx, now, false)
}

// Drain releases everything held regardless of hold time and commits. It is
// meant for tests and for an operator-requested catch-up; normal shutdown
// does not need it (held events are re-read after a restart).
func (p *Processor) Drain(ctx context.Context) (int, error) {
	return p.process(ctx, p.cfg.Now().UTC(), true)
}

func (p *Processor) process(ctx context.Context, now time.Time, force bool) (int, error) {
	defer p.publish()
	for _, r := range p.order.Release(now, force) {
		reports := p.engine.Process(r.Sample)
		e := resultstore.Entry{
			Step: p.nextStep, Kind: resultstore.KindEvent, Position: r.Position,
			DeviceID: r.Sample.DeviceID, BootID: r.Sample.BootID, Sequence: r.Sample.Sequence,
			ExtSequence: r.ExtSequence, DeviceTime: r.Sample.DeviceTime, Flags: r.Flags,
		}
		ev, err := eventstore.NewEvent(r.Sample)
		if err != nil {
			return 0, fmt.Errorf("%w: re-encode position %d: %v", ErrFatal, r.Position, err)
		}
		e.Fingerprint = ev.Fingerprint
		if err := p.add(e, reports); err != nil {
			return 0, err
		}
		for _, n := range r.Flags.Names() {
			p.stats.Flags[n]++
		}
		p.stats.Processed++
	}
	if p.lastTick.IsZero() || now.Sub(p.lastTick) >= p.cfg.TickInterval {
		reports := p.engine.Tick(now)
		if err := p.add(resultstore.Entry{Step: p.nextStep, Kind: resultstore.KindTick, TickAt: now}, reports); err != nil {
			return 0, err
		}
		p.lastTick = now
		p.stats.Ticks++
	}
	return p.commit(ctx)
}

func (p *Processor) add(e resultstore.Entry, reports []fusion.Report) error {
	sum, _, err := resultstore.HashReports(reports)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrFatal, err)
	}
	e.OutputHash = sum
	p.pending = append(p.pending, resultstore.Step{Entry: e, Reports: reports})
	p.nextStep++
	return nil
}

// commit persists pending steps with the new cursor. The engine has already
// processed them, so a commit that keeps failing is fatal: the in-memory
// state is then ahead of the journal and only a restart (replay) recovers.
func (p *Processor) commit(ctx context.Context) (int, error) {
	low := p.high
	if m, ok := p.order.MinHeldPosition(); ok {
		low = m - 1
	}
	if len(p.pending) == 0 && low == p.low {
		return 0, nil
	}
	var err error
	for attempt := 0; attempt <= p.cfg.CommitRetries; attempt++ {
		if attempt > 0 {
			p.stats.CommitRetries++
			select {
			case <-time.After(time.Duration(attempt) * 50 * time.Millisecond):
			case <-ctx.Done():
				return 0, fmt.Errorf("%w: commit interrupted: %v", ErrFatal, ctx.Err())
			}
		}
		// The commit must not be cancelled half-way by shutdown: it is
		// bounded by its own timeout instead.
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		err = p.results.Commit(cctx, p.cfg.Owner, p.pending, low, p.high)
		cancel()
		if err == nil || errors.Is(err, resultstore.ErrNotOwner) {
			break
		}
		p.log.Warn("results commit failed; retrying", "attempt", attempt+1, "steps", len(p.pending), "err", err)
	}
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrFatal, err)
	}
	n := len(p.pending)
	p.pending = p.pending[:0]
	p.low = low
	p.stats.Commits++
	p.stats.LowPosition, p.stats.HighPosition = p.low, p.high
	return n, nil
}

// Run recovers and then processes until ctx is cancelled. On cancellation
// it commits what is pending and returns nil; held events are left for the
// next run. It returns an error wrapping ErrFatal if the engine state could
// not be persisted.
func (p *Processor) Run(ctx context.Context) error {
	if !p.recovered {
		if _, err := p.Recover(ctx); err != nil {
			return err
		}
	}
	t := time.NewTicker(p.cfg.PollInterval)
	defer t.Stop()
	for {
		if _, err := p.Step(ctx); err != nil {
			if errors.Is(err, ErrFatal) || errors.Is(err, resultstore.ErrNotOwner) {
				return err
			}
			if ctx.Err() == nil {
				p.log.Warn("processing cycle failed; retrying", "err", err)
			}
		}
		select {
		case <-ctx.Done():
			p.log.Info("processor stopped", "processed", p.stats.Processed, "ticks", p.stats.Ticks,
				"held_for_next_run", p.order.Held(), "low_position", p.low, "high_position", p.high)
			return nil
		case <-t.C:
		}
	}
}

// publish makes the current counters visible to Stats. It runs on the
// processing goroutine at the end of every cycle.
func (p *Processor) publish() {
	s := p.stats
	s.Flags = make(map[string]uint64, len(p.stats.Flags))
	for k, v := range p.stats.Flags {
		s.Flags[k] = v
	}
	s.Held = p.order.Held()
	p.snapshot.Store(&s)
}

// Stats returns the counters as of the end of the last processing cycle.
// It is safe to call from any goroutine.
func (p *Processor) Stats() Stats {
	if s := p.snapshot.Load(); s != nil {
		return *s
	}
	return Stats{Flags: map[string]uint64{}}
}

// EngineStats exposes the fusion engine's counters. Like Step, it must be
// called from the processing goroutine.
func (p *Processor) EngineStats() fusion.Stats { return p.engine.Stats() }
