// Package resultstore persists the chronological processor's journal and the
// fusion results derived from it, in a SQLite database separate from the
// event store.
//
// # Model
//
// The journal is the authoritative record of what the processor fed to the
// fusion engine, step by step: an event (by event-store position, with its
// fingerprint) or a processing-time tick. Replaying the journal into a fresh
// engine reproduces the outputs exactly, which is how the processor
// recovers after a restart (ADR-15 in docs/architecture.md). Each step
// stores a SHA-256 of its outputs so the replay is verified, not assumed.
//
// Outputs (fusion reports and diagnostics, plus the orderer's chronology
// diagnostics) are derived data, written in the same transaction as their
// journal step.
//
// Journal rows are immutable except for output_hash, which only an explicit
// rebuild rewrites. A single processor owns the database at a time: Claim
// takes ownership and every Commit is fenced on it, so a second processor
// started on the same file makes the first one fail instead of
// interleaving journals.
package resultstore

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/flavioanjo/fleetcore/internal/chrono"
	"github.com/flavioanjo/fleetcore/internal/fusion"
	"github.com/flavioanjo/fleetcore/internal/sqlitedb"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// Errors.
var (
	ErrNotOwner      = errors.New("resultstore: another processor has claimed this results database")
	ErrConfigChanged = errors.New("resultstore: fusion configuration differs from the one that produced the journal")
	ErrSchemaTooNew  = sqlitedb.ErrSchemaTooNew
)

var migrations = []sqlitedb.Migration{{
	Version: 1,
	Name:    "journal, reports and diagnostics",
	Stmts: []string{
		`CREATE TABLE processor_state (
			id            INTEGER PRIMARY KEY CHECK (id = 1),
			owner         TEXT    NOT NULL,
			config_hash   TEXT    NOT NULL,
			low_position  INTEGER NOT NULL CHECK (low_position >= 0),
			high_position INTEGER NOT NULL CHECK (high_position >= low_position),
			next_step     INTEGER NOT NULL CHECK (next_step >= 1),
			updated_at_ns INTEGER NOT NULL
		) STRICT`,
		// One row per processing step. kind 'event': the event at
		// event-store position `position` was given to Engine.Process.
		// kind 'tick': Engine.Tick(tick_ns) was called.
		`CREATE TABLE journal (
			step           INTEGER PRIMARY KEY CHECK (step >= 1),
			kind           TEXT    NOT NULL CHECK (kind IN ('event', 'tick')),
			position       INTEGER UNIQUE,
			fingerprint    BLOB    CHECK (fingerprint IS NULL OR length(fingerprint) = 32),
			device_id      TEXT,
			boot_id        INTEGER,
			sequence       INTEGER CHECK (sequence IS NULL OR sequence BETWEEN 0 AND 4294967295),
			ext_sequence   INTEGER,
			device_time_ms INTEGER,
			flags          INTEGER NOT NULL DEFAULT 0,
			tick_ns        INTEGER,
			output_hash    BLOB    NOT NULL CHECK (length(output_hash) = 32),
			recorded_at_ns INTEGER NOT NULL,
			CHECK ((kind = 'event') = (position IS NOT NULL AND fingerprint IS NOT NULL AND device_id IS NOT NULL
			                           AND boot_id IS NOT NULL AND sequence IS NOT NULL AND device_time_ms IS NOT NULL)),
			CHECK ((kind = 'tick') = (tick_ns IS NOT NULL))
		) STRICT`,
		`CREATE TRIGGER journal_immutable
			BEFORE UPDATE OF step, kind, position, fingerprint, device_id, boot_id, sequence,
			                 ext_sequence, device_time_ms, flags, tick_ns, recorded_at_ns ON journal
		BEGIN
			SELECT RAISE(ABORT, 'fleetcore: journal entries are immutable');
		END`,
		`CREATE TRIGGER journal_no_delete BEFORE DELETE ON journal
		BEGIN
			SELECT RAISE(ABORT, 'fleetcore: journal entries are immutable');
		END`,
		// Fusion reports, in engine output order within each step.
		`CREATE TABLE reports (
			step           INTEGER NOT NULL REFERENCES journal (step),
			idx            INTEGER NOT NULL CHECK (idx >= 0),
			type           TEXT    NOT NULL CHECK (type IN ('event', 'estimate')),
			device_id      TEXT    NOT NULL,
			boot_id        INTEGER NOT NULL,
			sequence       INTEGER,
			event_id       INTEGER,
			outcome        TEXT,
			reason         TEXT,
			first_seq      INTEGER,
			last_seq       INTEGER,
			to_ms          INTEGER,
			delta_mm       INTEGER,
			trip_mm        INTEGER,
			uncertainty_mm INTEGER,
			confidence     TEXT,
			state          TEXT,
			report_json    TEXT    NOT NULL,
			PRIMARY KEY (step, idx),
			CHECK ((type = 'event') = (outcome IS NOT NULL)),
			CHECK ((type = 'estimate') = (trip_mm IS NOT NULL))
		) STRICT`,
		`CREATE INDEX reports_by_device ON reports (device_id, type, step, idx)`,
		`CREATE INDEX reports_by_outcome ON reports (outcome, reason) WHERE type = 'event'`,
		// Diagnostics: origin 'fusion' (from a report; idx is the report)
		// or 'chronology' (from the orderer's flags; idx is -1).
		`CREATE TABLE diagnostics (
			step      INTEGER NOT NULL REFERENCES journal (step),
			idx       INTEGER NOT NULL CHECK (idx >= -1),
			n         INTEGER NOT NULL CHECK (n >= 0),
			origin    TEXT    NOT NULL CHECK (origin IN ('fusion', 'chronology')),
			device_id TEXT,
			code      TEXT    NOT NULL,
			level     TEXT    NOT NULL,
			sequence  INTEGER,
			value     INTEGER,
			message   TEXT    NOT NULL,
			PRIMARY KEY (step, idx, n),
			CHECK ((origin = 'chronology') = (idx = -1))
		) STRICT`,
		`CREATE INDEX diagnostics_by_code ON diagnostics (code, step)`,
		// The most recent estimate per device.
		`CREATE VIEW latest_estimates AS
			SELECT r.* FROM reports r
			WHERE r.type = 'estimate' AND NOT EXISTS (
				SELECT 1 FROM reports n
				WHERE n.type = 'estimate' AND n.device_id = r.device_id
				  AND (n.step > r.step OR (n.step = r.step AND n.idx > r.idx)))`,
	},
}}

// SchemaVersion is the schema version this build creates and understands.
func SchemaVersion() int { return migrations[len(migrations)-1].Version }

// Options configures Open.
type Options struct {
	BusyTimeout time.Duration
	Now         func() time.Time
}

// Store is the results database.
type Store struct {
	db  *sqlitedb.DB
	now func() time.Time
}

// Open opens (creating if needed) the results database at path.
func Open(ctx context.Context, path string, opts Options) (*Store, error) {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	db, err := sqlitedb.Open(ctx, "resultstore", path, sqlitedb.Options{BusyTimeout: opts.BusyTimeout, Now: opts.Now}, migrations)
	if err != nil {
		return nil, err
	}
	return &Store{db: db, now: opts.Now}, nil
}

// Close checkpoints and closes the database.
func (s *Store) Close() error { return s.db.Close() }

// NewOwner returns a random owner token.
func NewOwner() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // crypto/rand does not fail on supported platforms
	}
	return hex.EncodeToString(b[:])
}

// State is the processor's durable cursor.
type State struct {
	// LowPosition: every event-store position at or below it has been
	// journaled. HighPosition: the highest position read.
	LowPosition, HighPosition int64
	// NextStep is the step number of the next journal entry.
	NextStep int64
}

// Claim takes ownership of the database for owner and returns the durable
// state. configHash identifies the fusion configuration: if the journal is
// not empty and was produced with another configuration, ErrConfigChanged
// is returned unless rebuild is set (the caller will then rewrite outputs).
func (s *Store) Claim(ctx context.Context, owner, configHash string, rebuild bool) (State, error) {
	tx, err := s.db.W.BeginTx(ctx, nil)
	if err != nil {
		return State{}, fmt.Errorf("resultstore: claim: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck
	var st State
	var oldHash string
	err = tx.QueryRowContext(ctx, `SELECT low_position, high_position, next_step, config_hash FROM processor_state WHERE id = 1`).
		Scan(&st.LowPosition, &st.HighPosition, &st.NextStep, &oldHash)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		st = State{NextStep: 1}
		if _, err := tx.ExecContext(ctx, `INSERT INTO processor_state (id, owner, config_hash, low_position, high_position, next_step, updated_at_ns)
			VALUES (1, ?, ?, 0, 0, 1, ?)`, owner, configHash, s.now().UnixNano()); err != nil {
			return State{}, fmt.Errorf("resultstore: claim: %w", err)
		}
	case err != nil:
		return State{}, fmt.Errorf("resultstore: claim: %w", err)
	default:
		if oldHash != configHash && st.NextStep > 1 && !rebuild {
			return State{}, fmt.Errorf("%w (journal %s, now %s)", ErrConfigChanged, oldHash, configHash)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE processor_state SET owner = ?, config_hash = ?, updated_at_ns = ? WHERE id = 1`,
			owner, configHash, s.now().UnixNano()); err != nil {
			return State{}, fmt.Errorf("resultstore: claim: %w", err)
		}
	}
	return st, tx.Commit()
}

// Kind is a journal entry kind.
type Kind string

// Journal entry kinds.
const (
	KindEvent Kind = "event"
	KindTick  Kind = "tick"
)

// Entry is one journal step.
type Entry struct {
	Step int64
	Kind Kind

	// Event steps.
	Position    int64
	Fingerprint [32]byte
	DeviceID    telemetry.DeviceID
	BootID      telemetry.BootID
	Sequence    telemetry.Sequence
	ExtSequence uint64
	DeviceTime  time.Time
	Flags       chrono.Flag

	// Tick steps.
	TickAt time.Time

	OutputHash [32]byte
}

// Step is a journal entry with the outputs it produced.
type Step struct {
	Entry   Entry
	Reports []fusion.Report
}

// HashReports returns the canonical hash of a step's reports and their
// JSON encodings. The hash is what replays are verified against.
func HashReports(rs []fusion.Report) ([32]byte, [][]byte, error) {
	h := sha256.New()
	enc := make([][]byte, len(rs))
	for i, r := range rs {
		b, err := json.Marshal(r)
		if err != nil {
			return [32]byte{}, nil, fmt.Errorf("resultstore: encode report: %w", err)
		}
		enc[i] = b
		h.Write(b)
		h.Write([]byte{'\n'})
	}
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum, enc, nil
}

// Commit writes steps (whose Entry.OutputHash must already be set) and the
// new cursor in one transaction, fenced on owner. Steps must be numbered
// consecutively from the state's NextStep.
func (s *Store) Commit(ctx context.Context, owner string, steps []Step, low, high int64) error {
	tx, err := s.db.W.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("resultstore: commit: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck
	var cur string
	var next int64
	if err := tx.QueryRowContext(ctx, `SELECT owner, next_step FROM processor_state WHERE id = 1`).Scan(&cur, &next); err != nil {
		return fmt.Errorf("resultstore: commit: %w", err)
	}
	if cur != owner {
		return ErrNotOwner
	}
	now := s.now().UnixNano()
	for _, st := range steps {
		e := st.Entry
		if e.Step != next {
			return fmt.Errorf("resultstore: commit: step %d, expected %d", e.Step, next)
		}
		next++
		if err := insertEntry(ctx, tx, e, now); err != nil {
			return err
		}
		if err := insertOutputs(ctx, tx, st); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE processor_state SET low_position = ?, high_position = ?, next_step = ?, updated_at_ns = ? WHERE id = 1`,
		low, high, next, now); err != nil {
		return fmt.Errorf("resultstore: commit: state: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("resultstore: commit: %w", err)
	}
	return nil
}

func insertEntry(ctx context.Context, tx *sql.Tx, e Entry, now int64) error {
	var err error
	switch e.Kind {
	case KindEvent:
		_, err = tx.ExecContext(ctx, `INSERT INTO journal
			(step, kind, position, fingerprint, device_id, boot_id, sequence, ext_sequence, device_time_ms, flags, output_hash, recorded_at_ns)
			VALUES (?, 'event', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			e.Step, e.Position, e.Fingerprint[:], string(e.DeviceID), int64(e.BootID), int64(e.Sequence),
			int64(e.ExtSequence), e.DeviceTime.UnixMilli(), int64(e.Flags), e.OutputHash[:], now)
	case KindTick:
		_, err = tx.ExecContext(ctx, `INSERT INTO journal (step, kind, tick_ns, output_hash, recorded_at_ns) VALUES (?, 'tick', ?, ?, ?)`,
			e.Step, e.TickAt.UnixNano(), e.OutputHash[:], now)
	default:
		err = fmt.Errorf("unknown kind %q", e.Kind)
	}
	if err != nil {
		return fmt.Errorf("resultstore: journal step %d: %w", e.Step, err)
	}
	return nil
}

func insertOutputs(ctx context.Context, tx *sql.Tx, st Step) error {
	_, enc, err := HashReports(st.Reports)
	if err != nil {
		return err
	}
	for i, r := range st.Reports {
		if err := insertReport(ctx, tx, st.Entry.Step, i, r, enc[i]); err != nil {
			return err
		}
	}
	if st.Entry.Kind == KindEvent {
		for n, name := range st.Entry.Flags.Names() {
			if _, err := tx.ExecContext(ctx, `INSERT INTO diagnostics (step, idx, n, origin, device_id, code, level, sequence, message)
				VALUES (?, -1, ?, 'chronology', ?, ?, ?, ?, ?)`,
				st.Entry.Step, n, string(st.Entry.DeviceID), name, chronoLevel(name), int64(st.Entry.Sequence), chronoMessage(name)); err != nil {
				return fmt.Errorf("resultstore: chronology diagnostic: %w", err)
			}
		}
	}
	return nil
}

func insertReport(ctx context.Context, tx *sql.Tx, step int64, idx int, r fusion.Report, js []byte) error {
	var (
		dev                                 string
		boot                                int64
		seq, eid, firstSeq, lastSeq, toMS   sql.NullInt64
		deltaMM, tripMM, uncMM              sql.NullInt64
		outcome, reason, confidence, stateV sql.NullString
		diags                               []fusion.Diagnostic
	)
	switch r.Type {
	case fusion.ReportEvent:
		ev := r.Event
		dev, boot = string(ev.DeviceID), int64(ev.BootID)
		seq = sql.NullInt64{Int64: int64(ev.Sequence), Valid: true}
		eid = sql.NullInt64{Int64: int64(ev.EventID), Valid: true}
		outcome = sql.NullString{String: string(ev.Outcome), Valid: true}
		reason = sql.NullString{String: string(ev.Reason), Valid: ev.Reason != ""}
		diags = ev.Diagnostics
	case fusion.ReportEstimate:
		es := r.Estimate
		dev, boot = string(es.DeviceID), int64(es.BootID)
		firstSeq = sql.NullInt64{Int64: int64(es.FirstSeq), Valid: true}
		lastSeq = sql.NullInt64{Int64: int64(es.LastSeq), Valid: true}
		toMS = sql.NullInt64{Int64: es.To.UnixMilli(), Valid: true}
		deltaMM = sql.NullInt64{Int64: int64(es.DeltaMM), Valid: true}
		tripMM = sql.NullInt64{Int64: int64(es.TripMM), Valid: true}
		uncMM = sql.NullInt64{Int64: int64(es.UncertaintyMM), Valid: true}
		confidence = sql.NullString{String: string(es.Confidence), Valid: true}
		stateV = sql.NullString{String: string(es.State), Valid: true}
		diags = es.Diagnostics
	default:
		return fmt.Errorf("resultstore: unknown report type %q", r.Type)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO reports
		(step, idx, type, device_id, boot_id, sequence, event_id, outcome, reason, first_seq, last_seq, to_ms,
		 delta_mm, trip_mm, uncertainty_mm, confidence, state, report_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		step, idx, string(r.Type), dev, boot, seq, eid, outcome, reason, firstSeq, lastSeq, toMS,
		deltaMM, tripMM, uncMM, confidence, stateV, string(js)); err != nil {
		return fmt.Errorf("resultstore: report %d/%d: %w", step, idx, err)
	}
	for n, d := range diags {
		var dseq, val sql.NullInt64
		if d.Seq != nil {
			dseq = sql.NullInt64{Int64: int64(*d.Seq), Valid: true}
		}
		if d.Value != nil {
			val = sql.NullInt64{Int64: *d.Value, Valid: true}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO diagnostics (step, idx, n, origin, device_id, code, level, sequence, value, message)
			VALUES (?, ?, ?, 'fusion', ?, ?, ?, ?, ?, ?)`,
			step, idx, n, dev, string(d.Code), string(d.Level), dseq, val, d.Message); err != nil {
			return fmt.Errorf("resultstore: diagnostic %d/%d/%d: %w", step, idx, n, err)
		}
	}
	return nil
}

func chronoLevel(name string) string {
	switch name {
	case "late", "clock_regression", "sequence_ambiguous", "sequence_reused", "forced_release":
		return "warning"
	default:
		return "info"
	}
}

func chronoMessage(name string) string {
	switch name {
	case "late":
		return "a later event of the same session was already processed; delivered anyway"
	case "clock_regression":
		return "device time earlier than an earlier-sequence event of the same session; sequence order kept"
	case "sequence_ambiguous":
		return "sequence number too far from the session's position to be placed; delivered in ingestion order"
	case "sequence_reused":
		return "another stored event with the same session and sequence number was processed before"
	case "chronology_ambiguous":
		return "device times of this device's sessions were too close to order them; ingestion order decided"
	case "forced_release":
		return "released before its hold expired because the orderer's memory bound was reached"
	default:
		return name
	}
}

// Rewrite replaces the outputs of already journaled steps (an explicit
// rebuild after a fusion change) and their output hashes, fenced on owner.
func (s *Store) Rewrite(ctx context.Context, owner string, steps []Step) error {
	tx, err := s.db.W.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("resultstore: rewrite: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck
	var cur string
	if err := tx.QueryRowContext(ctx, `SELECT owner FROM processor_state WHERE id = 1`).Scan(&cur); err != nil {
		return fmt.Errorf("resultstore: rewrite: %w", err)
	}
	if cur != owner {
		return ErrNotOwner
	}
	for _, st := range steps {
		step := st.Entry.Step
		for _, q := range []string{`DELETE FROM diagnostics WHERE step = ?`, `DELETE FROM reports WHERE step = ?`} {
			if _, err := tx.ExecContext(ctx, q, step); err != nil {
				return fmt.Errorf("resultstore: rewrite step %d: %w", step, err)
			}
		}
		if err := insertOutputs(ctx, tx, st); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE journal SET output_hash = ? WHERE step = ?`, st.Entry.OutputHash[:], step); err != nil {
			return fmt.Errorf("resultstore: rewrite step %d: %w", step, err)
		}
	}
	return tx.Commit()
}

// Journal returns up to limit journal entries with Step > after, in order.
func (s *Store) Journal(ctx context.Context, after int64, limit int) ([]Entry, error) {
	rows, err := s.db.R.QueryContext(ctx, `SELECT step, kind, position, fingerprint, device_id, boot_id, sequence, ext_sequence,
		device_time_ms, flags, tick_ns, output_hash FROM journal WHERE step > ? ORDER BY step LIMIT ?`, after, limit)
	if err != nil {
		return nil, fmt.Errorf("resultstore: journal: %w", err)
	}
	defer rows.Close()
	var out []Entry
	for rows.Next() {
		var (
			e                                Entry
			kind                             string
			pos, boot, seq, ext, devMS, tick sql.NullInt64
			fp                               []byte
			dev                              sql.NullString
			flags                            int64
			hash                             []byte
		)
		if err := rows.Scan(&e.Step, &kind, &pos, &fp, &dev, &boot, &seq, &ext, &devMS, &flags, &tick, &hash); err != nil {
			return nil, fmt.Errorf("resultstore: journal: %w", err)
		}
		e.Kind = Kind(kind)
		e.Position = pos.Int64
		copy(e.Fingerprint[:], fp)
		e.DeviceID = telemetry.DeviceID(dev.String)
		e.BootID = telemetry.BootID(boot.Int64)
		e.Sequence = telemetry.Sequence(seq.Int64)
		e.ExtSequence = uint64(ext.Int64)
		if devMS.Valid {
			e.DeviceTime = time.UnixMilli(devMS.Int64).UTC()
		}
		e.Flags = chrono.Flag(flags)
		if tick.Valid {
			e.TickAt = time.Unix(0, tick.Int64).UTC()
		}
		copy(e.OutputHash[:], hash)
		out = append(out, e)
	}
	return out, rows.Err()
}

// JournaledPositionsAbove returns the event positions above low that are
// already journaled (events released after the low watermark).
func (s *Store) JournaledPositionsAbove(ctx context.Context, low int64) (map[int64]bool, error) {
	rows, err := s.db.R.QueryContext(ctx, `SELECT position FROM journal WHERE kind = 'event' AND position > ?`, low)
	if err != nil {
		return nil, fmt.Errorf("resultstore: positions: %w", err)
	}
	defer rows.Close()
	out := map[int64]bool{}
	for rows.Next() {
		var p int64
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		out[p] = true
	}
	return out, rows.Err()
}

// EstimateRow is a persisted estimate.
type EstimateRow struct {
	Step          int64
	DeviceID      telemetry.DeviceID
	BootID        telemetry.BootID
	TripMM        telemetry.Millimetres
	UncertaintyMM telemetry.Millimetres
	Confidence    string
	State         string
	To            time.Time
}

// LatestEstimates returns the most recent estimate of every device, by
// DeviceID.
func (s *Store) LatestEstimates(ctx context.Context) ([]EstimateRow, error) {
	rows, err := s.db.R.QueryContext(ctx, `SELECT step, device_id, boot_id, trip_mm, uncertainty_mm, confidence, state, to_ms
		FROM latest_estimates ORDER BY device_id`)
	if err != nil {
		return nil, fmt.Errorf("resultstore: latest estimates: %w", err)
	}
	defer rows.Close()
	var out []EstimateRow
	for rows.Next() {
		var r EstimateRow
		var boot, trip, unc, toMS int64
		if err := rows.Scan(&r.Step, &r.DeviceID, &boot, &trip, &unc, &r.Confidence, &r.State, &toMS); err != nil {
			return nil, err
		}
		r.BootID, r.TripMM, r.UncertaintyMM = telemetry.BootID(boot), telemetry.Millimetres(trip), telemetry.Millimetres(unc)
		r.To = time.UnixMilli(toMS).UTC()
		out = append(out, r)
	}
	return out, rows.Err()
}

// Counts summarizes persisted outputs.
type Counts struct {
	Steps, EventSteps, TickSteps int64
	Reports, Estimates           int64
	Outcomes                     map[string]int64 // "outcome" or "outcome/reason"
	Diagnostics                  map[string]int64 // "origin:code"
}

// Count returns persisted totals.
func (s *Store) Count(ctx context.Context) (Counts, error) {
	c := Counts{Outcomes: map[string]int64{}, Diagnostics: map[string]int64{}}
	if err := s.db.R.QueryRowContext(ctx, `SELECT count(*), coalesce(sum(kind = 'event'), 0), coalesce(sum(kind = 'tick'), 0) FROM journal`).
		Scan(&c.Steps, &c.EventSteps, &c.TickSteps); err != nil {
		return c, err
	}
	if err := s.db.R.QueryRowContext(ctx, `SELECT count(*), coalesce(sum(type = 'estimate'), 0) FROM reports`).Scan(&c.Reports, &c.Estimates); err != nil {
		return c, err
	}
	// Final outcome per journaled event: the last event report for its
	// (device, boot, sequence, event id) across all steps.
	rows, err := s.db.R.QueryContext(ctx, `SELECT outcome, coalesce(reason, ''), count(*) FROM (
			SELECT r.outcome, r.reason FROM reports r
			WHERE r.type = 'event' AND NOT EXISTS (
				SELECT 1 FROM reports n WHERE n.type = 'event' AND n.device_id = r.device_id AND n.boot_id = r.boot_id
				  AND n.sequence = r.sequence AND n.event_id = r.event_id
				  AND (n.step > r.step OR (n.step = r.step AND n.idx > r.idx))))
		GROUP BY 1, 2`)
	if err != nil {
		return c, err
	}
	for rows.Next() {
		var o, r string
		var n int64
		if err := rows.Scan(&o, &r, &n); err != nil {
			rows.Close()
			return c, err
		}
		if r != "" {
			o += "/" + r
		}
		c.Outcomes[o] = n
	}
	rows.Close()
	rows, err = s.db.R.QueryContext(ctx, `SELECT origin || ':' || code, count(*) FROM diagnostics GROUP BY 1`)
	if err != nil {
		return c, err
	}
	defer rows.Close()
	for rows.Next() {
		var k string
		var n int64
		if err := rows.Scan(&k, &n); err != nil {
			return c, err
		}
		c.Diagnostics[k] = n
	}
	return c, rows.Err()
}

// DB exposes the underlying database for read-only inspection in tests and
// tools.
func (s *Store) DB() *sql.DB { return s.db.R }
