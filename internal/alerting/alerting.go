// Package alerting is the integration boundary between stored critical
// events (PANIC, SECURITY_VIOLATION) and an incident-response consumer.
// It does not dispatch emergency services; it tracks, durably, that each
// critical event was handed to a Sink and whether a human acknowledged it.
//
// An alert moves open → notified → acknowledged:
//
//   - open: the critical event is durably stored (the device has, or will
//     get, its ACK). Nobody has been told yet.
//   - notified: Sink.Notify returned nil (at-least-once; the sink must
//     deduplicate by Alert.ID).
//   - acknowledged: an operator explicitly took ownership. Only this state
//     means "handled"; neither the device ACK nor the notification does.
package alerting

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/flavioanjo/fleetcore/internal/eventstore"
	"github.com/flavioanjo/fleetcore/internal/reliable"
	"github.com/flavioanjo/fleetcore/internal/sqlitedb"
)

// Alert is a critical event handed to the incident-response consumer.
type Alert struct {
	ID         string // "<device>/<uid>": stable, for deduplication
	DeviceID   string
	UID        uint64
	Type       reliable.EventType
	DeviceTime time.Time
	StoredAt   time.Time
	Panic      *reliable.Panic
	Violation  *reliable.SecurityViolation
}

// Sink receives alerts (an incident-management system, a pager bridge).
// Notify must be idempotent on Alert.ID.
type Sink interface {
	Notify(ctx context.Context, a Alert) error
}

// State is an alert's handling state.
type State string

// States.
const (
	StateOpen         State = "open"
	StateNotified     State = "notified"
	StateAcknowledged State = "acknowledged"
)

var migrations = []sqlitedb.Migration{{
	Version: 1,
	Name:    "alerts",
	Stmts: []string{
		`CREATE TABLE alerts (
			device_id       TEXT    NOT NULL,
			event_uid       INTEGER NOT NULL,
			rpos            INTEGER NOT NULL,
			event_type      INTEGER NOT NULL,
			payload         BLOB    NOT NULL,
			device_time_ms  INTEGER NOT NULL,
			stored_at_ns    INTEGER NOT NULL,
			opened_at_ns    INTEGER NOT NULL,
			state           TEXT    NOT NULL CHECK (state IN ('open', 'notified', 'acknowledged')),
			notify_attempts INTEGER NOT NULL DEFAULT 0,
			next_notify_ns  INTEGER NOT NULL,
			notified_at_ns  INTEGER,
			acked_by        TEXT,
			acked_at_ns     INTEGER,
			ack_note        TEXT,
			PRIMARY KEY (device_id, event_uid)
		) STRICT`,
		`CREATE INDEX alerts_by_state ON alerts (state, next_notify_ns)`,
		`CREATE TABLE alert_cursor (id INTEGER PRIMARY KEY CHECK (id = 1), rpos INTEGER NOT NULL)`,
		`INSERT INTO alert_cursor VALUES (1, 0)`,
	},
}}

// Config configures a Consumer.
type Config struct {
	Path   string
	Events *eventstore.Store
	Sink   Sink
	// Retry backoff of failed notifications. Defaults 1s doubling to 1m.
	InitialBackoff, MaxBackoff time.Duration
	Batch                      int
	// Interval is the Run loop period. Default 200ms.
	Interval time.Duration
	// StepTimeout bounds one Run step (including Sink.Notify). Steps are not
	// interrupted by a stop request. Default 30s.
	StepTimeout time.Duration
	// MaxConsecutiveFailures is how many Run steps in a row may fail before
	// Run returns ErrFailed. Default 25.
	MaxConsecutiveFailures int
	Now                    func() time.Time
	Logger                 *slog.Logger
}

// ErrFailed is returned by Run when alert processing can no longer make
// progress: critical events are no longer being handed to the sink, and
// the supervising process must know.
var ErrFailed = errors.New("alerting: persistent failure; critical events are not being processed")

// Consumer moves critical events from the event store to the sink.
type Consumer struct {
	cfg Config
	db  *sqlitedb.DB
}

// Open opens the alert database.
func Open(ctx context.Context, cfg Config) (*Consumer, error) {
	if cfg.Events == nil || cfg.Sink == nil || cfg.Path == "" {
		return nil, errors.New("alerting: needs Path, Events and Sink")
	}
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = time.Second
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = time.Minute
	}
	if cfg.Batch <= 0 {
		cfg.Batch = 256
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 200 * time.Millisecond
	}
	if cfg.StepTimeout <= 0 {
		cfg.StepTimeout = 30 * time.Second
	}
	if cfg.MaxConsecutiveFailures <= 0 {
		cfg.MaxConsecutiveFailures = 25
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	db, err := sqlitedb.Open(ctx, "alerting", cfg.Path, sqlitedb.Options{Now: cfg.Now}, migrations)
	if err != nil {
		return nil, err
	}
	return &Consumer{cfg: cfg, db: db}, nil
}

// Close closes the database. Call it only after Run has returned.
func (c *Consumer) Close() error { return c.db.Close() }

// Run calls Step every Interval until ctx is cancelled; it then returns
// nil. A stop request is honoured between steps: the step in progress
// completes (bounded by StepTimeout), so a notification the sink accepted
// is recorded as such rather than repeated after a restart. Run returns an
// error wrapping ErrFailed after MaxConsecutiveFailures failed steps.
func (c *Consumer) Run(ctx context.Context) error {
	t := time.NewTicker(c.cfg.Interval)
	defer t.Stop()
	failures := 0
	for {
		if ctx.Err() != nil {
			return nil
		}
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.cfg.StepTimeout)
		_, _, err := c.Step(sctx)
		cancel()
		if err != nil {
			failures++
			c.cfg.Logger.Warn("alerting: step failed", "err", err, "consecutive_failures", failures)
			if failures >= c.cfg.MaxConsecutiveFailures {
				return fmt.Errorf("%w (%d consecutive failures; last: %v)", ErrFailed, failures, err)
			}
		} else {
			failures = 0
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// Step ingests new critical events (opening alerts atomically with the
// cursor) and notifies due alerts. It returns how many were opened and
// notified.
func (c *Consumer) Step(ctx context.Context) (opened, notified int, err error) {
	now := c.cfg.Now().UTC()
	var cur int64
	if err := c.db.R.QueryRowContext(ctx, `SELECT rpos FROM alert_cursor WHERE id = 1`).Scan(&cur); err != nil {
		return 0, 0, err
	}
	recs, err := c.cfg.Events.ReadReliableAfter(ctx, cur, c.cfg.Batch, reliable.EventPanic, reliable.EventSecurityViolation)
	if err != nil {
		return 0, 0, err
	}
	if len(recs) > 0 {
		tx, err := c.db.W.BeginTx(ctx, nil)
		if err != nil {
			return 0, 0, err
		}
		defer tx.Rollback() //nolint:errcheck
		res, err := tx.ExecContext(ctx, `UPDATE alert_cursor SET rpos = ? WHERE id = 1 AND rpos = ?`, recs[len(recs)-1].Position, cur)
		if err != nil {
			return 0, 0, err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return 0, 0, errors.New("alerting: cursor moved concurrently")
		}
		for _, r := range recs {
			payload, _ := r.Upload.Encode()
			res, err := tx.ExecContext(ctx, `INSERT INTO alerts (device_id, event_uid, rpos, event_type, payload, device_time_ms,
				stored_at_ns, opened_at_ns, state, next_notify_ns) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'open', ?)
				ON CONFLICT (device_id, event_uid) DO NOTHING`,
				r.DeviceID, int64(r.Upload.UID), r.Position, int64(r.Upload.Type), payload, r.Upload.DeviceTime.UnixMilli(),
				r.StoredAt.UnixNano(), now.UnixNano(), now.UnixNano())
			if err != nil {
				return 0, 0, err
			}
			if n, _ := res.RowsAffected(); n == 1 {
				opened++
			}
		}
		if err := tx.Commit(); err != nil {
			return 0, 0, err
		}
	}

	rows, err := c.db.R.QueryContext(ctx, `SELECT device_id, event_uid, event_type, payload, device_time_ms, stored_at_ns, notify_attempts
		FROM alerts WHERE state = 'open' AND next_notify_ns <= ? ORDER BY event_type, rpos LIMIT ?`, now.UnixNano(), c.cfg.Batch)
	if err != nil {
		return opened, 0, err
	}
	type due struct {
		a        Alert
		attempts int
	}
	var list []due
	for rows.Next() {
		var a Alert
		var uid, typ, devMs, stored int64
		var payload []byte
		var attempts int
		if err := rows.Scan(&a.DeviceID, &uid, &typ, &payload, &devMs, &stored, &attempts); err != nil {
			rows.Close()
			return opened, 0, err
		}
		a.UID, a.Type = uint64(uid), reliable.EventType(typ)
		a.ID = fmt.Sprintf("%s/%d", a.DeviceID, a.UID)
		a.DeviceTime, a.StoredAt = time.UnixMilli(devMs).UTC(), time.Unix(0, stored).UTC()
		if up, err := reliable.DecodeEventUpload(payload); err == nil {
			if b, err := reliable.DecodeBody(a.DeviceID, up); err == nil {
				switch v := b.(type) {
				case reliable.Panic:
					a.Panic = &v
				case reliable.SecurityViolation:
					a.Violation = &v
				}
			}
		}
		list = append(list, due{a, attempts})
	}
	rows.Close()
	for _, d := range list {
		if err := c.cfg.Sink.Notify(ctx, d.a); err != nil {
			b := c.cfg.InitialBackoff << min(d.attempts, 20)
			if b > c.cfg.MaxBackoff || b <= 0 {
				b = c.cfg.MaxBackoff
			}
			c.cfg.Logger.Warn("alerting: notification failed; will retry", "alert", d.a.ID, "err", err)
			if _, err := c.db.W.ExecContext(ctx, `UPDATE alerts SET notify_attempts = notify_attempts + 1, next_notify_ns = ?
				WHERE device_id = ? AND event_uid = ? AND state = 'open'`, now.Add(b).UnixNano(), d.a.DeviceID, int64(d.a.UID)); err != nil {
				return opened, notified, err
			}
			continue
		}
		if _, err := c.db.W.ExecContext(ctx, `UPDATE alerts SET state = 'notified', notify_attempts = notify_attempts + 1, notified_at_ns = ?
			WHERE device_id = ? AND event_uid = ? AND state = 'open'`, now.UnixNano(), d.a.DeviceID, int64(d.a.UID)); err != nil {
			return opened, notified, err
		}
		notified++
	}
	return opened, notified, nil
}

// ErrNotFound is returned for an unknown alert.
var ErrNotFound = errors.New("alerting: alert not found")

// Acknowledge records that operator took ownership of the alert. It is
// the only transition that marks an alert handled. Acknowledging twice
// keeps the first acknowledgement.
func (c *Consumer) Acknowledge(ctx context.Context, deviceID string, uid uint64, operator, note string) error {
	if operator == "" {
		return errors.New("alerting: an acknowledgement needs an operator identity")
	}
	res, err := c.db.W.ExecContext(ctx, `UPDATE alerts SET state = 'acknowledged', acked_by = ?, acked_at_ns = ?, ack_note = ?
		WHERE device_id = ? AND event_uid = ? AND state IN ('open', 'notified')`,
		operator, c.cfg.Now().UTC().UnixNano(), note, deviceID, int64(uid))
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		return nil
	}
	var st string
	if err := c.db.R.QueryRowContext(ctx, `SELECT state FROM alerts WHERE device_id = ? AND event_uid = ?`, deviceID, int64(uid)).Scan(&st); errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	return nil // already acknowledged
}

// Status is an alert's handling record.
type Status struct {
	State          State
	NotifyAttempts int
	OpenedAt       time.Time
	NotifiedAt     time.Time
	AckedBy        string
}

// Get returns an alert's status.
func (c *Consumer) Get(ctx context.Context, deviceID string, uid uint64) (Status, error) {
	var s Status
	var st string
	var opened int64
	var notified, acked sql.NullInt64
	var by sql.NullString
	err := c.db.R.QueryRowContext(ctx, `SELECT state, notify_attempts, opened_at_ns, notified_at_ns, acked_at_ns, acked_by FROM alerts
		WHERE device_id = ? AND event_uid = ?`, deviceID, int64(uid)).Scan(&st, &s.NotifyAttempts, &opened, &notified, &acked, &by)
	if errors.Is(err, sql.ErrNoRows) {
		return s, ErrNotFound
	}
	s.State, s.OpenedAt, s.AckedBy = State(st), time.Unix(0, opened).UTC(), by.String
	if notified.Valid {
		s.NotifiedAt = time.Unix(0, notified.Int64).UTC()
	}
	return s, err
}

// Unacknowledged returns the number of alerts not yet acknowledged.
func (c *Consumer) Unacknowledged(ctx context.Context) (int, error) {
	var n int
	err := c.db.R.QueryRowContext(ctx, `SELECT count(*) FROM alerts WHERE state <> 'acknowledged'`).Scan(&n)
	return n, err
}

// Entry is a listed alert.
type Entry struct {
	DeviceID string
	UID      uint64
	Type     reliable.EventType
	Status
}

// List returns alerts, not-yet-acknowledged first, then by age.
func (c *Consumer) List(ctx context.Context, limit int) ([]Entry, error) {
	rows, err := c.db.R.QueryContext(ctx, `SELECT device_id, event_uid, event_type FROM alerts
		ORDER BY state = 'acknowledged', rpos LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	var out []Entry
	for rows.Next() {
		var e Entry
		var uid, typ int64
		if err := rows.Scan(&e.DeviceID, &uid, &typ); err != nil {
			rows.Close()
			return nil, err
		}
		e.UID, e.Type = uint64(uid), reliable.EventType(typ)
		out = append(out, e)
	}
	rows.Close()
	for i := range out {
		if out[i].Status, err = c.Get(ctx, out[i].DeviceID, out[i].UID); err != nil {
			return nil, err
		}
	}
	return out, rows.Err()
}

// LogSink is a Sink that writes alerts to a logger (the reference
// integration point; a deployment replaces it with its incident system).
type LogSink struct{ Logger *slog.Logger }

// Notify implements Sink.
func (s LogSink) Notify(_ context.Context, a Alert) error {
	attrs := []any{"alert", a.ID, "type", a.Type.String(), "device_id", a.DeviceID, "device_time", a.DeviceTime, "stored_at", a.StoredAt}
	if a.Panic != nil && a.Panic.HasPosition {
		attrs = append(attrs, "lat_e7", a.Panic.Position.LatE7, "lon_e7", a.Panic.Position.LonE7, "position_age", a.Panic.PositionAge)
	}
	s.Logger.Warn("CRITICAL EVENT — needs operator acknowledgement (stored and notified; not handled)", attrs...)
	return nil
}
