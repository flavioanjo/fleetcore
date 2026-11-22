package device

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/flavioanjo/fleetcore/internal/reliable"
	"github.com/flavioanjo/fleetcore/internal/sqlitedb"
)

// The device's durable store (device.db). It survives process crashes and
// reboots; the guarantees of docs/reliable.md assume it also survives the
// failure being considered (no flash corruption).
var migrations = []sqlitedb.Migration{{
	Version: 1,
	Name:    "device state, outbox, commands",
	Stmts: []string{
		// session_gen and next_uid only ever increase. session_gen is
		// incremented and committed at every boot before the first frame
		// is sent, so it is never reused; next_uid is advanced in the same
		// transaction that queues an event, so a UID is never reused.
		`CREATE TABLE device_state (
			id          INTEGER PRIMARY KEY CHECK (id = 1),
			session_gen INTEGER NOT NULL CHECK (session_gen >= 0),
			next_uid    INTEGER NOT NULL CHECK (next_uid >= 1),
			boots       INTEGER NOT NULL
		) STRICT`,
		`INSERT INTO device_state (id, session_gen, next_uid, boots) VALUES (1, 0, 1, 0)`,
		// The outbound queue. A row is written before the event counts as
		// queued, and stays 'pending' until an authenticated ACK of durable
		// storage for this UID and fingerprint arrives. Rows are never
		// deleted while pending.
		`CREATE TABLE outbox (
			uid             INTEGER PRIMARY KEY CHECK (uid >= 1),
			event_type      INTEGER NOT NULL,
			class           INTEGER NOT NULL,
			payload         BLOB    NOT NULL,
			fingerprint     BLOB    NOT NULL CHECK (length(fingerprint) = 32),
			created_ns      INTEGER NOT NULL,
			state           TEXT    NOT NULL CHECK (state IN ('pending', 'acked', 'failed', 'expired')),
			attempts        INTEGER NOT NULL DEFAULT 0,
			first_sent_ns   INTEGER,
			last_sent_ns    INTEGER,
			next_attempt_ns INTEGER NOT NULL,
			done_ns         INTEGER,
			ack_status      INTEGER,
			ack_position    INTEGER
		) STRICT`,
		`CREATE INDEX outbox_due ON outbox (state, class, next_attempt_ns, uid)`,
		// Commands this device received. A command id is recorded once; a
		// repeated delivery never executes again. exec_started_ns is
		// committed before the actuator is driven (write-ahead), so a crash
		// during execution is detected at the next boot.
		`CREATE TABLE commands (
			command_id      BLOB    PRIMARY KEY CHECK (length(command_id) = 16),
			command_type    INTEGER NOT NULL,
			args            BLOB    NOT NULL,
			issued_ms       INTEGER NOT NULL,
			expires_ms      INTEGER NOT NULL,
			principal       TEXT    NOT NULL,
			received_ns     INTEGER NOT NULL,
			stage           INTEGER NOT NULL,
			reason          INTEGER NOT NULL DEFAULT 0,
			observed        INTEGER NOT NULL DEFAULT 0,
			exec_started_ns INTEGER,
			exec_done_ns    INTEGER,
			last_report_ns  INTEGER NOT NULL
		) STRICT`,
	},
}}

type store struct {
	db *sqlitedb.DB
}

func openStore(ctx context.Context, path string, now func() time.Time) (*store, error) {
	db, err := sqlitedb.Open(ctx, "device", path, sqlitedb.Options{Now: now, ReadConns: 2}, migrations)
	if err != nil {
		return nil, err
	}
	return &store{db: db}, nil
}

func (s *store) close() error { return s.db.Close() }

type deviceRow struct {
	sessionGen uint64
	nextUID    uint64
	boots      int64
}

func readState(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) (deviceRow, error) {
	var r deviceRow
	var g, u int64
	err := q.QueryRowContext(ctx, `SELECT session_gen, next_uid, boots FROM device_state WHERE id = 1`).Scan(&g, &u, &r.boots)
	r.sessionGen, r.nextUID = uint64(g), uint64(u)
	return r, err
}

// outboxRow is one queued event.
type outboxRow struct {
	uid         uint64
	typ         reliable.EventType
	class       reliable.Class
	payload     []byte
	fingerprint reliable.Fingerprint
	created     time.Time
	state       string
	attempts    int
	firstSent   time.Time
	nextAttempt time.Time
}

func scanOutbox(rows *sql.Rows) ([]outboxRow, error) {
	defer rows.Close()
	var out []outboxRow
	for rows.Next() {
		var r outboxRow
		var uid, typ, class, created, next int64
		var first sql.NullInt64
		var fp []byte
		if err := rows.Scan(&uid, &typ, &class, &r.payload, &fp, &created, &r.state, &r.attempts, &first, &next); err != nil {
			return nil, err
		}
		r.uid, r.typ, r.class = uint64(uid), reliable.EventType(typ), reliable.Class(class)
		copy(r.fingerprint[:], fp)
		r.created, r.nextAttempt = time.Unix(0, created), time.Unix(0, next)
		if first.Valid {
			r.firstSent = time.Unix(0, first.Int64)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

const outboxCols = `uid, event_type, class, payload, fingerprint, created_ns, state, attempts, first_sent_ns, next_attempt_ns`

// ErrQueueFull is returned when an event cannot be queued because the
// outbox is at capacity for its class. Nothing is queued; the caller
// decides (the device counts it). Queued events are never evicted to make
// room.
var ErrQueueFull = errors.New("device: outbound queue full")

// enqueueTx queues an event inside tx, allocating its UID. build receives
// the UID (TELEMETRY embeds it as the v1 EventID).
func enqueueTx(ctx context.Context, tx *sql.Tx, cfg *Config, now time.Time, build func(uid uint64) (reliable.Body, error)) (uint64, reliable.EventType, error) {
	st, err := readState(ctx, tx)
	if err != nil {
		return 0, 0, err
	}
	uid := st.nextUID
	body, err := build(uid)
	if err != nil {
		return 0, 0, err
	}
	class := body.EventType().Class()
	var pending int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM outbox WHERE state = 'pending'`).Scan(&pending); err != nil {
		return 0, 0, err
	}
	limit := cfg.Capacity - cfg.CriticalReserve
	if class.Critical() || class == reliable.ClassControl {
		limit = cfg.Capacity // the reserve is for critical events and command reports
	}
	if pending >= limit {
		return 0, 0, fmt.Errorf("%w (%d pending, limit %d for %s)", ErrQueueFull, pending, limit, class)
	}
	up, err := reliable.NewEvent(uid, now, body)
	if err != nil {
		return 0, 0, err
	}
	payload, err := up.Encode()
	if err != nil {
		return 0, 0, err
	}
	fp := reliable.EventFingerprint(cfg.DeviceID, payload)
	if _, err := tx.ExecContext(ctx, `INSERT INTO outbox (uid, event_type, class, payload, fingerprint, created_ns, state, next_attempt_ns)
		VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)`, int64(uid), int64(up.Type), int64(class), payload, fp[:], now.UnixNano(), now.UnixNano()); err != nil {
		return 0, 0, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE device_state SET next_uid = ? WHERE id = 1`, int64(uid+1)); err != nil {
		return 0, 0, err
	}
	return uid, up.Type, nil
}
