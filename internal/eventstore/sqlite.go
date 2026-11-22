package eventstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/flavioanjo/fleetcore/internal/protocol"
	"github.com/flavioanjo/fleetcore/internal/telemetry"

	// Pure-Go SQLite (no cgo). Registers the "sqlite" database/sql driver.
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// Errors returned by the store.
var (
	ErrNotFound     = errors.New("eventstore: event not found")
	ErrClosed       = errors.New("eventstore: store is closed")
	ErrSchemaTooNew = errors.New("eventstore: database schema is newer than this build")
	ErrCorrupt      = errors.New("eventstore: stored event failed integrity check")
)

// Options configures a Store. The zero value of every field selects the
// documented default.
type Options struct {
	// BusyTimeout is how long a connection waits for a lock held by another
	// connection or process before failing with SQLITE_BUSY. Default 5s.
	BusyTimeout time.Duration
	// ReadConns is the size of the read-only connection pool. Default 4.
	ReadConns int
	// Now returns the current time for stored_at and migration records.
	// Default time.Now.
	Now func() time.Time
}

// Store is a SQLite-backed event store.
//
// SQLite configuration:
//   - journal_mode=WAL: readers never block the writer and vice versa.
//   - synchronous=FULL: a committed transaction survives power loss (WAL
//     with NORMAL could lose the most recent commits).
//   - busy_timeout: see Options.BusyTimeout.
//   - foreign_keys=ON.
//   - Write pool: exactly one connection, and every write transaction is
//     BEGIN IMMEDIATE, so writes in this process are serialized by
//     database/sql and writes from other processes wait on busy_timeout
//     instead of failing with lock-upgrade deadlocks.
//   - Read pool: Options.ReadConns connections opened with query_only=ON.
//   - Close: checkpoints and truncates the WAL, then closes both pools.
//
// Store is safe for concurrent use.
type Store struct {
	w, r *sql.DB
	now  func() time.Time

	closeOnce sync.Once
	closeErr  error
	mu        sync.RWMutex
	closed    bool
}

// Open opens (creating if needed) the database at path and migrates it to
// the current schema.
func Open(ctx context.Context, path string, opts Options) (*Store, error) {
	if path == "" {
		return nil, errors.New("eventstore: empty database path")
	}
	if opts.BusyTimeout <= 0 {
		opts.BusyTimeout = 5 * time.Second
	}
	if opts.ReadConns <= 0 {
		opts.ReadConns = 4
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}

	common := url.Values{}
	common.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", opts.BusyTimeout.Milliseconds()))
	common.Add("_pragma", "foreign_keys(1)")
	common.Add("_pragma", "synchronous(FULL)")

	wq := cloneValues(common)
	wq.Add("_pragma", "journal_mode(WAL)")
	wq.Set("_txlock", "immediate")
	w, err := sql.Open("sqlite", path+"?"+wq.Encode())
	if err != nil {
		return nil, fmt.Errorf("eventstore: open: %w", err)
	}
	w.SetMaxOpenConns(1)
	w.SetMaxIdleConns(1)
	w.SetConnMaxLifetime(0)
	w.SetConnMaxIdleTime(0)

	// Connecting applies the pragmas. Switching a new database to WAL needs
	// an exclusive lock and SQLite does not invoke the busy handler for it,
	// so concurrent first opens are retried here, bounded by BusyTimeout.
	if err := retryBusy(ctx, opts.BusyTimeout, func() error { return w.PingContext(ctx) }); err != nil {
		w.Close()
		return nil, fmt.Errorf("eventstore: open %s: %w", path, err)
	}
	var mode string
	if err := w.QueryRowContext(ctx, `PRAGMA journal_mode`).Scan(&mode); err != nil || mode != "wal" {
		w.Close()
		return nil, fmt.Errorf("eventstore: could not enable WAL (journal_mode=%q): %v", mode, err)
	}
	if err := migrate(ctx, w, opts.Now()); err != nil {
		w.Close()
		return nil, err
	}

	rq := cloneValues(common)
	rq.Add("_pragma", "query_only(1)")
	r, err := sql.Open("sqlite", path+"?"+rq.Encode())
	if err != nil {
		w.Close()
		return nil, fmt.Errorf("eventstore: open reader: %w", err)
	}
	r.SetMaxOpenConns(opts.ReadConns)
	r.SetMaxIdleConns(opts.ReadConns)
	if err := r.PingContext(ctx); err != nil {
		r.Close()
		w.Close()
		return nil, fmt.Errorf("eventstore: open reader: %w", err)
	}
	return &Store{w: w, r: r, now: opts.Now}, nil
}

// retryBusy runs fn until it succeeds, fails with an error other than
// SQLITE_BUSY/SQLITE_LOCKED, or limit elapses.
func retryBusy(ctx context.Context, limit time.Duration, fn func() error) error {
	deadline := time.Now().Add(limit)
	delay := time.Millisecond
	for {
		err := fn()
		if err == nil || !isBusy(err) || time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(delay):
		}
		if delay < 50*time.Millisecond {
			delay *= 2
		}
	}
}

func isBusy(err error) bool {
	var se *sqlite.Error
	if !errors.As(err, &se) {
		return false
	}
	code := se.Code() & 0xff // primary result code
	return code == sqlite3.SQLITE_BUSY || code == sqlite3.SQLITE_LOCKED
}

func cloneValues(v url.Values) url.Values {
	out := url.Values{}
	for k, vs := range v {
		out[k] = append([]string(nil), vs...)
	}
	return out
}

// Close checkpoints the WAL and closes the database. It waits for in-flight
// operations; operations started afterwards fail with ErrClosed. Close is
// idempotent.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		var errs []error
		if _, err := s.w.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
			errs = append(errs, fmt.Errorf("eventstore: checkpoint: %w", err))
		}
		errs = append(errs, s.r.Close(), s.w.Close())
		s.closeErr = errors.Join(errs...)
	})
	return s.closeErr
}

// acquire guards an operation against a concurrent Close.
func (s *Store) acquire() (release func(), err error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, ErrClosed
	}
	return s.mu.RUnlock, nil
}

// Append stores one event. See AppendBatch.
func (s *Store) Append(ctx context.Context, ev Event) (Result, error) {
	res, err := s.AppendBatch(ctx, []Event{ev})
	if err != nil {
		return Result{}, err
	}
	return res[0], nil
}

// AppendBatch classifies and stores events in order, in one transaction:
// either every result is durable when it returns nil, or nothing was
// written. Events in one batch are classified against each other as well
// as against the store, so a batch containing a retransmission yields
// Inserted followed by Duplicate.
func (s *Store) AppendBatch(ctx context.Context, evs []Event) ([]Result, error) {
	if len(evs) == 0 {
		return nil, nil
	}
	release, err := s.acquire()
	if err != nil {
		return nil, err
	}
	defer release()

	for i := range evs {
		if err := checkEvent(evs[i]); err != nil {
			return nil, fmt.Errorf("eventstore: event %d (%s): %w", i, evs[i].Key(), err)
		}
	}

	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("eventstore: begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after Commit

	insert, err := tx.PrepareContext(ctx, `INSERT INTO events
		(device_id, boot_id, event_id, sequence, device_time_ms, ingested_at_ns,
		 source, quality, distance_kind, distance_mm, fingerprint, packet, stored_at_ns)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (device_id, boot_id, event_id) DO NOTHING
		RETURNING position`)
	if err != nil {
		return nil, fmt.Errorf("eventstore: prepare: %w", err)
	}
	defer insert.Close()
	otherBoots, err := tx.PrepareContext(ctx, `SELECT boot_id FROM events
		WHERE device_id = ? AND event_id = ? AND boot_id <> ?
		ORDER BY position LIMIT ?`)
	if err != nil {
		return nil, fmt.Errorf("eventstore: prepare: %w", err)
	}
	defer otherBoots.Close()

	storedAt := s.now().UnixNano()
	results := make([]Result, len(evs))
	for i, ev := range evs {
		smp := ev.Sample
		ing, _ := smp.IngestedAt.Get()
		var kind, mm sql.NullInt64
		if d, ok := smp.Distance.Get(); ok {
			kind = sql.NullInt64{Int64: int64(d.Kind), Valid: true}
			mm = sql.NullInt64{Int64: int64(d.Value), Valid: true}
		}
		var pos int64
		err := insert.QueryRowContext(ctx,
			string(smp.DeviceID), int64(smp.BootID), int64(smp.EventID), int64(smp.Sequence),
			smp.DeviceTime.UnixMilli(), ing.UnixNano(), int64(smp.Source), int64(smp.Quality),
			kind, mm, ev.Fingerprint[:], ev.Packet, storedAt,
		).Scan(&pos)
		switch {
		case err == nil:
			reused, err := queryBoots(ctx, otherBoots, smp)
			if err != nil {
				return nil, fmt.Errorf("eventstore: cross-boot check %s: %w", ev.Key(), err)
			}
			results[i] = Result{Outcome: Inserted, Position: pos, Existing: ev.Fingerprint, EventIDAlsoInBoots: reused}
			continue
		case !errors.Is(err, sql.ErrNoRows):
			return nil, fmt.Errorf("eventstore: insert %s: %w", ev.Key(), err)
		}

		// Identity already stored: compare payloads.
		var fp []byte
		if err := tx.QueryRowContext(ctx, `SELECT position, fingerprint FROM events
			WHERE device_id = ? AND boot_id = ? AND event_id = ?`,
			string(smp.DeviceID), int64(smp.BootID), int64(smp.EventID)).Scan(&pos, &fp); err != nil {
			return nil, fmt.Errorf("eventstore: lookup %s: %w", ev.Key(), err)
		}
		var existing Fingerprint
		copy(existing[:], fp)
		if existing == ev.Fingerprint {
			results[i] = Result{Outcome: Duplicate, Position: pos, Existing: existing}
			continue
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO event_conflicts
			(device_id, boot_id, event_id, fingerprint, packet, first_seen_ns, last_seen_ns, receipts)
			VALUES (?, ?, ?, ?, ?, ?, ?, 1)
			ON CONFLICT (device_id, boot_id, event_id, fingerprint) DO UPDATE SET
				receipts      = receipts + 1,
				first_seen_ns = min(first_seen_ns, excluded.first_seen_ns),
				last_seen_ns  = max(last_seen_ns, excluded.last_seen_ns)`,
			string(smp.DeviceID), int64(smp.BootID), int64(smp.EventID), ev.Fingerprint[:], ev.Packet, ing.UnixNano(), ing.UnixNano(),
		); err != nil {
			return nil, fmt.Errorf("eventstore: record conflict %s: %w", ev.Key(), err)
		}
		results[i] = Result{Outcome: Conflict, Position: pos, Existing: existing}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("eventstore: commit: %w", err)
	}
	return results, nil
}

// maxReportedBoots bounds Result.EventIDAlsoInBoots.
const maxReportedBoots = 8

func queryBoots(ctx context.Context, stmt *sql.Stmt, smp telemetry.Sample) ([]telemetry.BootID, error) {
	rows, err := stmt.QueryContext(ctx, string(smp.DeviceID), int64(smp.EventID), int64(smp.BootID), maxReportedBoots)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []telemetry.BootID
	for rows.Next() {
		var b int64
		if err := rows.Scan(&b); err != nil {
			return nil, err
		}
		out = append(out, telemetry.BootID(b))
	}
	return out, rows.Err()
}

// checkEvent rejects events not built by NewEvent (or altered since).
func checkEvent(ev Event) error {
	if !ev.Sample.IngestedAt.IsPresent() {
		return ErrNoIngestionTime
	}
	if sha256.Sum256(ev.Packet) != ev.Fingerprint {
		return errors.New("fingerprint does not match packet")
	}
	s, err := protocol.DecodeSample(ev.Packet)
	if err != nil {
		return err
	}
	if !sameWireFields(s, ev.Sample) {
		return errors.New("packet does not encode the sample")
	}
	return nil
}

// sameWireFields compares every transmitted field (time.Time by Equal, not
// ==, which would also compare locations and monotonic readings).
func sameWireFields(a, b telemetry.Sample) bool {
	return a.DeviceID == b.DeviceID && a.EventID == b.EventID && a.BootID == b.BootID &&
		a.Sequence == b.Sequence && a.DeviceTime.Equal(b.DeviceTime) &&
		a.Source == b.Source && a.Quality == b.Quality && a.Distance == b.Distance
}

// Get returns the stored event with the given identity, or ErrNotFound.
func (s *Store) Get(ctx context.Context, k Key) (Record, error) {
	release, err := s.acquire()
	if err != nil {
		return Record{}, err
	}
	defer release()
	row := s.r.QueryRowContext(ctx, `SELECT position, packet, fingerprint, ingested_at_ns, stored_at_ns
		FROM events WHERE device_id = ? AND boot_id = ? AND event_id = ?`, string(k.DeviceID), int64(k.BootID), int64(k.EventID))
	rec, err := scanRecord(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Record{}, ErrNotFound
	}
	return rec, err
}

// ReadAfter returns up to limit stored events with Position > after, in
// position order. Downstream consumers page through the store with it,
// persisting the last position they processed.
func (s *Store) ReadAfter(ctx context.Context, after int64, limit int) ([]Record, error) {
	if limit <= 0 {
		return nil, nil
	}
	return s.query(ctx, `SELECT position, packet, fingerprint, ingested_at_ns, stored_at_ns
		FROM events WHERE position > ? ORDER BY position LIMIT ?`, after, limit)
}

// ByEventID returns every stored event of a device with the given EventID,
// across all boots, in position order. More than one result means the
// producer reused the EventID across boots.
func (s *Store) ByEventID(ctx context.Context, dev telemetry.DeviceID, eid telemetry.EventID) ([]Record, error) {
	return s.query(ctx, `SELECT position, packet, fingerprint, ingested_at_ns, stored_at_ns
		FROM events WHERE device_id = ? AND event_id = ? ORDER BY position`, string(dev), int64(eid))
}

func (s *Store) query(ctx context.Context, q string, args ...any) ([]Record, error) {
	release, err := s.acquire()
	if err != nil {
		return nil, err
	}
	defer release()
	rows, err := s.r.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("eventstore: read: %w", err)
	}
	defer rows.Close()
	var out []Record
	for rows.Next() {
		rec, err := scanRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("eventstore: read: %w", err)
	}
	return out, nil
}

// Conflicts returns the conflicting payloads recorded for an identity,
// oldest first.
func (s *Store) Conflicts(ctx context.Context, k Key) ([]ConflictRecord, error) {
	release, err := s.acquire()
	if err != nil {
		return nil, err
	}
	defer release()
	rows, err := s.r.QueryContext(ctx, `SELECT fingerprint, packet, first_seen_ns, last_seen_ns, receipts
		FROM event_conflicts WHERE device_id = ? AND boot_id = ? AND event_id = ? ORDER BY id`, string(k.DeviceID), int64(k.BootID), int64(k.EventID))
	if err != nil {
		return nil, fmt.Errorf("eventstore: conflicts: %w", err)
	}
	defer rows.Close()
	var out []ConflictRecord
	for rows.Next() {
		var c ConflictRecord
		var fp []byte
		var first, last int64
		if err := rows.Scan(&fp, &c.Packet, &first, &last, &c.Count); err != nil {
			return nil, fmt.Errorf("eventstore: conflicts: %w", err)
		}
		c.Key = k
		copy(c.Fingerprint[:], fp)
		c.FirstSeenAt = time.Unix(0, first).UTC()
		c.LastSeenAt = time.Unix(0, last).UTC()
		out = append(out, c)
	}
	return out, rows.Err()
}

type scanner interface{ Scan(dest ...any) error }

// scanRecord rebuilds a record from its canonical packet, verifying the
// stored fingerprint so silent on-disk corruption is reported, not served.
func scanRecord(row scanner) (Record, error) {
	var (
		rec            Record
		pkt, fp        []byte
		ingNs, storeNs int64
	)
	if err := row.Scan(&rec.Position, &pkt, &fp, &ingNs, &storeNs); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Record{}, err
		}
		return Record{}, fmt.Errorf("eventstore: scan: %w", err)
	}
	sum := sha256.Sum256(pkt)
	if !bytes.Equal(sum[:], fp) {
		return Record{}, fmt.Errorf("%w: position %d: fingerprint mismatch", ErrCorrupt, rec.Position)
	}
	smp, err := protocol.DecodeSample(pkt)
	if err != nil {
		return Record{}, fmt.Errorf("%w: position %d: %v", ErrCorrupt, rec.Position, err)
	}
	smp.IngestedAt = telemetry.Some(time.Unix(0, ingNs).UTC())
	rec.Sample = smp
	rec.Fingerprint = sum
	rec.StoredAt = time.Unix(0, storeNs).UTC()
	return rec, nil
}
