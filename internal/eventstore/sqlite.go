package eventstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/flavioanjo/fleetcore/internal/protocol"
	"github.com/flavioanjo/fleetcore/internal/sqlitedb"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// Errors returned by the store.
var (
	ErrNotFound = errors.New("eventstore: event not found")
	ErrClosed   = errors.New("eventstore: store is closed")
	// ErrSchemaTooNew is sqlitedb.ErrSchemaTooNew: the database was
	// migrated by a newer build.
	ErrSchemaTooNew = sqlitedb.ErrSchemaTooNew
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

// Store is a SQLite-backed event store. The SQLite configuration (WAL,
// synchronous=FULL, a single BEGIN IMMEDIATE writer, a query_only read pool,
// checkpoint on close) is documented in internal/sqlitedb.
//
// Store is safe for concurrent use, also by several processes on one file.
type Store struct {
	db   *sqlitedb.DB
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
	if opts.Now == nil {
		opts.Now = time.Now
	}
	db, err := sqlitedb.Open(ctx, "eventstore", path, sqlitedb.Options{
		BusyTimeout: opts.BusyTimeout, ReadConns: opts.ReadConns, Now: opts.Now,
	}, migrations)
	if err != nil {
		return nil, err
	}
	return &Store{db: db, w: db.W, r: db.R, now: opts.Now}, nil
}

// Close checkpoints the WAL and closes the database. It waits for in-flight
// operations; operations started afterwards fail with ErrClosed. Close is
// idempotent.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		if err := s.db.Close(); err != nil {
			s.closeErr = fmt.Errorf("eventstore: close: %w", err)
		}
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

	a, err := newAppender(ctx, tx, s.now().UnixNano())
	if err != nil {
		return nil, err
	}
	defer a.close()
	results := make([]Result, len(evs))
	for i, ev := range evs {
		if results[i], err = a.append(ctx, ev); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("eventstore: commit: %w", err)
	}
	return results, nil
}

// appender classifies and stores version-1 events inside a caller's
// transaction. AppendBatch and AppendReliable share it, so both apply
// exactly the same identity and conflict rules.
type appender struct {
	tx                 *sql.Tx
	insert, otherBoots *sql.Stmt
	storedAt           int64
}

func newAppender(ctx context.Context, tx *sql.Tx, storedAt int64) (*appender, error) {
	insert, err := tx.PrepareContext(ctx, `INSERT INTO events
		(device_id, boot_id, event_id, sequence, device_time_ms, ingested_at_ns,
		 source, quality, distance_kind, distance_mm, fingerprint, packet, stored_at_ns)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (device_id, boot_id, event_id) DO NOTHING
		RETURNING position`)
	if err != nil {
		return nil, fmt.Errorf("eventstore: prepare: %w", err)
	}
	otherBoots, err := tx.PrepareContext(ctx, `SELECT boot_id FROM events
		WHERE device_id = ? AND event_id = ? AND boot_id <> ?
		ORDER BY position LIMIT ?`)
	if err != nil {
		insert.Close()
		return nil, fmt.Errorf("eventstore: prepare: %w", err)
	}
	return &appender{tx: tx, insert: insert, otherBoots: otherBoots, storedAt: storedAt}, nil
}

func (a *appender) close() {
	a.insert.Close()
	a.otherBoots.Close()
}

func (a *appender) append(ctx context.Context, ev Event) (Result, error) {
	tx := a.tx
	smp := ev.Sample
	ing, _ := smp.IngestedAt.Get()
	var kind, mm sql.NullInt64
	if d, ok := smp.Distance.Get(); ok {
		kind = sql.NullInt64{Int64: int64(d.Kind), Valid: true}
		mm = sql.NullInt64{Int64: int64(d.Value), Valid: true}
	}
	var pos int64
	err := a.insert.QueryRowContext(ctx,
		string(smp.DeviceID), int64(smp.BootID), int64(smp.EventID), int64(smp.Sequence),
		smp.DeviceTime.UnixMilli(), ing.UnixNano(), int64(smp.Source), int64(smp.Quality),
		kind, mm, ev.Fingerprint[:], ev.Packet, a.storedAt,
	).Scan(&pos)
	switch {
	case err == nil:
		reused, err := queryBoots(ctx, a.otherBoots, smp)
		if err != nil {
			return Result{}, fmt.Errorf("eventstore: cross-boot check %s: %w", ev.Key(), err)
		}
		return Result{Outcome: Inserted, Position: pos, Existing: ev.Fingerprint, EventIDAlsoInBoots: reused}, nil
	case !errors.Is(err, sql.ErrNoRows):
		return Result{}, fmt.Errorf("eventstore: insert %s: %w", ev.Key(), err)
	}

	// Identity already stored: compare payloads.
	var fp []byte
	if err := tx.QueryRowContext(ctx, `SELECT position, fingerprint FROM events
		WHERE device_id = ? AND boot_id = ? AND event_id = ?`,
		string(smp.DeviceID), int64(smp.BootID), int64(smp.EventID)).Scan(&pos, &fp); err != nil {
		return Result{}, fmt.Errorf("eventstore: lookup %s: %w", ev.Key(), err)
	}
	var existing Fingerprint
	copy(existing[:], fp)
	if existing == ev.Fingerprint {
		return Result{Outcome: Duplicate, Position: pos, Existing: existing}, nil
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
		return Result{}, fmt.Errorf("eventstore: record conflict %s: %w", ev.Key(), err)
	}
	return Result{Outcome: Conflict, Position: pos, Existing: existing}, nil
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

// ByPositions returns the stored events at the given positions, in the
// order requested. Every position must exist (ErrNotFound otherwise);
// repeated positions are returned repeatedly. Consumers that persist
// positions, such as the chronological processor's journal, use it to
// re-read exactly the events they processed.
func (s *Store) ByPositions(ctx context.Context, positions []int64) ([]Record, error) {
	const chunk = 500 // stays well below SQLite's bound-parameter limit
	found := make(map[int64]Record, len(positions))
	for start := 0; start < len(positions); start += chunk {
		part := positions[start:min(start+chunk, len(positions))]
		q := `SELECT position, packet, fingerprint, ingested_at_ns, stored_at_ns FROM events WHERE position IN (?` +
			strings.Repeat(",?", len(part)-1) + `)`
		args := make([]any, len(part))
		for i, p := range part {
			args[i] = p
		}
		recs, err := s.query(ctx, q, args...)
		if err != nil {
			return nil, err
		}
		for _, r := range recs {
			found[r.Position] = r
		}
	}
	out := make([]Record, len(positions))
	for i, p := range positions {
		r, ok := found[p]
		if !ok {
			return nil, fmt.Errorf("%w: position %d", ErrNotFound, p)
		}
		out[i] = r
	}
	return out, nil
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
