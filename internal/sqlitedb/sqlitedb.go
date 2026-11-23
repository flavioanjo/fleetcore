// Package sqlitedb opens FleetCore's SQLite databases with one fixed,
// documented configuration and applies versioned migrations. It is shared by
// the event store (internal/eventstore) and the results store
// (internal/resultstore) so both have identical durability and locking
// behaviour.
//
// Configuration:
//   - journal_mode=WAL: readers never block the writer and vice versa.
//   - synchronous=FULL: a committed transaction survives power loss (WAL with
//     NORMAL could lose the most recent commits).
//   - busy_timeout: Options.BusyTimeout.
//   - foreign_keys=ON.
//   - Write pool: exactly one connection, and every write transaction is
//     BEGIN IMMEDIATE, so writes in this process are serialized by
//     database/sql and writes from other processes wait on busy_timeout
//     instead of failing with lock-upgrade deadlocks.
//   - Read pool: Options.ReadConns connections opened with query_only=ON.
//   - Close: checkpoints and truncates the WAL, then closes both pools.
//   - Driver: modernc.org/sqlite behind a thin wrapper (driver.go) that runs
//     queries without context cancellation, working around a statement leak
//     in the driver when a query is cancelled mid-step.
package sqlitedb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"time"

	// Pure-Go SQLite (no cgo). Registers the "sqlite" database/sql driver.
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// ErrSchemaTooNew is returned when a database was migrated by a newer build.
var ErrSchemaTooNew = errors.New("database schema is newer than this build")

// Options configures Open. Zero fields select the defaults.
type Options struct {
	// BusyTimeout is how long a connection waits for a lock held by another
	// connection or process before failing with SQLITE_BUSY. Default 5s.
	BusyTimeout time.Duration
	// ReadConns is the size of the read-only connection pool. Default 4.
	ReadConns int
	// Now timestamps migration records. Default time.Now.
	Now func() time.Time
}

// Migration is one forward-only schema step. Versions are consecutive,
// starting at 1. A released migration is never edited; schema changes are
// new migrations.
type Migration struct {
	Version int
	Name    string
	Stmts   []string
}

// DB is an open database: W is the single-connection write pool, R the
// read-only pool.
type DB struct {
	W, R *sql.DB
}

// Open opens (creating if needed) the database at path, checks that WAL is
// active and applies the pending migrations. name prefixes error messages.
func Open(ctx context.Context, name, path string, opts Options, migrations []Migration) (*DB, error) {
	if path == "" {
		return nil, fmt.Errorf("%s: empty database path", name)
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
	registerDriver()
	w, err := sql.Open(driverName, path+"?"+wq.Encode())
	if err != nil {
		return nil, fmt.Errorf("%s: open: %w", name, err)
	}
	w.SetMaxOpenConns(1)
	w.SetMaxIdleConns(1)
	w.SetConnMaxLifetime(0)
	w.SetConnMaxIdleTime(0)

	// Connecting applies the pragmas. Switching a new database to WAL needs
	// an exclusive lock and SQLite does not invoke the busy handler for it,
	// so concurrent first opens are retried here, bounded by BusyTimeout.
	if err := RetryBusy(ctx, opts.BusyTimeout, func() error { return w.PingContext(ctx) }); err != nil {
		w.Close()
		return nil, fmt.Errorf("%s: open %s: %w", name, path, err)
	}
	var mode string
	if err := w.QueryRowContext(ctx, `PRAGMA journal_mode`).Scan(&mode); err != nil || mode != "wal" {
		w.Close()
		return nil, fmt.Errorf("%s: could not enable WAL (journal_mode=%q): %v", name, mode, err)
	}
	if err := migrate(ctx, name, w, opts.Now(), migrations); err != nil {
		w.Close()
		return nil, err
	}

	rq := cloneValues(common)
	rq.Add("_pragma", "query_only(1)")
	r, err := sql.Open(driverName, path+"?"+rq.Encode())
	if err != nil {
		w.Close()
		return nil, fmt.Errorf("%s: open reader: %w", name, err)
	}
	r.SetMaxOpenConns(opts.ReadConns)
	r.SetMaxIdleConns(opts.ReadConns)
	if err := r.PingContext(ctx); err != nil {
		r.Close()
		w.Close()
		return nil, fmt.Errorf("%s: open reader: %w", name, err)
	}
	return &DB{W: w, R: r}, nil
}

// Close checkpoints the WAL and closes both pools.
func (d *DB) Close() error {
	var errs []error
	if _, err := d.W.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		errs = append(errs, fmt.Errorf("checkpoint: %w", err))
	}
	errs = append(errs, d.R.Close(), d.W.Close())
	return errors.Join(errs...)
}

// RetryBusy runs fn until it succeeds, fails with an error other than
// SQLITE_BUSY/SQLITE_LOCKED, or limit elapses.
func RetryBusy(ctx context.Context, limit time.Duration, fn func() error) error {
	deadline := time.Now().Add(limit)
	delay := time.Millisecond
	for {
		err := fn()
		if err == nil || !IsBusy(err) || time.Now().After(deadline) {
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

// IsBusy reports whether err is SQLITE_BUSY or SQLITE_LOCKED.
func IsBusy(err error) bool {
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

// migrate brings the database to the last migration's version inside one
// immediate transaction, so concurrent openers serialize and a failed step
// leaves the schema unchanged. A database written by a newer build is
// refused rather than used with an unknown schema.
func migrate(ctx context.Context, name string, db *sql.DB, now time.Time, migrations []Migration) error {
	latest := 0
	for i, m := range migrations {
		if m.Version != i+1 {
			return fmt.Errorf("%s: migration %d has version %d; versions must be consecutive from 1", name, i, m.Version)
		}
		latest = m.Version
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("%s: migrate: begin: %w", name, err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after Commit

	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version       INTEGER PRIMARY KEY,
		name          TEXT    NOT NULL,
		applied_at_ns INTEGER NOT NULL
	) STRICT`); err != nil {
		return fmt.Errorf("%s: migrate: %w", name, err)
	}
	var current int
	if err := tx.QueryRowContext(ctx, `SELECT coalesce(max(version), 0) FROM schema_migrations`).Scan(&current); err != nil {
		return fmt.Errorf("%s: migrate: read version: %w", name, err)
	}
	if current > latest {
		return fmt.Errorf("%s: %w: database is at version %d, this build supports up to %d", name, ErrSchemaTooNew, current, latest)
	}
	for _, m := range migrations {
		if m.Version <= current {
			continue
		}
		for _, stmt := range m.Stmts {
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("%s: migration %d (%s): %w", name, m.Version, m.Name, err)
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (version, name, applied_at_ns) VALUES (?, ?, ?)`,
			m.Version, m.Name, now.UnixNano()); err != nil {
			return fmt.Errorf("%s: migration %d: record: %w", name, m.Version, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%s: migrate: commit: %w", name, err)
	}
	return nil
}
