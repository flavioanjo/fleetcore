package sqlitedb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"

	// Pure-Go SQLite (no cgo). Registers the "sqlite" database/sql driver.
	_ "modernc.org/sqlite"
)

// driverName is the database/sql driver every FleetCore database is opened
// with: modernc.org/sqlite wrapped by safeDriver.
const driverName = "sqlite-fleetcore"

var registerOnce sync.Once

func registerDriver() {
	registerOnce.Do(func() {
		db, err := sql.Open("sqlite", "")
		if err != nil {
			panic("sqlitedb: sqlite driver not registered: " + err.Error())
		}
		inner := db.Driver()
		db.Close()
		sql.Register(driverName, safeDriver{inner})
	})
}

// safeDriver works around a resource leak in modernc.org/sqlite (observed
// in v1.38.2): when a query's context is cancelled while sqlite3_step is
// producing its first row, stmt.query drops the rows it has just built
// without finalizing the prepared statement and returns ctx.Err(). The
// connection is marked interrupted, so database/sql discards it, and
// sqlite3_close_v2 with an unfinalized statement leaves a zombie that keeps
// the database file open and holds a read snapshot: Close reports success,
// WAL checkpoints are blocked for good, and on Windows the file stays
// locked (TestCancelledQueryReleasesConnection).
//
// safeDriver passes queries to the driver without the context's
// cancellation. A query that has started therefore runs to its first row
// (bounded by busy_timeout and by the size of FleetCore's queries); a
// context cancelled earlier is still refused, and database/sql still
// observes the original context while the rows are iterated (it closes
// them, finalizing the statement properly, and Rows.Err reports the
// cancellation). Exec and transactions keep full cancellation: the
// driver's exec path finalizes its statement in every case.
type safeDriver struct{ inner driver.Driver }

func (d safeDriver) Open(name string) (driver.Conn, error) {
	c, err := d.inner.Open(name)
	if err != nil {
		return nil, err
	}
	return &safeConn{inner: c}, nil
}

// innerConn is the set of interfaces the modernc connection implements and
// database/sql uses.
type innerConn interface {
	driver.Conn
	driver.ConnBeginTx
	driver.ConnPrepareContext
	driver.ExecerContext
	driver.QueryerContext
	driver.Pinger
	driver.SessionResetter
	driver.Validator
}

type safeConn struct{ inner driver.Conn }

var (
	_ innerConn   = (*safeConn)(nil)
	_ driver.Stmt = (*safeStmt)(nil)
)

var errUnsupported = errors.New("sqlitedb: driver connection lacks a required interface")

func (c *safeConn) full() (innerConn, error) {
	ic, ok := c.inner.(innerConn)
	if !ok {
		return nil, errUnsupported
	}
	return ic, nil
}

func (c *safeConn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}
func (c *safeConn) Close() error { return c.inner.Close() }
func (c *safeConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *safeConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	ic, err := c.full()
	if err != nil {
		return nil, err
	}
	return ic.BeginTx(ctx, opts)
}

func (c *safeConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	ic, err := c.full()
	if err != nil {
		return nil, err
	}
	s, err := ic.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &safeStmt{inner: s}, nil
}

func (c *safeConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	ic, err := c.full()
	if err != nil {
		return nil, err
	}
	return ic.ExecContext(ctx, query, args)
}

func (c *safeConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	ic, err := c.full()
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return ic.QueryContext(context.WithoutCancel(ctx), query, args)
}

func (c *safeConn) Ping(ctx context.Context) error {
	ic, err := c.full()
	if err != nil {
		return err
	}
	return ic.Ping(ctx)
}

func (c *safeConn) ResetSession(ctx context.Context) error {
	ic, err := c.full()
	if err != nil {
		return driver.ErrBadConn
	}
	return ic.ResetSession(ctx)
}

func (c *safeConn) IsValid() bool {
	ic, err := c.full()
	return err == nil && ic.IsValid()
}

type innerStmt interface {
	driver.Stmt
	driver.StmtExecContext
	driver.StmtQueryContext
}

type safeStmt struct{ inner driver.Stmt }

func (s *safeStmt) Close() error  { return s.inner.Close() }
func (s *safeStmt) NumInput() int { return s.inner.NumInput() }

//nolint:staticcheck // required by driver.Stmt; database/sql uses the Context variants
func (s *safeStmt) Exec(args []driver.Value) (driver.Result, error) { return s.inner.Exec(args) }

//nolint:staticcheck // required by driver.Stmt; database/sql uses the Context variants
func (s *safeStmt) Query(args []driver.Value) (driver.Rows, error) { return s.inner.Query(args) }

func (s *safeStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	is, ok := s.inner.(innerStmt)
	if !ok {
		return nil, errUnsupported
	}
	return is.ExecContext(ctx, args)
}

func (s *safeStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	is, ok := s.inner.(innerStmt)
	if !ok {
		return nil, errUnsupported
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return is.QueryContext(context.WithoutCancel(ctx), args)
}
