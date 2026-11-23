package sqlitedb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"modernc.org/sqlite"
)

// queryGate controls fc_test_gate, a SQL function evaluated once per row of
// an aggregate over an effectively endless series: the query runs inside
// sqlite3_step without producing its first row until the gate aborts it.
var queryGate struct {
	mu      sync.Mutex
	entered chan struct{} // closed on the first evaluation
	abort   atomic.Bool   // makes the function fail, ending the query
}

func init() {
	sqlite.MustRegisterScalarFunction("fc_test_gate", 1, func(*sqlite.FunctionContext, []driver.Value) (driver.Value, error) {
		queryGate.mu.Lock()
		if queryGate.entered != nil {
			close(queryGate.entered)
			queryGate.entered = nil
		}
		queryGate.mu.Unlock()
		if queryGate.abort.Load() {
			return nil, errors.New("aborted by the test")
		}
		return int64(1), nil
	})
}

const endlessAggregate = `WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c LIMIT 1000000000000)
	SELECT count(*) FROM c WHERE fc_test_gate(x)`

// runGated starts the endless query on db with a 100 ms deadline and
// reports whether it returned within grace after the deadline. If it did
// not, the gate aborts it and the goroutine is joined: nothing is left
// running and no connection in use is closed.
func runGated(t *testing.T, db *sql.DB, grace time.Duration) (returnedInTime bool, err error) {
	t.Helper()
	entered := make(chan struct{})
	queryGate.mu.Lock()
	queryGate.entered = entered
	queryGate.mu.Unlock()
	queryGate.abort.Store(false)
	defer queryGate.abort.Store(false)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		var n int64
		done <- db.QueryRowContext(ctx, endlessAggregate).Scan(&n)
	}()
	select {
	case <-entered: // the query is executing, before its first row
	case err := <-done:
		t.Fatalf("query ended before executing: %v", err)
	}
	<-ctx.Done()
	select {
	case err := <-done:
		return true, err
	case <-time.After(grace):
	}
	queryGate.abort.Store(true)
	select {
	case err := <-done:
		return false, err
	case <-time.After(30 * time.Second):
		t.Fatal("query did not end after the gate aborted it")
		return false, nil
	}
}

// Characterization of a known limitation (docs/reliable.md ADR-R12, §9):
// the wrapper gives queries a context without cancellation, so a deadline
// that expires while a query is executing, before its first row, does not
// stop that query. The unwrapped driver interrupts it. busy_timeout plays no
// part here (no lock is waited for). If this test starts failing because
// the wrapper honours the deadline, the limitation is gone: update the
// documentation.
func TestQueryDeadlineDuringExecution(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.db")
	d, err := Open(context.Background(), "test", path, Options{}, []Migration{{Version: 1, Name: "t", Stmts: []string{`CREATE TABLE t (v INTEGER)`}}})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	// Unwrapped driver: the deadline interrupts the executing query.
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	inTime, err := runGated(t, raw, 5*time.Second)
	if !inTime || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("driver: returned in time=%v err=%v; want interrupted by the deadline", inTime, err)
	}

	// Wrapper: the same deadline does not stop it; it ends only when the
	// query itself ends (here: the gate aborts it).
	inTime, err = runGated(t, d.R, 2*time.Second)
	if inTime {
		t.Fatalf("wrapper honoured the deadline during execution (err=%v): the documented limitation no longer holds", err)
	}
	if err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wrapper: query ended with %v, want the gate's abort error", err)
	}

	// Both pools remain usable and nothing holds the database afterwards.
	var one int
	if err := d.R.QueryRow(`SELECT 1`).Scan(&one); err != nil {
		t.Fatal(err)
	}
	if _, err := d.W.Exec(`INSERT INTO t VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	var busy, frames, ckpt int
	if err := d.W.QueryRow(`PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &frames, &ckpt); err != nil || busy != 0 {
		t.Fatalf("checkpoint after the gated queries: busy=%d err=%v", busy, err)
	}
}
