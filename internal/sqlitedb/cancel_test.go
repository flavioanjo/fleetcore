package sqlitedb

import (
	"context"
	"database/sql/driver"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"modernc.org/sqlite"
)

// cancelDuringStep is a SQL function used by the tests below: it cancels
// the context of the query evaluating it, then gives the driver's
// interrupt goroutine time to observe the cancellation, so the cancellation
// lands while sqlite3_step is producing the row.
var cancelDuringStep struct {
	sync.Mutex
	cancel context.CancelFunc
}

func init() {
	sqlite.MustRegisterScalarFunction("fc_test_cancel", 0, func(*sqlite.FunctionContext, []driver.Value) (driver.Value, error) {
		cancelDuringStep.Lock()
		c := cancelDuringStep.cancel
		cancelDuringStep.Unlock()
		if c != nil {
			c()
			// Not a correctness timeout: it only lets the driver's
			// context-watching goroutine run before the row is returned.
			time.Sleep(20 * time.Millisecond)
		}
		return int64(1), nil
	})
}

// Regression: modernc.org/sqlite v1.38.2 discards (without finalizing) the
// rows of a query whose context is cancelled while it steps to the first
// row. The interrupted connection is then dropped by database/sql and
// closed with sqlite3_close_v2, which turns it into a zombie that keeps the
// database file and a read snapshot open forever: Close returns nil, the
// WAL can no longer be checkpointed and, on Windows, the file cannot be
// deleted. Open's driver wrapper runs queries without the cancellation so
// this cannot happen; cancellation still stops iteration between rows.
func TestCancelledQueryReleasesConnection(t *testing.T) {
	for i := 0; i < 10; i++ {
		dir := t.TempDir()
		path := filepath.Join(dir, "x.db")
		d, err := Open(context.Background(), "test", path, Options{BusyTimeout: 200 * time.Millisecond},
			[]Migration{{Version: 1, Name: "t", Stmts: []string{`CREATE TABLE t (v INTEGER)`, `INSERT INTO t VALUES (1)`}}})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancelDuringStep.Lock()
		cancelDuringStep.cancel = cancel
		cancelDuringStep.Unlock()
		rows, err := d.R.QueryContext(ctx, `SELECT fc_test_cancel() FROM t`)
		cancelDuringStep.Lock()
		cancelDuringStep.cancel = nil
		cancelDuringStep.Unlock()
		if err == nil {
			for rows.Next() {
			}
			rows.Close()
		} else if !errors.Is(err, context.Canceled) {
			t.Fatalf("query: %v", err)
		}
		cancel()

		// A writer appends to the WAL; a TRUNCATE checkpoint must then be
		// able to reset it: no connection may still hold a read snapshot.
		if _, err := d.W.Exec(`INSERT INTO t VALUES (2)`); err != nil {
			t.Fatal(err)
		}
		var busy, logFrames, ckpt int
		if err := d.W.QueryRow(`PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &logFrames, &ckpt); err != nil {
			t.Fatal(err)
		}
		if busy != 0 {
			t.Fatalf("run %d: checkpoint blocked by a leaked reader (busy=%d)", i, busy)
		}
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
		if runtime.GOOS == "windows" {
			if err := os.Remove(path); err != nil {
				t.Fatalf("run %d: database file still open after Close: %v", i, err)
			}
		}
	}
}

// Cancellation still works for queries: a cancelled context is refused
// before the query starts, and stops iteration of an open result set.
func TestQueryCancellationStillObserved(t *testing.T) {
	d, err := Open(context.Background(), "test", filepath.Join(t.TempDir(), "x.db"), Options{},
		[]Migration{{Version: 1, Name: "t", Stmts: []string{`CREATE TABLE t (v INTEGER)`}}})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := d.R.QueryContext(ctx, `SELECT 1`); !errors.Is(err, context.Canceled) {
		t.Fatalf("query with a cancelled context: %v", err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	rows, err := d.R.QueryContext(ctx, `WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c LIMIT 100000) SELECT x FROM c`)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for rows.Next() {
		if n++; n == 10 {
			cancel()
		}
	}
	if err := rows.Err(); !errors.Is(err, context.Canceled) || n >= 100000 {
		t.Fatalf("iteration after cancel: n=%d err=%v", n, err)
	}
	rows.Close()
	cancel()
	// The pool is still usable.
	var one int
	if err := d.R.QueryRow(`SELECT 1`).Scan(&one); err != nil || one != 1 {
		t.Fatalf("pool after cancellation: %d %v", one, err)
	}
}
