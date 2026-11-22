package sqlitedb

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestOpenAppliesMigrationsOnce(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "x.db")
	ms := []Migration{{Version: 1, Name: "t", Stmts: []string{`CREATE TABLE t (x INTEGER) STRICT`}}}
	for i := 0; i < 2; i++ {
		db, err := Open(ctx, "test", path, Options{}, ms)
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		var mode string
		db.R.QueryRow(`PRAGMA journal_mode`).Scan(&mode)
		var n int
		db.R.QueryRow(`SELECT count(*) FROM schema_migrations`).Scan(&n)
		if mode != "wal" || n != 1 {
			t.Fatalf("open %d: journal_mode %q, %d migration records", i, mode, n)
		}
		if _, err := db.R.Exec(`INSERT INTO t VALUES (1)`); err == nil {
			t.Fatal("read pool accepted a write")
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := Open(ctx, "test", path, Options{}, nil); !errors.Is(err, ErrSchemaTooNew) {
		t.Fatalf("older build: %v, want ErrSchemaTooNew", err)
	}
}

func TestMigrationVersionsMustBeConsecutive(t *testing.T) {
	ms := []Migration{{Version: 1, Name: "a"}, {Version: 3, Name: "c"}}
	if _, err := Open(context.Background(), "test", filepath.Join(t.TempDir(), "x.db"), Options{}, ms); err == nil {
		t.Fatal("non-consecutive versions accepted")
	}
}
