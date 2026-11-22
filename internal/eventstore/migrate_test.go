package eventstore

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/flavioanjo/fleetcore/internal/sqlitedb"
)

// withMigrations temporarily extends the schema history (tests in this
// package do not run in parallel).
func withMigrations(t *testing.T, extra ...sqlitedb.Migration) {
	t.Helper()
	saved := migrations
	migrations = append(append([]sqlitedb.Migration(nil), saved...), extra...)
	t.Cleanup(func() { migrations = saved })
}

func schemaVersion(t *testing.T, st *Store) int {
	t.Helper()
	var v int
	if err := st.r.QueryRow(`SELECT max(version) FROM schema_migrations`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestMigrationUpgradePreservesData(t *testing.T) {
	path := tempDB(t)
	st := openStore(t, path)
	mustAppend(t, st, event(t, sample("dev-1", 1, 7, 1, 100)), Inserted)
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	withMigrations(t, sqlitedb.Migration{Version: 2, Name: "test: add annotation", Stmts: []string{
		`ALTER TABLE events ADD COLUMN annotation TEXT`,
		`CREATE TABLE annotations_meta (k TEXT PRIMARY KEY) STRICT`,
	}})
	st2 := openStore(t, path)
	if v := schemaVersion(t, st2); v != 2 {
		t.Fatalf("schema version %d after upgrade, want 2", v)
	}
	if _, err := st2.r.Exec(`SELECT annotation FROM events`); err != nil {
		t.Fatalf("migration 2 not applied: %v", err)
	}
	// Existing data and its uniqueness survive the upgrade.
	mustAppend(t, st2, event(t, sample("dev-1", 1, 7, 1, 100)), Duplicate)
	mustAppend(t, st2, event(t, sample("dev-1", 2, 7, 2, 100)), Inserted)
	var names []string
	rows, _ := st2.r.Query(`SELECT name FROM schema_migrations ORDER BY version`)
	for rows.Next() {
		var n string
		rows.Scan(&n)
		names = append(names, n)
	}
	rows.Close()
	if len(names) != 2 {
		t.Fatalf("migration history %v", names)
	}
}

func TestFailedMigrationRollsBack(t *testing.T) {
	path := tempDB(t)
	st := openStore(t, path)
	st.Close()

	withMigrations(t, sqlitedb.Migration{Version: 2, Name: "test: broken", Stmts: []string{
		`CREATE TABLE partial (x INTEGER) STRICT`,
		`THIS IS NOT SQL`,
	}})
	if _, err := Open(context.Background(), path, Options{}); err == nil {
		t.Fatal("broken migration succeeded")
	}
	migrations = migrations[:len(migrations)-1] // back to the real history
	st2 := openStore(t, path)
	if v := schemaVersion(t, st2); v != 1 {
		t.Fatalf("schema version %d after failed migration, want 1", v)
	}
	var n int
	st2.r.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE name = 'partial'`).Scan(&n)
	if n != 0 {
		t.Fatal("first statement of the failed migration was not rolled back")
	}
}

func TestSchemaTooNewIsRefused(t *testing.T) {
	path := tempDB(t)
	st := openStore(t, path)
	if _, err := st.w.Exec(`INSERT INTO schema_migrations VALUES (999, 'future', 0)`); err != nil {
		t.Fatal(err)
	}
	st.Close()
	if _, err := Open(context.Background(), path, Options{}); !errors.Is(err, ErrSchemaTooNew) {
		t.Fatalf("open of newer schema: %v, want ErrSchemaTooNew", err)
	}
}

func TestConcurrentOpenMigratesOnce(t *testing.T) {
	path := tempDB(t)
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			st, err := Open(context.Background(), path, Options{})
			if err != nil {
				errs <- err
				return
			}
			errs <- st.Close()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	st := openStore(t, path)
	var n int
	st.r.QueryRow(`SELECT count(*) FROM schema_migrations`).Scan(&n)
	if n != len(migrations) {
		t.Fatalf("%d migration records, want %d", n, len(migrations))
	}
}
