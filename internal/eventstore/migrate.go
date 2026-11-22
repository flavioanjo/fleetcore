package eventstore

import "github.com/flavioanjo/fleetcore/internal/sqlitedb"

// migrations is the complete schema history of the FleetCore event store.
//
// Storage conventions:
//   - event_id is a uint64 stored as the int64 with the same bit pattern
//     (SQLite integers are signed 64-bit); it is only compared for equality.
//   - device_time_ms is Unix milliseconds (UTC); *_ns columns are Unix
//     nanoseconds, which cover the domain's [2000, 2100) timestamp range.
//   - packet is the canonical version-1 encoding and the source of truth
//     when reading; the decoded columns exist for SQL queries and constraints.
var migrations = []sqlitedb.Migration{
	{
		Version: 1,
		Name:    "events and conflicts",
		Stmts: []string{
			`CREATE TABLE events (
				position       INTEGER PRIMARY KEY AUTOINCREMENT,
				device_id      TEXT    NOT NULL CHECK (length(device_id) BETWEEN 1 AND 32),
				event_id       INTEGER NOT NULL CHECK (event_id <> 0),
				boot_id        INTEGER NOT NULL CHECK (boot_id BETWEEN 1 AND 4294967295),
				sequence       INTEGER NOT NULL CHECK (sequence BETWEEN 0 AND 4294967295),
				device_time_ms INTEGER NOT NULL,
				ingested_at_ns INTEGER NOT NULL,
				source         INTEGER NOT NULL CHECK (source IN (1, 2, 3)),
				quality        INTEGER NOT NULL CHECK (quality IN (1, 2, 3)),
				distance_kind  INTEGER CHECK (distance_kind IN (1, 2)),
				distance_mm    INTEGER,
				fingerprint    BLOB    NOT NULL CHECK (length(fingerprint) = 32),
				packet         BLOB    NOT NULL CHECK (length(packet) BETWEEN 49 AND 512),
				stored_at_ns   INTEGER NOT NULL,
				CHECK ((distance_kind IS NULL) = (distance_mm IS NULL)),
				CHECK ((quality = 3) = (distance_kind IS NULL)),
				UNIQUE (device_id, boot_id, event_id)
			) STRICT`,
			`CREATE INDEX events_by_session ON events (device_id, boot_id, sequence)`,
			// Finds EventIDs reused across boots (domain contract violations).
			`CREATE INDEX events_by_event_id ON events (device_id, event_id)`,
			// Stored events are immutable: a conflicting payload can never
			// overwrite the first one, whatever code path issues the UPDATE.
			`CREATE TRIGGER events_immutable BEFORE UPDATE ON events
			BEGIN
				SELECT RAISE(ABORT, 'fleetcore: stored events are immutable');
			END`,
			`CREATE TABLE event_conflicts (
				id            INTEGER PRIMARY KEY,
				device_id     TEXT    NOT NULL,
				boot_id       INTEGER NOT NULL,
				event_id      INTEGER NOT NULL,
				fingerprint   BLOB    NOT NULL CHECK (length(fingerprint) = 32),
				packet        BLOB    NOT NULL,
				first_seen_ns INTEGER NOT NULL,
				last_seen_ns  INTEGER NOT NULL,
				receipts      INTEGER NOT NULL CHECK (receipts >= 1),
				UNIQUE (device_id, boot_id, event_id, fingerprint),
				FOREIGN KEY (device_id, boot_id, event_id) REFERENCES events (device_id, boot_id, event_id)
			) STRICT`,
		},
	},
}

// SchemaVersion is the schema version this build creates and understands.
func SchemaVersion() int { return migrations[len(migrations)-1].Version }
