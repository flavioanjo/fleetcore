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
	{
		// Protocol v2 (reliable communications). Additive only: version-1
		// tables, rows and readers are unchanged, so a v1-only deployment can
		// migrate at any time. See docs/reliable.md.
		Version: 2,
		Name:    "reliable events, conflicts and device sessions",
		Stmts: []string{
			// One row per (device_id, event_uid): the authoritative identity of
			// an event delivered over protocol v2. payload is the canonical
			// EventUpload payload; the fingerprint is reliable.EventFingerprint
			// of it. TELEMETRY events are also stored in events (event_position)
			// so the chronological processor and fusion consume them exactly as
			// version-1 events.
			`CREATE TABLE reliable_events (
				rpos           INTEGER PRIMARY KEY AUTOINCREMENT,
				device_id      TEXT    NOT NULL CHECK (length(device_id) BETWEEN 1 AND 32),
				event_uid      INTEGER NOT NULL CHECK (event_uid > 0),
				event_type     INTEGER NOT NULL CHECK (event_type BETWEEN 1 AND 8),
				class          INTEGER NOT NULL CHECK (class BETWEEN 1 AND 5),
				device_time_ms INTEGER NOT NULL,
				fingerprint    BLOB    NOT NULL CHECK (length(fingerprint) = 32),
				payload        BLOB    NOT NULL CHECK (length(payload) BETWEEN 19 AND 512),
				event_position INTEGER REFERENCES events (position),
				key_id         INTEGER NOT NULL CHECK (key_id > 0),
				session_gen    INTEGER NOT NULL CHECK (session_gen > 0),
				received_at_ns INTEGER NOT NULL,
				stored_at_ns   INTEGER NOT NULL,
				gateway        TEXT    NOT NULL,
				CHECK ((event_type = 1) = (event_position IS NOT NULL)),
				UNIQUE (device_id, event_uid)
			) STRICT`,
			`CREATE INDEX reliable_events_by_class ON reliable_events (class, rpos)`,
			`CREATE TRIGGER reliable_events_immutable BEFORE UPDATE ON reliable_events
			BEGIN
				SELECT RAISE(ABORT, 'fleetcore: stored reliable events are immutable');
			END`,
			// A payload that reused a stored (device_id, event_uid), or a
			// TELEMETRY event whose v1 identity is stored with another payload.
			// It is preserved here and never overwrites the stored event.
			`CREATE TABLE reliable_conflicts (
				id            INTEGER PRIMARY KEY,
				device_id     TEXT    NOT NULL,
				event_uid     INTEGER NOT NULL,
				fingerprint   BLOB    NOT NULL CHECK (length(fingerprint) = 32),
				payload       BLOB    NOT NULL,
				reason        TEXT    NOT NULL CHECK (reason IN ('uid_reused', 'telemetry_identity_conflict')),
				first_seen_ns INTEGER NOT NULL,
				last_seen_ns  INTEGER NOT NULL,
				receipts      INTEGER NOT NULL CHECK (receipts >= 1),
				UNIQUE (device_id, event_uid, fingerprint)
			) STRICT`,
			// The latest authenticated session of each device, shared by every
			// gateway: session_gen only increases, so frames of older sessions
			// are rejected as replays by any gateway, across restarts.
			`CREATE TABLE device_sessions (
				device_id     TEXT    PRIMARY KEY CHECK (length(device_id) BETWEEN 1 AND 32),
				key_id        INTEGER NOT NULL CHECK (key_id > 0),
				session_gen   INTEGER NOT NULL CHECK (session_gen > 0),
				first_seen_ns INTEGER NOT NULL,
				last_seen_ns  INTEGER NOT NULL,
				last_gateway  TEXT    NOT NULL
			) STRICT`,
		},
	},
}

// SchemaVersion is the schema version this build creates and understands.
func SchemaVersion() int { return migrations[len(migrations)-1].Version }
