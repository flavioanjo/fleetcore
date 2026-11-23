package commandstore

import "github.com/flavioanjo/fleetcore/internal/sqlitedb"

// migrations is the schema history of the command database (commands.db).
//
// Conventions: *_ms columns are Unix milliseconds (the protocol's
// resolution, so stored times equal transmitted times); *_ns are Unix
// nanoseconds; command ids are 16-byte blobs.
var migrations = []sqlitedb.Migration{
	{
		Version: 1,
		Name:    "commands, attempts, history, reports",
		Stmts: []string{
			`CREATE TABLE commands (
				command_id         BLOB    PRIMARY KEY CHECK (length(command_id) = 16),
				-- Request order: assigned max(seq)+1 in the creating transaction
				-- (writes are serialized), so it is strictly increasing and stable
				-- (unlike a rowid, which VACUUM may renumber on this table).
				seq                INTEGER NOT NULL UNIQUE CHECK (seq >= 1),
				principal          TEXT    NOT NULL CHECK (length(principal) BETWEEN 1 AND 64),
				request_id         TEXT    NOT NULL CHECK (length(request_id) BETWEEN 1 AND 128),
				roles              TEXT    NOT NULL,
				justification      TEXT    NOT NULL,
				device_id          TEXT    NOT NULL CHECK (length(device_id) BETWEEN 1 AND 32),
				command_type       INTEGER NOT NULL CHECK (command_type BETWEEN 1 AND 4),
				args               BLOB    NOT NULL,
				issued_at_ms       INTEGER NOT NULL,
				expires_at_ms      INTEGER NOT NULL,
				result_deadline_ms INTEGER NOT NULL,
				authority_key_id   INTEGER NOT NULL CHECK (authority_key_id > 0),
				signature          BLOB    NOT NULL CHECK (length(signature) = 64),
				state              TEXT    NOT NULL CHECK (state IN ('requested', 'eligible', 'sent', 'received',
					'accepted', 'rejected', 'succeeded', 'failed', 'unknown', 'expired')),
				state_reason       TEXT    NOT NULL DEFAULT '',
				needs_attention    INTEGER NOT NULL DEFAULT 0 CHECK (needs_attention IN (0, 1)),
				device_stage       INTEGER NOT NULL DEFAULT 0,
				attempts           INTEGER NOT NULL DEFAULT 0,
				next_attempt_ms    INTEGER NOT NULL,
				first_sent_ms      INTEGER,
				last_sent_ms       INTEGER,
				received_at_ms     INTEGER,
				decided_at_ms      INTEGER,
				completed_at_ms    INTEGER,
				device_reason      INTEGER,
				observed           INTEGER,
				resolved_late      INTEGER NOT NULL DEFAULT 0 CHECK (resolved_late IN (0, 1)),
				created_at_ns      INTEGER NOT NULL,
				updated_at_ns      INTEGER NOT NULL,
				CHECK (expires_at_ms > issued_at_ms AND result_deadline_ms >= expires_at_ms),
				UNIQUE (principal, request_id)
			) STRICT`,
			`CREATE INDEX commands_by_state ON commands (state, next_attempt_ms)`,
			`CREATE INDEX commands_by_device ON commands (device_id, state)`,
			// The immutable fields can never change, whatever code path runs.
			`CREATE TRIGGER commands_identity_immutable BEFORE UPDATE OF command_id, principal, request_id, roles,
				justification, device_id, command_type, args, issued_at_ms, expires_at_ms, result_deadline_ms,
				authority_key_id, signature ON commands
			BEGIN
				SELECT RAISE(ABORT, 'fleetcore: command identity and authorization are immutable');
			END`,
			// One row per delivery attempt, written before the datagram is sent
			// ('sending') and completed after. An attempt left 'sending' (crash
			// between the two writes) may or may not have been transmitted.
			`CREATE TABLE command_attempts (
				command_id    BLOB    NOT NULL REFERENCES commands (command_id),
				attempt       INTEGER NOT NULL CHECK (attempt >= 1),
				started_at_ms INTEGER NOT NULL,
				outcome       TEXT    NOT NULL CHECK (outcome IN ('sending', 'sent', 'unreachable', 'error')),
				transport     TEXT    NOT NULL DEFAULT '',
				detail        TEXT    NOT NULL DEFAULT '',
				PRIMARY KEY (command_id, attempt)
			) STRICT`,
			// Append-only audit trail of every state transition.
			`CREATE TABLE command_history (
				id          INTEGER PRIMARY KEY,
				command_id  BLOB    NOT NULL REFERENCES commands (command_id),
				at_ns       INTEGER NOT NULL,
				from_state  TEXT    NOT NULL,
				to_state    TEXT    NOT NULL,
				source      TEXT    NOT NULL,
				detail      TEXT    NOT NULL
			) STRICT`,
			`CREATE INDEX command_history_by_command ON command_history (command_id, id)`,
			`CREATE TRIGGER command_history_no_update BEFORE UPDATE ON command_history
			BEGIN SELECT RAISE(ABORT, 'fleetcore: command history is append-only'); END`,
			`CREATE TRIGGER command_history_no_delete BEFORE DELETE ON command_history
			BEGIN SELECT RAISE(ABORT, 'fleetcore: command history is append-only'); END`,
			// Requests refused by authorization, for audit. They never become
			// commands and are never signed.
			`CREATE TABLE denied_requests (
				id           INTEGER PRIMARY KEY,
				principal    TEXT    NOT NULL,
				request_id   TEXT    NOT NULL,
				device_id    TEXT    NOT NULL,
				command_type INTEGER NOT NULL,
				reason       TEXT    NOT NULL,
				at_ns        INTEGER NOT NULL
			) STRICT`,
			// Every COMMAND_RESULT report applied (or refused), keyed by the
			// reliable event that carried it, so re-applying is a no-op.
			`CREATE TABLE command_reports (
				device_id      TEXT    NOT NULL,
				event_uid      INTEGER NOT NULL,
				rpos           INTEGER NOT NULL,
				command_id     BLOB    NOT NULL CHECK (length(command_id) = 16),
				stage          INTEGER NOT NULL,
				reason         INTEGER NOT NULL,
				observed       INTEGER NOT NULL,
				device_time_ms INTEGER NOT NULL,
				verdict        TEXT    NOT NULL CHECK (verdict IN ('applied', 'stale', 'unknown_command',
					'wrong_device', 'type_mismatch', 'unexpected')),
				applied_at_ns  INTEGER NOT NULL,
				PRIMARY KEY (device_id, event_uid)
			) STRICT`,
			`CREATE INDEX command_reports_by_command ON command_reports (command_id)`,
			// The latest state each device reported for its actuators
			// (BLOCK_STATE, CARGO_STATE). Device-reported, not independently
			// verified.
			`CREATE TABLE device_reported_state (
				device_id      TEXT    NOT NULL,
				kind           TEXT    NOT NULL CHECK (kind IN ('block', 'cargo')),
				state          INTEGER NOT NULL,
				latch          INTEGER NOT NULL,
				cause          INTEGER NOT NULL,
				command_id     BLOB,
				device_time_ms INTEGER NOT NULL,
				event_uid      INTEGER NOT NULL,
				rpos           INTEGER NOT NULL,
				PRIMARY KEY (device_id, kind)
			) STRICT`,
			// Position of the last reliable event consumed by the applier.
			`CREATE TABLE applier_cursor (
				id   INTEGER PRIMARY KEY CHECK (id = 1),
				rpos INTEGER NOT NULL CHECK (rpos >= 0)
			) STRICT`,
			`INSERT INTO applier_cursor (id, rpos) VALUES (1, 0)`,
		},
	},
}

// SchemaVersion is the command database schema version of this build.
func SchemaVersion() int { return migrations[len(migrations)-1].Version }
