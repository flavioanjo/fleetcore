package eventstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/flavioanjo/fleetcore/internal/reliable"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// # Reliable (protocol v2) events
//
// The durable identity of a v2 event is (DeviceID, UID); its content
// identity is reliable.EventFingerprint over the canonical payload. The
// classification mirrors version 1:
//
//   - identity unknown                      → Inserted
//   - identity known, fingerprint equal     → Duplicate (no write)
//   - identity known, fingerprint different → Conflict (stored event kept;
//     payload preserved in reliable_conflicts)
//
// A TELEMETRY event is additionally stored in the version-1 events table,
// in the same transaction, so the chronological processor consumes it
// once. If its v1 identity (DeviceID, BootID, EventID) is already stored
// with an identical payload (sent earlier over v1), the existing row is
// linked and nothing is inserted: the sample is applied downstream once. If
// it is stored with a different payload, the event is a Conflict and no
// reliable row is written.
//
// A gateway sends a success ACK only for Inserted or Duplicate, and only
// after AppendReliable returned, i.e. after the transaction committed.

// ReliableEvent is a validated v2 event ready to be stored. Build it with
// NewReliableEvent.
type ReliableEvent struct {
	DeviceID    string
	Upload      reliable.EventUpload
	Payload     []byte // canonical encoding of Upload
	Fingerprint reliable.Fingerprint
	Body        reliable.Body
	// Telemetry is the version-1 event of a TELEMETRY upload, else nil.
	Telemetry  *Event
	KeyID      uint32
	SessionGen uint64
	ReceivedAt time.Time
	Gateway    string
}

// NewReliableEvent validates an authenticated upload of deviceID and
// prepares it for storage. The error, if any, is a *reliable.Error with
// CodeInvalidEvent (the gateway answers it with a signed AckInvalid).
func NewReliableEvent(deviceID string, up reliable.EventUpload, keyID uint32, sessionGen uint64, receivedAt time.Time, gateway string) (ReliableEvent, error) {
	payload, err := up.Encode()
	if err != nil {
		return ReliableEvent{}, err
	}
	body, err := reliable.DecodeBody(deviceID, up)
	if err != nil {
		return ReliableEvent{}, err
	}
	re := ReliableEvent{
		DeviceID: deviceID, Upload: up, Payload: payload, Body: body,
		Fingerprint: reliable.EventFingerprint(deviceID, payload),
		KeyID:       keyID, SessionGen: sessionGen, ReceivedAt: receivedAt.UTC(), Gateway: gateway,
	}
	if t, ok := body.(reliable.Telemetry); ok {
		s := t.Sample
		s.IngestedAt = telemetry.Some(re.ReceivedAt)
		ev, err := NewEvent(s)
		if err != nil {
			return ReliableEvent{}, &reliable.Error{Code: reliable.CodeInvalidEvent, Err: err}
		}
		re.Telemetry = &ev
	}
	return re, nil
}

// ReliableResult describes what happened to one appended v2 event.
type ReliableResult struct {
	Outcome Outcome
	// Position is the reliable-event position (rpos) of the stored event
	// with this identity; 0 for a telemetry identity conflict (nothing was
	// stored under this UID).
	Position int64
	// EventPosition is the version-1 events row of a TELEMETRY event.
	EventPosition int64
	// Existing is the fingerprint stored under (DeviceID, UID), for
	// Duplicate and uid conflicts.
	Existing reliable.Fingerprint
	// ConflictReason is set for Conflict.
	ConflictReason string
}

// Conflict reasons.
const (
	ConflictUIDReused         = "uid_reused"
	ConflictTelemetryIdentity = "telemetry_identity_conflict"
)

// AppendReliable classifies and stores v2 events in one transaction:
// either every result is durable when it returns nil, or nothing was
// written.
func (s *Store) AppendReliable(ctx context.Context, evs []ReliableEvent) ([]ReliableResult, error) {
	if len(evs) == 0 {
		return nil, nil
	}
	release, err := s.acquire()
	if err != nil {
		return nil, err
	}
	defer release()
	for i := range evs {
		if err := checkReliable(evs[i]); err != nil {
			return nil, fmt.Errorf("eventstore: reliable event %d (%s/%d): %w", i, evs[i].DeviceID, evs[i].Upload.UID, err)
		}
	}
	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("eventstore: begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck
	now := s.now().UnixNano()
	a, err := newAppender(ctx, tx, now)
	if err != nil {
		return nil, err
	}
	defer a.close()

	out := make([]ReliableResult, len(evs))
	for i, ev := range evs {
		if out[i], err = appendReliable(ctx, tx, a, ev, now); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("eventstore: commit: %w", err)
	}
	return out, nil
}

func appendReliable(ctx context.Context, tx *sql.Tx, a *appender, ev ReliableEvent, now int64) (ReliableResult, error) {
	var rpos int64
	var fp []byte
	var evPos sql.NullInt64
	err := tx.QueryRowContext(ctx, `SELECT rpos, fingerprint, event_position FROM reliable_events
		WHERE device_id = ? AND event_uid = ?`, ev.DeviceID, int64(ev.Upload.UID)).Scan(&rpos, &fp, &evPos)
	switch {
	case err == nil:
		var existing reliable.Fingerprint
		copy(existing[:], fp)
		if existing == ev.Fingerprint {
			return ReliableResult{Outcome: Duplicate, Position: rpos, EventPosition: evPos.Int64, Existing: existing}, nil
		}
		if err := recordReliableConflict(ctx, tx, ev, ConflictUIDReused); err != nil {
			return ReliableResult{}, err
		}
		return ReliableResult{Outcome: Conflict, Position: rpos, Existing: existing, ConflictReason: ConflictUIDReused}, nil
	case !errors.Is(err, sql.ErrNoRows):
		return ReliableResult{}, fmt.Errorf("eventstore: reliable lookup: %w", err)
	}

	var link sql.NullInt64
	if ev.Telemetry != nil {
		r, err := a.append(ctx, *ev.Telemetry)
		if err != nil {
			return ReliableResult{}, err
		}
		if r.Outcome == Conflict {
			if err := recordReliableConflict(ctx, tx, ev, ConflictTelemetryIdentity); err != nil {
				return ReliableResult{}, err
			}
			return ReliableResult{Outcome: Conflict, ConflictReason: ConflictTelemetryIdentity}, nil
		}
		link = sql.NullInt64{Int64: r.Position, Valid: true}
	}
	err = tx.QueryRowContext(ctx, `INSERT INTO reliable_events
		(device_id, event_uid, event_type, class, device_time_ms, fingerprint, payload, event_position,
		 key_id, session_gen, received_at_ns, stored_at_ns, gateway)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING rpos`,
		ev.DeviceID, int64(ev.Upload.UID), int64(ev.Upload.Type), int64(ev.Upload.Type.Class()),
		ev.Upload.DeviceTime.UnixMilli(), ev.Fingerprint[:], ev.Payload, link,
		int64(ev.KeyID), int64(ev.SessionGen), ev.ReceivedAt.UnixNano(), now, ev.Gateway).Scan(&rpos)
	if err != nil {
		return ReliableResult{}, fmt.Errorf("eventstore: insert reliable event: %w", err)
	}
	return ReliableResult{Outcome: Inserted, Position: rpos, EventPosition: link.Int64, Existing: ev.Fingerprint}, nil
}

func recordReliableConflict(ctx context.Context, tx *sql.Tx, ev ReliableEvent, reason string) error {
	at := ev.ReceivedAt.UnixNano()
	_, err := tx.ExecContext(ctx, `INSERT INTO reliable_conflicts
		(device_id, event_uid, fingerprint, payload, reason, first_seen_ns, last_seen_ns, receipts)
		VALUES (?, ?, ?, ?, ?, ?, ?, 1)
		ON CONFLICT (device_id, event_uid, fingerprint) DO UPDATE SET
			receipts      = receipts + 1,
			first_seen_ns = min(first_seen_ns, excluded.first_seen_ns),
			last_seen_ns  = max(last_seen_ns, excluded.last_seen_ns)`,
		ev.DeviceID, int64(ev.Upload.UID), ev.Fingerprint[:], ev.Payload, reason, at, at)
	if err != nil {
		return fmt.Errorf("eventstore: record reliable conflict: %w", err)
	}
	return nil
}

// checkReliable rejects events not built by NewReliableEvent (or altered).
func checkReliable(ev ReliableEvent) error {
	p, err := ev.Upload.Encode()
	if err != nil {
		return err
	}
	if string(p) != string(ev.Payload) || reliable.EventFingerprint(ev.DeviceID, p) != ev.Fingerprint {
		return errors.New("payload or fingerprint does not match the upload")
	}
	if (ev.Upload.Type == reliable.EventTelemetry) != (ev.Telemetry != nil) {
		return errors.New("telemetry event without its v1 event (or vice versa)")
	}
	if ev.Telemetry != nil {
		if err := checkEvent(*ev.Telemetry); err != nil {
			return err
		}
	}
	if ev.KeyID == 0 || ev.SessionGen == 0 || ev.SessionGen > reliable.MaxIdentity || ev.Gateway == "" {
		return errors.New("missing envelope metadata")
	}
	return nil
}

// ReliableRecord is a stored v2 event.
type ReliableRecord struct {
	Position      int64
	DeviceID      string
	Upload        reliable.EventUpload
	Fingerprint   reliable.Fingerprint
	Class         reliable.Class
	EventPosition int64 // TELEMETRY: the v1 events row
	KeyID         uint32
	SessionGen    uint64
	ReceivedAt    time.Time // first receipt
	StoredAt      time.Time
	Gateway       string // gateway that stored it
}

// Body decodes the record's typed body.
func (r ReliableRecord) Body() (reliable.Body, error) {
	return reliable.DecodeBody(r.DeviceID, r.Upload)
}

const reliableCols = `rpos, device_id, payload, fingerprint, class, event_position, key_id, session_gen, received_at_ns, stored_at_ns, gateway`

func scanReliable(row scanner) (ReliableRecord, error) {
	var r ReliableRecord
	var payload, fp []byte
	var class, keyID, gen, recv, stored int64
	var evPos sql.NullInt64
	if err := row.Scan(&r.Position, &r.DeviceID, &payload, &fp, &class, &evPos, &keyID, &gen, &recv, &stored, &r.Gateway); err != nil {
		return r, err
	}
	up, err := reliable.DecodeEventUpload(payload)
	if err != nil {
		return r, fmt.Errorf("%w: reliable event %d: %v", ErrCorrupt, r.Position, err)
	}
	copy(r.Fingerprint[:], fp)
	if reliable.EventFingerprint(r.DeviceID, payload) != r.Fingerprint {
		return r, fmt.Errorf("%w: reliable event %d fingerprint", ErrCorrupt, r.Position)
	}
	r.Upload, r.Class, r.EventPosition = up, reliable.Class(class), evPos.Int64
	r.KeyID, r.SessionGen = uint32(keyID), uint64(gen)
	r.ReceivedAt, r.StoredAt = time.Unix(0, recv).UTC(), time.Unix(0, stored).UTC()
	return r, nil
}

// ReadReliableAfter returns up to limit v2 events with position > after,
// in position order, optionally restricted to the given types.
func (s *Store) ReadReliableAfter(ctx context.Context, after int64, limit int, types ...reliable.EventType) ([]ReliableRecord, error) {
	release, err := s.acquire()
	if err != nil {
		return nil, err
	}
	defer release()
	q := `SELECT ` + reliableCols + ` FROM reliable_events WHERE rpos > ?`
	args := []any{after}
	if len(types) > 0 {
		q += ` AND event_type IN (?` + strings.Repeat(`, ?`, len(types)-1) + `)`
		for _, t := range types {
			args = append(args, int64(t))
		}
	}
	q += ` ORDER BY rpos LIMIT ?`
	args = append(args, limit)
	rows, err := s.r.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("eventstore: read reliable: %w", err)
	}
	defer rows.Close()
	var out []ReliableRecord
	for rows.Next() {
		r, err := scanReliable(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// GetReliable returns the stored v2 event (deviceID, uid), or ErrNotFound.
func (s *Store) GetReliable(ctx context.Context, deviceID string, uid uint64) (ReliableRecord, error) {
	release, err := s.acquire()
	if err != nil {
		return ReliableRecord{}, err
	}
	defer release()
	r, err := scanReliable(s.r.QueryRowContext(ctx, `SELECT `+reliableCols+` FROM reliable_events
		WHERE device_id = ? AND event_uid = ?`, deviceID, int64(uid)))
	if errors.Is(err, sql.ErrNoRows) {
		return r, ErrNotFound
	}
	return r, err
}

// ReliableConflict is a preserved conflicting v2 payload.
type ReliableConflict struct {
	DeviceID    string
	UID         uint64
	Fingerprint reliable.Fingerprint
	Payload     []byte
	Reason      string
	Receipts    int64
}

// ReliableConflicts returns the conflicting payloads recorded for a device
// (all UIDs when uid is 0).
func (s *Store) ReliableConflicts(ctx context.Context, deviceID string, uid uint64) ([]ReliableConflict, error) {
	release, err := s.acquire()
	if err != nil {
		return nil, err
	}
	defer release()
	rows, err := s.r.QueryContext(ctx, `SELECT event_uid, fingerprint, payload, reason, receipts FROM reliable_conflicts
		WHERE device_id = ? AND (? = 0 OR event_uid = ?) ORDER BY id`, deviceID, int64(uid), int64(uid))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ReliableConflict
	for rows.Next() {
		c := ReliableConflict{DeviceID: deviceID}
		var u int64
		var fp []byte
		if err := rows.Scan(&u, &fp, &c.Payload, &c.Reason, &c.Receipts); err != nil {
			return nil, err
		}
		c.UID = uint64(u)
		copy(c.Fingerprint[:], fp)
		out = append(out, c)
	}
	return out, rows.Err()
}

// CountReliable returns the number of stored v2 events per type.
func (s *Store) CountReliable(ctx context.Context) (map[reliable.EventType]int64, error) {
	release, err := s.acquire()
	if err != nil {
		return nil, err
	}
	defer release()
	rows, err := s.r.QueryContext(ctx, `SELECT event_type, count(*) FROM reliable_events GROUP BY event_type`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[reliable.EventType]int64{}
	for rows.Next() {
		var t, n int64
		if err := rows.Scan(&t, &n); err != nil {
			return nil, err
		}
		out[reliable.EventType(t)] = n
	}
	return out, rows.Err()
}

// SessionState is a device's latest authenticated session.
type SessionState struct {
	KeyID      uint32
	SessionGen uint64
	LastSeen   time.Time
	Gateway    string
}

// Session verdicts.
var (
	// ErrStaleSession: the device has already been seen in a newer session
	// (a replay of an old session's frame, or a device that lost its
	// session counter).
	ErrStaleSession = errors.New("eventstore: session older than the device's latest")
	// ErrSessionKeyMismatch: same session generation, different key
	// version; a device starts a new session when it changes key.
	ErrSessionKeyMismatch = errors.New("eventstore: session generation reused with another key")
)

// ObserveSession records an authenticated frame's session for the device
// and returns the device's session state. It never lowers the stored
// generation: a frame of an older generation yields ErrStaleSession (and
// the current state), so all gateways sharing this store reject it.
func (s *Store) ObserveSession(ctx context.Context, deviceID string, keyID uint32, gen uint64, at time.Time, gateway string) (SessionState, error) {
	if gen == 0 || gen > reliable.MaxIdentity || keyID == 0 {
		return SessionState{}, errors.New("eventstore: invalid session")
	}
	release, err := s.acquire()
	if err != nil {
		return SessionState{}, err
	}
	defer release()
	ns := at.UTC().UnixNano()
	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return SessionState{}, fmt.Errorf("eventstore: begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck
	if _, err := tx.ExecContext(ctx, `INSERT INTO device_sessions (device_id, key_id, session_gen, first_seen_ns, last_seen_ns, last_gateway)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (device_id) DO UPDATE SET
			key_id = excluded.key_id, session_gen = excluded.session_gen,
			first_seen_ns = excluded.first_seen_ns, last_seen_ns = excluded.last_seen_ns, last_gateway = excluded.last_gateway
		WHERE excluded.session_gen > device_sessions.session_gen`,
		deviceID, int64(keyID), int64(gen), ns, ns, gateway); err != nil {
		return SessionState{}, fmt.Errorf("eventstore: observe session: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE device_sessions SET last_seen_ns = max(last_seen_ns, ?), last_gateway = ?
		WHERE device_id = ? AND session_gen = ? AND key_id = ?`, ns, gateway, deviceID, int64(gen), int64(keyID)); err != nil {
		return SessionState{}, fmt.Errorf("eventstore: observe session: %w", err)
	}
	st, err := scanSession(tx.QueryRowContext(ctx, `SELECT key_id, session_gen, last_seen_ns, last_gateway FROM device_sessions WHERE device_id = ?`, deviceID))
	if err != nil {
		return SessionState{}, err
	}
	if err := tx.Commit(); err != nil {
		return SessionState{}, fmt.Errorf("eventstore: commit: %w", err)
	}
	switch {
	case st.SessionGen > gen:
		return st, ErrStaleSession
	case st.KeyID != keyID:
		return st, ErrSessionKeyMismatch
	}
	return st, nil
}

// DeviceSession returns the device's latest session, or ErrNotFound.
func (s *Store) DeviceSession(ctx context.Context, deviceID string) (SessionState, error) {
	release, err := s.acquire()
	if err != nil {
		return SessionState{}, err
	}
	defer release()
	st, err := scanSession(s.r.QueryRowContext(ctx, `SELECT key_id, session_gen, last_seen_ns, last_gateway FROM device_sessions WHERE device_id = ?`, deviceID))
	if errors.Is(err, sql.ErrNoRows) {
		return st, ErrNotFound
	}
	return st, err
}

func scanSession(row scanner) (SessionState, error) {
	var st SessionState
	var k, g, seen int64
	if err := row.Scan(&k, &g, &seen, &st.Gateway); err != nil {
		return st, err
	}
	st.KeyID, st.SessionGen, st.LastSeen = uint32(k), uint64(g), time.Unix(0, seen).UTC()
	return st, nil
}
