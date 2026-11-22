package commandstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/flavioanjo/fleetcore/internal/reliable"
)

// update applies a state transition (or, with to == c.State, only column
// changes) guarded by the state the caller read, and records it in the
// history. It fails with ErrInvalidTransition if the transition is not
// allowed or the command changed concurrently.
func update(ctx context.Context, tx *sql.Tx, c Command, to State, source, detail string, now time.Time, set map[string]any) error {
	if to != c.State && !canMove(c.State, to) {
		return fmt.Errorf("%w: %s → %s", ErrInvalidTransition, c.State, to)
	}
	cols := []string{`state = ?`, `updated_at_ns = ?`}
	args := []any{string(to), now.UnixNano()}
	for k, v := range set {
		cols = append(cols, k+` = ?`)
		args = append(args, v)
	}
	args = append(args, c.ID[:], string(c.State))
	res, err := tx.ExecContext(ctx, `UPDATE commands SET `+strings.Join(cols, `, `)+` WHERE command_id = ? AND state = ?`, args...)
	if err != nil {
		return fmt.Errorf("commandstore: update: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("%w: %s changed concurrently", ErrInvalidTransition, c.ID)
	}
	if to != c.State {
		return history(ctx, tx, c.ID, c.State, to, source, detail, now)
	}
	return nil
}

func inList(states []State) (string, []any) {
	args := make([]any, len(states))
	for i, s := range states {
		args[i] = string(s)
	}
	return `(?` + strings.Repeat(`, ?`, len(states)-1) + `)`, args
}

// Promote makes requested commands eligible for dispatch. A command is
// eligible when it has not expired and, if it actuates, no other actuating
// command of the same device is in flight (actuations of one device are
// serialized in request order, so BLOCK and UNBLOCK can never race).
// Non-actuating commands (REQUEST_POSITION) are always eligible.
func (s *Store) Promote(ctx context.Context) (int, error) {
	release, err := s.acquire()
	if err != nil {
		return 0, err
	}
	defer release()
	now := s.now().UTC()
	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck
	in, args := inList(inFlight)
	busy := map[string]bool{}
	rows, err := tx.QueryContext(ctx, `SELECT DISTINCT device_id FROM commands WHERE state IN `+in+` AND command_type IN (1, 2, 3)`, args...)
	if err != nil {
		return 0, err
	}
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			rows.Close()
			return 0, err
		}
		busy[d] = true
	}
	rows.Close()
	cands, err := listQ(ctx, tx, `SELECT `+cmdCols+` FROM commands WHERE state = ? AND expires_at_ms > ? ORDER BY created_at_ns, command_id`,
		string(Requested), ms(now))
	if err != nil {
		return 0, err
	}
	n := 0
	for _, c := range cands {
		if c.Type.Actuating() {
			if busy[c.DeviceID] {
				continue
			}
			busy[c.DeviceID] = true
		}
		if err := update(ctx, tx, c, Eligible, "dispatcher", "eligible for dispatch", now, map[string]any{"next_attempt_ms": ms(now)}); err != nil {
			return 0, err
		}
		n++
	}
	return n, tx.Commit()
}

// Due returns commands whose next delivery attempt is due: eligible
// commands before their execution deadline (first transmissions never
// happen after expiry), and sent/received/accepted commands before their
// result deadline (redelivery is idempotent on the device and solicits its
// latest report).
func (s *Store) Due(ctx context.Context, limit int) ([]Command, error) {
	release, err := s.acquire()
	if err != nil {
		return nil, err
	}
	defer release()
	now := ms(s.now())
	return listQ(ctx, s.r, `SELECT `+cmdCols+` FROM commands
		WHERE next_attempt_ms <= ? AND (
			(state = 'eligible' AND expires_at_ms > ?) OR
			(state IN ('sent', 'received', 'accepted') AND result_deadline_ms > ?))
		ORDER BY next_attempt_ms, created_at_ns LIMIT ?`, now, now, now, limit)
}

// BeginAttempt records delivery attempt number n+1 as 'sending' before it
// is transmitted, and schedules the next attempt after backoff. If the
// process crashes before FinishAttempt, the attempt stays 'sending': it may
// have been transmitted, and the command can then only become unknown,
// never expired.
func (s *Store) BeginAttempt(ctx context.Context, id reliable.CommandID, transport string, backoff time.Duration) (Command, int, error) {
	release, err := s.acquire()
	if err != nil {
		return Command{}, 0, err
	}
	defer release()
	now := s.now().UTC()
	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return Command{}, 0, err
	}
	defer tx.Rollback() //nolint:errcheck
	c, err := getTx(ctx, tx, `command_id = ?`, id[:])
	if err != nil {
		return Command{}, 0, err
	}
	switch {
	case c.State == Eligible && now.Before(c.ExpiresAt):
	case (c.State == Sent || c.State == Received || c.State == Accepted) && now.Before(c.ResultDeadline):
	default:
		return Command{}, 0, fmt.Errorf("%w: %s is %s; no delivery due", ErrInvalidTransition, c.ID, c.State)
	}
	n := c.Attempts + 1
	if _, err := tx.ExecContext(ctx, `INSERT INTO command_attempts (command_id, attempt, started_at_ms, outcome, transport)
		VALUES (?, ?, ?, 'sending', ?)`, id[:], n, ms(now), transport); err != nil {
		return Command{}, 0, err
	}
	if err := update(ctx, tx, c, c.State, "dispatcher", "", now, map[string]any{
		"attempts": n, "next_attempt_ms": ms(now.Add(backoff)),
	}); err != nil {
		return Command{}, 0, err
	}
	if err := tx.Commit(); err != nil {
		return Command{}, 0, err
	}
	c.Attempts, c.NextAttempt = n, now.Add(backoff)
	return c, n, nil
}

// Attempt outcomes.
const (
	AttemptSent        = "sent"        // handed to the network (not: delivered)
	AttemptUnreachable = "unreachable" // no transport knows the device's current session/address
	AttemptError       = "error"       // building or sending failed
)

// FinishAttempt records an attempt's outcome. The first transmitted
// attempt moves an eligible command to sent.
func (s *Store) FinishAttempt(ctx context.Context, id reliable.CommandID, n int, outcome, detail string) error {
	if outcome != AttemptSent && outcome != AttemptUnreachable && outcome != AttemptError {
		return fmt.Errorf("commandstore: attempt outcome %q", outcome)
	}
	release, err := s.acquire()
	if err != nil {
		return err
	}
	defer release()
	now := s.now().UTC()
	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	res, err := tx.ExecContext(ctx, `UPDATE command_attempts SET outcome = ?, detail = ? WHERE command_id = ? AND attempt = ? AND outcome = 'sending'`,
		outcome, detail, id[:], n)
	if err != nil {
		return err
	}
	if k, _ := res.RowsAffected(); k != 1 {
		return fmt.Errorf("commandstore: attempt %d of %s is not pending", n, id)
	}
	if outcome == AttemptSent {
		c, err := getTx(ctx, tx, `command_id = ?`, id[:])
		if err != nil {
			return err
		}
		set := map[string]any{"last_sent_ms": ms(now)}
		if c.FirstSent.IsZero() {
			set["first_sent_ms"] = ms(now)
		}
		to := c.State
		if c.State == Eligible {
			to = Sent
		}
		if err := update(ctx, tx, c, to, "dispatcher", fmt.Sprintf("attempt %d transmitted", n), now, set); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Sweep applies deadlines. A requested or eligible command whose execution
// deadline passed becomes expired if no attempt can have been transmitted,
// and unknown otherwise. A sent, received or accepted command whose result
// deadline passed without a final report becomes unknown. Both need
// operator attention.
func (s *Store) Sweep(ctx context.Context) (expired, unknown int, err error) {
	release, err := s.acquire()
	if err != nil {
		return 0, 0, err
	}
	defer release()
	now := s.now().UTC()
	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback() //nolint:errcheck
	late, err := listQ(ctx, tx, `SELECT `+cmdCols+` FROM commands
		WHERE (state IN ('requested', 'eligible') AND expires_at_ms <= ?)
		   OR (state IN ('sent', 'received', 'accepted') AND result_deadline_ms <= ?)`, ms(now), ms(now))
	if err != nil {
		return 0, 0, err
	}
	for _, c := range late {
		to, reason := Unknown, ""
		switch c.State {
		case Requested, Eligible:
			var maybeSent int
			if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM command_attempts WHERE command_id = ? AND outcome IN ('sending', 'sent')`,
				c.ID[:]).Scan(&maybeSent); err != nil {
				return 0, 0, err
			}
			if maybeSent == 0 {
				to, reason = Expired, "deadline passed before any transmission; not executed"
				expired++
			} else {
				reason = "deadline passed; an attempt may have been transmitted without acknowledgement"
				unknown++
			}
		default:
			reason = fmt.Sprintf("no final report by the result deadline (last device stage: %s)", c.DeviceStage)
			unknown++
		}
		if err := update(ctx, tx, c, to, "sweeper", reason, now, map[string]any{"needs_attention": 1, "state_reason": reason}); err != nil {
			return 0, 0, err
		}
	}
	return expired, unknown, tx.Commit()
}

// Report is a COMMAND_RESULT carried by a stored reliable event.
type Report struct {
	DeviceID   string
	EventUID   uint64
	Position   int64 // reliable-event position
	DeviceTime time.Time
	Result     reliable.CommandResult
}

// StateReport is a BLOCK_STATE or CARGO_STATE carried by a stored event.
type StateReport struct {
	DeviceID   string
	EventUID   uint64
	Position   int64
	DeviceTime time.Time
	Block      *reliable.BlockState
	Cargo      *reliable.CargoState
}

// Report verdicts.
const (
	VerdictApplied        = "applied"
	VerdictStale          = "stale"
	VerdictUnknownCommand = "unknown_command"
	VerdictWrongDevice    = "wrong_device"
	VerdictTypeMismatch   = "type_mismatch"
	VerdictUnexpected     = "unexpected"
)

// Cursor returns the position of the last reliable event applied.
func (s *Store) Cursor(ctx context.Context) (int64, error) {
	release, err := s.acquire()
	if err != nil {
		return 0, err
	}
	defer release()
	var c int64
	err = s.r.QueryRowContext(ctx, `SELECT rpos FROM applier_cursor WHERE id = 1`).Scan(&c)
	return c, err
}

// ErrCursorMoved is returned when another applier advanced the cursor.
var ErrCursorMoved = errors.New("commandstore: applier cursor moved concurrently")

// Apply applies reports read from the event store up to position cursor,
// atomically with advancing the applier cursor from `from` to `cursor`.
// Each report is keyed by its event (device, UID), so applying a report
// twice is a no-op; the cursor check makes two appliers safe.
func (s *Store) Apply(ctx context.Context, from, cursor int64, reports []Report, states []StateReport) ([]string, error) {
	release, err := s.acquire()
	if err != nil {
		return nil, err
	}
	defer release()
	now := s.now().UTC()
	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck
	res, err := tx.ExecContext(ctx, `UPDATE applier_cursor SET rpos = ? WHERE id = 1 AND rpos = ?`, cursor, from)
	if err != nil {
		return nil, err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return nil, ErrCursorMoved
	}
	verdicts := make([]string, len(reports))
	for i, r := range reports {
		v, err := applyReport(ctx, tx, r, now)
		if err != nil {
			return nil, fmt.Errorf("commandstore: apply report %s/%d: %w", r.DeviceID, r.EventUID, err)
		}
		verdicts[i] = v
	}
	for _, st := range states {
		if err := applyState(ctx, tx, st); err != nil {
			return nil, err
		}
	}
	return verdicts, tx.Commit()
}

func applyReport(ctx context.Context, tx *sql.Tx, r Report, now time.Time) (string, error) {
	res := r.Result
	verdict := VerdictApplied
	c, err := getTx(ctx, tx, `command_id = ?`, res.CommandID[:])
	switch {
	case errors.Is(err, ErrNotFound):
		verdict = VerdictUnknownCommand
	case err != nil:
		return "", err
	case c.DeviceID != r.DeviceID:
		verdict = VerdictWrongDevice // a device can never report on another device's command
	case c.Type != res.CommandType:
		verdict = VerdictTypeMismatch
	case res.Stage.Rank() <= c.DeviceStage.Rank():
		verdict = VerdictStale
	case c.State == Expired || c.State == Requested || c.State.Final():
		// Never transmitted, or already final with a lower-ranked stage
		// (e.g. "succeeded" after "rejected"): the device contradicts the
		// record. Kept for audit and flagged, never applied.
		verdict = VerdictUnexpected
	}
	ins, err := tx.ExecContext(ctx, `INSERT INTO command_reports
		(device_id, event_uid, rpos, command_id, stage, reason, observed, device_time_ms, verdict, applied_at_ns)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (device_id, event_uid) DO NOTHING`,
		r.DeviceID, int64(r.EventUID), r.Position, res.CommandID[:], int64(res.Stage), int64(res.Reason),
		int64(res.Observed), ms(r.DeviceTime), verdict, now.UnixNano())
	if err != nil {
		return "", err
	}
	if n, _ := ins.RowsAffected(); n == 0 {
		return VerdictStale, nil // this exact report was applied before
	}
	if verdict == VerdictUnexpected {
		return verdict, update(ctx, tx, c, c.State, "device", "", now, map[string]any{"needs_attention": 1,
			"state_reason": "device report " + res.Stage.String() + " contradicts the recorded state " + string(c.State)})
	}
	if verdict != VerdictApplied {
		return verdict, nil
	}

	set := map[string]any{"device_stage": int64(res.Stage), "device_reason": int64(res.Reason), "observed": int64(res.Observed)}
	if c.ReceivedAt.IsZero() {
		set["received_at_ms"] = ms(now)
	}
	to := c.State
	detail := fmt.Sprintf("device reported %s (%s)", res.Stage, res.Reason)
	switch res.Stage {
	case reliable.StageReceived:
		if c.State != Unknown {
			to = Received
		}
	case reliable.StageAccepted:
		set["decided_at_ms"] = ms(now)
		if c.State != Unknown {
			to = Accepted
		}
	case reliable.StageRejected, reliable.StageSucceeded, reliable.StageFailed:
		set["completed_at_ms"] = ms(now)
		if res.Stage == reliable.StageRejected {
			set["decided_at_ms"] = ms(now)
		}
		to = map[reliable.Stage]State{reliable.StageRejected: Rejected, reliable.StageSucceeded: Succeeded, reliable.StageFailed: Failed}[res.Stage]
		set["state_reason"] = res.Reason.String()
		if c.State == Unknown {
			set["resolved_late"] = 1
			set["needs_attention"] = 0
			detail += "; resolves the unknown outcome"
		}
	case reliable.StageUnknown:
		set["completed_at_ms"] = ms(now)
		set["needs_attention"] = 1
		set["state_reason"] = "device: " + res.Reason.String()
		to = Unknown
	}
	return verdict, update(ctx, tx, c, to, "device", detail, now, set)
}

func applyState(ctx context.Context, tx *sql.Tx, st StateReport) error {
	var kind string
	var state, latch, cause int64
	var cid []byte
	switch {
	case st.Block != nil:
		kind, state, cause = "block", int64(st.Block.State), int64(st.Block.Cause)
		if !st.Block.CommandID.IsZero() {
			cid = st.Block.CommandID[:]
		}
	case st.Cargo != nil:
		kind, state, latch, cause = "cargo", int64(st.Cargo.Door), int64(st.Cargo.Latch), int64(st.Cargo.Cause)
		if !st.Cargo.CommandID.IsZero() {
			cid = st.Cargo.CommandID[:]
		}
	default:
		return errors.New("commandstore: empty state report")
	}
	// Later reports of a device (higher event UID) win, whatever the order
	// they were stored in.
	_, err := tx.ExecContext(ctx, `INSERT INTO device_reported_state
		(device_id, kind, state, latch, cause, command_id, device_time_ms, event_uid, rpos)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (device_id, kind) DO UPDATE SET state = excluded.state, latch = excluded.latch, cause = excluded.cause,
			command_id = excluded.command_id, device_time_ms = excluded.device_time_ms, event_uid = excluded.event_uid, rpos = excluded.rpos
		WHERE excluded.event_uid > device_reported_state.event_uid`,
		st.DeviceID, kind, state, latch, cause, cid, ms(st.DeviceTime), int64(st.EventUID), st.Position)
	return err
}

// ReportedState is the latest actuator state a device reported.
type ReportedState struct {
	Block      reliable.BlockStateValue // 0 if never reported
	BlockAt    time.Time
	Door       reliable.DoorState
	Latch      reliable.LatchState
	CargoAt    time.Time
	BlockCause reliable.StateCause
}

// DeviceReportedState returns what the device last reported. It is the
// device's claim, not an independent observation.
func (s *Store) DeviceReportedState(ctx context.Context, deviceID string) (ReportedState, error) {
	release, err := s.acquire()
	if err != nil {
		return ReportedState{}, err
	}
	defer release()
	rows, err := s.r.QueryContext(ctx, `SELECT kind, state, latch, cause, device_time_ms FROM device_reported_state WHERE device_id = ?`, deviceID)
	if err != nil {
		return ReportedState{}, err
	}
	defer rows.Close()
	var out ReportedState
	for rows.Next() {
		var kind string
		var state, latch, cause, at int64
		if err := rows.Scan(&kind, &state, &latch, &cause, &at); err != nil {
			return out, err
		}
		if kind == "block" {
			out.Block, out.BlockCause, out.BlockAt = reliable.BlockStateValue(state), reliable.StateCause(cause), time.UnixMilli(at).UTC()
		} else {
			out.Door, out.Latch, out.CargoAt = reliable.DoorState(state), reliable.LatchState(latch), time.UnixMilli(at).UTC()
		}
	}
	return out, rows.Err()
}

// StoredReport is one recorded device report.
type StoredReport struct {
	EventUID uint64
	Stage    reliable.Stage
	Reason   reliable.Reason
	Verdict  string
}

// Reports returns the reports recorded for a command, in event order.
func (s *Store) Reports(ctx context.Context, id reliable.CommandID) ([]StoredReport, error) {
	release, err := s.acquire()
	if err != nil {
		return nil, err
	}
	defer release()
	rows, err := s.r.QueryContext(ctx, `SELECT event_uid, stage, reason, verdict FROM command_reports WHERE command_id = ? ORDER BY event_uid`, id[:])
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StoredReport
	for rows.Next() {
		var r StoredReport
		var uid, stage, reason int64
		if err := rows.Scan(&uid, &stage, &reason, &r.Verdict); err != nil {
			return nil, err
		}
		r.EventUID, r.Stage, r.Reason = uint64(uid), reliable.Stage(stage), reliable.Reason(reason)
		out = append(out, r)
	}
	return out, rows.Err()
}
