package device

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/flavioanjo/fleetcore/internal/reliable"
)

// # Command handling
//
// A delivery is processed in this order:
//
//  1. The frame was authenticated for this device and session
//     (HandleDownlink). A delivery whose attempt freshness bound
//     (AttemptNotAfter, device clock) has passed is ignored: it is a
//     delayed or replayed datagram, and the server will retry.
//  2. A known CommandID is a duplicate delivery: nothing is executed
//     again. If the server keeps redelivering, the device re-reports the
//     command's current stage (rate-limited by Config.StatusRepeat).
//  3. The command authority signature is verified. A command with an
//     invalid signature is not recorded and not executed; a
//     SECURITY_VIOLATION is reported (rate-limited).
//  4. In ONE transaction: the command is recorded, COMMAND_RESULT
//     received is queued, the decision (type supported, arguments valid,
//     not expired, safety interlocks) is recorded with its
//     COMMAND_RESULT, and, if accepted, exec_started is set. If the
//     reports cannot be queued (queue full), nothing is recorded and
//     nothing executes: the device never executes what it cannot report.
//  5. The actuator is driven and the sensor read back.
//  6. In one transaction: the outcome, its COMMAND_RESULT and the new
//     BLOCK_STATE/CARGO_STATE are recorded.
//
// A crash between 4 and 6 leaves exec_started without exec_done; the next
// Boot reports the command as unknown (ReasonInterrupted) with the state
// read from the sensor, and never executes it again.

func (d *Device) handleCommand(ctx context.Context, cd reliable.CommandDelivery) error {
	now := d.cfg.Now().UTC()
	if now.After(cd.AttemptNotAfter) {
		d.m.StaleAttempts++
		return nil
	}
	var stage, reason, observed int64
	var lastReport int64
	err := d.store.db.W.QueryRowContext(ctx, `SELECT stage, reason, observed, last_report_ns FROM commands WHERE command_id = ?`, cd.ID[:]).
		Scan(&stage, &reason, &observed, &lastReport)
	switch {
	case err == nil:
		d.m.DuplicateDeliveries++
		return d.rereport(ctx, cd, reliable.Stage(stage), reliable.Reason(reason), uint8(observed), lastReport, now)
	case !errors.Is(err, sql.ErrNoRows):
		return err
	}
	if err := d.cfg.Authority.Verify(cd.AuthorityKeyID, cd.Statement(), cd.Signature); err != nil {
		d.m.AuthorityRejected++
		d.violation(ctx, reliable.SecurityInvalidAuthority, 0)
		return nil
	}
	d.m.CommandsReceived++

	decision, dreason := d.decide(cd, now)
	tx, err := d.store.db.W.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	var started sql.NullInt64
	if decision == reliable.StageAccepted || decision == reliable.StageSucceeded {
		started = sql.NullInt64{Int64: now.UnixNano(), Valid: true}
	}
	observedNow := d.observe(cd)
	if _, err := tx.ExecContext(ctx, `INSERT INTO commands (command_id, command_type, args, issued_ms, expires_ms, principal,
		received_ns, stage, reason, observed, exec_started_ns, exec_done_ns, last_report_ns)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		cd.ID[:], int64(cd.Type), append([]byte{}, cd.Args...), cd.IssuedAt.UnixMilli(), cd.ExpiresAt.UnixMilli(), cd.Principal,
		now.UnixNano(), int64(decision), int64(dreason), int64(observedNow), started, nil, now.UnixNano()); err != nil {
		return err
	}
	if err := d.report(ctx, tx, now, cd, reliable.StageReceived, 0, 0); err != nil {
		return err
	}
	if decision == reliable.StageSucceeded {
		// Already in the target state (confirmed by the sensor): accepted and
		// immediately succeeded, nothing actuated.
		if err := d.report(ctx, tx, now, cd, reliable.StageAccepted, 0, 0); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE commands SET exec_done_ns = ? WHERE command_id = ?`, now.UnixNano(), cd.ID[:]); err != nil {
			return err
		}
	}
	if err := d.report(ctx, tx, now, cd, decision, dreason, observedNow); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if decision == reliable.StageRejected {
		d.m.CommandsRejected++
		return nil
	}
	if decision == reliable.StageSucceeded {
		d.m.CommandsSucceeded++
		return d.queueStates(ctx, cd, reliable.CauseCommand)
	}

	// From here the actuator may move. Its outcome is recorded whatever the
	// caller does with ctx: an orderly stop must not make the command look
	// interrupted (unknown) at the next boot. Only a crash can leave the
	// outcome unrecorded. Nothing below waits on ctx.
	ctx = context.WithoutCancel(ctx)

	// Execute (step 5).
	final, freason, obs := d.execute(ctx, cd)
	if h := d.cfg.Hooks; h != nil && h.DuringExecution != nil {
		if err := h.DuringExecution(cd.ID); err != nil {
			return err // simulated crash: the outcome is not recorded
		}
	}
	tx, err = d.store.db.W.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	if _, err := tx.ExecContext(ctx, `UPDATE commands SET stage = ?, reason = ?, observed = ?, exec_done_ns = ?, last_report_ns = ?
		WHERE command_id = ?`, int64(final), int64(freason), int64(obs), now.UnixNano(), now.UnixNano(), cd.ID[:]); err != nil {
		return err
	}
	if err := d.report(ctx, tx, now, cd, final, freason, obs); err != nil {
		return err
	}
	if err := d.stateEventsTx(ctx, tx, now, cd, reliable.CauseCommand); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	switch final {
	case reliable.StageSucceeded:
		d.m.CommandsSucceeded++
	case reliable.StageFailed:
		d.m.CommandsFailed++
	default:
		d.m.CommandsUnknown++
	}
	return nil
}

// report queues a COMMAND_RESULT inside tx.
func (d *Device) report(ctx context.Context, tx *sql.Tx, now time.Time, cd reliable.CommandDelivery, st reliable.Stage, r reliable.Reason, obs uint8) error {
	typ := cd.Type // echoed even when unsupported, so the server can correlate the rejection
	_, _, err := enqueueTx(ctx, tx, &d.cfg, now, func(uint64) (reliable.Body, error) {
		return reliable.CommandResult{CommandID: cd.ID, CommandType: typ, Stage: st, Reason: r, Observed: obs}, nil
	})
	return err
}

// decide evaluates a new command: the outcome is StageAccepted (execute),
// StageRejected with a reason, or StageSucceeded when the target state
// already holds.
func (d *Device) decide(cd reliable.CommandDelivery, now time.Time) (reliable.Stage, reliable.Reason) {
	switch {
	case !cd.Type.Valid():
		return reliable.StageRejected, reliable.ReasonUnsupported
	case !reliable.ValidArgs(cd.Type, cd.Args):
		return reliable.StageRejected, reliable.ReasonBadArguments
	case !now.Before(cd.ExpiresAt):
		return reliable.StageRejected, reliable.ReasonExpired
	}
	hw := d.cfg.Hardware
	switch cd.Type {
	case reliable.CommandBlock, reliable.CommandUnblock:
		v, ok := hw.Vehicle()
		if !ok {
			return reliable.StageRejected, reliable.ReasonSensorUnavailable
		}
		want := reliable.Blocked
		if cd.Type == reliable.CommandUnblock {
			want = reliable.Unblocked
		}
		cur := hw.ImmobilizerState()
		if cur == want {
			return reliable.StageSucceeded, reliable.ReasonAlreadyInState
		}
		if cd.Type == reliable.CommandBlock && !v.Stationary() {
			// Never immobilize a moving vehicle.
			return reliable.StageRejected, reliable.ReasonVehicleMoving
		}
		if cur == reliable.BlockUnknown {
			return reliable.StageRejected, reliable.ReasonSensorUnavailable
		}
	case reliable.CommandOpenCargo:
		v, ok := hw.Vehicle()
		switch {
		case !ok:
			return reliable.StageRejected, reliable.ReasonSensorUnavailable
		case !v.Stationary():
			return reliable.StageRejected, reliable.ReasonVehicleMoving
		case v.IgnitionOn:
			return reliable.StageRejected, reliable.ReasonIgnitionOn
		}
		comp, _ := reliable.ParseCargoArgs(cd.Args)
		if hw.CargoDoor(comp) == reliable.DoorOpen {
			return reliable.StageSucceeded, reliable.ReasonAlreadyInState
		}
	}
	return reliable.StageAccepted, reliable.ReasonNone
}

// observe reads the sensor relevant to the command.
func (d *Device) observe(cd reliable.CommandDelivery) uint8 {
	switch cd.Type {
	case reliable.CommandBlock, reliable.CommandUnblock:
		return uint8(d.cfg.Hardware.ImmobilizerState())
	case reliable.CommandOpenCargo:
		comp, ok := reliable.ParseCargoArgs(cd.Args)
		if !ok {
			return 0
		}
		return uint8(d.cfg.Hardware.CargoDoor(comp))
	}
	return 0
}

// execute drives the actuator and determines the outcome from the sensor,
// never from the actuator call alone.
func (d *Device) execute(ctx context.Context, cd reliable.CommandDelivery) (reliable.Stage, reliable.Reason, uint8) {
	hw := d.cfg.Hardware
	d.m.Actuations++
	switch cd.Type {
	case reliable.CommandBlock, reliable.CommandUnblock:
		want := reliable.Blocked
		if cd.Type == reliable.CommandUnblock {
			want = reliable.Unblocked
		}
		err := hw.SetImmobilizer(want == reliable.Blocked)
		got := hw.ImmobilizerState()
		switch {
		case got == want:
			return reliable.StageSucceeded, reliable.ReasonNone, uint8(got)
		case got == reliable.BlockUnknown:
			return reliable.StageUnknown, reliable.ReasonSensorUnavailable, uint8(got)
		case err != nil:
			return reliable.StageFailed, reliable.ReasonActuatorFault, uint8(got)
		default:
			return reliable.StageFailed, reliable.ReasonNotConfirmed, uint8(got)
		}
	case reliable.CommandOpenCargo:
		comp, _ := reliable.ParseCargoArgs(cd.Args)
		err := hw.ReleaseCargoLatch(comp)
		door := hw.CargoDoor(comp)
		switch {
		case door == reliable.DoorOpen:
			return reliable.StageSucceeded, reliable.ReasonNone, uint8(door)
		case err != nil:
			return reliable.StageFailed, reliable.ReasonActuatorFault, uint8(door)
		default:
			// The latch was driven but the door sensor does not show the
			// compartment open: an electrical actuation is not proof.
			return reliable.StageUnknown, reliable.ReasonNotConfirmed, uint8(door)
		}
	case reliable.CommandRequestPosition:
		d.m.Actuations-- // not a physical operation
		maxAge, _ := reliable.ParsePositionArgs(cd.Args)
		fix, ok := d.readFix()
		if !ok || d.cfg.Now().Sub(fix.FixTime) > maxAge {
			return reliable.StageFailed, reliable.ReasonPositionUnavailable, 0
		}
		if _, err := d.enqueueLocked(ctx, reliable.Position{PositionFix: fix, Reason: reliable.PositionRequested, RequestID: cd.ID}); err != nil {
			return reliable.StageFailed, reliable.ReasonPositionUnavailable, 0
		}
		return reliable.StageSucceeded, reliable.ReasonNone, 0
	}
	return reliable.StageRejected, reliable.ReasonUnsupported, 0
}

func (d *Device) enqueueLocked(ctx context.Context, b reliable.Body) (uint64, error) {
	return d.enqueue(ctx, func(uint64, time.Time) (reliable.Body, error) { return b, nil })
}

// stateEventsTx queues the actuator state after a command, inside tx.
func (d *Device) stateEventsTx(ctx context.Context, tx *sql.Tx, now time.Time, cd reliable.CommandDelivery, cause reliable.StateCause) error {
	hw := d.cfg.Hardware
	var body reliable.Body
	switch cd.Type {
	case reliable.CommandBlock, reliable.CommandUnblock:
		body = reliable.BlockState{State: hw.ImmobilizerState(), Cause: cause, CommandID: cd.ID}
	case reliable.CommandOpenCargo:
		comp, _ := reliable.ParseCargoArgs(cd.Args)
		body = reliable.CargoState{Door: hw.CargoDoor(comp), Latch: hw.CargoLatch(comp), Cause: cause, CommandID: cd.ID}
	default:
		return nil
	}
	_, _, err := enqueueTx(ctx, tx, &d.cfg, now, func(uint64) (reliable.Body, error) { return body, nil })
	return err
}

func (d *Device) queueStates(ctx context.Context, cd reliable.CommandDelivery, cause reliable.StateCause) error {
	now := d.cfg.Now().UTC()
	tx, err := d.store.db.W.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	if err := d.stateEventsTx(ctx, tx, now, cd, cause); err != nil {
		return err
	}
	return tx.Commit()
}

// rereport answers a duplicate delivery: nothing executes again; the
// current stage is re-reported if the last report is older than
// StatusRepeat (the server may have lost track of it).
func (d *Device) rereport(ctx context.Context, cd reliable.CommandDelivery, st reliable.Stage, r reliable.Reason, obs uint8, last int64, now time.Time) error {
	if now.Sub(time.Unix(0, last)) < d.cfg.StatusRepeat {
		return nil
	}
	tx, err := d.store.db.W.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	if err := d.report(ctx, tx, now, cd, st, r, obs); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE commands SET last_report_ns = ? WHERE command_id = ?`, now.UnixNano(), cd.ID[:]); err != nil {
		return err
	}
	d.m.StatusReReports++
	return tx.Commit()
}

// recoverInterrupted runs at Boot: commands whose execution started but
// whose outcome was never recorded are reported unknown, with the sensor
// state, and are never executed again.
func (d *Device) recoverInterrupted(ctx context.Context, tx *sql.Tx, now time.Time) error {
	rows, err := tx.QueryContext(ctx, `SELECT command_id, command_type, args, issued_ms, expires_ms, principal FROM commands
		WHERE exec_started_ns IS NOT NULL AND exec_done_ns IS NULL`)
	if err != nil {
		return err
	}
	var cds []reliable.CommandDelivery
	for rows.Next() {
		var cd reliable.CommandDelivery
		var id []byte
		var typ, issued, expires int64
		if err := rows.Scan(&id, &typ, &cd.Args, &issued, &expires, &cd.Principal); err != nil {
			rows.Close()
			return err
		}
		copy(cd.ID[:], id)
		cd.Type, cd.IssuedAt, cd.ExpiresAt = reliable.CommandType(typ), time.UnixMilli(issued), time.UnixMilli(expires)
		cd.DeviceID = d.cfg.DeviceID
		cds = append(cds, cd)
	}
	rows.Close()
	for _, cd := range cds {
		obs := d.observe(cd)
		if _, err := tx.ExecContext(ctx, `UPDATE commands SET stage = ?, reason = ?, observed = ?, exec_done_ns = ?, last_report_ns = ?
			WHERE command_id = ?`, int64(reliable.StageUnknown), int64(reliable.ReasonInterrupted), int64(obs), now.UnixNano(), now.UnixNano(), cd.ID[:]); err != nil {
			return err
		}
		if err := d.report(ctx, tx, now, cd, reliable.StageUnknown, reliable.ReasonInterrupted, obs); err != nil {
			return fmt.Errorf("device: report interrupted command: %w", err)
		}
		if err := d.stateEventsTx(ctx, tx, now, cd, reliable.CauseInterrupted); err != nil {
			return err
		}
		d.m.CommandsUnknown++
		d.m.InterruptedRecovered++
	}
	return nil
}
