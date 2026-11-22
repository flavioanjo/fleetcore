// Package dispatcher drives commands through their lifecycle: it makes
// requested commands eligible, sends due delivery attempts through the
// gateways, applies deadlines, and applies the reports devices send back
// (COMMAND_RESULT, BLOCK_STATE, CARGO_STATE events in the event store) to
// the command store.
//
// Every step is idempotent and all state is in the command store, so a
// dispatcher can be restarted at any point (or two can run: attempts are
// guarded by state checks and the applier by a compare-and-set cursor).
// Delivery is at-least-once; execution at most once is the device's
// guarantee (it deduplicates by CommandID).
package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/flavioanjo/fleetcore/internal/commandstore"
	"github.com/flavioanjo/fleetcore/internal/eventstore"
	"github.com/flavioanjo/fleetcore/internal/reliable"
)

// Transport delivers a command to a device. *gateway.Gateway implements
// it. ErrUnreachable-like errors (the transport does not know the device's
// current session) make the dispatcher try the next transport.
type Transport interface {
	SendCommand(reliable.CommandDelivery) error
}

// Named pairs a transport with a name recorded in attempt history.
type Named struct {
	Name string
	Transport
}

// Config configures a Dispatcher.
type Config struct {
	Commands   *commandstore.Store
	Events     *eventstore.Store
	Transports []Named
	// AttemptValidity bounds how long the device accepts one attempt
	// (AttemptNotAfter). Default 30s, at most reliable.MaxAttemptValidity.
	AttemptValidity time.Duration
	// Retry backoff of unacknowledged deliveries: InitialBackoff doubling up
	// to MaxBackoff. Defaults 2s and 30s.
	InitialBackoff, MaxBackoff time.Duration
	// PollInterval: after the device reported receipt, the delivery is
	// repeated at this interval until a final report or the result
	// deadline (the device answers repeats with its current stage, without
	// executing again). Default 30s.
	PollInterval time.Duration
	// Batch bounds commands per step and events per apply. Default 256.
	Batch int
	// Interval is the Run loop period. Default 200ms.
	Interval time.Duration
	Now      func() time.Time
	Logger   *slog.Logger
}

// Dispatcher runs the command lifecycle.
type Dispatcher struct {
	cfg Config
	log *slog.Logger
}

// New validates cfg.
func New(cfg Config) (*Dispatcher, error) {
	if cfg.Commands == nil || cfg.Events == nil || len(cfg.Transports) == 0 {
		return nil, errors.New("dispatcher: needs Commands, Events and at least one transport")
	}
	defD := func(v *time.Duration, d time.Duration) {
		if *v <= 0 {
			*v = d
		}
	}
	defD(&cfg.AttemptValidity, 30*time.Second)
	if cfg.AttemptValidity > reliable.MaxAttemptValidity {
		return nil, fmt.Errorf("dispatcher: AttemptValidity above %v", reliable.MaxAttemptValidity)
	}
	defD(&cfg.InitialBackoff, 2*time.Second)
	defD(&cfg.MaxBackoff, 30*time.Second)
	defD(&cfg.PollInterval, 30*time.Second)
	defD(&cfg.Interval, 200*time.Millisecond)
	if cfg.Batch <= 0 {
		cfg.Batch = 256
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Dispatcher{cfg: cfg, log: cfg.Logger}, nil
}

// StepStats reports one dispatch step.
type StepStats struct {
	Promoted, Expired, Unknown int
	Sent, Unreachable, Failed  int
}

// Step runs one dispatch cycle: promote, sweep deadlines, send due
// attempts.
func (d *Dispatcher) Step(ctx context.Context) (StepStats, error) {
	var s StepStats
	var err error
	if s.Promoted, err = d.cfg.Commands.Promote(ctx); err != nil {
		return s, err
	}
	if s.Expired, s.Unknown, err = d.cfg.Commands.Sweep(ctx); err != nil {
		return s, err
	}
	due, err := d.cfg.Commands.Due(ctx, d.cfg.Batch)
	if err != nil {
		return s, err
	}
	for _, c := range due {
		if err := d.attempt(ctx, c, &s); err != nil {
			return s, err
		}
	}
	return s, nil
}

func (d *Dispatcher) backoff(c commandstore.Command) time.Duration {
	if c.State == commandstore.Received || c.State == commandstore.Accepted {
		return d.cfg.PollInterval
	}
	b := d.cfg.InitialBackoff
	for i := 0; i < c.Attempts && b < d.cfg.MaxBackoff; i++ {
		b *= 2
	}
	if b > d.cfg.MaxBackoff {
		b = d.cfg.MaxBackoff
	}
	return b
}

func (d *Dispatcher) attempt(ctx context.Context, c commandstore.Command, s *StepStats) error {
	c, n, err := d.cfg.Commands.BeginAttempt(ctx, c.ID, "", d.backoff(c))
	if errors.Is(err, commandstore.ErrInvalidTransition) {
		return nil // changed meanwhile (report applied, or another dispatcher)
	}
	if err != nil {
		return err
	}
	now := time.UnixMilli(d.cfg.Now().UnixMilli()).UTC()
	del := reliable.CommandDelivery{Command: c.Command, Attempt: uint32(n), AttemptSentAt: now,
		AttemptNotAfter: now.Add(d.cfg.AttemptValidity), AuthorityKeyID: c.AuthorityKeyID, Signature: c.Signature}
	outcome, detail := commandstore.AttemptUnreachable, "no transport knows the device's current session"
	for _, t := range d.cfg.Transports {
		err := t.SendCommand(del)
		if err == nil {
			outcome, detail = commandstore.AttemptSent, "via "+t.Name
			break
		}
		if !isUnreachable(err) {
			outcome, detail = commandstore.AttemptError, t.Name+": "+err.Error()
		}
	}
	switch outcome {
	case commandstore.AttemptSent:
		s.Sent++
	case commandstore.AttemptUnreachable:
		s.Unreachable++
	default:
		s.Failed++
		d.log.Warn("dispatcher: delivery failed", "command", c.ID.String(), "attempt", n, "detail", detail)
	}
	return d.cfg.Commands.FinishAttempt(ctx, c.ID, n, outcome, detail)
}

func isUnreachable(err error) bool { return errors.Is(err, reliable.ErrDeviceUnreachable) }

// Apply consumes device reports from the event store into the command
// store. It returns the number of events applied.
func (d *Dispatcher) Apply(ctx context.Context) (int, error) {
	from, err := d.cfg.Commands.Cursor(ctx)
	if err != nil {
		return 0, err
	}
	recs, err := d.cfg.Events.ReadReliableAfter(ctx, from, d.cfg.Batch,
		reliable.EventCommandResult, reliable.EventBlockState, reliable.EventCargoState)
	if err != nil || len(recs) == 0 {
		return 0, err
	}
	var reports []commandstore.Report
	var states []commandstore.StateReport
	for _, r := range recs {
		b, err := r.Body()
		if err != nil {
			return 0, fmt.Errorf("dispatcher: stored event %d: %w", r.Position, err)
		}
		switch v := b.(type) {
		case reliable.CommandResult:
			reports = append(reports, commandstore.Report{DeviceID: r.DeviceID, EventUID: r.Upload.UID, Position: r.Position,
				DeviceTime: r.Upload.DeviceTime, Result: v})
		case reliable.BlockState:
			states = append(states, commandstore.StateReport{DeviceID: r.DeviceID, EventUID: r.Upload.UID, Position: r.Position,
				DeviceTime: r.Upload.DeviceTime, Block: &v})
		case reliable.CargoState:
			states = append(states, commandstore.StateReport{DeviceID: r.DeviceID, EventUID: r.Upload.UID, Position: r.Position,
				DeviceTime: r.Upload.DeviceTime, Cargo: &v})
		}
	}
	if _, err := d.cfg.Commands.Apply(ctx, from, recs[len(recs)-1].Position, reports, states); err != nil {
		if errors.Is(err, commandstore.ErrCursorMoved) {
			return 0, nil // another applier got there first
		}
		return 0, err
	}
	return len(recs), nil
}

// Run applies reports and dispatches until ctx is cancelled.
func (d *Dispatcher) Run(ctx context.Context) error {
	t := time.NewTicker(d.cfg.Interval)
	defer t.Stop()
	for {
		for {
			n, err := d.Apply(ctx)
			if err != nil {
				d.log.Warn("dispatcher: apply failed", "err", err)
				break
			}
			if n < d.cfg.Batch {
				break
			}
		}
		if _, err := d.Step(ctx); err != nil && ctx.Err() == nil {
			d.log.Warn("dispatcher: step failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}
