package commandstore

import (
	"context"
	"crypto/ed25519"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/reliable"
	"github.com/flavioanjo/fleetcore/internal/security"
)

var t0 = time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)

type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

type fixture struct {
	t     *testing.T
	path  string
	auth  *security.Authority
	clk   *clock
	store *Store
}

func newFixture(t *testing.T) *fixture {
	a, err := security.NewAuthority(1)
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, path: filepath.Join(t.TempDir(), "commands.db"), auth: a, clk: &clock{t: t0}}
	f.reopen()
	return f
}

func (f *fixture) reopen() {
	if f.store != nil {
		f.store.Close()
	}
	s, err := Open(context.Background(), f.path, f.auth, Options{Now: f.clk.now})
	if err != nil {
		f.t.Fatal(err)
	}
	f.store = s
	f.t.Cleanup(func() { s.Close() })
}

var (
	security_op = Principal{ID: "ops:alice", Roles: []string{"security_operator", "fleet_viewer"}}
	cargo_op    = Principal{ID: "ops:bob", Roles: []string{"cargo_operator"}}
	viewer      = Principal{ID: "ops:carol", Roles: []string{"fleet_viewer"}}
)

func req(id string, p Principal, dev string, typ reliable.CommandType) Request {
	r := Request{RequestID: id, Principal: p, DeviceID: dev, Type: typ, TTL: time.Minute, Justification: "test"}
	switch typ {
	case reliable.CommandOpenCargo:
		r.Args = reliable.CargoArgs(1)
	case reliable.CommandRequestPosition:
		r.Args = reliable.PositionArgs(30 * time.Second)
	}
	return r
}

func (f *fixture) create(r Request) Command {
	f.t.Helper()
	c, created, err := f.store.Create(context.Background(), r)
	if err != nil || !created {
		f.t.Fatalf("create %s: %v (created %v)", r.RequestID, err, created)
	}
	return c
}

func (f *fixture) get(id reliable.CommandID) Command {
	f.t.Helper()
	c, err := f.store.Get(context.Background(), id)
	if err != nil {
		f.t.Fatal(err)
	}
	return c
}

func TestCreateAuthorizesSignsAndIsIdempotent(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	c := f.create(req("r1", security_op, "DEV-1", reliable.CommandBlock))
	if c.State != Requested || c.ID.IsZero() || c.Principal != "ops:alice" || !c.ExpiresAt.Equal(t0.Add(time.Minute)) {
		t.Fatalf("command %+v", c)
	}
	v := security.NewVerifier(map[uint32]ed25519.PublicKey{1: f.auth.Public()})
	if err := v.Verify(c.AuthorityKeyID, c.Statement(), c.Signature); err != nil {
		t.Fatalf("stored signature: %v", err)
	}
	// Same request again: same command, nothing new.
	again, created, err := f.store.Create(ctx, req("r1", security_op, "DEV-1", reliable.CommandBlock))
	if err != nil || created || again.ID != c.ID {
		t.Fatalf("idempotent create: %v created=%v", err, created)
	}
	// Same id, other parameters.
	if _, _, err := f.store.Create(ctx, req("r1", security_op, "DEV-2", reliable.CommandBlock)); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("conflicting reuse: %v", err)
	}
	// Unauthorized: audited, never stored or signed.
	for _, r := range []Request{req("r2", viewer, "DEV-1", reliable.CommandBlock), req("r3", security_op, "DEV-1", reliable.CommandOpenCargo),
		req("r4", cargo_op, "DEV-1", reliable.CommandUnblock)} {
		if _, _, err := f.store.Create(ctx, r); !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("%s by %s: %v", r.Type, r.Principal.ID, err)
		}
	}
	if n, _ := f.store.DeniedCount(ctx); n != 3 {
		t.Fatalf("denied audit %d", n)
	}
	if all, _ := f.store.List(ctx, Filter{}); len(all) != 1 {
		t.Fatalf("%d commands stored", len(all))
	}
	// Invalid requests.
	bad := []Request{
		func() Request {
			r := req("b1", security_op, "DEV-1", reliable.CommandBlock)
			r.TTL = time.Hour
			return r
		}(),
		func() Request {
			r := req("b2", security_op, "DEV-1", reliable.CommandBlock)
			r.Justification = " "
			return r
		}(),
		func() Request {
			r := req("b3", cargo_op, "DEV-1", reliable.CommandOpenCargo)
			r.Args = []byte{9}
			return r
		}(),
		func() Request { r := req("b4", security_op, "bad id!", reliable.CommandBlock); return r }(),
		func() Request { r := req("b5", security_op, "DEV-1", 9); return r }(),
	}
	for _, r := range bad {
		if _, _, err := f.store.Create(ctx, r); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("%s: %v", r.RequestID, err)
		}
	}
	// Identity and authorization fields are immutable in the database.
	if _, err := f.store.w.Exec(`UPDATE commands SET device_id = 'DEV-9'`); err == nil {
		t.Fatal("device id updated")
	}
	if _, err := f.store.w.Exec(`DELETE FROM command_history`); err == nil {
		t.Fatal("history deleted")
	}
}

func TestPromoteSerializesActuationsPerDevice(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	block := f.create(req("a", security_op, "DEV-1", reliable.CommandBlock))
	unblock := f.create(req("b", security_op, "DEV-1", reliable.CommandUnblock))
	pos := f.create(req("c", viewer, "DEV-1", reliable.CommandRequestPosition))
	other := f.create(req("d", security_op, "DEV-2", reliable.CommandBlock))
	if n, err := f.store.Promote(ctx); err != nil || n != 3 {
		t.Fatalf("promoted %d: %v", n, err)
	}
	for id, want := range map[reliable.CommandID]State{block.ID: Eligible, unblock.ID: Requested, pos.ID: Eligible, other.ID: Eligible} {
		if got := f.get(id).State; got != want {
			t.Fatalf("%s: %s, want %s", f.get(id).Type, got, want)
		}
	}
	// BLOCK finishes; UNBLOCK becomes eligible.
	f.deliver(block.ID)
	f.report(block, 1, reliable.StageSucceeded, reliable.ReasonNone)
	if n, _ := f.store.Promote(ctx); n != 1 || f.get(unblock.ID).State != Eligible {
		t.Fatalf("unblock not promoted after block finished: %s", f.get(unblock.ID).State)
	}
}

func (f *fixture) deliver(id reliable.CommandID) int {
	f.t.Helper()
	_, n, err := f.store.BeginAttempt(context.Background(), id, "gw-a", 5*time.Second)
	if err != nil {
		f.t.Fatal(err)
	}
	if err := f.store.FinishAttempt(context.Background(), id, n, AttemptSent, ""); err != nil {
		f.t.Fatal(err)
	}
	return n
}

var nextUID uint64 = 100

func (f *fixture) report(c Command, _ int, st reliable.Stage, reason reliable.Reason) string {
	f.t.Helper()
	return f.reportFrom(c.DeviceID, c, st, reason)
}

func (f *fixture) reportFrom(dev string, c Command, st reliable.Stage, reason reliable.Reason) string {
	f.t.Helper()
	nextUID++
	cur, _ := f.store.Cursor(context.Background())
	v, err := f.store.Apply(context.Background(), cur, cur+1, []Report{{DeviceID: dev, EventUID: nextUID, Position: cur + 1, DeviceTime: f.clk.t,
		Result: reliable.CommandResult{CommandID: c.ID, CommandType: c.Type, Stage: st, Reason: reason}}}, nil)
	if err != nil {
		f.t.Fatal(err)
	}
	return v[0]
}

func TestAttemptsDueAndDeadlines(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	neverSent := f.create(req("a", security_op, "DEV-1", reliable.CommandBlock))
	crashed := f.create(req("b", security_op, "DEV-2", reliable.CommandBlock))
	silent := f.create(req("c", security_op, "DEV-3", reliable.CommandBlock))
	f.store.Promote(ctx)

	// Attempt of "crashed" begins but never finishes (process died).
	if _, n, err := f.store.BeginAttempt(ctx, crashed.ID, "gw-a", 5*time.Second); err != nil || n != 1 {
		t.Fatalf("begin: %d %v", n, err)
	}
	if f.deliver(silent.ID) != 1 || f.get(silent.ID).State != Sent {
		t.Fatal("silent not sent")
	}
	// Not due again before the backoff.
	due, _ := f.store.Due(ctx, 10)
	for _, c := range due {
		if c.ID == silent.ID || c.ID == crashed.ID {
			t.Fatalf("%s due before backoff", c.Type)
		}
	}
	f.clk.advance(6 * time.Second)
	due, _ = f.store.Due(ctx, 10)
	if len(due) != 3 {
		t.Fatalf("%d due after backoff, want 3", len(due))
	}

	// Execution deadline passes.
	f.clk.advance(time.Minute)
	if e, u, err := f.store.Sweep(ctx); err != nil || e != 1 || u != 1 {
		t.Fatalf("sweep at expiry: expired %d unknown %d %v", e, u, err)
	}
	if c := f.get(neverSent.ID); c.State != Expired || !c.NeedsAttention {
		t.Fatalf("never-sent command %s attention=%v", c.State, c.NeedsAttention)
	}
	if c := f.get(crashed.ID); c.State != Unknown || !c.NeedsAttention {
		t.Fatalf("possibly-sent command %s (must be unknown, not expired)", c.State)
	}
	// "silent" was transmitted: still awaiting a result until the result
	// deadline (redelivery polls the device), then unknown, never failed.
	if c := f.get(silent.ID); c.State != Sent {
		t.Fatalf("silent at expiry: %s", c.State)
	}
	if _, _, err := f.store.BeginAttempt(ctx, neverSent.ID, "gw-a", time.Second); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("attempt after expiry: %v", err)
	}
	f.clk.advance(DefaultPolicy().ResultGrace)
	if _, u, _ := f.store.Sweep(ctx); u != 1 {
		t.Fatalf("result deadline sweep: %d unknown", u)
	}
	if c := f.get(silent.ID); c.State != Unknown || !c.NeedsAttention {
		t.Fatalf("silent after result deadline: %s", c.State)
	}
	// A late final report resolves the unknown outcome.
	if v := f.report(silent, 0, reliable.StageSucceeded, reliable.ReasonNone); v != VerdictApplied {
		t.Fatalf("late result verdict %s", v)
	}
	if c := f.get(silent.ID); c.State != Succeeded || !c.ResolvedLate || c.NeedsAttention {
		t.Fatalf("resolved %s late=%v attention=%v", c.State, c.ResolvedLate, c.NeedsAttention)
	}
	// Restart: everything is durable.
	f.reopen()
	if c := f.get(crashed.ID); c.State != Unknown {
		t.Fatalf("after reopen: %s", c.State)
	}
	at, _ := f.store.Attempts(ctx, crashed.ID)
	if len(at) != 1 || at[0].Outcome != "sending" {
		t.Fatalf("attempts %+v", at)
	}
}

func TestReportsAreRankedIsolatedAndIdempotent(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	c := f.create(req("a", cargo_op, "DEV-1", reliable.CommandOpenCargo))
	f.store.Promote(ctx)
	f.deliver(c.ID)

	// Another device cannot advance this command.
	if v := f.reportFrom("DEV-2", c, reliable.StageSucceeded, 0); v != VerdictWrongDevice || f.get(c.ID).State != Sent {
		t.Fatalf("foreign report: %s, state %s", v, f.get(c.ID).State)
	}
	// Forward jump: accepted before received.
	if v := f.report(c, 0, reliable.StageAccepted, 0); v != VerdictApplied || f.get(c.ID).State != Accepted {
		t.Fatalf("accepted: %s %s", v, f.get(c.ID).State)
	}
	// The late "received" is stale and changes nothing.
	if v := f.report(c, 0, reliable.StageReceived, 0); v != VerdictStale || f.get(c.ID).State != Accepted {
		t.Fatalf("stale received: %s %s", v, f.get(c.ID).State)
	}
	// Wrong type for the id.
	wrong := c
	wrong.Type = reliable.CommandBlock
	if v := f.report(wrong, 0, reliable.StageSucceeded, 0); v != VerdictTypeMismatch {
		t.Fatalf("type mismatch: %s", v)
	}
	// Device-reported unknown (e.g. cargo latch released, door sensor did not confirm).
	if v := f.report(c, 0, reliable.StageUnknown, reliable.ReasonNotConfirmed); v != VerdictApplied {
		t.Fatal(v)
	}
	got := f.get(c.ID)
	if got.State != Unknown || !got.NeedsAttention || got.DeviceReason != reliable.ReasonNotConfirmed {
		t.Fatalf("device unknown: %+v", got)
	}
	// Unknown command id.
	ghost := c
	ghost.ID = reliable.CommandID{0xee}
	if v := f.report(ghost, 0, reliable.StageReceived, 0); v != VerdictUnknownCommand {
		t.Fatalf("ghost: %s", v)
	}
	// Re-applying the same event is a no-op.
	cur, _ := f.store.Cursor(ctx)
	rep := Report{DeviceID: "DEV-1", EventUID: 5000, Position: cur + 1, Result: reliable.CommandResult{CommandID: c.ID,
		CommandType: c.Type, Stage: reliable.StageFailed}}
	if _, err := f.store.Apply(ctx, cur, cur+1, []Report{rep}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Apply(ctx, cur, cur+1, []Report{rep}, nil); !errors.Is(err, ErrCursorMoved) {
		t.Fatalf("stale cursor: %v", err)
	}
	v, _ := f.store.Apply(ctx, cur+1, cur+2, []Report{rep}, nil)
	if v[0] != VerdictStale {
		t.Fatalf("re-applied report: %s", v[0])
	}
	reps, _ := f.store.Reports(ctx, c.ID)
	if len(reps) != 6 {
		t.Fatalf("%d reports recorded", len(reps))
	}
	h, _ := f.store.History(ctx, c.ID)
	var path []State
	for _, e := range h {
		path = append(path, e.To)
	}
	want := []State{Requested, Eligible, Sent, Accepted, Unknown}
	if len(path) != len(want) {
		t.Fatalf("history %v", path)
	}
	for i := range want {
		if path[i] != want[i] {
			t.Fatalf("history %v, want %v", path, want)
		}
	}
}

func TestContradictoryReportIsFlaggedNotApplied(t *testing.T) {
	f := newFixture(t)
	c := f.create(req("a", security_op, "DEV-1", reliable.CommandBlock))
	f.store.Promote(context.Background())
	f.deliver(c.ID)
	f.report(c, 0, reliable.StageRejected, reliable.ReasonVehicleMoving)
	if got := f.get(c.ID); got.State != Rejected || got.StateReason != "vehicle_moving" {
		t.Fatalf("rejected: %+v", got)
	}
	if v := f.report(c, 0, reliable.StageSucceeded, 0); v != VerdictUnexpected {
		t.Fatalf("succeeded after rejected: %s", v)
	}
	if got := f.get(c.ID); got.State != Rejected || !got.NeedsAttention {
		t.Fatalf("contradiction: %s attention=%v", got.State, got.NeedsAttention)
	}
}

func TestDeviceReportedStateKeepsLatestByUID(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	cid := reliable.CommandID{1}
	apply := func(uid uint64, s reliable.BlockStateValue) {
		cur, _ := f.store.Cursor(ctx)
		if _, err := f.store.Apply(ctx, cur, cur+1, nil, []StateReport{{DeviceID: "DEV-1", EventUID: uid, Position: cur + 1,
			DeviceTime: t0, Block: &reliable.BlockState{State: s, Cause: reliable.CauseCommand, CommandID: cid}}}); err != nil {
			t.Fatal(err)
		}
	}
	apply(10, reliable.Blocked)
	apply(9, reliable.Unblocked) // older report stored later
	st, _ := f.store.DeviceReportedState(ctx, "DEV-1")
	if st.Block != reliable.Blocked {
		t.Fatalf("reported state %v, want blocked", st.Block)
	}
}
