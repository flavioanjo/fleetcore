// Command fleet-command provisions keys, requests vehicle commands and
// inspects their lifecycle and critical-event alerts.
//
//	fleet-command keygen  -dir ./data -devices TRK-1,TRK-2
//	fleet-command request -data ./data -principal ops:alice -roles security_operator \
//	    -device TRK-1 -type BLOCK -ttl 2m -reason "stolen vehicle report 123"
//	fleet-command show    -data ./data -id COMMAND_ID
//	fleet-command list    -data ./data [-device TRK-1] [-attention]
//	fleet-command alerts  -data ./data
//	fleet-command ack     -data ./data -device TRK-1 -uid 42 -operator ops:dave -note "driver reached"
//
// The principal and roles flags stand in for an authenticated operator
// identity, which a production deployment takes from its identity
// provider. Commands are signed with the authority key in
// DATA/authority.json.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/flavioanjo/fleetcore/internal/alerting"
	"github.com/flavioanjo/fleetcore/internal/commandstore"
	"github.com/flavioanjo/fleetcore/internal/eventstore"
	"github.com/flavioanjo/fleetcore/internal/reliable"
	"github.com/flavioanjo/fleetcore/internal/security"
)

func main() { os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr)) }

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: fleet-command keygen|request|show|list|alerts|ack [flags]")
		return 2
	}
	fs := flag.NewFlagSet("fleet-command "+args[0], flag.ContinueOnError)
	fs.SetOutput(stderr)
	data := fs.String("data", ".", "data directory (commands.db, events.db, alerts.db, authority.json)")
	var err error
	switch args[0] {
	case "keygen":
		dir := fs.String("dir", ".", "output directory")
		devs := fs.String("devices", "", "comma-separated device ids")
		if fs.Parse(args[1:]) != nil {
			return 2
		}
		err = keygen(stdout, *dir, *devs)
	case "request":
		principal := fs.String("principal", "", "requesting operator identity")
		roles := fs.String("roles", "", "comma-separated roles of the principal")
		dev := fs.String("device", "", "target device id")
		typ := fs.String("type", "", "BLOCK, UNBLOCK, OPEN_CARGO or REQUEST_POSITION")
		ttl := fs.Duration("ttl", 2*time.Minute, "execution deadline from now")
		reason := fs.String("reason", "", "justification (required for actuations)")
		comp := fs.Int("compartment", 1, "OPEN_CARGO compartment (1-4)")
		maxAge := fs.Duration("max-age", time.Minute, "REQUEST_POSITION: oldest acceptable fix")
		reqID := fs.String("request-id", "", "idempotency key (default random); repeat it to retry safely")
		if fs.Parse(args[1:]) != nil {
			return 2
		}
		err = request(ctx, stdout, *data, *principal, *roles, *dev, *typ, *ttl, *reason, *comp, *maxAge, *reqID)
	case "show":
		id := fs.String("id", "", "command id")
		if fs.Parse(args[1:]) != nil {
			return 2
		}
		err = show(ctx, stdout, *data, *id)
	case "list":
		dev := fs.String("device", "", "only this device")
		att := fs.Bool("attention", false, "only commands needing operator attention")
		if fs.Parse(args[1:]) != nil {
			return 2
		}
		err = list(ctx, stdout, *data, *dev, *att)
	case "alerts", "ack":
		dev := fs.String("device", "", "device id (ack)")
		uid := fs.Uint64("uid", 0, "event uid (ack)")
		op := fs.String("operator", "", "acknowledging operator (ack)")
		note := fs.String("note", "", "note (ack)")
		if fs.Parse(args[1:]) != nil {
			return 2
		}
		err = alerts(ctx, stdout, *data, args[0] == "ack", *dev, *uid, *op, *note)
	default:
		fmt.Fprintf(stderr, "fleet-command: unknown subcommand %q\n", args[0])
		return 2
	}
	if err != nil {
		fmt.Fprintln(stderr, "fleet-command:", err)
		return 1
	}
	return 0
}

func keygen(w io.Writer, dir, devs string) error {
	if devs == "" {
		return errors.New("-devices is required")
	}
	var keys []security.DeviceKey
	for _, d := range strings.Split(devs, ",") {
		s, err := security.GenerateSecret()
		if err != nil {
			return err
		}
		keys = append(keys, security.DeviceKey{DeviceID: strings.TrimSpace(d), KeyID: 1, Secret: s, State: security.KeyActive})
	}
	if err := security.WriteKeyFile(filepath.Join(dir, "keys.json"), keys); err != nil {
		return err
	}
	a, err := security.NewAuthority(1)
	if err != nil {
		return err
	}
	pub, err := security.WriteAuthorityFile(filepath.Join(dir, "authority.json"), a)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "authority.pub"), []byte(pub+"\n"), 0o644); err != nil {
		return err
	}
	fmt.Fprintf(w, "wrote %s (device keys, secret), %s (authority private key, secret) and %s (public key for devices)\n",
		filepath.Join(dir, "keys.json"), filepath.Join(dir, "authority.json"), filepath.Join(dir, "authority.pub"))
	return nil
}

func openCommands(ctx context.Context, data string) (*commandstore.Store, error) {
	a, err := security.LoadAuthorityFile(filepath.Join(data, "authority.json"))
	if err != nil {
		return nil, err
	}
	return commandstore.Open(ctx, filepath.Join(data, "commands.db"), a, commandstore.Options{})
}

func request(ctx context.Context, w io.Writer, data, principal, roles, dev, typ string, ttl time.Duration, reason string, comp int, maxAge time.Duration, reqID string) error {
	ct, err := reliable.ParseCommandType(typ)
	if err != nil {
		return err
	}
	var args []byte
	switch ct {
	case reliable.CommandOpenCargo:
		args = reliable.CargoArgs(uint8(comp))
	case reliable.CommandRequestPosition:
		args = reliable.PositionArgs(maxAge)
	}
	if reqID == "" {
		var b [8]byte
		rand.Read(b[:]) //nolint:errcheck
		reqID = hex.EncodeToString(b[:])
	}
	cs, err := openCommands(ctx, data)
	if err != nil {
		return err
	}
	defer cs.Close()
	var rl []string
	if roles != "" {
		rl = strings.Split(roles, ",")
	}
	c, created, err := cs.Create(ctx, commandstore.Request{RequestID: reqID, Principal: commandstore.Principal{ID: principal, Roles: rl},
		DeviceID: dev, Type: ct, Args: args, TTL: ttl, Justification: reason})
	if err != nil {
		return err
	}
	verb := "created"
	if !created {
		verb = "already exists (same request id)"
	}
	fmt.Fprintf(w, "command %s %s: %s for %s, expires %s, state %s\n", c.ID, verb, c.Type, c.DeviceID, c.ExpiresAt.Format(time.RFC3339), c.State)
	return nil
}

func show(ctx context.Context, w io.Writer, data, id string) error {
	cid, err := reliable.ParseCommandID(id)
	if err != nil {
		return err
	}
	cs, err := openCommands(ctx, data)
	if err != nil {
		return err
	}
	defer cs.Close()
	c, err := cs.Get(ctx, cid)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "command %s\n  type %s  device %s  principal %s (%s)\n  state %s %s  needs_attention=%v\n  device stage %s reason %s observed %d\n",
		c.ID, c.Type, c.DeviceID, c.Principal, strings.Join(c.Roles, ","), c.State, c.StateReason, c.NeedsAttention, c.DeviceStage, c.DeviceReason, c.Observed)
	fmt.Fprintln(w, "  note: device-reported outcome; not an independent verification of the vehicle")
	h, _ := cs.History(ctx, cid)
	for _, e := range h {
		fmt.Fprintf(w, "  %s  %-9s -> %-9s  [%s] %s\n", e.At.Format("15:04:05.000"), e.From, e.To, e.Source, e.Detail)
	}
	at, _ := cs.Attempts(ctx, cid)
	for _, a := range at {
		fmt.Fprintf(w, "  attempt %d at %s: %s %s\n", a.N, a.StartedAt.Format("15:04:05.000"), a.Outcome, a.Detail)
	}
	rs, _ := cs.Reports(ctx, cid)
	for _, r := range rs {
		fmt.Fprintf(w, "  report uid %d: %s (%s) -> %s\n", r.EventUID, r.Stage, r.Reason, r.Verdict)
	}
	return nil
}

func list(ctx context.Context, w io.Writer, data, dev string, att bool) error {
	cs, err := openCommands(ctx, data)
	if err != nil {
		return err
	}
	defer cs.Close()
	l, err := cs.List(ctx, commandstore.Filter{DeviceID: dev, NeedsAttention: att})
	if err != nil {
		return err
	}
	for _, c := range l {
		fmt.Fprintf(w, "%s  %-16s %-8s %-9s attention=%v  %s\n", c.ID, c.Type, c.DeviceID, c.State, c.NeedsAttention, c.StateReason)
	}
	return nil
}

func alerts(ctx context.Context, w io.Writer, data string, ack bool, dev string, uid uint64, op, note string) error {
	ev, err := eventstore.Open(ctx, filepath.Join(data, "events.db"), eventstore.Options{})
	if err != nil {
		return err
	}
	defer ev.Close()
	c, err := alerting.Open(ctx, alerting.Config{Path: filepath.Join(data, "alerts.db"), Events: ev, Sink: alerting.LogSink{}})
	if err != nil {
		return err
	}
	defer c.Close()
	if ack {
		if err := c.Acknowledge(ctx, dev, uid, op, note); err != nil {
			return err
		}
		fmt.Fprintf(w, "alert %s/%d acknowledged by %s\n", dev, uid, op)
		return nil
	}
	l, err := c.List(ctx, 1000)
	if err != nil {
		return err
	}
	for _, e := range l {
		fmt.Fprintf(w, "%s/%d  %-18s %-12s notified=%s acked_by=%s\n", e.DeviceID, e.UID, e.Type, e.State,
			e.NotifiedAt.Format(time.RFC3339), e.AckedBy)
	}
	return nil
}
