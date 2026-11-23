// Command fleet-reliable-demo runs narrated, deterministic demonstrations
// of the reliable communications system in the simulated world
// (internal/simnet): real stores and production logic, simulated time,
// network and vehicle.
//
//	fleet-reliable-demo                # all scenarios
//	fleet-reliable-demo -scenario block,panic -seed 7
//
// Scenarios: block, unblock, open-cargo, request-position, panic,
// lost-acks, refused (moving vehicle, unauthorized, forged, expired).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/flavioanjo/fleetcore/internal/commandstore"
	"github.com/flavioanjo/fleetcore/internal/reliable"
	"github.com/flavioanjo/fleetcore/internal/security"
	"github.com/flavioanjo/fleetcore/internal/simnet"
)

var (
	secOp   = commandstore.Principal{ID: "ops:alice", Roles: []string{"security_operator", "fleet_viewer"}}
	cargoOp = commandstore.Principal{ID: "ops:bob", Roles: []string{"cargo_operator"}}
	viewer  = commandstore.Principal{ID: "ops:carol", Roles: []string{"fleet_viewer"}}
)

const step = 250 * time.Millisecond

func main() { os.Exit(run(os.Args[1:], os.Stdout)) }

func run(args []string, out io.Writer) int {
	fs := flag.NewFlagSet("fleet-reliable-demo", flag.ContinueOnError)
	scen := fs.String("scenario", "all", "comma-separated scenarios, or all")
	seed := fs.Int64("seed", 1, "simulation seed")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	all := []string{"block", "unblock", "open-cargo", "request-position", "panic", "lost-acks", "refused"}
	list := all
	if *scen != "all" {
		list = strings.Split(*scen, ",")
	}
	for _, s := range list {
		dir, err := os.MkdirTemp("", "fleet-demo-*")
		if err != nil {
			fmt.Fprintln(out, err)
			return 1
		}
		err = demo(out, dir, s, *seed)
		os.RemoveAll(dir)
		if err != nil {
			fmt.Fprintf(out, "\n!! scenario %s FAILED: %v\n", s, err)
			return 1
		}
	}
	return 0
}

type narrator struct {
	out io.Writer
	w   *simnet.World
	t0  time.Time
}

func (n narrator) say(format string, args ...any) {
	fmt.Fprintf(n.out, "  [t+%6.2fs] %s\n", n.w.Now().Sub(n.t0).Seconds(), fmt.Sprintf(format, args...))
}

func (n narrator) waitCommand(id reliable.CommandID, limit time.Duration, states ...commandstore.State) (commandstore.Command, error) {
	last := commandstore.State("")
	ok, err := n.w.RunUntil(limit, step, func() bool {
		c := n.w.Command(id)
		if c.State != last {
			n.say("server: command %s is %s%s", c.Type, c.State, suffix(c))
			last = c.State
		}
		for _, s := range states {
			if c.State == s {
				return true
			}
		}
		return false
	})
	if err != nil {
		return commandstore.Command{}, err
	}
	if !ok {
		return n.w.Command(id), fmt.Errorf("command stuck in %s", last)
	}
	return n.w.Command(id), nil
}

func suffix(c commandstore.Command) string {
	if c.DeviceStage == 0 {
		return ""
	}
	return fmt.Sprintf(" (device reported %s, reason %s)", c.DeviceStage, c.DeviceReason)
}

func demo(out io.Writer, dir, name string, seed int64) error {
	ctx := context.Background()
	w, err := simnet.New(ctx, simnet.Config{Dir: dir, Seed: seed})
	if err != nil {
		return err
	}
	defer w.Close()
	n := narrator{out: out, w: w, t0: w.Now()}
	d, err := w.AddDevice("TRK-1")
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "\n== %s ==\n", name)
	if err := w.Run(time.Second, step); err != nil {
		return err
	}
	n.say("device TRK-1 booted (session %d) and announced itself to gw-a", d.Dev.SessionGen())
	switch name {
	case "block", "unblock":
		n.say("vehicle parked: speed 0, ignition off; immobilizer %s", d.Vehicle.ImmobilizerState())
		c, err := w.Request("demo-block", secOp, "TRK-1", reliable.CommandBlock, nil, 2*time.Minute)
		if err != nil {
			return err
		}
		n.say("%s requests BLOCK (role security_operator); command %s signed by the authority", secOp.ID, c.ID)
		if c, err = n.waitCommand(c.ID, time.Minute, commandstore.Succeeded); err != nil {
			return err
		}
		n.say("ground truth: immobilizer blocked=%v, actuator driven %d time(s)", d.Vehicle.Truth().Blocked, d.Vehicle.Truth().ImmobilizerDrives)
		h, _ := w.Commands.History(ctx, c.ID)
		for _, e := range h {
			n.say("  history: %-9s -> %-9s [%s] %s", e.From, e.To, e.Source, e.Detail)
		}
		w.Run(3*time.Second, step) //nolint:errcheck
		rs, _ := w.Commands.DeviceReportedState(ctx, "TRK-1")
		n.say("server: last BLOCK_STATE reported by the device: %s (device claim, not independent verification)", rs.Block)
		if name == "unblock" {
			u, err := w.Request("demo-unblock", secOp, "TRK-1", reliable.CommandUnblock, nil, 2*time.Minute)
			if err != nil {
				return err
			}
			n.say("%s requests UNBLOCK", secOp.ID)
			if _, err := n.waitCommand(u.ID, time.Minute, commandstore.Succeeded); err != nil {
				return err
			}
			w.Run(3*time.Second, step) //nolint:errcheck
			rs, _ := w.Commands.DeviceReportedState(ctx, "TRK-1")
			n.say("ground truth blocked=%v; reported BLOCK_STATE %s; actuator moved %d time(s) in total",
				d.Vehicle.Truth().Blocked, rs.Block, d.Vehicle.Truth().ImmobilizerMoves)
		}
	case "open-cargo":
		c, err := w.Request("demo-cargo", cargoOp, "TRK-1", reliable.CommandOpenCargo, reliable.CargoArgs(1), 2*time.Minute)
		if err != nil {
			return err
		}
		n.say("%s requests OPEN_CARGO compartment 1 (role cargo_operator)", cargoOp.ID)
		if _, err := n.waitCommand(c.ID, time.Minute, commandstore.Succeeded); err != nil {
			return err
		}
		w.Run(3*time.Second, step) //nolint:errcheck
		rs, _ := w.Commands.DeviceReportedState(ctx, "TRK-1")
		n.say("CARGO_STATE: door %s, latch %s; success required the door sensor, not just the latch actuation", rs.Door, rs.Latch)
	case "request-position":
		c, err := w.Request("demo-pos-1", viewer, "TRK-1", reliable.CommandRequestPosition, reliable.PositionArgs(30*time.Second), time.Minute)
		if err != nil {
			return err
		}
		n.say("REQUEST_POSITION with no GNSS fix")
		if _, err := n.waitCommand(c.ID, time.Minute, commandstore.Failed); err != nil {
			return err
		}
		n.say("no position was fabricated: the result is an explicit position_unavailable")
		d.Vehicle.SetFix(reliable.PositionFix{Fix: reliable.Fix3D, LatE7: -235505200, LonE7: -466333100, HDOPx10: 8, Satellites: 11}, true)
		c2, _ := w.Request("demo-pos-2", viewer, "TRK-1", reliable.CommandRequestPosition, reliable.PositionArgs(30*time.Second), time.Minute)
		n.say("GNSS fix acquired; REQUEST_POSITION again")
		if _, err := n.waitCommand(c2.ID, time.Minute, commandstore.Succeeded); err != nil {
			return err
		}
		w.Run(2*time.Second, step) //nolint:errcheck
		recs, _ := w.Events.ReadReliableAfter(ctx, 0, 100, reliable.EventPosition)
		if len(recs) == 1 {
			b, _ := recs[0].Body()
			p := b.(reliable.Position)
			n.say("POSITION stored: %.5f, %.5f fix %s, answering command %s", float64(p.LatE7)/1e7, float64(p.LonE7)/1e7, p.Fix, p.RequestID)
		}
	case "panic":
		d.Vehicle.SetFix(reliable.PositionFix{Fix: reliable.Fix3D, LatE7: -229068000, LonE7: -431729000, HDOPx10: 7, Satellites: 12}, true)
		d.Offline = true
		uid, err := d.Dev.Panic(ctx)
		if err != nil {
			return err
		}
		n.say("network lost; PANIC pressed: event uid %d durably queued with the last fix", uid)
		w.Run(20*time.Second, step) //nolint:errcheck
		st, _, _ := d.Dev.EventState(ctx, uid)
		n.say("20 s offline: panic is %s on the device", st)
		d.Crash()
		n.say("device power cycle")
		w.Run(5*time.Second, step) //nolint:errcheck
		if err := d.Start(); err != nil {
			return err
		}
		st, _, _ = d.Dev.EventState(ctx, uid)
		n.say("device rebooted (session %d): panic still %s in the durable queue", d.Dev.SessionGen(), st)
		d.Offline = false
		n.say("network restored")
		w.RunUntil(time.Minute, step, func() bool { s, _, _ := d.Dev.EventState(ctx, uid); return s == "acked" }) //nolint:errcheck
		w.Run(2*time.Second, step)                                                                                //nolint:errcheck
		cnt, _ := w.Events.CountReliable(ctx)
		as, _ := w.Alerts.Get(ctx, "TRK-1", uid)
		n.say("server stored %d PANIC row(s); device has its ACK; alert state: %s", cnt[reliable.EventPanic], as.State)
		n.say("ACK means durably stored, NOT handled: the alert waits for a human")
		if err := w.Alerts.Acknowledge(ctx, "TRK-1", uid, "ops:dave", "driver contacted"); err != nil {
			return err
		}
		as, _ = w.Alerts.Get(ctx, "TRK-1", uid)
		n.say("ops:dave acknowledges: alert %s", as.State)
	case "lost-acks":
		lostDeliveries, lostAcks := 0, 0
		w.Net.AddFilter(func(dg simnet.Datagram) bool {
			h, err := reliable.ParseHeader(dg.Data)
			switch {
			case dg.Uplink || err != nil:
			case h.Type == reliable.TypeCommandDelivery && lostDeliveries < 3:
				lostDeliveries++
				return false
			case h.Type == reliable.TypeEventAck && lostAcks < 6:
				lostAcks++
				return false
			}
			return true
		})
		c, _ := w.Request("demo-lost", secOp, "TRK-1", reliable.CommandBlock, nil, 2*time.Minute)
		n.say("the next 3 command deliveries and the next 6 event ACKs (of the device's COMMAND_RESULT reports) will be lost")
		if _, err := n.waitCommand(c.ID, 2*time.Minute, commandstore.Succeeded); err != nil {
			return err
		}
		w.Run(20*time.Second, step) //nolint:errcheck // let the device retransmit its unacknowledged reports
		at, _ := w.Commands.Attempts(ctx, c.ID)
		m := d.Dev.Metrics()
		reps, _ := w.Commands.Reports(ctx, c.ID)
		n.say("%d COMMAND_RESULT reports applied once each (%d rows); lost deliveries %d, lost ACKs %d", len(reps), len(reps), lostDeliveries, lostAcks)
		n.say("%d delivery attempts, %d duplicate deliveries seen by the device, %d event retransmissions", len(at), m.DuplicateDeliveries, m.Retransmissions)
		n.say("actuator driven %d time(s): retries never repeat execution", d.Vehicle.Truth().ImmobilizerDrives)
	case "refused":
		d.Vehicle.Drive(700, true)
		c, _ := w.Request("demo-moving", secOp, "TRK-1", reliable.CommandBlock, nil, time.Minute)
		n.say("vehicle moving at 70 km/h; BLOCK requested")
		if _, err := n.waitCommand(c.ID, time.Minute, commandstore.Rejected); err != nil {
			return err
		}
		d.Vehicle.Drive(0, false)
		if _, err := w.Request("demo-unauth", viewer, "TRK-1", reliable.CommandBlock, nil, time.Minute); errors.Is(err, commandstore.ErrUnauthorized) {
			n.say("%s (fleet_viewer) requests BLOCK: refused by authorization, audited, never signed or sent", viewer.ID)
		}
		rogue, _ := security.NewAuthority(1)
		now := time.UnixMilli(w.Now().UnixMilli()).UTC()
		cmd := reliable.Command{ID: reliable.CommandID{0xba, 0xd0}, DeviceID: "TRK-1", Type: reliable.CommandUnblock,
			IssuedAt: now, ExpiresAt: now.Add(time.Minute), Principal: "attacker"}
		w.Gateways[0].SendCommand(reliable.CommandDelivery{Command: cmd, Attempt: 1, AttemptSentAt: now, //nolint:errcheck
			AttemptNotAfter: now.Add(30 * time.Second), AuthorityKeyID: 1, Signature: rogue.Sign(cmd.Statement())})
		w.Run(3*time.Second, step) //nolint:errcheck
		v, _ := w.Events.ReadReliableAfter(ctx, 0, 10, reliable.EventSecurityViolation)
		n.say("UNBLOCK with a valid transport tag but a forged authority signature: not executed; %d SECURITY_VIOLATION reported", len(v))
		d.Offline = true
		for _, g := range w.Gateways {
			g.Reboot()
		}
		e, _ := w.Request("demo-expire", secOp, "TRK-1", reliable.CommandBlock, nil, 10*time.Second)
		n.say("device unreachable; BLOCK with a 10 s deadline")
		if _, err := n.waitCommand(e.ID, time.Minute, commandstore.Expired, commandstore.Unknown); err != nil {
			return err
		}
		n.say("never transmitted, so provably not executed: expired (needs attention). Actuator driven %d time(s)", d.Vehicle.Truth().ImmobilizerDrives)
	default:
		return fmt.Errorf("unknown scenario %q", name)
	}
	if as, _ := w.Alerts.Unacknowledged(ctx); as > 0 {
		n.say("note: %d critical alert(s) still awaiting operator acknowledgement", as)
	}
	return nil
}
