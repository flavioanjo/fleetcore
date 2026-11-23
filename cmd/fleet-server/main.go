// Command fleet-server runs the reliable communications server side in one
// process: one or more protocol v2 gateways (UDP) sharing one event store,
// the command dispatcher (which sends deliveries through those gateways)
// and the alerting consumer for critical events.
//
//	fleet-server -listen 127.0.0.1:7500,127.0.0.1:7501 -data ./data \
//	    -keys keys.json -authority authority.json
//
// keys.json holds device keys and authority.json the command authority's
// private key (both created by `fleet-command keygen`). They are secrets:
// keep them 0600 and out of version control; they are never logged.
//
// Several fleet-server processes may share the data directory; each
// dispatcher then only reaches devices whose frames arrive at its own
// gateways, and records the other attempts as unreachable.
//
// Supervision: the gateways, the dispatcher and the alerting consumer are
// essential. If any of them stops on its own (error, panic, or return
// without a stop request), the server stops all of them and exits with
// status 1, logging which service failed and why: it never keeps running
// with command delivery or alert processing stopped. On SIGINT/SIGTERM
// every service finishes its work in progress (bounded by -shutdown-grace)
// and the server exits 0. The stores are closed only after every service
// has returned. Exit status 2 is a usage or configuration error.
//
// This is demonstration software: alerts go to the log (LogSink), which is
// not an emergency notification service; see README.md, "Deployment
// blockers".
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/flavioanjo/fleetcore/internal/alerting"
	"github.com/flavioanjo/fleetcore/internal/commandstore"
	"github.com/flavioanjo/fleetcore/internal/dispatcher"
	"github.com/flavioanjo/fleetcore/internal/eventstore"
	"github.com/flavioanjo/fleetcore/internal/gateway"
	"github.com/flavioanjo/fleetcore/internal/security"
	"github.com/flavioanjo/fleetcore/internal/sessions"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stderr, nil))
}

// wrapService, when set by tests, wraps each essential service (fault
// injection). Nil in production.
var wrapService func(name string, run func(context.Context) error) func(context.Context) error

// closer is a store to close at shutdown, in order.
type closer struct {
	name  string
	close func() error
}

func closeAll(log *slog.Logger, cs []closer) bool {
	ok := true
	for i := len(cs) - 1; i >= 0; i-- {
		if err := cs[i].close(); err != nil {
			log.Error("closing store failed", "store", cs[i].name, "err", err)
			ok = false
		}
	}
	return ok
}

func run(ctx context.Context, args []string, stderr io.Writer, ready chan<- []string) int {
	fs := flag.NewFlagSet("fleet-server", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		listen    = fs.String("listen", "127.0.0.1:7500", "comma-separated UDP addresses, one gateway each")
		data      = fs.String("data", ".", "directory of events.db, commands.db and alerts.db")
		keysPath  = fs.String("keys", "keys.json", "device key file (secret)")
		authPath  = fs.String("authority", "authority.json", "command authority private key (secret)")
		interval  = fs.Duration("interval", 200*time.Millisecond, "dispatcher and alerting period")
		logLevel  = fs.String("log-level", "info", "debug, info, warn or error")
		statsEach = fs.Duration("stats-interval", 30*time.Second, "statistics log period (0 disables)")
		grace     = fs.Duration("shutdown-grace", 45*time.Second, "how long services may take to finish work in progress when stopping")
	)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		fmt.Fprintln(stderr, "fleet-server: -log-level:", err)
		return 2
	}
	log := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: level}))
	keys, err := security.LoadKeyFile(*keysPath)
	if err != nil {
		log.Error("cannot load device keys", "err", err)
		return 2
	}
	auth, err := security.LoadAuthorityFile(*authPath)
	if err != nil {
		log.Error("cannot load command authority", "err", err)
		return 2
	}

	// Stores, closed in reverse order once nothing uses them any more.
	var stores []closer
	fail := func(code int, msg string, err error) int {
		log.Error(msg, "err", err)
		closeAll(log, stores)
		return code
	}
	events, err := eventstore.Open(ctx, filepath.Join(*data, "events.db"), eventstore.Options{})
	if err != nil {
		return fail(1, "cannot open event store", err)
	}
	stores = append(stores, closer{"events", events.Close})
	cmds, err := commandstore.Open(ctx, filepath.Join(*data, "commands.db"), auth, commandstore.Options{})
	if err != nil {
		return fail(1, "cannot open command store", err)
	}
	stores = append(stores, closer{"commands", cmds.Close})
	alerts, err := alerting.Open(ctx, alerting.Config{Path: filepath.Join(*data, "alerts.db"), Events: events,
		Sink: alerting.LogSink{Logger: log}, Logger: log, Interval: *interval})
	if err != nil {
		return fail(1, "cannot open alert store", err)
	}
	stores = append(stores, closer{"alerts", alerts.Close})

	var gws []*gateway.Gateway
	var transports []dispatcher.Named
	var addrs []string
	releaseGateways := func() { // gateways created but never run: close their sockets
		done, cancel := context.WithCancel(context.Background())
		cancel()
		for _, g := range gws {
			g.Run(done) //nolint:errcheck
		}
	}
	for i, a := range strings.Split(*listen, ",") {
		id := fmt.Sprintf("gw-%d", i+1)
		g, err := gateway.New(gateway.Config{Addr: strings.TrimSpace(a), Logger: log.With("gateway", id),
			Reliable: &gateway.ReliableConfig{Keys: keys, Store: events, GatewayID: id}}, events, sessions.New(sessions.Config{}))
		if err != nil {
			releaseGateways()
			return fail(2, "cannot start gateway "+a, err)
		}
		gws = append(gws, g)
		transports = append(transports, dispatcher.Named{Name: id, Transport: g})
		addrs = append(addrs, g.Addr().String())
	}
	disp, err := dispatcher.New(dispatcher.Config{Commands: cmds, Events: events, Transports: transports, Interval: *interval, Logger: log})
	if err != nil {
		releaseGateways()
		return fail(2, "cannot start dispatcher", err)
	}

	services := make([]service, 0, len(gws)+3)
	for i, g := range gws {
		services = append(services, service{fmt.Sprintf("gateway gw-%d", i+1), g.Run})
	}
	services = append(services, service{"dispatcher", disp.Run}, service{"alerting", alerts.Run})
	if *statsEach > 0 {
		services = append(services, service{"stats", func(ctx context.Context) error {
			t := time.NewTicker(*statsEach)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return nil
				case <-t.C:
					for i, g := range gws {
						s := g.ReliableStats()
						log.Info("v2 stats", "gateway", i+1, "received", s.Received.Load(), "stored", s.Stored.Load(),
							"duplicates", s.Duplicates.Load(), "conflicts", s.Conflicts.Load(), "commands_sent", s.CommandsSent.Load())
					}
				}
			}
		}})
	}
	if wrapService != nil {
		for i := range services {
			services[i].run = wrapService(services[i].name, services[i].run)
		}
	}

	log.Info("fleet-server ready", "gateways", addrs, "data", *data, "authority_key_id", auth.KeyID())
	if ready != nil {
		ready <- addrs
	}
	failure, stopped := supervise(ctx, log, *grace, services)
	if !stopped {
		// Some service still uses the stores; closing them now would race
		// with it. The process exit releases them; SQLite commits are
		// durable, so nothing committed is lost.
		log.Error("fleet-server stopped", "status", "failed", "cause", failure, "stores", "left open (services still running)")
		return 1
	}
	closed := closeAll(log, stores)
	switch {
	case failure != nil:
		log.Error("fleet-server stopped", "status", "failed", "cause", failure, "stores_closed", closed)
		return 1
	case !closed:
		log.Error("fleet-server stopped", "status", "failed", "cause", "closing a store failed")
		return 1
	}
	log.Info("fleet-server stopped", "status", "ok")
	return 0
}
