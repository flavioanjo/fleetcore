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
	"sync"
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
	events, err := eventstore.Open(ctx, filepath.Join(*data, "events.db"), eventstore.Options{})
	if err != nil {
		log.Error("cannot open event store", "err", err)
		return 1
	}
	defer events.Close()
	cmds, err := commandstore.Open(ctx, filepath.Join(*data, "commands.db"), auth, commandstore.Options{})
	if err != nil {
		log.Error("cannot open command store", "err", err)
		return 1
	}
	defer cmds.Close()
	alerts, err := alerting.Open(ctx, alerting.Config{Path: filepath.Join(*data, "alerts.db"), Events: events,
		Sink: alerting.LogSink{Logger: log}, Logger: log})
	if err != nil {
		log.Error("cannot open alert store", "err", err)
		return 1
	}
	defer alerts.Close()

	var gws []*gateway.Gateway
	var transports []dispatcher.Named
	var addrs []string
	for i, a := range strings.Split(*listen, ",") {
		id := fmt.Sprintf("gw-%d", i+1)
		g, err := gateway.New(gateway.Config{Addr: strings.TrimSpace(a), Logger: log.With("gateway", id),
			Reliable: &gateway.ReliableConfig{Keys: keys, Store: events, GatewayID: id}}, events, sessions.New(sessions.Config{}))
		if err != nil {
			log.Error("cannot start gateway", "addr", a, "err", err)
			return 2
		}
		gws = append(gws, g)
		transports = append(transports, dispatcher.Named{Name: id, Transport: g})
		addrs = append(addrs, g.Addr().String())
	}
	disp, err := dispatcher.New(dispatcher.Config{Commands: cmds, Events: events, Transports: transports, Interval: *interval, Logger: log})
	if err != nil {
		log.Error("cannot start dispatcher", "err", err)
		return 2
	}
	log.Info("fleet-server ready", "gateways", addrs, "data", *data, "authority_key_id", auth.KeyID())
	if ready != nil {
		ready <- addrs
	}
	var wg sync.WaitGroup
	errs := make(chan error, len(gws))
	for _, g := range gws {
		wg.Add(1)
		go func(g *gateway.Gateway) { defer wg.Done(); errs <- g.Run(ctx) }(g)
	}
	wg.Add(2)
	go func() { defer wg.Done(); disp.Run(ctx) }() //nolint:errcheck
	go func() {
		defer wg.Done()
		t := time.NewTicker(*interval)
		defer t.Stop()
		for {
			if _, _, err := alerts.Step(ctx); err != nil && ctx.Err() == nil {
				log.Warn("alerting step failed", "err", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	if *statsEach > 0 {
		go func() {
			t := time.NewTicker(*statsEach)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					for i, g := range gws {
						s := g.ReliableStats()
						log.Info("v2 stats", "gateway", i+1, "received", s.Received.Load(), "stored", s.Stored.Load(),
							"duplicates", s.Duplicates.Load(), "conflicts", s.Conflicts.Load(), "commands_sent", s.CommandsSent.Load())
					}
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			log.Error("gateway failed", "err", err)
			return 1
		}
	}
	return 0
}
