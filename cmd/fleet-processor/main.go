// Command fleet-processor reads the events stored by one or more
// fleet-gateway processes, orders them chronologically, feeds them to the
// fusion engine and persists the journal, estimates and diagnostics.
//
// Example:
//
//	fleet-gateway   -listen :7400 -db events.db
//	fleet-gateway   -listen :7401 -db events.db
//	fleet-processor -events events.db -results results.db
//
// Only one processor may own a results database; a second one takes over
// and the first stops. On start the journal is replayed and verified, so a
// restart continues exactly where the previous run left off. With -verify,
// the replay is done and the command exits.
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
	"syscall"
	"time"

	"github.com/flavioanjo/fleetcore/internal/chrono"
	"github.com/flavioanjo/fleetcore/internal/eventstore"
	"github.com/flavioanjo/fleetcore/internal/processor"
	"github.com/flavioanjo/fleetcore/internal/resultstore"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stderr))
}

// run returns the exit code: 0 clean shutdown, 1 runtime failure, 2 usage
// error.
func run(ctx context.Context, args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("fleet-processor", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		eventsPath    = fs.String("events", "fleetcore-events.db", "event store written by fleet-gateway")
		resultsPath   = fs.String("results", "fleetcore-results.db", "results database (created if missing)")
		hold          = fs.Duration("hold", 2*time.Second, "processing time a device's next event is held for late, earlier events")
		tolerance     = fs.Duration("session-tolerance", time.Second, "device-time difference below which two sessions' order is taken from ingestion order")
		maxHeld       = fs.Int("max-held", 65536, "maximum events held by the orderer")
		tick          = fs.Duration("tick", time.Second, "interval between fusion Engine.Tick calls")
		poll          = fs.Duration("poll", 50*time.Millisecond, "interval between processing cycles")
		rebuild       = fs.Bool("rebuild", false, "rewrite stored outputs that a replay does not reproduce (after an intentional fusion change)")
		verify        = fs.Bool("verify", false, "replay and verify the journal, then exit")
		statsInterval = fs.Duration("stats-interval", time.Minute, "interval between statistics log lines (0 disables)")
		logLevel      = fs.String("log-level", "info", "log level: debug, info, warn, error")
		logFormat     = fs.String("log-format", "json", "log format: json or text")
	)
	fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: fleet-processor [flags]")
		fmt.Fprintln(stderr, "Orders stored events chronologically, fuses them and persists the results.")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "fleet-processor: unexpected argument %q\n", fs.Arg(0))
		return 2
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		fmt.Fprintf(stderr, "fleet-processor: -log-level: %v\n", err)
		return 2
	}
	var logger *slog.Logger
	switch *logFormat {
	case "json":
		logger = slog.New(slog.NewJSONHandler(stderr, &slog.HandlerOptions{Level: level}))
	case "text":
		logger = slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: level}))
	default:
		fmt.Fprintln(stderr, "fleet-processor: -log-format must be json or text")
		return 2
	}
	if *hold < 0 || *tolerance < 0 || *tick <= 0 || *poll <= 0 || *maxHeld <= 0 {
		fmt.Fprintln(stderr, "fleet-processor: durations and limits must be positive")
		return 2
	}

	events, err := eventstore.Open(ctx, *eventsPath, eventstore.Options{})
	if err != nil {
		logger.Error("cannot open event store", "db", *eventsPath, "err", err)
		return 1
	}
	defer events.Close()
	results, err := resultstore.Open(ctx, *resultsPath, resultstore.Options{})
	if err != nil {
		logger.Error("cannot open results database", "db", *resultsPath, "err", err)
		return 1
	}
	defer results.Close()

	p, err := processor.New(processor.Config{
		Chrono:       chrono.Config{Hold: *hold, SessionTolerance: *tolerance, MaxHeld: *maxHeld},
		TickInterval: *tick, PollInterval: *poll, Rebuild: *rebuild, Logger: logger,
	}, events, results)
	if err != nil {
		logger.Error("invalid configuration", "err", err)
		return 2
	}
	if _, err := p.Recover(ctx); err != nil {
		logger.Error("recovery failed", "err", err)
		return 1
	}
	if *verify {
		logger.Info("journal verified")
		return 0
	}
	if *statsInterval > 0 {
		go func() {
			t := time.NewTicker(*statsInterval)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					st := p.Stats()
					logger.Info("stats", "processed", st.Processed, "ticks", st.Ticks, "held", st.Held,
						"flags", st.Flags, "low_position", st.LowPosition, "high_position", st.HighPosition)
				}
			}
		}()
	}
	if err := p.Run(ctx); err != nil {
		logger.Error("processor failed", "err", err)
		return 1
	}
	return 0
}
