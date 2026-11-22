// Command fleet-gateway receives FleetCore telemetry packets over UDP,
// validates and deduplicates them, and stores accepted events in a local
// SQLite database.
//
// Example:
//
//	fleet-gateway -listen 127.0.0.1:7400 -db fleetcore-events.db
//
// It stops gracefully on SIGINT or SIGTERM: it stops reading, stores every
// queued event (bounded by -drain-timeout) and closes the database.
//
// Packets are not authenticated: their CRC-32C detects corruption only. Do
// not expose the listener to untrusted networks.
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

	"github.com/flavioanjo/fleetcore/internal/eventstore"
	"github.com/flavioanjo/fleetcore/internal/gateway"
	"github.com/flavioanjo/fleetcore/internal/protocol"
	"github.com/flavioanjo/fleetcore/internal/sessions"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stderr, nil))
}

// run executes the gateway until ctx is cancelled and returns the process
// exit code: 0 clean shutdown, 1 runtime failure, 2 usage error. If ready
// is non-nil it receives the bound address once the gateway is listening.
func run(ctx context.Context, args []string, stderr io.Writer, ready chan<- string) int {
	fs := flag.NewFlagSet("fleet-gateway", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		listen        = fs.String("listen", ":7400", "UDP listen address")
		dbPath        = fs.String("db", "fleetcore-events.db", "SQLite database file (created if missing)")
		maxDatagram   = fs.Int("max-datagram", protocol.MaxPacketSize, fmt.Sprintf("largest accepted datagram in bytes (%d-%d)", protocol.MinPacketSize, protocol.MaxPacketSize))
		readBuffer    = fs.Int("read-buffer", 0, "socket receive buffer in bytes (0 = OS default)")
		queue         = fs.Int("queue", 4096, "decode queue capacity; datagrams beyond it are dropped")
		workers       = fs.Int("workers", 0, "decode workers (0 = GOMAXPROCS)")
		storeQueue    = fs.Int("store-queue", 4096, "store queue capacity")
		batch         = fs.Int("batch", 256, "maximum events per store transaction")
		batchDelay    = fs.Duration("batch-delay", 10*time.Millisecond, "longest wait for a batch to fill")
		drainTimeout  = fs.Duration("drain-timeout", 10*time.Second, "longest wait to store queued events on shutdown")
		busyTimeout   = fs.Duration("busy-timeout", 5*time.Second, "SQLite busy timeout")
		dedupCache    = fs.Int("dedup-cache", 65536, "recently stored identities cached in memory (negative disables)")
		maxDevices    = fs.Int("max-devices", 100_000, "devices tracked by the session registry")
		sessionIdle   = fs.Duration("session-idle", time.Hour, "retire a session after this much inactivity")
		deviceIdle    = fs.Duration("device-idle", 24*time.Hour, "forget a device after this much inactivity")
		statsInterval = fs.Duration("stats-interval", time.Minute, "interval between statistics log lines (0 disables)")
		logLevel      = fs.String("log-level", "info", "log level: debug, info, warn, error")
		logFormat     = fs.String("log-format", "json", "log format: json or text")
	)
	fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: fleet-gateway [flags]")
		fmt.Fprintln(stderr, "Receives FleetCore UDP telemetry and stores it in SQLite.")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "fleet-gateway: unexpected argument %q\n", fs.Arg(0))
		return 2
	}

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		fmt.Fprintf(stderr, "fleet-gateway: -log-level: %v\n", err)
		return 2
	}
	hopts := &slog.HandlerOptions{Level: level}
	var logger *slog.Logger
	switch *logFormat {
	case "json":
		logger = slog.New(slog.NewJSONHandler(stderr, hopts))
	case "text":
		logger = slog.New(slog.NewTextHandler(stderr, hopts))
	default:
		fmt.Fprintf(stderr, "fleet-gateway: -log-format must be json or text\n")
		return 2
	}

	store, err := eventstore.Open(ctx, *dbPath, eventstore.Options{BusyTimeout: *busyTimeout})
	if err != nil {
		logger.Error("cannot open event store", "db", *dbPath, "err", err)
		return 1
	}
	defer func() {
		if err := store.Close(); err != nil {
			logger.Error("closing event store", "err", err)
		}
	}()

	reg := sessions.New(sessions.Config{
		MaxDevices:         *maxDevices,
		SessionIdleTimeout: *sessionIdle,
		DeviceIdleTimeout:  *deviceIdle,
	})
	gw, err := gateway.New(gateway.Config{
		Addr:            *listen,
		MaxDatagramSize: *maxDatagram,
		ReadBuffer:      *readBuffer,
		QueueSize:       *queue,
		Workers:         *workers,
		StoreQueueSize:  *storeQueue,
		BatchSize:       *batch,
		BatchDelay:      *batchDelay,
		DrainTimeout:    *drainTimeout,
		DedupCacheSize:  *dedupCache,
		Logger:          logger,
	}, store, reg)
	if err != nil {
		logger.Error("cannot start gateway", "err", err)
		return 2
	}
	logger.Info("event store ready", "db", *dbPath, "schema_version", eventstore.SchemaVersion())
	if ready != nil {
		ready <- gw.Addr().String()
	}

	if *statsInterval > 0 {
		go logStats(ctx, logger, gw, reg, *statsInterval)
	}
	if err := gw.Run(ctx); err != nil {
		logger.Error("gateway failed", "err", err)
		return 1
	}
	return 0
}

func logStats(ctx context.Context, logger *slog.Logger, gw *gateway.Gateway, reg *sessions.Registry, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			st, rs := gw.Stats(), reg.Stats()
			logger.Info("stats",
				"received", st.Received, "inserted", st.Inserted, "duplicates", st.Duplicates,
				"conflicts", st.Conflicts, "rejected", st.RejectedBy, "queue_full", st.QueueFull,
				"store_failed", st.StoreFailed, "diagnostics", st.Diagnostics,
				"devices", rs.Devices, "sessions", rs.Sessions)
		}
	}
}
