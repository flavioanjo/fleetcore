// Command fleet-device runs one simulated tracking device speaking
// protocol v2 over UDP: a durable outbound queue (device.db), periodic
// telemetry and positions, an optional PANIC, and command execution
// against a simulated vehicle (no real hardware is ever driven).
//
//	fleet-device -id TRK-1 -keys ./data/keys.json -authority-pub ./data/authority.pub \
//	    -gateway 127.0.0.1:7500 -db ./data/device-TRK-1.db -fix -23.55052,-46.63331 -panic-after 5s
//
// Flags simulate the vehicle: -speed (km/h) and -ignition gate BLOCK and
// OPEN_CARGO; -fix gives a GNSS fix (none by default, so REQUEST_POSITION
// fails explicitly); -door-stays-closed makes OPEN_CARGO unconfirmed.
// -offline-for drops the network for an initial period (PANIC then waits
// in the durable queue). Killing the process and restarting it with the
// same -db resumes the queue.
package main

import (
	"context"
	"crypto/ed25519"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net/netip"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/flavioanjo/fleetcore/internal/device"
	"github.com/flavioanjo/fleetcore/internal/reliable"
	"github.com/flavioanjo/fleetcore/internal/security"
	"github.com/flavioanjo/fleetcore/internal/simhw"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stderr))
}

// offlineTransport drops sends while offline is set.
type offlineTransport struct {
	*device.UDPTransport
	offline atomic.Bool
}

func (t *offlineTransport) Send(p []byte) error {
	if t.offline.Load() {
		return errors.New("simulated: no network")
	}
	return t.UDPTransport.Send(p)
}

func run(ctx context.Context, args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("fleet-device", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		id         = fs.String("id", "TRK-1", "device id")
		keysPath   = fs.String("keys", "keys.json", "key file holding this device's key (secret)")
		pubPath    = fs.String("authority-pub", "authority.pub", "command authority public key (hex file)")
		gw         = fs.String("gateway", "127.0.0.1:7500", "gateway UDP address")
		dbPath     = fs.String("db", "", "device store (default device-<id>.db)")
		telemetryI = fs.Duration("telemetry", 2*time.Second, "telemetry period (0 disables)")
		speed      = fs.Float64("speed", 0, "simulated speed in km/h")
		ignition   = fs.Bool("ignition", false, "simulated ignition on")
		fixFlag    = fs.String("fix", "", "simulated GNSS fix lat,lon in degrees (empty: no fix)")
		doorClosed = fs.Bool("door-stays-closed", false, "OPEN_CARGO releases the latch but the door stays closed")
		panicAfter = fs.Duration("panic-after", 0, "press the panic button after this delay (0: never)")
		offlineFor = fs.Duration("offline-for", 0, "no network for this initial period")
		duration   = fs.Duration("duration", 0, "stop after this long (0: until interrupted)")
		statsEvery = fs.Duration("stats-interval", 5*time.Second, "status log period")
	)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	log := slog.New(slog.NewTextHandler(stderr, nil)).With("device", *id)
	keys, err := security.LoadKeyFile(*keysPath)
	if err != nil {
		log.Error("cannot load keys", "err", err)
		return 2
	}
	k, err := keys.Lookup(*id, 1)
	if err != nil {
		log.Error("no key for this device", "err", err)
		return 2
	}
	pubHex, err := os.ReadFile(*pubPath)
	if err != nil {
		log.Error("cannot read authority public key", "err", err)
		return 2
	}
	pub, err := security.ParsePublicKeyHex(strings.TrimSpace(string(pubHex)))
	if err != nil {
		log.Error("bad authority public key", "err", err)
		return 2
	}
	gwAddr, err := netip.ParseAddrPort(*gw)
	if err != nil {
		log.Error("bad -gateway", "err", err)
		return 2
	}
	if *dbPath == "" {
		*dbPath = "device-" + *id + ".db"
	}
	veh := simhw.New(time.Now)
	veh.Drive(uint16(*speed*10), *ignition)
	veh.SetFaults(simhw.Faults{DoorStaysClosed: *doorClosed})
	if *fixFlag != "" {
		parts := strings.Split(*fixFlag, ",")
		lat, err1 := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
		var lon float64
		var err2 error = errors.New("missing longitude")
		if len(parts) == 2 {
			lon, err2 = strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
		}
		if err1 != nil || err2 != nil {
			log.Error("bad -fix (want lat,lon)")
			return 2
		}
		veh.SetFix(reliable.PositionFix{Fix: reliable.Fix3D, LatE7: int32(lat * 1e7), LonE7: int32(lon * 1e7), HDOPx10: 9, Satellites: 10}, true)
	}
	udp, err := device.NewUDPTransport("0.0.0.0:0", gwAddr)
	if err != nil {
		log.Error("cannot open socket", "err", err)
		return 1
	}
	defer udp.Close()
	tr := &offlineTransport{UDPTransport: udp}
	tr.offline.Store(*offlineFor > 0)
	d, err := device.Open(ctx, device.Config{DeviceID: *id, KeyID: 1, Key: k.Secret,
		Authority: security.NewVerifier(map[uint32]ed25519.PublicKey{1: pub}),
		StorePath: *dbPath, Transport: tr, Hardware: veh, Logger: log,
		InitialBackoff: time.Second, MaxBackoff: 30 * time.Second, CriticalMaxBackoff: 5 * time.Second, HeartbeatInterval: 5 * time.Second})
	if err != nil {
		log.Error("cannot open device store", "err", err)
		return 1
	}
	defer d.Close()
	if err := d.Boot(ctx); err != nil {
		log.Error("boot failed", "err", err)
		return 1
	}
	log.Info("device booted", "session_gen", d.SessionGen(), "gateway", gwAddr.String(), "store", *dbPath)
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}
	go udp.Receive(ctx, d) //nolint:errcheck

	start := time.Now()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	var lastTel, lastStats time.Time
	panicked := false
	for {
		select {
		case <-ctx.Done():
			q, _ := d.Queue(context.Background())
			log.Info("device stopping; pending events stay queued for the next run", "pending", q.PendingTotal())
			return 0
		case now := <-tick.C:
			if tr.offline.Load() && now.Sub(start) >= *offlineFor {
				tr.offline.Store(false)
				log.Info("network restored")
			}
			if *telemetryI > 0 && now.Sub(lastTel) >= *telemetryI {
				lastTel = now
				d.EnqueueTelemetry(ctx, telemetry.Sample{Source: telemetry.SourceGPS, Quality: telemetry.QualityGood, //nolint:errcheck
					Distance: telemetry.Some(telemetry.Distance{Kind: telemetry.DistanceDelta, Value: telemetry.Millimetres(*speed * 1000 / 3.6 * telemetryI.Seconds())})})
			}
			if *panicAfter > 0 && !panicked && now.Sub(start) >= *panicAfter {
				panicked = true
				uid, err := d.Panic(ctx)
				log.Warn("PANIC button pressed; event durably queued", "uid", uid, "err", err)
			}
			d.Tick(ctx) //nolint:errcheck
			if now.Sub(lastStats) >= *statsEvery {
				lastStats = now
				m := d.Metrics()
				q, _ := d.Queue(ctx)
				log.Info("status", "pending", q.PendingTotal(), "acked", m.Acked, "retransmissions", m.Retransmissions,
					"commands_received", m.CommandsReceived, "succeeded", m.CommandsSucceeded, "rejected", m.CommandsRejected,
					"failed", m.CommandsFailed, "unknown", m.CommandsUnknown, "duplicate_deliveries", m.DuplicateDeliveries,
					"immobilizer", veh.ImmobilizerState().String(), "cargo_door", veh.CargoDoor(1).String(),
					"critical_ack_latency_max", m.CriticalAckLatency.Max)
			}
		}
	}
}
