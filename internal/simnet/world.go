package simnet

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/netip"
	"path/filepath"
	"sync"
	"time"

	"github.com/flavioanjo/fleetcore/internal/alerting"
	"github.com/flavioanjo/fleetcore/internal/commandstore"
	"github.com/flavioanjo/fleetcore/internal/device"
	"github.com/flavioanjo/fleetcore/internal/dispatcher"
	"github.com/flavioanjo/fleetcore/internal/eventstore"
	"github.com/flavioanjo/fleetcore/internal/gateway"
	"github.com/flavioanjo/fleetcore/internal/reliable"
	"github.com/flavioanjo/fleetcore/internal/security"
	"github.com/flavioanjo/fleetcore/internal/simhw"
)

// Epoch is the default simulation start time.
var Epoch = time.Date(2026, 11, 22, 8, 0, 0, 0, time.UTC)

// Config configures a World.
type Config struct {
	Dir      string // databases live here (persistent across component restarts)
	Seed     int64
	Gateways int // default 2
	Start    time.Time
	// Device configures every device (DeviceID, keys, paths, transport,
	// hardware, clock and RNG are set by the world).
	Device func(*device.Config)
	Logger *slog.Logger
}

// World is a simulated deployment.
type World struct {
	cfg  Config
	ctx  context.Context
	mu   sync.Mutex
	now  time.Time
	Net  *Network
	Keys *security.MemoryKeyStore
	Auth *security.Authority

	Events   *eventstore.Store
	Commands *commandstore.Store
	Dispatch *dispatcher.Dispatcher
	Alerts   *alerting.Consumer
	Sink     *RecordingSink

	Gateways []*GatewayNode
	Devices  map[string]*DeviceNode
	order    []string
	log      *slog.Logger
}

// GatewayNode is one gateway core with its address.
type GatewayNode struct {
	ID    string
	Addr  netip.AddrPort
	Core  *gateway.Reliable
	Hooks gateway.ReliableHooks
	Down  bool // rebooting: datagrams to it are lost
	w     *World
}

// DeviceNode is one simulated device with its vehicle.
type DeviceNode struct {
	ID      string
	Addr    netip.AddrPort
	Dev     *device.Device
	Vehicle *simhw.Vehicle
	Key     security.Secret
	// Gateway is the index of the gateway the device sends to.
	Gateway int
	// Offline: the modem has no network (sends fail locally).
	Offline bool
	// Skew offsets the device clock from the world clock (clock faults).
	Skew  time.Duration
	Hooks device.Hooks
	w     *World
	rng   *rand.Rand
}

// RecordingSink is an alerting.Sink that records notifications and can be
// made to fail.
type RecordingSink struct {
	mu    sync.Mutex
	Seen  map[string]int
	Order []string
	Fail  bool
}

// Notify implements alerting.Sink.
func (s *RecordingSink) Notify(_ context.Context, a alerting.Alert) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Fail {
		return errors.New("sink unavailable")
	}
	s.Seen[a.ID]++
	s.Order = append(s.Order, a.ID)
	return nil
}

// New builds a world: shared event store, command store, two gateways
// (default), dispatcher and alert consumer. Devices are added with
// AddDevice.
func New(ctx context.Context, cfg Config) (*World, error) {
	if cfg.Dir == "" {
		return nil, errors.New("simnet: Dir required")
	}
	if cfg.Gateways <= 0 {
		cfg.Gateways = 2
	}
	if cfg.Start.IsZero() {
		cfg.Start = Epoch
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	w := &World{cfg: cfg, ctx: ctx, now: cfg.Start, Net: NewNetwork(cfg.Seed), Keys: security.NewMemoryKeyStore(),
		Devices: map[string]*DeviceNode{}, Sink: &RecordingSink{Seen: map[string]int{}}, log: cfg.Logger}
	var err error
	if w.Auth, err = security.NewAuthority(1); err != nil {
		return nil, err
	}
	if w.Events, err = eventstore.Open(ctx, filepath.Join(cfg.Dir, "events.db"), eventstore.Options{Now: w.Now}); err != nil {
		return nil, err
	}
	if err := w.openCommands(); err != nil {
		return nil, err
	}
	for i := 0; i < cfg.Gateways; i++ {
		g := &GatewayNode{ID: fmt.Sprintf("gw-%c", 'a'+i), Addr: netip.AddrPortFrom(netip.AddrFrom4([4]byte{10, 0, 0, byte(1 + i)}), 7400), w: w}
		if err := g.start(); err != nil {
			return nil, err
		}
		w.Gateways = append(w.Gateways, g)
	}
	if err := w.RestartDispatcher(); err != nil {
		return nil, err
	}
	if w.Alerts, err = alerting.Open(ctx, alerting.Config{Path: filepath.Join(cfg.Dir, "alerts.db"), Events: w.Events, Sink: w.Sink,
		Now: w.Now, Logger: w.log}); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *World) openCommands() error {
	var err error
	w.Commands, err = commandstore.Open(w.ctx, filepath.Join(w.cfg.Dir, "commands.db"), w.Auth, commandstore.Options{Now: w.Now})
	return err
}

// Close closes every database.
func (w *World) Close() {
	for _, id := range w.order {
		w.Devices[id].Dev.Close()
	}
	w.Alerts.Close()
	w.Commands.Close()
	w.Events.Close()
}

// Now is the world (server) clock.
func (w *World) Now() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.now
}

func (g *GatewayNode) start() error {
	core, err := gateway.NewReliable(gateway.ReliableConfig{Keys: g.w.Keys, Store: g.w.Events, GatewayID: g.ID, Hooks: &g.Hooks,
		RetryBackoff: time.Millisecond}, g.w.Now, g.w.log)
	if err != nil {
		return err
	}
	g.Core = core
	return nil
}

// Reboot restarts the gateway process: its memory (sessions, replay
// windows, device addresses) is lost; the shared store is not.
func (g *GatewayNode) Reboot() error { return g.start() }

// SendCommand implements dispatcher.Transport through the simulated
// network.
func (g *GatewayNode) SendCommand(d reliable.CommandDelivery) error {
	if g.Down {
		return reliable.ErrDeviceUnreachable
	}
	o, err := g.Core.CommandPacket(d)
	if err != nil {
		return err
	}
	g.w.Net.send(Datagram{Data: o.Data, From: g.Addr, To: o.To}, g.w.Now())
	return nil
}

// RestartDispatcher replaces the dispatcher, reopening the command store
// (a dispatcher process restart).
func (w *World) RestartDispatcher() error {
	if w.Dispatch != nil {
		w.Commands.Close()
		if err := w.openCommands(); err != nil {
			return err
		}
	}
	var ts []dispatcher.Named
	for _, g := range w.Gateways {
		ts = append(ts, dispatcher.Named{Name: g.ID, Transport: g})
	}
	d, err := dispatcher.New(dispatcher.Config{Commands: w.Commands, Events: w.Events, Transports: ts, Now: w.Now, Logger: w.log,
		InitialBackoff: 2 * time.Second, MaxBackoff: 10 * time.Second, PollInterval: 10 * time.Second})
	if err != nil {
		return err
	}
	w.Dispatch = d
	return nil
}

// AddDevice provisions a device (key, vehicle) and boots it.
func (w *World) AddDevice(id string) (*DeviceNode, error) {
	key, err := security.GenerateSecret()
	if err != nil {
		return nil, err
	}
	if err := w.Keys.Put(security.DeviceKey{DeviceID: id, KeyID: 1, Secret: key}); err != nil {
		return nil, err
	}
	n := &DeviceNode{ID: id, Key: key, w: w, Addr: netip.AddrPortFrom(netip.AddrFrom4([4]byte{100, 64, 0, byte(1 + len(w.order))}), 5000),
		rng: rand.New(rand.NewSource(w.cfg.Seed + int64(len(w.order)) + 1))}
	n.Vehicle = simhw.New(n.Now)
	w.Devices[id] = n
	w.order = append(w.order, id)
	return n, n.Start()
}

// Now is the device clock (world clock plus skew).
func (n *DeviceNode) Now() time.Time { return n.w.Now().Add(n.Skew) }

// Send implements device.Transport.
func (n *DeviceNode) Send(p []byte) error {
	if n.Offline {
		return errors.New("simnet: no network")
	}
	g := n.w.Gateways[n.Gateway]
	n.w.Net.send(Datagram{Data: p, From: n.Addr, To: g.Addr, Uplink: true}, n.w.Now())
	return nil
}

// Start opens and boots the device process (first start or after a crash).
func (n *DeviceNode) Start() error {
	if n.Dev != nil {
		n.Dev.Close()
	}
	cfg := device.Config{DeviceID: n.ID, KeyID: 1, Key: n.Key,
		Authority: security.NewVerifier(map[uint32]ed25519.PublicKey{n.w.Auth.KeyID(): n.w.Auth.Public()}),
		StorePath: filepath.Join(n.w.cfg.Dir, "device-"+n.ID+".db"), Transport: n, Hardware: n.Vehicle,
		Now: n.Now, Rand: n.rng, Logger: n.w.log, Hooks: &n.Hooks}
	if n.w.cfg.Device != nil {
		n.w.cfg.Device(&cfg)
	}
	d, err := device.Open(n.w.ctx, cfg)
	if err != nil {
		return err
	}
	if err := d.Boot(n.w.ctx); err != nil {
		d.Close()
		return err
	}
	n.Dev = d
	return nil
}

// Crash stops the device process without any shutdown work.
func (n *DeviceNode) Crash() {
	if n.Dev != nil {
		n.Dev.Close()
		n.Dev = nil
	}
}

func (w *World) deviceAt(a netip.AddrPort) *DeviceNode {
	for _, id := range w.order {
		if w.Devices[id].Addr == a {
			return w.Devices[id]
		}
	}
	return nil
}

// Step advances the world clock by dt and runs one round: devices tick,
// due uplink datagrams reach their gateways (one batch per gateway), due
// downlink datagrams reach devices, the dispatcher applies reports and
// dispatches, and the alert consumer runs.
func (w *World) Step(dt time.Duration) error {
	w.mu.Lock()
	w.now = w.now.Add(dt)
	w.mu.Unlock()
	ctx := w.ctx
	for _, id := range w.order {
		if n := w.Devices[id]; n.Dev != nil {
			if _, err := n.Dev.Tick(ctx); err != nil {
				return fmt.Errorf("device %s tick: %w", id, err)
			}
		}
	}
	batches := map[netip.AddrPort][]gateway.Inbound{}
	var down []Datagram
	for _, d := range w.Net.due(w.Now()) {
		if d.Uplink {
			batches[d.To] = append(batches[d.To], gateway.Inbound{Data: d.Data, From: d.From, At: w.Now()})
		} else {
			down = append(down, d)
		}
	}
	for _, g := range w.Gateways {
		in := batches[g.Addr]
		if len(in) == 0 {
			continue
		}
		if g.Down {
			w.Net.Dropped += uint64(len(in))
			continue
		}
		for _, o := range g.Core.HandleBatch(ctx, in) {
			w.Net.send(Datagram{Data: o.Data, From: g.Addr, To: o.To}, w.Now())
		}
	}
	for _, d := range down {
		if n := w.deviceAt(d.To); n != nil && n.Dev != nil && !n.Offline {
			n.Dev.HandleDownlink(ctx, d.Data) //nolint:errcheck // rejections are counted by the device
		}
	}
	for {
		k, err := w.Dispatch.Apply(ctx)
		if err != nil {
			return err
		}
		if k == 0 {
			break
		}
	}
	if _, err := w.Dispatch.Step(ctx); err != nil {
		return err
	}
	if _, _, err := w.Alerts.Step(ctx); err != nil {
		return err
	}
	return nil
}

// Run steps the world for total, in steps of dt.
func (w *World) Run(total, dt time.Duration) error {
	for t := time.Duration(0); t < total; t += dt {
		if err := w.Step(dt); err != nil {
			return err
		}
	}
	return nil
}

// RunUntil steps until cond holds or limit elapses; it reports whether
// cond held.
func (w *World) RunUntil(limit, dt time.Duration, cond func() bool) (bool, error) {
	for t := time.Duration(0); t < limit; t += dt {
		if cond() {
			return true, nil
		}
		if err := w.Step(dt); err != nil {
			return false, err
		}
	}
	return cond(), nil
}

// Request creates a command as principal (with the reference policy).
func (w *World) Request(reqID string, p commandstore.Principal, dev string, typ reliable.CommandType, args []byte, ttl time.Duration) (commandstore.Command, error) {
	c, _, err := w.Commands.Create(w.ctx, commandstore.Request{RequestID: reqID, Principal: p, DeviceID: dev, Type: typ,
		Args: args, TTL: ttl, Justification: "simulation"})
	return c, err
}

// Command returns a command's current record.
func (w *World) Command(id reliable.CommandID) commandstore.Command {
	c, _ := w.Commands.Get(w.ctx, id)
	return c
}
