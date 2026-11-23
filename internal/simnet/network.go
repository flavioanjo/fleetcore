// Package simnet runs the reliable communications system in a
// deterministic simulated world: a controllable clock, a datagram network
// with seeded loss, duplication, reordering and delay, simulated devices
// and vehicles, gateway cores sharing one durable event store, the command
// dispatcher and the alerting consumer, all on real SQLite databases.
//
// Production decision logic (device, gateway, commandstore, dispatcher,
// alerting) runs unmodified; the simulation only supplies time, transport
// and hardware, and injects faults through the components' documented
// hooks. Simulation truth (simhw.Vehicle, the network's counters) is kept
// apart from what the components record, so tests can compare the two.
package simnet

import (
	"math/rand"
	"net/netip"
	"sort"
	"time"
)

// Faults are per-direction network impairments. Probabilities are in
// [0, 1].
type Faults struct {
	Loss      float64
	Duplicate float64
	// Delay is added to every datagram; Jitter adds a uniform [0, Jitter)
	// on top, which reorders datagrams.
	Delay, Jitter time.Duration
}

// Filter decides the fate of one datagram: return false to drop it. It
// sees the decoded direction and endpoints and may inspect the bytes.
type Filter func(d Datagram) bool

// Datagram is a datagram in flight.
type Datagram struct {
	Data     []byte
	From, To netip.AddrPort
	Uplink   bool
	at       time.Time
	seq      uint64
}

// Network is a deterministic datagram network.
type Network struct {
	rng        *rand.Rand
	Up, Down   Faults
	filters    []Filter
	inflight   []Datagram
	seq        uint64
	Delivered  uint64
	Dropped    uint64
	Duplicated uint64
}

// NewNetwork returns a network with a seeded random source.
func NewNetwork(seed int64) *Network { return &Network{rng: rand.New(rand.NewSource(seed))} }

// AddFilter installs a datagram filter (fault injection by content).
func (n *Network) AddFilter(f Filter) { n.filters = append(n.filters, f) }

// ClearFilters removes all filters.
func (n *Network) ClearFilters() { n.filters = nil }

// send queues a datagram at time now, applying the impairments.
func (n *Network) send(d Datagram, now time.Time) {
	f := n.Down
	if d.Uplink {
		f = n.Up
	}
	for _, flt := range n.filters {
		if !flt(d) {
			n.Dropped++
			return
		}
	}
	if n.rng.Float64() < f.Loss {
		n.Dropped++
		return
	}
	copies := 1
	if n.rng.Float64() < f.Duplicate {
		copies = 2
		n.Duplicated++
	}
	for i := 0; i < copies; i++ {
		c := d
		c.Data = append([]byte(nil), d.Data...)
		c.at = now.Add(f.Delay)
		if f.Jitter > 0 {
			c.at = c.at.Add(time.Duration(n.rng.Int63n(int64(f.Jitter))))
		}
		n.seq++
		c.seq = n.seq
		n.inflight = append(n.inflight, c)
	}
}

// due removes and returns the datagrams due at now, in arrival order.
func (n *Network) due(now time.Time) []Datagram {
	var out, keep []Datagram
	for _, d := range n.inflight {
		if d.at.After(now) {
			keep = append(keep, d)
		} else {
			out = append(out, d)
		}
	}
	n.inflight = keep
	sort.SliceStable(out, func(i, j int) bool {
		if !out[i].at.Equal(out[j].at) {
			return out[i].at.Before(out[j].at)
		}
		return out[i].seq < out[j].seq
	})
	n.Delivered += uint64(len(out))
	return out
}

// InFlight is the number of datagrams not yet delivered.
func (n *Network) InFlight() int { return len(n.inflight) }
