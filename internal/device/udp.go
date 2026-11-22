package device

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/flavioanjo/fleetcore/internal/reliable"
)

// UDPTransport sends a device's datagrams to one gateway at a time over a
// UDP socket and receives the gateways' answers on the same socket (so a
// NAT mapping created by the uplink also carries the downlink). The
// gateway can be switched at any time (failover).
type UDPTransport struct {
	conn *net.UDPConn
	mu   sync.Mutex
	gw   netip.AddrPort
}

// NewUDPTransport binds a local UDP socket ("127.0.0.1:0" for tests).
func NewUDPTransport(local string, gateway netip.AddrPort) (*UDPTransport, error) {
	addr, err := net.ResolveUDPAddr("udp", local)
	if err != nil {
		return nil, err
	}
	c, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	return &UDPTransport{conn: c, gw: gateway}, nil
}

// SetGateway switches the gateway subsequent datagrams go to.
func (t *UDPTransport) SetGateway(a netip.AddrPort) {
	t.mu.Lock()
	t.gw = a
	t.mu.Unlock()
}

// Send implements Transport.
func (t *UDPTransport) Send(p []byte) error {
	t.mu.Lock()
	gw := t.gw
	t.mu.Unlock()
	_, err := t.conn.WriteToUDPAddrPort(p, gw)
	return err
}

// LocalAddr is the device's socket address.
func (t *UDPTransport) LocalAddr() netip.AddrPort { return t.conn.LocalAddr().(*net.UDPAddr).AddrPort() }

// Close closes the socket (Receive returns).
func (t *UDPTransport) Close() error { return t.conn.Close() }

// Receive reads downlink datagrams and hands them to the device until the
// socket is closed or ctx is cancelled. Rejected datagrams are counted by
// the device and otherwise ignored.
func (t *UDPTransport) Receive(ctx context.Context, d *Device) error {
	buf := make([]byte, reliable.MaxPacketSize+1)
	go func() {
		<-ctx.Done()
		t.conn.SetReadDeadline(time.Now()) //nolint:errcheck
	}()
	for {
		n, _, err := t.conn.ReadFromUDPAddrPort(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
				return nil
			}
			continue
		}
		if n > reliable.MaxPacketSize {
			continue
		}
		d.HandleDownlink(ctx, append([]byte(nil), buf[:n]...)) //nolint:errcheck
	}
}
