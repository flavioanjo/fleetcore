package reliable

import (
	"bytes"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/security"
)

// FuzzOpen: arbitrary datagrams never panic the decoder, and any packet
// that authenticates re-seals to identical bytes (canonical framing).
func FuzzOpen(f *testing.F) {
	ks := keyStore(f, "DEV-1")
	for _, b := range sampleBodies() {
		f.Add(uploadFrame(f, ks, "DEV-1", 5, 9, b))
	}
	f.Add([]byte{0xfc, 0x7e, 2})
	f.Fuzz(func(t *testing.T, pkt []byte) {
		fr, err := Open(pkt, security.Uplink, ks)
		if err != nil {
			return
		}
		re, err := Seal(fr.Header, fr.Payload, secretOf(t, ks, fr.DeviceID))
		if err != nil || !bytes.Equal(re, pkt) {
			t.Fatalf("accepted packet does not re-seal identically: %v", err)
		}
		ev, err := DecodeEventUpload(fr.Payload)
		if err != nil {
			return
		}
		body, err := DecodeBody(fr.DeviceID, ev)
		if err != nil {
			return
		}
		if b, err := EncodeBody(body); err != nil || !bytes.Equal(b, ev.Body) {
			t.Fatalf("accepted body does not re-encode identically: %v", err)
		}
	})
}

// FuzzEventPayload exercises the event payload and body decoders directly
// (no tag in the way), checking canonical re-encoding.
func FuzzEventPayload(f *testing.F) {
	for _, b := range sampleBodies() {
		ev, _ := NewEvent(42, t0, b)
		p, _ := ev.Encode()
		f.Add(p)
	}
	f.Fuzz(func(t *testing.T, p []byte) {
		ev, err := DecodeEventUpload(p)
		if err != nil {
			return
		}
		re, err := ev.Encode()
		if err != nil || !bytes.Equal(re, p) {
			t.Fatalf("event payload not canonical: %v", err)
		}
		body, err := DecodeBody("DEV-1", ev)
		if err != nil {
			return
		}
		if b, err := EncodeBody(body); err != nil || !bytes.Equal(b, ev.Body) {
			t.Fatalf("%s body not canonical: %v", ev.Type, err)
		}
	})
}

// FuzzDownlink exercises the ACK and command delivery decoders.
func FuzzDownlink(f *testing.F) {
	a, _ := EventAck{UID: 7, Status: AckStored, Position: 1}.Encode()
	f.Add(a)
	cmd := Command{ID: CommandID{9}, DeviceID: "DEV-1", Type: CommandOpenCargo, Args: CargoArgs(1),
		IssuedAt: t0, ExpiresAt: t0.Add(time.Minute), Principal: "ops"}
	d, _ := CommandDelivery{Command: cmd, Attempt: 1, AttemptSentAt: t0, AttemptNotAfter: t0.Add(time.Minute),
		AuthorityKeyID: 1, Signature: make([]byte, 64)}.Encode()
	f.Add(d)
	f.Fuzz(func(t *testing.T, p []byte) {
		if ack, err := DecodeEventAck(p); err == nil {
			if re, err := ack.Encode(); err != nil || !bytes.Equal(re, p) {
				t.Fatal("ack not canonical")
			}
		}
		if dl, err := DecodeCommandDelivery("DEV-1", p); err == nil {
			if re, err := dl.Encode(); err != nil || !bytes.Equal(re, p) {
				t.Fatalf("delivery not canonical: %v", err)
			}
		}
		if hb, err := DecodeHeartbeat(p); err == nil {
			if re, err := hb.Encode(); err != nil || !bytes.Equal(re, p) {
				t.Fatal("heartbeat not canonical")
			}
		}
	})
}
