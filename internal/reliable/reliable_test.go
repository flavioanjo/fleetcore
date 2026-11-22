package reliable

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/protocol"
	"github.com/flavioanjo/fleetcore/internal/security"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

var t0 = time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)

func testKey() security.Secret {
	var s security.Secret
	for i := range s {
		s[i] = byte(i + 1)
	}
	return s
}

func keyStore(t testing.TB, devs ...string) *security.MemoryKeyStore {
	ks := security.NewMemoryKeyStore()
	for _, d := range devs {
		k := testKey()
		k[0] = byte(len(d)) ^ d[len(d)-1]
		if err := ks.Put(security.DeviceKey{DeviceID: d, KeyID: 1, Secret: k}); err != nil {
			t.Fatal(err)
		}
	}
	return ks
}

func secretOf(t testing.TB, ks *security.MemoryKeyStore, dev string) *security.Secret {
	k, err := ks.Lookup(dev, 1)
	if err != nil {
		t.Fatal(err)
	}
	return &k.Secret
}

var fix = PositionFix{Fix: Fix3D, LatE7: -235505200, LonE7: -466333100, SpeedKmhX10: 0, HeadingCdeg: 9000,
	HDOPx10: 9, Satellites: 11, FixTime: t0.Add(-2 * time.Second)}

func sampleBodies() []Body {
	cid := CommandID{1, 2, 3}
	return []Body{
		Telemetry{Sample: telemetry.Sample{DeviceID: "DEV-1", EventID: 42, BootID: 3, Sequence: ^telemetry.Sequence(0),
			DeviceTime: t0, Source: telemetry.SourceGPS, Quality: telemetry.QualityGood,
			Distance: telemetry.Some(telemetry.Distance{Kind: telemetry.DistanceDelta, Value: 1234})}},
		Position{PositionFix: fix, Reason: PositionPeriodic},
		Position{PositionFix: fix, Reason: PositionRequested, RequestID: cid},
		Panic{Trigger: PanicButton},
		Panic{Trigger: PanicButton, HasPosition: true, Position: fix, PositionAge: 2 * time.Second},
		SecurityViolation{Code: SecurityTamper, Detail: 7},
		Diagnostic{Code: 3, Value: -9},
		BlockState{State: Blocked, Cause: CauseCommand, CommandID: cid},
		BlockState{State: BlockUnknown, Cause: CauseBoot},
		CargoState{Door: DoorClosed, Latch: LatchReleased, Cause: CauseCommand, CommandID: cid},
		CommandResult{CommandID: cid, CommandType: CommandOpenCargo, Stage: StageUnknown, Reason: ReasonNotConfirmed, Observed: uint8(DoorClosed)},
	}
}

func TestEventBodiesRoundTrip(t *testing.T) {
	for _, b := range sampleBodies() {
		ev, err := NewEvent(42, t0, b)
		if err != nil {
			t.Fatalf("%T: %v", b, err)
		}
		p, err := ev.Encode()
		if err != nil {
			t.Fatal(err)
		}
		back, err := DecodeEventUpload(p)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeBody("DEV-1", back)
		if err != nil {
			t.Fatalf("%T: %v", b, err)
		}
		re, _ := EncodeBody(got)
		if !bytes.Equal(re, ev.Body) {
			t.Fatalf("%T does not re-encode identically", b)
		}
		if back.Type.Class() != b.EventType().Class() {
			t.Fatal("class")
		}
	}
	if !EventPanic.Class().Critical() || !EventSecurityViolation.Class().Critical() || EventTelemetry.Class().Critical() {
		t.Fatal("criticality")
	}
}

func TestInvalidBodiesRejected(t *testing.T) {
	bad := []Body{
		Position{PositionFix: PositionFix{Fix: FixNone}, Reason: PositionPeriodic},           // no fabricated position
		Position{PositionFix: PositionFix{Fix: FixNone, LatE7: 1}, Reason: PositionPeriodic}, // coordinates without fix
		Position{PositionFix: fix, Reason: PositionRequested},                                // requested without id
		Position{PositionFix: func() PositionFix { f := fix; f.LatE7 = 900000001; return f }(), Reason: PositionPeriodic},
		Position{PositionFix: func() PositionFix { f := fix; f.Satellites = 3; return f }(), Reason: PositionPeriodic}, // 3D needs 4
		Panic{Trigger: 2},
		Panic{Trigger: PanicButton, PositionAge: time.Second}, // age without position
		Panic{Trigger: PanicButton, HasPosition: true, Position: PositionFix{Fix: FixNone}},
		BlockState{State: Blocked, Cause: CauseCommand}, // command cause without id
		BlockState{State: 9, Cause: CauseBoot},
		CargoState{Door: DoorOpen, Latch: LatchLocked, Cause: CauseBoot, CommandID: CommandID{1}}, // id without command cause
		CommandResult{CommandType: CommandBlock, Stage: StageReceived},                            // zero id
		CommandResult{CommandID: CommandID{1}, CommandType: 0, Stage: StageReceived},
		CommandResult{CommandID: CommandID{1}, CommandType: CommandBlock, Stage: 0},
		SecurityViolation{Code: 0},
		Diagnostic{},
		Telemetry{Sample: telemetry.Sample{DeviceID: "DEV-1"}},
	}
	for i, b := range bad {
		if _, err := NewEvent(1, t0, b); !errors.Is(err, ErrInvalidEvent) {
			t.Errorf("case %d (%T): %v, want invalid_event", i, b, err)
		}
	}
	// A telemetry body whose identity disagrees with the envelope.
	ev, _ := NewEvent(42, t0, sampleBodies()[0])
	for _, mut := range []func(*EventUpload){
		func(e *EventUpload) { e.UID = 43 },
		func(e *EventUpload) { e.DeviceTime = t0.Add(time.Second) },
	} {
		e := ev
		mut(&e)
		if _, err := DecodeBody("DEV-1", e); !errors.Is(err, ErrInvalidEvent) {
			t.Errorf("mismatched envelope: %v", err)
		}
	}
	if _, err := DecodeBody("DEV-2", ev); !errors.Is(err, ErrInvalidEvent) {
		t.Errorf("telemetry for another device: %v", err)
	}
	// Trailing body bytes.
	pe, _ := NewEvent(1, t0, Diagnostic{Code: 1})
	pe.Body = append(pe.Body, 0)
	if _, err := DecodeBody("DEV-1", pe); !errors.Is(err, ErrInvalidEvent) {
		t.Errorf("trailing body byte: %v", err)
	}
}

func uploadFrame(t testing.TB, ks *security.MemoryKeyStore, dev string, gen, ctr uint64, b Body) []byte {
	ev, err := NewEvent(42, t0, b)
	if err != nil {
		t.Fatal(err)
	}
	p, _ := ev.Encode()
	pkt, err := Seal(Header{Type: TypeEventUpload, KeyID: 1, SessionGen: gen, Counter: ctr, DeviceID: dev}, p, secretOf(t, ks, dev))
	if err != nil {
		t.Fatal(err)
	}
	return pkt
}

func TestSealOpenAndContextBinding(t *testing.T) {
	ks := keyStore(t, "DEV-1", "DEV-2")
	pkt := uploadFrame(t, ks, "DEV-1", 5, 9, Diagnostic{Code: 1, Value: 2})
	f, err := Open(pkt, security.Uplink, ks)
	if err != nil {
		t.Fatal(err)
	}
	if f.DeviceID != "DEV-1" || f.SessionGen != 5 || f.Counter != 9 || f.Type != TypeEventUpload {
		t.Fatalf("header %+v", f.Header)
	}
	// Reflection: an uplink frame is never a valid downlink frame.
	if _, err := Open(pkt, security.Downlink, ks); !errors.Is(err, ErrWrongDirection) {
		t.Fatalf("reflected frame: %v", err)
	}
	// Re-labelling the frame for another device, session, key or type fails the tag.
	relabel := func(mut func(h *Header)) error {
		h, _ := ParseHeader(pkt)
		mut(&h)
		forged, err := Seal(h, f.Payload, secretOf(t, ks, "DEV-1"))
		if err != nil {
			return err
		}
		// Keep the original tag: an attacker without the key cannot recompute it.
		copy(forged[len(forged)-TagSize:], pkt[len(pkt)-TagSize:])
		_, err = Open(forged, security.Uplink, ks)
		return err
	}
	for name, mut := range map[string]func(*Header){
		"device":  func(h *Header) { h.DeviceID = "DEV-2" },
		"session": func(h *Header) { h.SessionGen = 6 },
		"counter": func(h *Header) { h.Counter = 10 },
		"type":    func(h *Header) { h.Type = TypeHeartbeat },
	} {
		if err := relabel(mut); !errors.Is(err, ErrBadMAC) {
			t.Errorf("relabelled %s: %v", name, err)
		}
	}
	// DEV-2's key cannot produce frames DEV-1 accepts.
	ev, _ := NewEvent(42, t0, Diagnostic{Code: 1})
	p, _ := ev.Encode()
	cross, _ := Seal(Header{Type: TypeEventUpload, KeyID: 1, SessionGen: 5, Counter: 9, DeviceID: "DEV-1"}, p, secretOf(t, ks, "DEV-2"))
	if _, err := Open(cross, security.Uplink, ks); !errors.Is(err, ErrBadMAC) {
		t.Fatalf("frame keyed by another device: %v", err)
	}
	// Unknown and revoked keys.
	h, _ := ParseHeader(pkt)
	h.KeyID = 2
	other, _ := Seal(h, f.Payload, secretOf(t, ks, "DEV-1"))
	if _, err := Open(other, security.Uplink, ks); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("unknown key version: %v", err)
	}
	ks.SetState("DEV-1", 1, security.KeyRevoked)
	if _, err := Open(pkt, security.Uplink, ks); !errors.Is(err, ErrRevokedKey) {
		t.Fatalf("revoked key: %v", err)
	}
}

// Flipping any single bit of an authenticated packet makes it invalid.
func TestEveryBitFlipIsRejected(t *testing.T) {
	ks := keyStore(t, "DEV-1")
	for _, b := range sampleBodies()[1:4] {
		pkt := uploadFrame(t, ks, "DEV-1", 5, 9, b)
		for i := 0; i < len(pkt); i++ {
			for bit := 0; bit < 8; bit++ {
				m := append([]byte(nil), pkt...)
				m[i] ^= 1 << bit
				if _, err := Open(m, security.Uplink, ks); err == nil {
					t.Fatalf("bit %d of byte %d flipped and accepted", bit, i)
				}
			}
		}
	}
}

func TestFailClosedFraming(t *testing.T) {
	ks := keyStore(t, "DEV-1")
	pkt := uploadFrame(t, ks, "DEV-1", 5, 9, Diagnostic{Code: 1})
	cases := map[string]struct {
		pkt  []byte
		want error
	}{
		"trailing":  {append(append([]byte(nil), pkt...), 0), ErrLengthMismatch},
		"truncated": {pkt[:len(pkt)-1], ErrTruncated},
		"short":     {pkt[:20], ErrTruncated},
		"oversize":  {make([]byte, MaxPacketSize+1), ErrTooLarge},
		"magic":     {func() []byte { m := append([]byte(nil), pkt...); m[0] = 0; return m }(), ErrBadMagic},
		"v1":        {func() []byte { m := append([]byte(nil), pkt...); m[2] = 1; return m }(), ErrUnsupportedVersion},
		"flags":     {func() []byte { m := append([]byte(nil), pkt...); m[4] = 1; return m }(), ErrReservedFlags},
		"type":      {func() []byte { m := append([]byte(nil), pkt...); m[3] = 0x7f; return m }(), ErrUnknownType},
		"devid":     {func() []byte { m := append([]byte(nil), pkt...); m[28] = '!'; return m }(), ErrBadDeviceID},
		"zero ctr":  {func() []byte { m := append([]byte(nil), pkt...); copy(m[19:27], make([]byte, 8)); return m }(), ErrBadHeader},
	}
	for name, c := range cases {
		if _, err := Open(c.pkt, security.Uplink, ks); !errors.Is(err, c.want) {
			t.Errorf("%s: %v, want %v", name, err, c.want)
		}
	}
}

func TestVersionsCoexist(t *testing.T) {
	s := sampleBodies()[0].(Telemetry).Sample
	v1, err := protocol.EncodeSample(s)
	if err != nil {
		t.Fatal(err)
	}
	if IsVersion2(v1) {
		t.Fatal("v1 packet classified as v2")
	}
	ks := keyStore(t, "DEV-1")
	v2 := uploadFrame(t, ks, "DEV-1", 1, 1, Telemetry{Sample: s})
	if !IsVersion2(v2) {
		t.Fatal("v2 packet not classified as v2")
	}
	if _, err := protocol.DecodeSample(v2); !errors.Is(err, protocol.ErrUnsupportedVersion) {
		t.Fatalf("v1 decoder on a v2 packet: %v", err)
	}
	if _, err := Open(v1, security.Uplink, ks); err == nil {
		t.Fatal("v2 decoder accepted a v1 packet")
	}
}

func TestAckAndDeliveryRoundTrip(t *testing.T) {
	ks := keyStore(t, "DEV-1")
	fp := EventFingerprint("DEV-1", []byte("payload"))
	for _, a := range []EventAck{{UID: 7, Fingerprint: fp, Status: AckStored, Position: 3},
		{UID: 7, Fingerprint: fp, Status: AckConflict}} {
		p, err := a.Encode()
		if err != nil {
			t.Fatal(err)
		}
		pkt, _ := Seal(Header{Type: TypeEventAck, KeyID: 1, SessionGen: 2, Counter: 99, DeviceID: "DEV-1"}, p, secretOf(t, ks, "DEV-1"))
		f, err := OpenWithKey(pkt, security.Downlink, "DEV-1", 1, secretOf(t, ks, "DEV-1"))
		if err != nil {
			t.Fatal(err)
		}
		back, err := DecodeEventAck(f.Payload)
		if err != nil || back != a {
			t.Fatalf("ack %+v %v", back, err)
		}
		if _, err := OpenWithKey(pkt, security.Downlink, "DEV-2", 1, secretOf(t, ks, "DEV-1")); !errors.Is(err, ErrWrongDevice) {
			t.Fatalf("ack for another device: %v", err)
		}
	}
	if _, err := (EventAck{UID: 1, Status: AckStored}).Encode(); err == nil {
		t.Fatal("success ack without position")
	}

	a, _ := security.NewAuthority(1)
	cmd := Command{ID: CommandID{9}, DeviceID: "DEV-1", Type: CommandRequestPosition, Args: PositionArgs(30 * time.Second),
		IssuedAt: t0, ExpiresAt: t0.Add(time.Minute), Principal: "ops:alice"}
	d := CommandDelivery{Command: cmd, Attempt: 1, AttemptSentAt: t0, AttemptNotAfter: t0.Add(30 * time.Second),
		AuthorityKeyID: 1, Signature: a.Sign(cmd.Statement())}
	p, err := d.Encode()
	if err != nil {
		t.Fatal(err)
	}
	back, err := DecodeCommandDelivery("DEV-1", p)
	if err != nil {
		t.Fatal(err)
	}
	re, _ := back.Encode()
	if !bytes.Equal(re, p) || back.Command.Check() != nil {
		t.Fatal("delivery does not round-trip")
	}
	v := security.NewVerifier(map[uint32]ed25519.PublicKey{1: a.Public()})
	if err := v.Verify(1, back.Statement(), back.Signature); err != nil {
		t.Fatal(err)
	}
	// The signature binds the device: the same command for another device fails.
	other := back.Command
	other.DeviceID = "DEV-2"
	if err := v.Verify(1, other.Statement(), back.Signature); err == nil {
		t.Fatal("signature valid for another device")
	}
	// An unsupported type still decodes (to be rejected by the device), but
	// fails Check.
	unk := d
	unk.Type, unk.Args = 77, nil
	pu, err := unk.Encode()
	if err != nil {
		t.Fatal(err)
	}
	bu, err := DecodeCommandDelivery("DEV-1", pu)
	if err != nil || bu.Command.Check() == nil {
		t.Fatalf("unsupported command: %v", err)
	}
	// Attempt validity window is bounded.
	long := d
	long.AttemptNotAfter = t0.Add(MaxAttemptValidity + time.Second)
	if _, err := long.Encode(); err == nil {
		t.Fatal("unbounded attempt validity accepted")
	}
}

// TestGoldenEventFrame pins the exact bytes of one v2 frame (test key
// 0x01..0x20, a DIAGNOSTIC event), so any change to framing, key
// derivation or MAC coverage is caught.
func TestGoldenEventFrame(t *testing.T) {
	ks := security.NewMemoryKeyStore()
	ks.Put(security.DeviceKey{DeviceID: "SIM-00001", KeyID: 1, Secret: testKey()})
	ev, _ := NewEvent(0x0102030405060708, time.UnixMilli(1773480600125), Diagnostic{Code: 0x0203, Value: 5})
	p, _ := ev.Encode()
	pkt, err := Seal(Header{Type: TypeEventUpload, KeyID: 1, SessionGen: 3, Counter: 17, DeviceID: "SIM-00001"}, p, secretOf(t, ks, "SIM-00001"))
	if err != nil {
		t.Fatal(err)
	}
	got := hex.EncodeToString(pkt)
	if got != goldenFrame {
		t.Fatalf("golden frame changed:\n got %s\nwant %s", got, goldenFrame)
	}
}

// goldenFrame was computed independently of this package (Python hashlib
// and hmac, following docs/protocol-v2.md) and matches it byte for byte.
const goldenFrame = "fc7e021000001d00000001000000000000000300000000000000110953494d2d3030303031" +
	"0102030405060708050000019cebae623d000a02030000000000000005" +
	"5256d6eeb8a2b8edc9484ac2abd1fca1c54643a13efb17c9793f4f0b14196767"
