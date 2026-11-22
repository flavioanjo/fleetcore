package protocol

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"strings"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

var t0 = time.Date(2026, time.March, 14, 9, 30, 0, 125_000_000, time.UTC)

func sampleCAN() telemetry.Sample {
	return telemetry.Sample{
		DeviceID:   "SIM-00001",
		EventID:    0x0102030405060708,
		BootID:     0xA1B2C3D4,
		Sequence:   17,
		DeviceTime: t0,
		Source:     telemetry.SourceCAN,
		Quality:    telemetry.QualityGood,
		Distance:   telemetry.Some(telemetry.Distance{Kind: telemetry.DistanceCumulative, Value: 98_765_432_100}),
	}
}

func sampleGPSUnavailable() telemetry.Sample {
	s := sampleCAN()
	s.Source = telemetry.SourceGPS
	s.Quality = telemetry.QualityUnavailable
	s.Distance = telemetry.None[telemetry.Distance]()
	return s
}

func mustEncode(t *testing.T, s telemetry.Sample) []byte {
	t.Helper()
	b, err := EncodeSample(s)
	if err != nil {
		t.Fatalf("EncodeSample: %v", err)
	}
	return b
}

// reseal recomputes the checksum after a test has modified a packet.
func reseal(p []byte) []byte {
	end := len(p) - ChecksumSize
	binary.BigEndian.PutUint32(p[end:], crc32.Checksum(p[:end], crcTable))
	return p
}

func TestCRC32CKnownAnswer(t *testing.T) {
	// Standard CRC-32C check value.
	if got := crc32.Checksum([]byte("123456789"), crcTable); got != 0xE3069283 {
		t.Fatalf("crc32c check = 0x%08x, want 0xE3069283", got)
	}
}

// TestGoldenEncoding pins the exact wire bytes. If this test fails, the wire
// format changed and docs/protocol.md plus the protocol version must be
// reviewed.
func TestGoldenEncoding(t *testing.T) {
	want := "fc7e" + "01" + "01" + "00" + "002e" + // header, payload 46 bytes
		"0102030405060708" + // event_id
		"a1b2c3d4" + // boot_id
		"00000011" + // sequence 17
		"0000019cebae623d" + // device_time 1773480600125 ms
		"02" + "01" + "01" + "01" + // source CAN, quality good, presence, kind cumulative
		"00000016fee0e524" + // 98_765_432_100 mm
		"09" + hex.EncodeToString([]byte("SIM-00001"))
	want += "b3c55772" // crc32c, also quoted in docs/protocol.md

	if ms := t0.UnixMilli(); ms != 0x19cebae623d {
		t.Fatalf("test fixture: t0 = %d ms (0x%x)", ms, ms)
	}
	got := hex.EncodeToString(mustEncode(t, sampleCAN()))
	if got != want {
		t.Fatalf("encoding mismatch\n got %s\nwant %s", got, want)
	}
}

func TestRoundTrip(t *testing.T) {
	maxID := telemetry.DeviceID(strings.Repeat("Z", telemetry.MaxDeviceIDLen))
	cases := map[string]telemetry.Sample{
		"can good":        sampleCAN(),
		"gps unavailable": sampleGPSUnavailable(),
		"pulse degraded zero delta": func() telemetry.Sample {
			s := sampleCAN()
			s.Source, s.Quality = telemetry.SourcePulse, telemetry.QualityDegraded
			s.Distance = telemetry.Some(telemetry.Distance{Kind: telemetry.DistanceDelta, Value: 0})
			return s
		}(),
		"boundaries": func() telemetry.Sample {
			s := sampleCAN()
			s.DeviceID = maxID
			s.EventID = ^telemetry.EventID(0)
			s.BootID = ^telemetry.BootID(0)
			s.Sequence = ^telemetry.Sequence(0)
			s.DeviceTime = telemetry.MaxTimestamp.Add(-time.Millisecond)
			s.Distance = telemetry.Some(telemetry.Distance{Kind: telemetry.DistanceCumulative, Value: telemetry.MaxCumulativeDistance})
			return s
		}(),
		"minimums": func() telemetry.Sample {
			s := sampleCAN()
			s.DeviceID = "A"
			s.EventID, s.BootID, s.Sequence = 1, 1, 0
			s.DeviceTime = telemetry.MinTimestamp
			s.Distance = telemetry.Some(telemetry.Distance{Kind: telemetry.DistanceCumulative, Value: 0})
			return s
		}(),
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			pkt := mustEncode(t, in)
			if len(pkt) > MaxPacketSize {
				t.Fatalf("packet %d bytes exceeds max", len(pkt))
			}
			out, err := DecodeSample(pkt)
			if err != nil {
				t.Fatalf("DecodeSample: %v", err)
			}
			if !out.DeviceTime.Equal(in.DeviceTime) {
				t.Fatalf("device time %v, want %v", out.DeviceTime, in.DeviceTime)
			}
			out.DeviceTime, in.DeviceTime = time.Time{}, time.Time{}
			if out != in {
				t.Fatalf("round trip mismatch\n in %+v\nout %+v", in, out)
			}
		})
	}
}

func TestDeterministicEncoding(t *testing.T) {
	a := mustEncode(t, sampleCAN())
	b := mustEncode(t, sampleCAN())
	if !bytes.Equal(a, b) {
		t.Fatal("same sample encoded to different bytes")
	}
	s, err := DecodeSample(a)
	if err != nil {
		t.Fatal(err)
	}
	if c := mustEncode(t, s); !bytes.Equal(a, c) {
		t.Fatal("decode/encode did not reproduce the original packet")
	}
}

func TestIngestedAtIsNotTransmitted(t *testing.T) {
	s := sampleCAN()
	s.IngestedAt = telemetry.Some(t0.Add(time.Second))
	if !bytes.Equal(mustEncode(t, s), mustEncode(t, sampleCAN())) {
		t.Fatal("IngestedAt changed the wire encoding")
	}
	out, err := DecodeSample(mustEncode(t, s))
	if err != nil {
		t.Fatal(err)
	}
	if out.IngestedAt.IsPresent() {
		t.Fatal("decoded sample has IngestedAt set")
	}
}

func TestDecodeRejectsMalformedPackets(t *testing.T) {
	good := func() []byte { return mustEncode(t, sampleCAN()) }
	tests := []struct {
		name string
		pkt  func() []byte
		want error
	}{
		{"empty", func() []byte { return nil }, ErrTruncated},
		{"header only", func() []byte { return good()[:HeaderSize] }, ErrTruncated},
		{"too large", func() []byte { return make([]byte, MaxPacketSize+1) }, ErrTooLarge},
		{"bad magic", func() []byte { p := good(); p[0] = 0x00; return reseal(p) }, ErrBadMagic},
		{"version 0", func() []byte { p := good(); p[2] = 0; return reseal(p) }, ErrUnsupportedVersion},
		{"version 2", func() []byte { p := good(); p[2] = 2; return reseal(p) }, ErrUnsupportedVersion},
		{"version 255", func() []byte { p := good(); p[2] = 255; return reseal(p) }, ErrUnsupportedVersion},
		{"unknown type", func() []byte { p := good(); p[3] = 0x7F; return reseal(p) }, ErrUnknownMessageType},
		{"type zero", func() []byte { p := good(); p[3] = 0; return reseal(p) }, ErrUnknownMessageType},
		{"reserved flags", func() []byte { p := good(); p[4] = 0x80; return reseal(p) }, ErrReservedFlags},
		{"declared length over max", func() []byte { p := good(); binary.BigEndian.PutUint16(p[5:7], MaxPayloadSize+1); return p }, ErrTooLarge},
		{"declared length too long", func() []byte {
			p := good()
			binary.BigEndian.PutUint16(p[5:7], uint16(len(p)-MinPacketSize+1))
			return p
		}, ErrTruncated},
		{"declared length too short", func() []byte {
			p := good()
			binary.BigEndian.PutUint16(p[5:7], uint16(len(p)-MinPacketSize-1))
			return p
		}, ErrLengthMismatch},
		{"trailing byte", func() []byte { return append(good(), 0) }, ErrLengthMismatch},
		{"corrupt checksum", func() []byte { p := good(); p[len(p)-1] ^= 0x01; return p }, ErrChecksumMismatch},
		{"corrupt payload", func() []byte { p := good(); p[HeaderSize+3] ^= 0x10; return p }, ErrChecksumMismatch},
		{"payload shorter than sample minimum", func() []byte {
			pkt, _ := EncodeFrame(Frame{Version: 1, Type: TypeTelemetrySample, Payload: make([]byte, SampleMinPayloadSize-1)})
			return pkt
		}, ErrLengthMismatch},
		{"device id length zero", func() []byte { return withPayload(t, good(), func(p []byte) []byte { p[36] = 0; return p }) }, ErrMalformedPayload},
		{"device id length over max", func() []byte {
			return withPayload(t, good(), func(p []byte) []byte { p[36] = telemetry.MaxDeviceIDLen + 1; return p })
		}, ErrMalformedPayload},
		{"device id length disagrees with payload", func() []byte {
			return withPayload(t, good(), func(p []byte) []byte { p[36]--; return p })
		}, ErrLengthMismatch},
		{"reserved presence bits", func() []byte { return withPayload(t, good(), func(p []byte) []byte { p[26] |= 0x02; return p }) }, ErrMalformedPayload},
		{"absent distance with non-zero value", func() []byte {
			return withPayload(t, mustEncode(t, sampleGPSUnavailable()), func(p []byte) []byte { p[35] = 1; return p })
		}, ErrMalformedPayload},
		{"absent distance with quality good", func() []byte {
			return withPayload(t, good(), func(p []byte) []byte { clear(p[26:36]); return p })
		}, ErrInvalidSample},
		{"source zero", func() []byte { return withPayload(t, good(), func(p []byte) []byte { p[24] = 0; return p }) }, ErrInvalidSample},
		{"quality reserved", func() []byte { return withPayload(t, good(), func(p []byte) []byte { p[25] = 99; return p }) }, ErrInvalidSample},
		{"event id zero", func() []byte { return withPayload(t, good(), func(p []byte) []byte { clear(p[0:8]); return p }) }, ErrInvalidSample},
		{"negative distance", func() []byte { return withPayload(t, good(), func(p []byte) []byte { p[28] = 0xFF; return p }) }, ErrInvalidSample},
		{"device time before epoch", func() []byte { return withPayload(t, good(), func(p []byte) []byte { clear(p[16:24]); return p }) }, ErrInvalidSample},
		{"device id bad byte", func() []byte { return withPayload(t, good(), func(p []byte) []byte { p[len(p)-1] = ' '; return p }) }, ErrInvalidSample},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeSample(tc.pkt())
			if !errors.Is(err, tc.want) {
				t.Fatalf("DecodeSample error = %v, want %v", err, tc.want)
			}
			var pe *Error
			if !errors.As(err, &pe) {
				t.Fatalf("error %T is not *protocol.Error", err)
			}
		})
	}
}

// withPayload decodes pkt, lets mutate change the payload, and re-frames it
// with a correct length and checksum so the payload checks are exercised.
func withPayload(t *testing.T, pkt []byte, mutate func([]byte) []byte) []byte {
	t.Helper()
	f, err := DecodeFrame(pkt)
	if err != nil {
		t.Fatal(err)
	}
	f.Payload = mutate(f.Payload)
	out, err := EncodeFrame(f)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestEveryTruncationIsRejected(t *testing.T) {
	pkt := mustEncode(t, sampleCAN())
	for n := 0; n < len(pkt); n++ {
		_, err := DecodeSample(pkt[:n])
		if err == nil {
			t.Fatalf("truncated to %d bytes: accepted", n)
		}
		if !errors.Is(err, ErrTruncated) {
			t.Fatalf("truncated to %d bytes: error %v, want %v", n, err, ErrTruncated)
		}
	}
}

func TestEverySingleBitFlipIsRejected(t *testing.T) {
	pkt := mustEncode(t, sampleCAN())
	for i := range pkt {
		for bit := 0; bit < 8; bit++ {
			p := bytes.Clone(pkt)
			p[i] ^= 1 << bit
			if _, err := DecodeSample(p); err == nil {
				t.Fatalf("bit %d of byte %d flipped: packet accepted", bit, i)
			}
		}
	}
}

func TestEncodeRejectsInvalidInput(t *testing.T) {
	bad := sampleCAN()
	bad.EventID = 0
	if _, err := EncodeSample(bad); !errors.Is(err, ErrInvalidSample) {
		t.Fatalf("EncodeSample(invalid) = %v, want %v", err, ErrInvalidSample)
	} else if !errors.Is(err, telemetry.ErrMissing) {
		t.Fatalf("EncodeSample error %v does not wrap the validation reason", err)
	}

	frameTests := []struct {
		name string
		f    Frame
		want error
	}{
		{"version", Frame{Version: 2, Type: TypeTelemetrySample}, ErrUnsupportedVersion},
		{"type", Frame{Version: 1, Type: 0x42}, ErrUnknownMessageType},
		{"payload too large", Frame{Version: 1, Type: TypeTelemetrySample, Payload: make([]byte, MaxPayloadSize+1)}, ErrTooLarge},
	}
	for _, tc := range frameTests {
		if _, err := EncodeFrame(tc.f); !errors.Is(err, tc.want) {
			t.Errorf("%s: EncodeFrame error %v, want %v", tc.name, err, tc.want)
		}
	}
	if pkt, err := EncodeFrame(Frame{Version: 1, Type: TypeTelemetrySample, Payload: make([]byte, MaxPayloadSize)}); err != nil || len(pkt) != MaxPacketSize {
		t.Fatalf("max payload: len %d, err %v", len(pkt), err)
	}
}

func TestDecodeFrameDoesNotAlias(t *testing.T) {
	pkt := mustEncode(t, sampleCAN())
	f, err := DecodeFrame(pkt)
	if err != nil {
		t.Fatal(err)
	}
	f.Payload[0] ^= 0xFF
	if _, err := DecodeSample(pkt); err != nil {
		t.Fatalf("mutating decoded payload changed the packet: %v", err)
	}
}

// FuzzDecodeSample checks that the decoder never panics and that every
// accepted packet re-encodes to the identical bytes (canonical encoding).
func FuzzDecodeSample(f *testing.F) {
	for _, s := range []telemetry.Sample{sampleCAN(), sampleGPSUnavailable()} {
		pkt, err := EncodeSample(s)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(pkt)
	}
	f.Add([]byte{})
	f.Add([]byte{0xFC, 0x7E, 0x01})
	f.Fuzz(func(t *testing.T, pkt []byte) {
		s, err := DecodeSample(pkt)
		if err != nil {
			var pe *Error
			if !errors.As(err, &pe) {
				t.Fatalf("error %T is not *protocol.Error", err)
			}
			return
		}
		again, err := EncodeSample(s)
		if err != nil {
			t.Fatalf("accepted sample does not re-encode: %v", err)
		}
		if !bytes.Equal(again, pkt) {
			t.Fatalf("non-canonical packet accepted\n in %x\nout %x", pkt, again)
		}
	})
}
