package protocol

import (
	"encoding/binary"
	"time"

	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// Telemetry sample payload layout (version 1), all integers big-endian:
//
//	offset size field
//	     0    8 event_id        uint64
//	     8    4 boot_id         uint32
//	    12    4 sequence        uint32
//	    16    8 device_time     int64, Unix milliseconds UTC
//	    24    1 source          uint8
//	    25    1 quality         uint8
//	    26    1 presence        uint8, bit 0 = distance present, bits 1-7 reserved (0)
//	    27    1 distance_kind   uint8, 0 when distance absent
//	    28    8 distance_mm     int64, 0 when distance absent
//	    36    1 device_id_len   uint8, 1..32
//	    37    n device_id       ASCII
const (
	samplePresenceDistance = 0x01
	sampleFixedSize        = 37
	// SampleMinPayloadSize and SampleMaxPayloadSize bound the payload length
	// of a TypeTelemetrySample message.
	SampleMinPayloadSize = sampleFixedSize + 1
	SampleMaxPayloadSize = sampleFixedSize + telemetry.MaxDeviceIDLen
)

// EncodeSample validates s and encodes it as a complete version-1 packet.
// IngestedAt is receiver-side metadata and is not transmitted.
func EncodeSample(s telemetry.Sample) ([]byte, error) {
	if err := s.Validate(); err != nil {
		return nil, &Error{Code: CodeInvalidSample, Err: err}
	}
	p := make([]byte, sampleFixedSize+len(s.DeviceID))
	binary.BigEndian.PutUint64(p[0:8], uint64(s.EventID))
	binary.BigEndian.PutUint32(p[8:12], uint32(s.BootID))
	binary.BigEndian.PutUint32(p[12:16], uint32(s.Sequence))
	binary.BigEndian.PutUint64(p[16:24], uint64(s.DeviceTime.UnixMilli()))
	p[24] = uint8(s.Source)
	p[25] = uint8(s.Quality)
	if d, ok := s.Distance.Get(); ok {
		p[26] = samplePresenceDistance
		p[27] = uint8(d.Kind)
		binary.BigEndian.PutUint64(p[28:36], uint64(d.Value))
	}
	p[36] = uint8(len(s.DeviceID))
	copy(p[sampleFixedSize:], s.DeviceID)
	return EncodeFrame(Frame{Version: CurrentVersion, Type: TypeTelemetrySample, Payload: p})
}

// DecodeSample decodes a complete packet that must carry a telemetry sample.
// The returned sample has passed telemetry validation; IngestedAt is absent
// (the caller sets it on receipt).
func DecodeSample(packet []byte) (telemetry.Sample, error) {
	f, err := DecodeFrame(packet)
	if err != nil {
		return telemetry.Sample{}, err
	}
	if f.Type != TypeTelemetrySample {
		return telemetry.Sample{}, newError(CodeUnknownMessageType, "expected telemetry sample, got type 0x%02x", uint8(f.Type))
	}
	return decodeSamplePayload(f.Payload)
}

func decodeSamplePayload(p []byte) (telemetry.Sample, error) {
	if len(p) < SampleMinPayloadSize {
		return telemetry.Sample{}, newError(CodeLengthMismatch, "sample payload %d bytes, minimum %d", len(p), SampleMinPayloadSize)
	}
	idLen := int(p[36])
	if idLen == 0 || idLen > telemetry.MaxDeviceIDLen {
		return telemetry.Sample{}, newError(CodeMalformedPayload, "device_id_len %d outside [1, %d]", idLen, telemetry.MaxDeviceIDLen)
	}
	if want := sampleFixedSize + idLen; len(p) != want {
		return telemetry.Sample{}, newError(CodeLengthMismatch, "sample payload %d bytes, device_id_len %d requires %d", len(p), idLen, want)
	}

	presence := p[26]
	if presence&^samplePresenceDistance != 0 {
		return telemetry.Sample{}, newError(CodeMalformedPayload, "reserved presence bits 0x%02x", presence)
	}
	kind := telemetry.DistanceKind(p[27])
	value := telemetry.Millimetres(int64(binary.BigEndian.Uint64(p[28:36])))

	s := telemetry.Sample{
		DeviceID:   telemetry.DeviceID(p[sampleFixedSize:]),
		EventID:    telemetry.EventID(binary.BigEndian.Uint64(p[0:8])),
		BootID:     telemetry.BootID(binary.BigEndian.Uint32(p[8:12])),
		Sequence:   telemetry.Sequence(binary.BigEndian.Uint32(p[12:16])),
		DeviceTime: time.UnixMilli(int64(binary.BigEndian.Uint64(p[16:24]))).UTC(),
		Source:     telemetry.Source(p[24]),
		Quality:    telemetry.Quality(p[25]),
	}
	if presence&samplePresenceDistance != 0 {
		s.Distance = telemetry.Some(telemetry.Distance{Kind: kind, Value: value})
	} else if kind != 0 || value != 0 {
		// Canonical encoding: absent fields are all-zero, so that every
		// accepted packet has exactly one byte representation.
		return telemetry.Sample{}, newError(CodeMalformedPayload, "distance absent but kind/value bytes are non-zero")
	}

	if err := s.Validate(); err != nil {
		return telemetry.Sample{}, &Error{Code: CodeInvalidSample, Err: err}
	}
	return s, nil
}
