// Package reliable implements FleetCore protocol version 2: authenticated,
// acknowledged event delivery and vehicle command delivery. The normative
// specification is docs/protocol-v2.md; this package is its reference
// implementation. It performs no I/O and holds no state: replay windows,
// session high-water marks, queues and stores live in their own packages.
//
// # Frame
//
//	magic(2) version(1)=2 type(1) flags(1)=0 length(2)
//	key_id(4) session_gen(8) counter(8) device_id_len(1) device_id(n)
//	payload(length)
//	tag(32) = HMAC-SHA256(message key, every preceding byte)
//
// All integers are big-endian. The message key is derived per device key,
// direction, message type, key id and session generation
// (security.MessageKey), so a tag is only valid in the exact context it was
// produced for.
//
// Version 1 (internal/protocol) is unchanged. The two versions share the
// magic and the version byte position, so a receiver dispatches on byte 2.
//
// # Canonical encoding and fail-closed decoding
//
// Every message has exactly one encoding; decoders reject reserved bits,
// unknown values, trailing bytes and length disagreements, and every
// accepted packet re-encodes to identical bytes (checked by the fuzz
// targets). Checks are ordered so that nothing is parsed from a payload
// before its tag has been verified.
package reliable

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/flavioanjo/fleetcore/internal/security"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// Framing constants.
const (
	Magic    uint16 = 0xFC7E
	Version2 uint8  = 2

	// FixedHeaderSize is the header up to and including device_id_len
	// (the device id follows it).
	FixedHeaderSize = 28
	// TagSize is the size of the HMAC-SHA256 tag.
	TagSize = sha256.Size
	// MaxPacketSize is the largest v2 packet (one datagram).
	MaxPacketSize = 512
	// MinPacketSize is a packet with a 1-byte device id and no payload.
	MinPacketSize = FixedHeaderSize + 1 + TagSize
	// MaxIdentity bounds session generations and event UIDs (2^63-1), so
	// they are stored as non-negative SQLite integers and compare correctly.
	// A device incrementing either once per millisecond would need 292
	// million years to reach it.
	MaxIdentity = 1<<63 - 1
	// MaxPayloadSize is the largest payload with a 1-byte device id.
	MaxPayloadSize = MaxPacketSize - MinPacketSize
)

// MessageType identifies a v2 message.
type MessageType uint8

// Message types. Each has one direction; its tag key is derived with the
// type as purpose.
const (
	TypeEventUpload     MessageType = 0x10 // device → server
	TypeEventAck        MessageType = 0x11 // server → device
	TypeHeartbeat       MessageType = 0x12 // device → server
	TypeCommandDelivery MessageType = 0x20 // server → device
)

// Direction returns the direction a message type travels in, or 0.
func (t MessageType) Direction() security.Direction {
	switch t {
	case TypeEventUpload, TypeHeartbeat:
		return security.Uplink
	case TypeEventAck, TypeCommandDelivery:
		return security.Downlink
	}
	return 0
}

func (t MessageType) String() string {
	switch t {
	case TypeEventUpload:
		return "event_upload"
	case TypeEventAck:
		return "event_ack"
	case TypeHeartbeat:
		return "heartbeat"
	case TypeCommandDelivery:
		return "command_delivery"
	}
	return "unknown"
}

// Header is the authenticated envelope metadata of a v2 frame.
type Header struct {
	Type MessageType
	// KeyID is the version of the device master key that keys the tag.
	KeyID uint32
	// SessionGen is the device session generation: a durable counter the
	// device increments (and persists) before every boot's first message.
	// It is part of the key derivation, so tags never verify across
	// sessions.
	SessionGen uint64
	// Counter: uplink, the per-session message counter (starts at 1,
	// strictly increasing, checked against a replay window); downlink, a
	// random non-zero nonce (several server processes send downlink without
	// coordinating a counter; downlink messages are idempotent instead).
	Counter  uint64
	DeviceID string
}

// Frame is an authenticated frame.
type Frame struct {
	Header
	Payload []byte
}

func (h Header) validate() error {
	if h.Type.Direction() == 0 {
		return errf(CodeUnknownType, "type 0x%02x", uint8(h.Type))
	}
	if err := telemetry.DeviceID(h.DeviceID).Validate(); err != nil {
		return &Error{Code: CodeBadDeviceID, Err: err}
	}
	if h.KeyID == 0 || h.SessionGen == 0 || h.Counter == 0 {
		return errf(CodeBadHeader, "key id, session generation and counter must be non-zero")
	}
	if h.SessionGen > MaxIdentity {
		return errf(CodeBadHeader, "session generation above 2^63-1")
	}
	return nil
}

func headerSize(dev string) int { return FixedHeaderSize + len(dev) }

// Seal encodes and authenticates a frame with the device master key.
func Seal(h Header, payload []byte, master *security.Secret) ([]byte, error) {
	if err := h.validate(); err != nil {
		return nil, err
	}
	hs := headerSize(h.DeviceID)
	if hs+len(payload)+TagSize > MaxPacketSize {
		return nil, errf(CodeTooLarge, "payload %d bytes", len(payload))
	}
	pkt := make([]byte, hs, hs+len(payload)+TagSize)
	binary.BigEndian.PutUint16(pkt[0:], Magic)
	pkt[2] = Version2
	pkt[3] = uint8(h.Type)
	pkt[4] = 0
	binary.BigEndian.PutUint16(pkt[5:], uint16(len(payload)))
	binary.BigEndian.PutUint32(pkt[7:], h.KeyID)
	binary.BigEndian.PutUint64(pkt[11:], h.SessionGen)
	binary.BigEndian.PutUint64(pkt[19:], h.Counter)
	pkt[27] = byte(len(h.DeviceID))
	copy(pkt[28:], h.DeviceID)
	pkt = append(pkt, payload...)
	return append(pkt, tag(master, h, pkt)...), nil
}

func tag(master *security.Secret, h Header, msg []byte) []byte {
	k := security.MessageKey(master, h.Type.Direction(), uint8(h.Type), h.DeviceID, h.KeyID, h.SessionGen)
	m := hmac.New(sha256.New, k[:])
	m.Write(msg)
	return m.Sum(nil)
}

// IsVersion2 reports whether a datagram claims to be a v2 packet (magic
// and version byte). It does not validate anything else.
func IsVersion2(pkt []byte) bool {
	return len(pkt) >= 3 && binary.BigEndian.Uint16(pkt) == Magic && pkt[2] == Version2
}

// ParseHeader runs the structural checks of a v2 packet and returns its
// header, without authenticating it. The header of an unauthenticated
// packet must only be used to look up keys; nothing else may be trusted.
//
// Order: size, magic, version, declared length, reserved flags, type,
// device id, non-zero header fields.
func ParseHeader(pkt []byte) (Header, error) {
	if len(pkt) > MaxPacketSize {
		return Header{}, errf(CodeTooLarge, "packet %d bytes, max %d", len(pkt), MaxPacketSize)
	}
	if len(pkt) < MinPacketSize {
		return Header{}, errf(CodeTruncated, "packet %d bytes, minimum %d", len(pkt), MinPacketSize)
	}
	if binary.BigEndian.Uint16(pkt) != Magic {
		return Header{}, errf(CodeBadMagic, "0x%04x", binary.BigEndian.Uint16(pkt))
	}
	if pkt[2] != Version2 {
		return Header{}, errf(CodeUnsupportedVersion, "version %d", pkt[2])
	}
	n := int(binary.BigEndian.Uint16(pkt[5:]))
	devLen := int(pkt[27])
	total := FixedHeaderSize + devLen + n + TagSize
	if total > MaxPacketSize {
		return Header{}, errf(CodeTooLarge, "declared sizes need %d bytes", total)
	}
	if len(pkt) < total {
		return Header{}, errf(CodeTruncated, "declared sizes need %d bytes, got %d", total, len(pkt))
	}
	if len(pkt) > total {
		return Header{}, errf(CodeLengthMismatch, "%d trailing bytes", len(pkt)-total)
	}
	if pkt[4] != 0 {
		return Header{}, errf(CodeReservedFlags, "flags 0x%02x", pkt[4])
	}
	h := Header{
		Type:       MessageType(pkt[3]),
		KeyID:      binary.BigEndian.Uint32(pkt[7:]),
		SessionGen: binary.BigEndian.Uint64(pkt[11:]),
		Counter:    binary.BigEndian.Uint64(pkt[19:]),
		DeviceID:   string(pkt[28 : 28+devLen]),
	}
	if err := h.validate(); err != nil {
		return Header{}, err
	}
	return h, nil
}

// Open parses and authenticates a packet received in direction dir. The
// key is looked up by (device id, key id); a packet whose tag does not
// verify is rejected with CodeBadMAC before any payload byte is examined.
// The returned payload does not alias pkt. Replay and session checks are
// the caller's (they need state).
func Open(pkt []byte, dir security.Direction, keys security.KeyStore) (Frame, error) {
	h, err := ParseHeader(pkt)
	if err != nil {
		return Frame{}, err
	}
	if h.Type.Direction() != dir {
		return Frame{}, errf(CodeWrongDirection, "%s is not a %v message", h.Type, dir)
	}
	k, err := keys.Lookup(h.DeviceID, h.KeyID)
	switch {
	case errors.Is(err, security.ErrKeyRevoked):
		return Frame{}, &Error{Code: CodeRevokedKey, Detail: h.DeviceID}
	case err != nil:
		return Frame{}, &Error{Code: CodeUnknownKey, Detail: h.DeviceID}
	}
	return openWith(pkt, h, &k.Secret)
}

// OpenWithKey authenticates a packet with a known master key (the device
// side, which has exactly one key per version). It also checks the
// direction and that the frame belongs to deviceID.
func OpenWithKey(pkt []byte, dir security.Direction, deviceID string, keyID uint32, master *security.Secret) (Frame, error) {
	h, err := ParseHeader(pkt)
	if err != nil {
		return Frame{}, err
	}
	if h.Type.Direction() != dir {
		return Frame{}, errf(CodeWrongDirection, "%s is not a %v message", h.Type, dir)
	}
	if h.DeviceID != deviceID {
		return Frame{}, errf(CodeWrongDevice, "frame for another device")
	}
	if h.KeyID != keyID {
		return Frame{}, &Error{Code: CodeUnknownKey, Detail: "key version not provisioned on this device"}
	}
	return openWith(pkt, h, master)
}

func openWith(pkt []byte, h Header, master *security.Secret) (Frame, error) {
	end := len(pkt) - TagSize
	if !hmac.Equal(tag(master, h, pkt[:end]), pkt[end:]) {
		return Frame{}, &Error{Code: CodeBadMAC}
	}
	hs := headerSize(h.DeviceID)
	return Frame{Header: h, Payload: append([]byte(nil), pkt[hs:end]...)}, nil
}
