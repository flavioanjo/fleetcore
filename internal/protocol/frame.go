// Package protocol implements the FleetCore binary telemetry protocol.
//
// The wire format is specified in docs/protocol.md. In summary, a packet is
//
//	magic(2) version(1) type(1) flags(1) length(2) payload(length) crc32c(4)
//
// with all multi-byte integers big-endian and the CRC-32C (Castagnoli)
// computed over every byte before it. A packet is self-delimiting and is
// designed to fit in a single datagram.
//
// Encoding is deterministic: a given input always produces the same bytes,
// and every packet the decoder accepts re-encodes to exactly the same bytes.
package protocol

import (
	"encoding/binary"
	"hash/crc32"
)

// Framing constants.
const (
	// Magic is the two-byte packet prefix ("FC" in spirit; 0xFC 0x7E on the wire).
	Magic uint16 = 0xFC7E

	// Version1 is the only protocol version defined so far.
	Version1 uint8 = 1
	// CurrentVersion is the version the encoder emits.
	CurrentVersion = Version1

	// HeaderSize is the size of the fixed header in bytes.
	HeaderSize = 7
	// ChecksumSize is the size of the trailing CRC-32C in bytes.
	ChecksumSize = 4
	// MaxPacketSize is the largest packet the protocol permits.
	MaxPacketSize = 512
	// MaxPayloadSize is the largest payload that fits in MaxPacketSize.
	MaxPayloadSize = MaxPacketSize - HeaderSize - ChecksumSize
	// MinPacketSize is the size of a packet with an empty payload.
	MinPacketSize = HeaderSize + ChecksumSize
)

// MessageType identifies the payload schema.
type MessageType uint8

// Message types defined in version 1.
const (
	TypeTelemetrySample MessageType = 0x01
)

func (t MessageType) known() bool {
	return t == TypeTelemetrySample
}

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Frame is a decoded packet envelope.
type Frame struct {
	Version uint8
	Type    MessageType
	Payload []byte
}

// EncodeFrame serializes f into a packet. Flags are always zero in version 1.
func EncodeFrame(f Frame) ([]byte, error) {
	if f.Version != Version1 {
		return nil, newError(CodeUnsupportedVersion, "version %d", f.Version)
	}
	if !f.Type.known() {
		return nil, newError(CodeUnknownMessageType, "type 0x%02x", uint8(f.Type))
	}
	if len(f.Payload) > MaxPayloadSize {
		return nil, newError(CodeTooLarge, "payload %d bytes, max %d", len(f.Payload), MaxPayloadSize)
	}
	pkt := make([]byte, HeaderSize+len(f.Payload)+ChecksumSize)
	binary.BigEndian.PutUint16(pkt[0:2], Magic)
	pkt[2] = f.Version
	pkt[3] = uint8(f.Type)
	pkt[4] = 0 // flags
	binary.BigEndian.PutUint16(pkt[5:7], uint16(len(f.Payload)))
	copy(pkt[HeaderSize:], f.Payload)
	end := HeaderSize + len(f.Payload)
	binary.BigEndian.PutUint32(pkt[end:], crc32.Checksum(pkt[:end], crcTable))
	return pkt, nil
}

// DecodeFrame parses and verifies one complete packet. The packet must be
// exactly one frame: trailing bytes are rejected. Checks run in this order,
// and the first failure is returned:
//
//  1. size: at most MaxPacketSize, at least MinPacketSize
//  2. magic
//  3. version
//  4. declared payload length vs. MaxPayloadSize and the actual packet size
//  5. CRC-32C
//  6. reserved flags
//  7. message type
//
// The returned payload is a copy and does not alias packet.
func DecodeFrame(packet []byte) (Frame, error) {
	if len(packet) > MaxPacketSize {
		return Frame{}, newError(CodeTooLarge, "packet %d bytes, max %d", len(packet), MaxPacketSize)
	}
	if len(packet) < MinPacketSize {
		return Frame{}, newError(CodeTruncated, "packet %d bytes, minimum %d", len(packet), MinPacketSize)
	}
	if m := binary.BigEndian.Uint16(packet[0:2]); m != Magic {
		return Frame{}, newError(CodeBadMagic, "got 0x%04x", m)
	}
	version := packet[2]
	if version != Version1 {
		return Frame{}, newError(CodeUnsupportedVersion, "version %d", version)
	}
	typ := MessageType(packet[3])
	flags := packet[4]
	n := int(binary.BigEndian.Uint16(packet[5:7]))
	if n > MaxPayloadSize {
		return Frame{}, newError(CodeTooLarge, "declared payload %d bytes, max %d", n, MaxPayloadSize)
	}
	total := HeaderSize + n + ChecksumSize
	if len(packet) < total {
		return Frame{}, newError(CodeTruncated, "declared payload %d bytes needs %d-byte packet, got %d", n, total, len(packet))
	}
	if len(packet) > total {
		return Frame{}, newError(CodeLengthMismatch, "%d trailing bytes after frame", len(packet)-total)
	}
	end := total - ChecksumSize
	want := binary.BigEndian.Uint32(packet[end:])
	if got := crc32.Checksum(packet[:end], crcTable); got != want {
		return Frame{}, newError(CodeChecksumMismatch, "computed 0x%08x, packet carries 0x%08x", got, want)
	}
	if flags != 0 {
		return Frame{}, newError(CodeReservedFlags, "flags 0x%02x", flags)
	}
	if !typ.known() {
		return Frame{}, newError(CodeUnknownMessageType, "type 0x%02x", uint8(typ))
	}
	payload := make([]byte, n)
	copy(payload, packet[HeaderSize:end])
	return Frame{Version: version, Type: typ, Payload: payload}, nil
}
