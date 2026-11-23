# FleetCore Telemetry Protocol — Version 1

This document is the normative specification of the FleetCore binary
telemetry protocol. It is an original format designed for this project. The
reference implementation is `internal/protocol`.

## Design goals

- **Self-delimiting packets** that fit in one datagram (max 512 bytes), so a
  future UDP gateway needs no stream reassembly.
- **Fail closed.** Anything not explicitly valid is rejected: unknown
  versions, types and flags, reserved bits, trailing bytes and length
  disagreements.
- **Canonical encoding.** Each message has exactly one valid byte
  representation, and every accepted packet re-encodes to identical bytes.
  This makes deduplication, hashing and golden tests reliable.
- **No dependencies** beyond the Go standard library.

## Conventions

- All multi-byte integers are **big-endian** (network byte order).
- Signed integers are two's complement.
- Offsets and sizes are in bytes.
- "Reserved" fields must be zero when encoding, and the decoder rejects
  packets in which they are non-zero.

## Packet layout

```
+--------+---------+------+-------+--------+-----------------+----------+
| magic  | version | type | flags | length | payload         | crc32c   |
| 2      | 1       | 1    | 1     | 2      | length bytes    | 4        |
+--------+---------+------+-------+--------+-----------------+----------+
 0        2         3      4       5        7                 7+length
```

| Field   | Size | Description |
|---------|------|-------------|
| magic   | 2    | Constant `0xFC 0x7E`. |
| version | 1    | Protocol version. This document defines version `1`. |
| type    | 1    | Message type (see below). |
| flags   | 1    | Reserved in version 1; must be `0x00`. |
| length  | 2    | Payload length in bytes, `0`–`501`. |
| payload | n    | Message-type-specific payload. |
| crc32c  | 4    | CRC-32C (Castagnoli, polynomial `0x1EDC6F41`, reflected, init and final XOR `0xFFFFFFFF`) over bytes `0 .. 7+length-1`, i.e. header and payload. Check value: CRC-32C of ASCII `123456789` is `0xE3069283`. |

Size limits:

| Constant          | Value |
|-------------------|-------|
| Header size       | 7     |
| Checksum size     | 4     |
| Minimum packet    | 11 (empty payload) |
| Maximum packet    | 512   |
| Maximum payload   | 501   |

A packet must be exactly `7 + length + 4` bytes. Shorter input is
*truncated*; longer input has *trailing bytes* and is rejected (the protocol
never packs several frames into one buffer).

### Why CRC-32C

CRC-32C detects all single-bit and burst errors up to 32 bits, is in the Go
standard library, and is hardware-accelerated on common CPUs. It protects
against **accidental corruption only**: it is not authentication. Message
authentication is a deliberate non-goal of version 1. It is provided by
protocol version 2 ([protocol-v2.md](protocol-v2.md)), which carries a
version-1 sample unchanged inside an authenticated, acknowledged frame.

## Decoding order

Decoders must apply checks in this order and report the first failure. The
order makes errors deterministic and lets cheap structural checks run before
the checksum.

1. Packet size ≤ 512, else **too large**; packet size ≥ 11, else **truncated**.
2. Magic, else **bad magic**.
3. Version, else **unsupported version**.
4. Declared length ≤ 501, else **too large**; packet at least
   `7 + length + 4` bytes, else **truncated**; no trailing bytes, else
   **length mismatch**.
5. CRC-32C, else **checksum mismatch**.
6. Flags are zero, else **reserved flags**.
7. Message type is defined, else **unknown message type**.
8. Payload decoding (message-specific), then domain validation, else
   **malformed payload**, **length mismatch** or **invalid sample**.

## Message types (version 1)

| Code   | Name             | Payload |
|--------|------------------|---------|
| `0x00` | —                | Reserved, never valid. |
| `0x01` | TelemetrySample  | See below. |
| other  | —                | Reserved for future versions. |

### `0x01` TelemetrySample

| Offset | Size | Field          | Type   | Description |
|--------|------|----------------|--------|-------------|
| 0      | 8    | event_id       | uint64 | Non-zero event identifier. |
| 8      | 4    | boot_id        | uint32 | Non-zero identifier of the device power cycle. |
| 12     | 4    | sequence       | uint32 | Per-boot sample counter. |
| 16     | 8    | device_time    | int64  | Milliseconds since the Unix epoch, UTC, by the device clock. |
| 24     | 1    | source         | uint8  | `1` GPS, `2` CAN odometer, `3` pulse counter. `0` and others reserved. |
| 25     | 1    | quality        | uint8  | `1` good, `2` degraded, `3` unavailable. `0` and others reserved. |
| 26     | 1    | presence       | uint8  | Bit 0: distance present. Bits 1–7 reserved (0). |
| 27     | 1    | distance_kind  | uint8  | `1` cumulative, `2` delta. Must be `0` when distance is absent. |
| 28     | 8    | distance_mm    | int64  | Distance in millimetres. Must be `0` when distance is absent. |
| 36     | 1    | device_id_len  | uint8  | `1`–`32`. |
| 37     | n    | device_id      | bytes  | ASCII `[A-Za-z0-9_-]`, `device_id_len` bytes. |

The payload length must equal `37 + device_id_len`, so it is between 38 and
69 bytes and a sample packet is between 49 and 80 bytes.

Absent distance is signalled **only** by the presence bit. A present distance
of 0 mm is a valid measurement ("no movement"), which is why the protocol
does not use zero as a sentinel.

After structural decoding, the sample must satisfy the domain rules of
`internal/telemetry` (see the `Sample` documentation), including:

- `distance_kind` must match the source: GPS and pulse counter report
  **delta**, CAN reports **cumulative**.
- Distance present ⇔ quality is not `unavailable`.
- Cumulative distance in `[0, 10 000 000 km]`; delta in `[0, 500 km]`.
- `device_time` in `[2000-01-01T00:00:00Z, 2100-01-01T00:00:00Z)`.

The ingestion timestamp is assigned by the receiver and is **not** carried
on the wire.

## Example

The CAN sample below (device `SIM-00001`, event `0x0102030405060708`, boot
`0xA1B2C3D4`, sequence 17, time `2026-03-14T09:30:00.125Z`, odometer
98 765 432 100 mm, quality good) is pinned by `TestGoldenEncoding`:

```
fc7e 01 01 00 002e                      header: magic, v1, type 1, flags 0, length 46
0102030405060708                        event_id
a1b2c3d4                                boot_id
00000011                                sequence = 17
0000019cebae623d                        device_time = 1773480600125 ms
02 01 01 01                             source CAN, quality good, present, cumulative
00000016fee0e524                        distance_mm = 98765432100
09 53494d2d3030303031                   device_id_len 9, "SIM-00001"
b3c55772                                crc32c over all preceding bytes
```

Total: 57 bytes (7 header + 46 payload + 4 checksum).

## Versioning rules

- The version byte identifies the entire format: header, flags and every
  payload schema. Any incompatible change requires a new version.
- Decoders reject versions they do not implement. There is no "best effort"
  parsing of unknown versions.
- New message types or flags may only be introduced together with a new
  version number, so a version-1 decoder never silently misinterprets them.
