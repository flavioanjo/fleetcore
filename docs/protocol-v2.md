# FleetCore Protocol — Version 2 (reliable communications)

This is the normative specification of protocol version 2: authenticated,
acknowledged event delivery from devices and authenticated, authorized
command delivery to devices. The reference implementation is
`internal/reliable` (codec), `internal/security` (key derivation, command
authority), `internal/gateway/reliable.go` (server), `internal/device`
(device). Design rationale, guarantees and the threat model are in
[reliable.md](reliable.md).

Version 1 ([protocol.md](protocol.md)) is unchanged and remains valid. Both
versions share the magic `0xFC 0x7E` and the position of the version byte
(offset 2), so a receiver dispatches on byte 2. A version-1 decoder rejects
v2 packets as an unsupported version; a v2 decoder rejects v1 packets.

## Conventions

- Integers are big-endian; signed integers two's complement.
- Times are `int64` Unix milliseconds, UTC, in `[2000-01-01, 2100-01-01)`.
- "Reserved" fields must be zero; decoders reject non-zero values.
- Every message has exactly one valid encoding. Decoders reject trailing
  bytes, length disagreements, unknown values and reserved bits, and every
  accepted packet re-encodes to identical bytes (checked by fuzzing).
- Session generations and event UIDs are at most 2^63−1.

## Frame

```
offset size field
0      2    magic          0xFC7E
2      1    version        2
3      1    type           message type (below)
4      1    flags          reserved, 0
5      2    length         payload length L
7      4    key_id         device master key version (non-zero)
11     8    session_gen    device session generation (non-zero, ≤ 2^63-1)
19     8    counter        uplink: per-session counter; downlink: random nonce (non-zero)
27     1    device_id_len  n, 1..32
28     n    device_id      ASCII [A-Za-z0-9_-]
28+n   L    payload
28+n+L 32   tag            HMAC-SHA256(message key, bytes [0, 28+n+L))
```

| Constant | Value |
|---|---|
| Maximum packet | 512 bytes (one datagram) |
| Minimum packet | 61 bytes (1-byte device id, empty payload) |
| Tag | 32 bytes (untruncated HMAC-SHA256) |

There is no CRC: the tag detects any modification, accidental or not.

### Message key

```
message_key = HKDF-SHA256(
    IKM  = device master key (32 bytes) for (device_id, key_id),
    salt = "fleetcore/v2/hkdf-salt",
    info = "fleetcore/v2/mac" || 0x00 || direction || type || key_id(4) || session_gen(8)
           || device_id_len(1) || device_id,
    L    = 32)
```

`direction` is `0x01` for device→server (uplink) and `0x02` for
server→device (downlink); `type` is the message type. A tag therefore only
verifies for the exact device, key version, session, direction and message
type it was produced for: a frame cannot be re-labelled, reflected,
replayed into another session or accepted as another message type.

HKDF is RFC 5869 over `crypto/hmac` (verified against the RFC vectors and,
on Go ≥ 1.24, against `crypto/hkdf`).

### Decoding order

1. Size ≤ 512 (too_large), ≥ 61 (truncated).
2. Magic (bad_magic), version 2 (unsupported_version).
3. Declared sizes: `28 + n + L + 32` ≤ 512 (too_large), equal to the
   packet size (truncated / length_mismatch).
4. Flags zero (reserved_flags).
5. Type defined (unknown_type); device id valid (bad_device_id); key id,
   session generation and counter non-zero and session generation ≤ 2^63−1
   (bad_header).
6. Direction of the type matches the receiver (wrong_direction).
7. Key lookup by (device_id, key_id): unknown_key / revoked_key.
8. Tag (bad_mac), compared in constant time.
9. Stateful checks (receiver): session (stale_session), replay window
   (replay).
10. Payload decoding (malformed_payload, length_mismatch) and domain
    validation (invalid_event, invalid_command).

Nothing in the payload is interpreted before step 8. A receiver never
answers a packet rejected before step 9 (it may be forged); it answers an
authenticated invalid event with a signed `AckInvalid`.

## Sessions and replay

- **Device.** At every boot the device increments `session_gen` in its
  durable store and commits it before sending anything. Generations are
  never reused, so neither is any (key, session) pair. Within a session the
  uplink `counter` starts at 1 and increases by one per transmitted frame
  (retransmissions get new counters).
- **Server.** The highest generation seen per device is stored durably in
  the shared event store (`device_sessions`). A frame of a lower generation
  is rejected by every gateway (stale_session), across restarts; the check is
  repeated atomically when the event is stored, so an event admitted just
  before another gateway recorded a newer session is not stored either. A frame of
  a higher generation starts a new session. Within a session each gateway
  keeps a 1024-counter sliding window (RFC 4303 §3.4.3): duplicates and
  counters more than 1023 below the highest are rejected (replay).
- **Downlink.** The server seals downlink frames in the device's current
  session (the latest it has authenticated). `counter` is a random nonce:
  several server processes send downlink without a shared counter. The
  device accepts only frames of its own current session. Downlink messages
  are idempotent (below), so a replayed downlink frame within the session
  has no effect.
- **Key rotation.** A device starts a new session to switch to a new key
  version. The same generation with another key id is rejected. Keys have
  states active / retired (still accepted) / revoked (rejected).

## Message types

| Code | Name | Direction | Payload |
|---|---|---|---|
| `0x10` | EventUpload | uplink | event |
| `0x11` | EventAck | downlink | verdict on one event |
| `0x12` | Heartbeat | uplink | session announcement |
| `0x20` | CommandDelivery | downlink | one command delivery attempt |

### `0x10` EventUpload

```
0  8  event_uid    non-zero, ≤ 2^63-1, never reused by the device
8  1  event_type   below
9  8  device_time  generation time, device clock
17 2  body_len     ≤ 256
19 m  body         typed body, canonical
```

**Fingerprint** — `SHA-256("fleetcore/v2/event" || 0x00 || device_id_len ||
device_id || payload)`. It excludes the envelope (key id, session, counter,
tag), so a retransmission in a later session has the same fingerprint.

| Code | Type | Class | Body |
|---|---|---|---|
| `0x01` | TELEMETRY | routine | a canonical version-1 TelemetrySample packet (49–80 bytes). Its device id must equal the frame's, its event_id the event_uid and its device_time the envelope's. Stored also as a v1 event, so fusion consumes it. |
| `0x02` | POSITION | routine | fix(24) reason(1) request_id(16) — 41 bytes |
| `0x03` | PANIC | emergency | trigger(1) has_position(1) fix(24) position_age_ms(4) — 30 bytes |
| `0x04` | SECURITY_VIOLATION | security | code(1) detail(4) |
| `0x05` | DIAGNOSTIC | diagnostic | code(2) value(8) |
| `0x06` | BLOCK_STATE | control | state(1) cause(1) command_id(16) |
| `0x07` | CARGO_STATE | control | door(1) latch(1) cause(1) command_id(16) |
| `0x08` | COMMAND_RESULT | control | command_id(16) command_type(1) stage(1) reason(2) observed(1) |

Critical classes: emergency and security.

**Fix** (24 bytes): `fix(1) lat_e7(4, int32) lon_e7(4, int32)
speed_kmh_x10(2) heading_cdeg(2) hdop_x10(2) satellites(1) fix_time(8)`.
`fix`: 1 none, 2 2D, 3 3D. A fix of 1 (none) has every other field zero —
no coordinates are ever transmitted without a fix. A 2D/3D fix has
latitude in ±90°, longitude in ±180°, speed ≤ 500 km/h, heading < 360°,
HDOP 0.1–99.9, satellites ≥ 3 (2D) / ≥ 4 (3D) and ≤ 64.

**POSITION** always carries a 2D/3D fix. `reason` 1 periodic (request_id
zero) or 2 requested (request_id = the REQUEST_POSITION command id).

**PANIC**: `trigger` 1 = panic button. `has_position` 1: the most recent
valid fix and its age (≤ 24 h); 0: fix bytes and age zero.

**SECURITY_VIOLATION** codes: 1 tamper, 2 command with an invalid
authority signature, 3 burst of downlink authentication failures.

**BLOCK_STATE** `state`: 1 blocked, 2 unblocked, 3 unknown (sensor).
**CARGO_STATE** `door`: 1 open, 2 closed, 3 unknown; `latch`: 1 locked,
2 released, 3 unknown. `cause`: 1 after a command, 2 at boot, 3 observed
change, 4 after an interrupted execution; command_id is non-zero exactly
for causes 1 and 4.

**COMMAND_RESULT** `stage`: 1 received (durably recorded), 2 accepted,
3 rejected, 4 succeeded, 5 failed, 6 unknown. `reason` (u16): 0 none,
1 expired, 2 unsupported, 3 bad_arguments, 4 vehicle_moving,
5 ignition_on, 6 actuator_fault, 7 not_confirmed_by_sensor,
8 position_unavailable, 9 interrupted, 10 already_in_state,
11 sensor_unavailable, 12 attempt_stale, 13 busy. `observed`: the actuator
state read back (BLOCK_STATE or door code), 0 if not applicable.
`command_type` echoes the delivered type (any non-zero code, so a device can
report a type it does not implement as unsupported).

### `0x11` EventAck (49 bytes)

```
0  8  event_uid
8  32 fingerprint      of the event being answered
40 1  status
41 8  server_position  > 0 exactly for success statuses
```

| Status | Meaning |
|---|---|
| 1 stored | committed now |
| 2 duplicate | an identical event with this device and UID was already committed |
| 3 conflict | this UID is committed with another fingerprint; permanent |
| 4 invalid | authenticated but invalid body; permanent |
| 5 retry | the server could not commit; retry later |

Only 1 and 2 are success, and both mean *durably committed to the event
store*: a gateway sends them only after the store transaction returned.
A device completes an event only on a success ACK in its current session
whose UID **and** fingerprint match a pending event of its own.

### `0x12` Heartbeat (12 bytes)

`device_time(8) pending_events(4)`. Announces the device's session and
address when it has no events to send. Never answered.

### `0x20` CommandDelivery

```
command_id(16) type(1) schema_version(1)=1 args_len(1) args
issued_at(8) expires_at(8) principal_len(1) principal(1..64)
attempt(4) attempt_sent_at(8) attempt_not_after(8)
authority_key_id(4) authority_signature(64)
```

The device id is the frame's. Command types (schema 1): 1 BLOCK (no args),
2 UNBLOCK (no args), 3 OPEN_CARGO (compartment, 1 byte, 1–4),
4 REQUEST_POSITION (maximum fix age in seconds, 2 bytes, 1–3600).
`expires_at` is the last instant execution may start. `attempt_not_after −
attempt_sent_at` ≤ 5 minutes: the device ignores an attempt received after
`attempt_not_after` (a delayed or replayed datagram).

**Authority signature** — Ed25519 (RFC 8032) by the command authority over
`"fleetcore/v2/command-authorization" || 0x00 || statement`, where

```
statement = "FCCMD1" command_id(16) device_id_len(1) device_id type(1) schema_version(1)
            args_len(1) args issued_at(8) expires_at(8) principal_len(1) principal
```

The signature binds what, where, by whom and until when; the attempt fields
change per attempt and are covered by the frame tag instead. Devices are
provisioned with the authority public key(s) by version.

A delivery with an unknown type or invalid arguments still decodes (so the
device can reject it with a COMMAND_RESULT); everything else fails closed.

## Idempotence of every message

| Message | Repeated or replayed within its session |
|---|---|
| EventUpload | Duplicate: stored once, answered `duplicate` |
| EventAck | the event is already complete: ignored |
| Heartbeat | refreshes the session's last-seen time |
| CommandDelivery | the device knows the CommandID: never executes again; may re-report its current stage |

## Versioning

The version byte identifies the whole format. Message types, flags, event
types, command types and command schema version may only change with a new
protocol or schema version. Decoders reject what they do not implement.

## Golden frame

An EventUpload of a DIAGNOSTIC (code 0x0203, value 5), device `SIM-00001`,
key id 1, session 3, counter 17, uid `0x0102030405060708`, device time
`2026-03-14T09:30:00.125Z`, master key `0x01 0x02 … 0x20`
(`TestGoldenEventFrame`; computed independently in Python):

```
fc7e 02 10 00 001d                           magic, v2, EventUpload, flags 0, length 29
00000001 0000000000000003 0000000000000011   key id 1, session 3, counter 17
09 53494d2d3030303031                        device id "SIM-00001"
0102030405060708 05 0000019cebae623d 000a    uid, DIAGNOSTIC, device time, body length 10
0203 0000000000000005                        code, value
5256d6eeb8a2b8edc9484ac2abd1fca1c54643a13efb17c9793f4f0b14196767   HMAC-SHA256 tag
```
