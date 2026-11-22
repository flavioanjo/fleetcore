# FleetCore Event Ingestion Pipeline

This document describes how `cmd/fleet-gateway` turns UDP datagrams into
durable, deduplicated telemetry events, and what each stage guarantees. The
pipeline is independent of telemetry fusion: it does not estimate distance
or reconcile odometers. It preserves reliable, traceable telemetry for
downstream consumers.

```
UDP socket
   │  read loop (1 goroutine): stamp IngestedAt, size check
   ▼
decode queue (bounded, QueueSize) ── full ──► drop newest, count QueueFull
   │  decode workers (Workers goroutines): protocol.DecodeSample, validation
   ▼
store queue (bounded, StoreQueueSize; producers block)
   │  writer (1 goroutine): batches of ≤ BatchSize or BatchDelay
   ▼
in-memory dedup cache ── exact retransmission hit ──► Duplicate (no DB call)
   ▼
eventstore (SQLite, one transaction per batch) ──► Inserted / Duplicate / Conflict
   ▼  Inserted only
session registry ──► provisional diagnostics (logs, counters)
```

| Package | Role |
|---------|------|
| `internal/gateway` | UDP receiver, bounded queues, batching writer, shutdown. |
| `internal/eventstore` | Durable storage; authoritative event identity and deduplication. |
| `internal/sessions` | In-memory, bounded session tracking and ordering diagnostics. |
| `cmd/fleet-gateway` | Executable wiring the three together. |

None of them imports `internal/fusion` or `internal/simulator` (the simulator
is used in tests only). No Task 001 contract (`internal/telemetry`,
`internal/protocol`, `docs/protocol.md`) was changed.

## Gateway

### Input validation

Every datagram read is counted in `Stats.Received` and ends in exactly one
outcome: a rejection reason, `QueueFull`, `Inserted`, `Duplicates`,
`Conflicts`, `StoreFailed` or `ShutdownDropped`.

1. Size: larger than `-max-datagram` (≤ 512, the protocol maximum) →
   `oversize`. The read buffer has one spare byte so truncating platforms
   are detected; Windows' `WSAEMSGSIZE` is mapped to the same reason.
2. `protocol.DecodeSample`: framing, CRC-32C, canonical payload and domain
   validation. Each `protocol.ErrorCode` has its own rejection reason
   (`checksum_mismatch`, `bad_magic`, `truncated`, …).
3. Revalidation with the ingestion time: a device time more than
   `telemetry.MaxClockSkew` (5 min) ahead of `IngestedAt` →
   `inconsistent_with_ingestion_time`.

Rejections are logged at debug level, with the source address and detail,
and always counted. A flood of garbage cannot flood the logs at the default
level.

### Ingestion timestamp

`IngestedAt` comes from the gateway clock (UTC) and is read immediately after
the datagram, before any queueing. It measures arrival, not processing, and
the device clock is never used for it. A retransmission keeps the
`IngestedAt` of the first stored copy.

### Backpressure

UDP has no flow control, so load is shed at exactly one, explicit point:

- The read loop never blocks on the pipeline. If the decode queue is full,
  the arriving datagram is dropped and counted (`QueueFull`), with a warning
  logged at most once per second.
- Downstream stages block instead of dropping. A slow store fills the store
  queue, which blocks the decode workers, which fills the decode queue,
  which causes drops at the socket.
- The kernel receive buffer (`-read-buffer`) absorbs bursts before that.

The gateway runs a fixed `2 + Workers` goroutines, plus one stats logger in
the command. Memory is bounded by the queue capacities × 513 bytes, plus the
dedup cache and the session registry limits (see
[Benchmark results](#benchmark-results)).

### Store failures

A failed batch is retried `StoreRetries` times with exponential backoff
starting at `RetryBackoff`, each attempt bounded by `WriteTimeout`. After
that its events are dropped, counted in `StoreFailed` and logged at error
level. A batch is one transaction, so a failure never leaves part of it
stored.

### Graceful shutdown

On SIGINT or SIGTERM (context cancellation):

1. The socket is closed; the read loop exits; no new datagrams are accepted.
2. The decode queue is closed and drained by the workers.
3. The store queue is closed; the writer flushes everything left.
4. If this takes longer than `-drain-timeout`, store operations are
   cancelled and the remaining events are counted in `ShutdownDropped`.
5. The event store checkpoints its WAL and closes.

Store operations use their own context, separate from the shutdown signal,
so shutting down does not abort transactions that are already queued.

## Event identity and deduplication

### Verified domain assumption

`internal/telemetry` documents *EventID is unique per DeviceID* as a stream
invariant that no single sample can prove. The reference producer does not
keep it across reboots: `internal/simulator` builds
`EventID = (vehicle index + 1) << 32 | sequence`, and the sequence restarts
with every boot, so after a reboot a device reuses EventIDs for new
measurements.

### Adopted identity: `(DeviceID, BootID, EventID)`

The durable identity is the triple, enforced by
`UNIQUE(device_id, boot_id, event_id)`. Each event also carries a **payload
fingerprint**: SHA-256 of the sample's canonical version-1 encoding
(`protocol.EncodeSample`). The protocol is canonical, so equal fingerprints
mean every transmitted field is equal. `IngestedAt` is not on the wire and is
excluded.

The triple asks less of producers than the domain contract: EventIDs must be
unique per *boot*, not per device. Producers that honour the domain contract
are unaffected. The contract itself is unchanged (`internal/telemetry` is not
modified), and **violations of it are detected, not hidden**. When an
inserted event's `(DeviceID, EventID)` already exists under other BootIDs:

- the store reports those BootIDs (`Result.EventIDAlsoInBoots`);
- the gateway counts the event (`Stats.EventIDReused`) and logs a warning;
- `Store.ByEventID` and the `events_by_event_id` index find every occurrence
  later.

The violating event is still stored.

Alternatives considered:

| Key | Problem |
|-----|---------|
| `(DeviceID, EventID)` | Every post-reboot event of the simulator collides with a pre-reboot one and becomes a conflict, which keeps legitimate telemetry out of the event table. |
| `(DeviceID, BootID, Sequence)` | Sequence is not an event identifier. A repeated `(BootID, Sequence)` with distinct EventIDs is an anomaly to report, not a duplicate to collapse. |
| fingerprint only | Two identical measurements with different EventIDs would be merged. |

### Classification

| Incoming event | Outcome |
|----------------|---------|
| Identity not stored | **Inserted** |
| Same identity, same fingerprint (retransmission, including a late one after a reboot) | **Duplicate**: nothing written |
| Same identity, different fingerprint | **Conflict**: stored event kept unchanged; payload recorded in `event_conflicts` (repeat receipts increment a counter) |
| Same EventID, different device | Different identities, both **Inserted** |
| Same EventID, same device, different boot (reboot) | Different identities, both **Inserted**; counted as `EventIDReused` |
| Identical measurement, different EventID | Different identities, both **Inserted**. A repeated `(BootID, Sequence)` is reported by the session registry as `sequence_reuse` |
| Event arriving after a session transition | Classified by identity only. Lateness never drops it |

**First stored wins.** When two payloads compete for one identity, the one
committed first is the stored event. With UDP, arrival order is not send
order, so this is not a judgement about which payload is correct. The
conflict table keeps the other one, byte for byte, for investigation.
Nothing is silently overwritten: a database trigger rejects any `UPDATE` of a
stored event, whatever code path issues it.

### Limitation: BootID reuse

BootID is a 32-bit value chosen by the device, and the protocol cannot tell a
reused BootID from a continuing session. If a device reuses a BootID **and**
an EventID within it:

- a **different payload** is a Conflict: preserved in `event_conflicts` but
  not in `events`, so consumers reading `events` do not see it;
- an **identical payload** (every transmitted field equal, including device
  time) is a Duplicate. This loses no information.

With random 32-bit BootIDs, the chance that a device repeats some BootID
over *k* boots is about k²/2³³, roughly 0.01 % at 1 000 boots. Removing the
limitation needs a wider or monotonic boot identifier on the wire, which is a
protocol change (a Task 001 contract) and out of scope here. The session
registry also notices a reused BootID: sequence numbers restart inside a
known session, so it reports `sequence_reuse` or `out_of_order`, or
`retired_session` if the old session had expired.

### The in-memory cache

The gateway keeps an LRU of identities it has seen the store accept (default
65 536). An exact retransmission that hits the cache is answered as
`Duplicate` without touching the database. A miss, or a hit with a different
fingerprint, always goes to the store. Entries are created only from store
results, so the cache can never report an unstored event as stored. After a
restart the cache is empty and the database constraint still deduplicates.

## Delivery and durability guarantees

### No acknowledgements

Protocol version 1 has **no acknowledgement message**, and the gateway never
answers a datagram. **A transmitter therefore gets no delivery guarantee and
no way to know whether a packet was stored.** A datagram can be:

- lost in the network;
- dropped by the kernel when the socket buffer is full;
- shed by the gateway when its queue is full;
- rejected as malformed;
- abandoned when shutdown's drain deadline passes;
- dropped after store retries are exhausted.

The gateway counts each of these, but only on its own side. Devices that
need delivery must retransmit, which is safe because retransmissions are
deduplicated. ACKs are planned for a later protocol version.

### What the gateway does guarantee

- **Durability:** an event counted as `Inserted` is committed in a SQLite
  transaction with `synchronous=FULL`. It survives process crashes and power
  loss, provided the storage device honours flushes.
- **Atomic batches:** a batch is one transaction, so it is either entirely
  stored or not stored at all. A failed batch is never partially visible.
- **Idempotence:** storing the same event any number of times, across any
  number of restarts, yields exactly one row.
- **No overwrite:** a stored event never changes, and conflicting payloads
  are kept separately.
- **No silent loss inside the gateway:** every datagram read ends in exactly
  one counted outcome (`Stats`).
- **Lateness is not a reason to drop:** event age, session state and
  ordering never affect whether an event is stored.

### Failure modes

| Failure | Effect | Visible as |
|---------|--------|-----------|
| Datagram lost in the network or kernel buffer | Not received | Not counted (no ACK, see above) |
| Decode queue full | Datagram dropped | `QueueFull`, rate-limited warning |
| Malformed, corrupted or inconsistent | Rejected | `RejectedBy[reason]`, debug log |
| Store error | Batch retried, then dropped | `StoreRetries`, `StoreFailed`, error log |
| Shutdown drain deadline | Remaining events dropped | `ShutdownDropped`, warning |
| Process crash | Queued, uncommitted events lost; committed ones intact | Missing sequences in the stored history |
| Database from a newer build | Gateway refuses to start | `ErrSchemaTooNew` |
| On-disk corruption of a row | Read returns an error, not data | `ErrCorrupt` |

## Event store

See ADR-9 in [architecture.md](architecture.md) for the driver choice.

### Schema (version 1)

- `events`: one row per identity.
  - `position INTEGER PRIMARY KEY AUTOINCREMENT` gives an increasing,
    never-reused position that consumers page through (`ReadAfter`).
  - `UNIQUE(device_id, boot_id, event_id)` is the identity. Indexes on
    `(device_id, boot_id, sequence)` (session history) and
    `(device_id, event_id)` (cross-boot EventID reuse).
  - `packet` holds the canonical wire bytes and is the source of truth when
    reading. The decoded columns exist for SQL queries and `CHECK`
    constraints: non-zero IDs, uint32 ranges, source and quality enums,
    distance present ⇔ quality ≠ unavailable.
  - `event_id` is the uint64 stored as an int64 with the same bit pattern.
  - `STRICT` table.
- `event_conflicts`: conflicting payloads,
  `UNIQUE(device_id, boot_id, event_id, fingerprint)`, foreign key to
  `events`.
- `schema_migrations`: applied versions.

Reads re-verify `sha256(packet) == fingerprint` and decode the packet, so
on-disk corruption is reported as `ErrCorrupt` instead of being returned as
data.

### Migrations

Migrations are an ordered, forward-only list in `migrate.go`. Open applies
the pending ones in one `BEGIN IMMEDIATE` transaction, so concurrent openers
serialize and a failed migration leaves the schema unchanged. A database
whose version is newer than the build is refused (`ErrSchemaTooNew`). Once
released, a migration is never edited; version 1 is the first released
schema.

Tests cover:

- upgrading a populated v1 database with a later migration;
- rollback of a failing migration;
- refusal of a newer schema;
- concurrent first opens (switching a new file to WAL is retried while
  another opener holds the lock).

### SQLite settings

| Setting | Value | Why |
|---------|-------|-----|
| `journal_mode` | WAL (verified on open) | Readers and the writer do not block each other. |
| `synchronous` | FULL | A committed batch survives power loss. NORMAL in WAL can lose the latest commits. |
| `busy_timeout` | 5 s (`-busy-timeout`) | Another process holding the lock causes waiting, not an immediate failure. |
| Write transactions | `BEGIN IMMEDIATE` | Takes the write lock up front, so no deadlocks from upgrading a read lock. |
| Write pool | 1 connection | SQLite allows one writer at a time; serializing in-process avoids lock contention. |
| Read pool | 4 connections, `query_only` | Concurrent reads that cannot write by construction. |
| `foreign_keys` | ON | Enforces the conflicts → events relation. |
| Close | `wal_checkpoint(TRUNCATE)`, then close | Leaves a compact database file. Waits for in-flight operations; later calls return `ErrClosed`. |

Each batch is one transaction (default ≤ 256 events), which spreads the
FULL-sync cost over many events.

## Session registry

The registry is advisory and in memory. It sees only events the store newly
inserted, so retransmissions never look like reordering, and it never
decides what is stored.

Per device it tracks up to `MaxSessionsPerDevice` sessions, one per BootID.
For each session it keeps:

- first and last ingestion time, and the device-time range;
- lowest and highest sequence, in serial arithmetic so wraparound is
  handled;
- received, missing, gap, out-of-order and reuse counts;
- a bitmap of the last `SequenceWindow` sequence numbers.

| Diagnostic | Meaning |
|------------|---------|
| `new_session` | First sample of a BootID not tracked or remembered. |
| `retired_session` | Sample for a BootID whose session expired or was evicted (a delayed event from an older session). Tracked again. |
| `earlier_observed_session` | Sample for a tracked session other than the device's most recently observed one. |
| `chronology_ambiguous` | A new session appeared while another was active within `OverlapWindow`, or its device time is not after the other session's latest. |
| `sequence_gap` | The sequence jumped ahead; `missing` numbers were skipped. |
| `out_of_order` | Sequence behind the highest, not seen before (may fill a gap). |
| `sequence_reuse` | `(BootID, Sequence)` already received with another EventID. |
| `sequence_unverifiable` | Older than the tracking window; whether it fills a gap or reuses a number cannot be determined. |

**Chronology.** BootID is opaque and never compared numerically for order,
and device clocks can reset on boot. Sessions are therefore ordered only by
when the registry first observed them. When arrival order and device time
disagree, or sessions overlap, the registry reports `chronology_ambiguous`
and does not pick an order.

**Bounds.**

| Limit | Default | When exceeded |
|-------|---------|---------------|
| `MaxDevices` | 100 000 | Least recently active device is forgotten. |
| `MaxSessionsPerDevice` | 4 | Least recently active session is retired. |
| `MaxRetiredPerDevice` | 16 | Oldest retired BootID is forgotten. |
| `SequenceWindow` | 1024 | Bits kept per session; older samples are `sequence_unverifiable`. |
| `SessionIdleTimeout` | 1 h | Idle session is retired. |
| `DeviceIdleTimeout` | 24 h | Idle device is forgotten. |

Both timeouts are measured in ingestion time. All times come from the
samples' `IngestedAt`, so behaviour is deterministic in tests.

**Provisional.** Registry state lives in memory only and is lost on
restart. Diagnostics are logged with `"provisional": true` and counted in
gateway stats; they are **not persisted**. After a restart the registry
starts empty: the first sample of each device is reported as `new_session`,
and gaps or reordering relative to pre-restart traffic go undetected. Stored
events are unaffected.

### Reconstructing history

The store keeps what is needed to rebuild session history. For every
accepted event it has:

- `boot_id`, `sequence`, `device_time_ms` and `ingested_at_ns`;
- the canonical packet;
- a `position` recording insertion order, which is the order the live
  registry observed events in.

Conflicting payloads are in `event_conflicts`.

**No automatic reconstruction is implemented.** The gateway does not replay
the store into the registry on start-up. A consumer can rebuild session state
by reading `Store.ReadAfter` from position 0 and passing each record's
sample to a fresh `sessions.Registry`. `TestConcurrentDevicesOverUDP` checks
that this replay reproduces the live registry's state exactly. For ad-hoc
analysis:

```sql
-- Per-session history; missing sequence numbers are gaps.
SELECT boot_id, sequence, device_time_ms, ingested_at_ns
FROM events WHERE device_id = 'SIM-00001'
ORDER BY boot_id, sequence;

-- EventIDs reused across boots (domain-contract violations).
SELECT device_id, event_id, count(*) AS boots
FROM events GROUP BY device_id, event_id HAVING boots > 1;
```

## Security review

The review covered the Task 003 changes: the untrusted-input path, resource
bounds, storage and logging.

**Trust model.** Anyone who can send UDP to the listener can submit
well-formed packets for any DeviceID. CRC-32C detects accidental corruption;
it is **not** authentication. Run the gateway only on trusted networks.

| Threat | Status |
|--------|--------|
| Spoofed telemetry for real devices | **Not prevented.** Needs transport authentication (future protocol work). |
| Identity pre-emption: a forged packet stored first makes the genuine one a conflict ("first stored wins") | **Not prevented**, but detectable: the genuine payload is kept in `event_conflicts` and counted. |
| Disk exhaustion through unlimited distinct identities or conflicting payloads | **Not bounded** by the gateway. Conflict rows are unique per (identity, fingerprint), so varying payloads add rows. Mitigate with network restrictions and disk monitoring; quotas are future work. |
| Memory exhaustion | Bounded. Queues have fixed capacity (≤ 513 B per datagram), the dedup cache is an LRU, and the registry caps devices, sessions, retired BootIDs and sequence windows. |
| Goroutine exhaustion | Bounded: a fixed `2 + Workers`; nothing is spawned per packet. |
| Log flooding | Per-datagram rejections log at debug level only. Conflict, EventID-reuse and session-diagnostic logs are rate-limited to 20 per second, with a "suppressed" count. Queue-full warnings appear at most once per second. Counters are never rate-limited. |
| Reflection or amplification | Not possible: the gateway never sends datagrams. |
| Parser attacks | The decoder is strict, canonical and fuzzed (`FuzzDecodeSample`). Every truncation and every single-bit flip of a valid packet is tested over real UDP. |
| SQL injection | All statements are parameterized. The only text column, `device_id`, is validated to `[A-Za-z0-9_-]{1,32}`. |
| Log injection | DeviceIDs are validated before logging. Logs are structured (JSON by default) and escape all values. |
| Tampering with stored rows | Rows are immutable (trigger), and reads verify the SHA-256 against the packet. This detects accidental corruption, not a party with write access to the file. |
| Database file exposure | The file is created with the process's default permissions (umask). It holds device identifiers and distances; restrict the directory as needed. |
| Dependencies | `modernc.org/sqlite` v1.38.2 and its `modernc.org/*` and `golang.org/x/sys` dependencies, pinned by `go.sum`. `govulncheck` (v1.1.4) finds no vulnerability reachable from FleetCore code. It does list GO-2026-5024 in the indirect `golang.org/x/sys` v0.34.0, which FleetCore never calls. The fix, v0.44.0, requires Go 1.25, so upgrading means raising the module's minimum Go version: a separate, repository-wide decision. |

## Benchmark results

Reproduce with:

```bash
go test -run='^$' -bench=. -benchmem -count=5 ./internal/eventstore ./internal/sessions ./internal/gateway
```

**Conditions.** These are medians of 5 runs, measured 2026-11-22 on an Intel
Core i7-10750H (6 cores, 12 threads), 32 GB RAM, Crucial P310 NVMe SSD,
Windows 11 Pro, go1.27.0 windows/amd64, `modernc.org/sqlite` v1.38.2. The
database is in the OS temp directory on that SSD, with WAL and
`synchronous=FULL`. Loopback UDP. Absolute numbers depend heavily on the
disk's flush latency; compare them only on the same machine.

| Benchmark | What it measures | Result |
|-----------|------------------|--------|
| `AppendBatch/batch=1` | Durable insert, one transaction per event | 781 events/s (1.28 ms/event) |
| `AppendBatch/batch=64` | Durable insert, 64 events per transaction | 5 571 events/s (179 µs/event) |
| `AppendBatch/batch=256` | Durable insert, 256 events per transaction (gateway default) | 6 189 events/s (161 µs/event), 3.2 KB and 63 allocs per event |
| `AppendDuplicates` | Retransmissions resolved by the database (no cache) | 8 514 events/s (118 µs/event) |
| `PipelineUDP` | End to end: 8 devices → loopback UDP → decode → SQLite, ~7 000 packets per run | **5 901 stored events/s**, 0 lost, 9.7–11.2 MiB heap in use |
| `DecodeToEvent` | Per-packet worker CPU: decode, validate, re-encode, SHA-256 | 838 ns, 176 B, 4 allocs |
| `DedupCache` | LRU add + lookup | 313 ns |
| `ObserveInOrder` | Session registry, in-order sample | 132 ns, 0 allocs |
| `RegistryMemory` | Retained heap per tracked device (1 session, 1024-bit window) | 711 B (1 k devices), 700 B (10 k), 691 B (100 k) |

**Reading the numbers.**

- **The bottleneck is the store.** It is per-row work inside the SQLite
  engine (pure Go), not the fsync: batching from 1 to 64 gives ×7, from 64
  to 256 only a further ×1.1.
- **Decoding is far cheaper.** One decode worker can process about 1.2 M
  packets/s.
- **Sustained load above the store rate** (about 6 000 events/s on this
  machine) first fills the queues (16 384 + 16 384 events in the benchmark,
  4 096 + 4 096 by default) and is then shed at the socket and counted in
  `QueueFull`.
- **The session registry's memory is linear:** about 70 MB at the default
  100 000-device cap with one session per device. Each additional tracked
  session adds its state plus a 128-byte sequence bitmap; only the
  one-session case is measured here.
- **Queues are bounded** at ≤ 513 B per datagram (about 4 MB at the default
  capacities). The dedup cache holds 65 536 entries by default.

## Running it

```bash
go run ./cmd/fleet-gateway -listen 127.0.0.1:7400 -db fleetcore-events.db -log-format text
```

Run `fleet-gateway -h` for every flag. The defaults match the values above.
