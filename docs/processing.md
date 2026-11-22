# FleetCore End-to-End Processing

Task 004 connects the ingestion pipeline to the fusion engine and persists
the results:

```
devices ─UDP─► fleet-gateway #1 ─┐
devices ─UDP─► fleet-gateway #2 ─┴─► events.db (SQLite, shared)
                                         │  position = central ingestion order
                                         ▼
                                  fleet-processor
                    ReadAfter ─► chrono.Orderer ─► fusion.Engine ─► results.db
                                 (hybrid order)    Process / Tick   journal, reports,
                                                                    estimates, diagnostics
```

| Package / command | Role |
|-------------------|------|
| `internal/chrono` | Deterministic orderer: hybrid chronology, extended sequence numbers, holding, flags. |
| `internal/processor` | Processing loop: read, order, fuse, tick, commit; recovery by verified journal replay. |
| `internal/resultstore` | Results database: journal, fusion reports, estimates, diagnostics, processor state. |
| `internal/sqlitedb` | SQLite configuration and migrations shared by the event and results stores. |
| `cmd/fleet-processor` | Executable running the processor. |

Related design: [ingestion.md](ingestion.md) (gateway, event store),
[fusion.md](fusion.md) (engine), [integration.md](integration.md) (the
differences between the two), ADR-14 to ADR-17 in
[architecture.md](architecture.md).

## 1. Two gateways, one store

Any number of `fleet-gateway` processes can write to the same `events.db`.
SQLite serializes their write transactions (`BEGIN IMMEDIATE`, busy
timeout), and the `UNIQUE(device_id, boot_id, event_id)` constraint
deduplicates across them: a packet received by both gateways is stored once
and counted as a `Duplicate` by the second. This is tested in
`TestEndToEndTwoGatewaysSimulatorUDPSQLite`.

The event-store **position** (`INTEGER PRIMARY KEY AUTOINCREMENT`) is the
central, persistent ingestion order of the whole system. Because writers are
serialized, positions become visible in increasing order, so a reader that
pages with `ReadAfter(last position)` never skips a committed event.

**Position is commit order, not occurrence order or send order.** Each
gateway batches its writes, and the two processes compete for SQLite's write
lock, which is not fair. A packet sent later through one gateway can
therefore be committed before a packet sent earlier through the other. Tests
observed this. A single wait for the lock lasts at most `-busy-timeout`, but
a gateway retries a failed commit, so the skew has no hard bound. The
inversion observed in the tests spanned tens of milliseconds, far below the
default `Hold` of 2 s. Consequences:

- **Ordering:** within a session the processor orders by sequence, not
  position, and holds each event (§2.3) long enough to absorb the skew.
- **Conflicts:** "first stored wins" (ingestion.md) means first committed.
  When two different payloads for one identity race through different
  gateways, the winner is not defined by send order. Both payloads are kept:
  one in `events`, one in `event_conflicts`.
- **Session diagnostics** of each gateway's registry cover only that
  gateway's traffic. They remain provisional and per gateway.

## 2. Chronological ordering (ADR-14)

### 2.1 The rule

| Between | Ordered by | Why |
|---------|-----------|-----|
| Events of one session (DeviceID + BootID) | Extended sequence number | The device assigns sequence numbers in generation order. Its clock can jump (fusion ADR-9). |
| Sessions of one device | Device time, when it is evidence; otherwise the central ingestion order | BootIDs are opaque. Device time is the only ordering evidence between boots, but close or contradictory clocks prove nothing. |
| Devices | Central ingestion order | Clocks of different devices are not comparable, and fusion processes devices independently. |

Precisely, for one device, among the next held event ("head") of each of its
sessions:

1. Let *m* be the earliest head device time.
2. Every head with a device time below *m* + `SessionTolerance` (default
   1 s) is a candidate: device time cannot order them.
3. The candidate with the lowest position goes next. If there was more than
   one candidate, the event is flagged `chronology_ambiguous`.

The rule depends only on the set of heads, never on iteration order. The
position is a **deterministic tie-breaker, not evidence** of when an event
happened.

A **regressing device clock** inside a session never reorders events: they
keep sequence order and are flagged `clock_regression`. Ordering by device
time there would hand fusion higher sequence numbers first. Its reorder
window is measured in device time, so it would then declare spurious gaps.

### 2.2 Extended sequence numbers (32-bit wrap)

`telemetry.Sequence` is 32-bit and may wrap after 2³²−1; the public type is
unchanged. Inside the orderer and inside the fusion engine, a session's
sequence numbers are mapped to an internal 64-bit space by **bounded serial
arithmetic**:

- a value is placed at the extended position nearest to the highest number
  seen in the session;
- this works only if that position is less than 2³⁰ away
  (`chrono.SerialLimit`);
- a value 2³⁰ or more away in either direction is **ambiguous**. It could be
  far in the past or far in the future, and it is not guessed:
  - the orderer releases it in ingestion order (`sequence_ambiguous`);
  - fusion rejects it (`rejected` / `sequence_ambiguous`).

So …, 2³²−2, 2³²−1, 0, 1, … is one contiguous run. Reports, journal rows and
diagnostics carry the 32-bit wire values; the journal also stores the
extended value (`ext_sequence`) for audit.

The two implementations (`chrono.Extender`, fusion's internal
`seqExtender`) are kept separate so fusion still depends only on
`internal/telemetry`. `TestExtenderParityWithChrono` checks over 200 000
random steps that they agree.

### 2.3 Holding

Events reach the processor in commit order. Each device's next event is
**held** for `Hold` (default 2 s) of processing time after it was read, so
that an earlier event committed shortly afterwards (by the other gateway,
or after a retransmission) still goes first.

An event arriving after a later event of its session was already released is
**not dropped**. It is released, flagged:

- `late`: a lower sequence number than one already released;
- `sequence_reused`: the same sequence number as one already released (a
  distinct stored event).

Fusion then decides what to do with it, by its own rules: if it is within
fusion's reorder window it is still applied in order; otherwise it becomes
`requires_reconciliation`.

The memory bound is `MaxHeld` (default 65 536 events). Beyond it, the oldest
candidates are released early (`forced_release`). Per-session ordering state
is kept for at most `MaxSessions` (default 100 000) sessions. An evicted
session restarts its ordering state, so lateness relative to events released
before the eviction is no longer detected; `SessionsEvicted` counts
evictions.

### 2.4 Flags

| Flag | Meaning |
|------|---------|
| `late` | A later event of the same session was already processed. |
| `sequence_reused` | Same session and sequence as an event already processed. |
| `clock_regression` | Device time earlier than an earlier-sequence event of the session; sequence order kept. |
| `sequence_ambiguous` | Sequence 2³⁰ or more from the session's position; delivered in ingestion order. |
| `chronology_ambiguous` | Device times of this device's sessions too close to order them; ingestion order decided. |
| `forced_release` | Released before its hold expired because `MaxHeld` was reached. |

Flags are persisted as `chronology` diagnostics. None of them causes an event
to be withheld.

## 3. Determinism and recovery (ADR-15)

Live ordering depends on *when* events arrive, and fusion's idle drain
depends on *when* `Engine.Tick` is called. Neither can be re-derived from the
stored events alone. The processor therefore **records its decisions**.

- **Journal.** Every call into the engine is a journal step: an `event`
  step (store position and fingerprint) or a `tick` step (the processing
  time passed to `Tick`).
- **One transaction per cycle.** Each step is committed together with the
  reports it produced and the processor's cursor. Steps are numbered
  consecutively, and `UNIQUE(position)` guarantees no event is journaled
  twice. Journal rows are immutable (trigger).
- **Verified replay.** On start, the journal is replayed into a fresh engine.
  Each step's outputs are hashed (SHA-256 of their canonical JSON) and
  compared with the stored hash. A mismatch stops the processor
  (`ErrReplayMismatch`) instead of continuing from an unknown state. Each
  replayed event's fingerprint is also checked against the event store
  (`ErrWrongEventStore`).
- **Cursor.**
  - `low_position`: every position at or below it is journaled.
  - `high_position`: the highest position ever read (it never decreases).

  Events read but still held when the process stopped are not in the
  journal, so `low_position < high_position` and positions in
  `(low_position, high_position]` may or may not be journaled. After a
  restart the read cursor starts at `low_position`, not at `high_position`:
  positions already journaled are skipped (their effect is in the replayed
  state), the others are held again for a full hold window, so nothing is
  lost or repeated. While the re-read is catching up, `low_position` never
  passes the read cursor. `TestRestartReplaysAndContinuesWithoutLossOrRepeat`
  and the tests in `internal/processor/recovery_test.go` (held events with
  no new input, interleaved journaled and held positions, repeated
  restarts, crash before and after a commit, fencing) check this.
- **No `Flush`.** The processor never flushes the engine: an open epoch
  stays open across a restart, exactly as without one.

The guarantee: **a restart never changes, duplicates or loses persisted
results.** The persisted output always equals a replay of the persisted
journal.

**Rebuild.** If fusion changes intentionally (or its configuration changes,
detected by a configuration hash), the replay no longer reproduces the
stored outputs. `fleet-processor -rebuild` replays the journal with the new
engine and rewrites the outputs and hashes. The journal (the order) is kept.

**Ownership.**
- A results database has one owner at a time.
- Starting a processor takes ownership (`Claim`).
- Every commit is fenced on it, so the previous owner's next commit fails
  (`ErrNotOwner`) and it stops. Two processors never interleave a journal.

## 4. Ticks

The processor calls `Engine.Tick(now)` every `TickInterval` (default 1 s)
with its processing clock, and journals each call (fusion.md §3.5). A silent
device is drained after fusion's `IdleTimeout` (60 s) of processing time,
whether or not other devices are active. A replay ticks at exactly the
journaled times, so idle drains are reproduced.

## 5. Results database

`results.db` is separate from `events.db`:

- the event store stays the system of record and is never written by the
  processor;
- the processor's commits do not compete with the gateways for the event
  store's write lock;
- results can be rebuilt without touching the events.

Both use the configuration in `internal/sqlitedb`: WAL, `synchronous=FULL`,
single `BEGIN IMMEDIATE` writer, read-only pool and versioned migrations.

| Table / view | Contents |
|--------------|----------|
| `processor_state` | Owner, fusion configuration hash, `low_position`, `high_position`, `next_step`. |
| `journal` | One row per step: `kind` (`event`/`tick`), position, fingerprint, device, boot, 32-bit and extended sequence, device time, chronology flags, tick time, output hash. |
| `reports` | Every fusion report, in output order per step. Event reports have outcome and reason; estimates have trip, delta, uncertainty, confidence and state. The full report is kept as `report_json`. |
| `diagnostics` | One row per fusion diagnostic (code, level, sequence, value, message) and per chronology flag (`origin = 'chronology'`). |
| `latest_estimates` (view) | The most recent estimate per device. |

Example queries:

```sql
-- Current distance per device.
SELECT device_id, trip_mm, uncertainty_mm, confidence, state FROM latest_estimates;

-- Events fusion could not apply and that a reconciliation step should revisit.
SELECT step, device_id, boot_id, sequence FROM reports
WHERE type = 'event' AND outcome = 'requires_reconciliation';

-- Chronology problems by kind.
SELECT code, count(*) FROM diagnostics WHERE origin = 'chronology' GROUP BY code;
```

## 6. Late, duplicate and conflicting events

| Case | Where it is preserved | What fusion sees |
|------|----------------------|------------------|
| Late (arrives after later events) | `events` (always); the journal step is flagged `late` | The event, in journal order; fusion applies it if within its window, otherwise `requires_reconciliation`. Both outcomes are persisted. |
| Retransmission (identical) | Stored once; the gateway counts the duplicate | Never re-delivered: each stored event is journaled exactly once. Identical retransmissions carry no new information. |
| Same identity, different payload (conflict) | First committed in `events`; the other in `event_conflicts` | Only the first. If the two differ in sequence number, the other slot surfaces as a fusion gap (integration.md §1). |
| Distinct event reusing `(BootID, Sequence)` | Both in `events` | Both, the second flagged `sequence_reused`; fusion rejects it as `sequence_conflict`. |
| Ambiguous sequence | `events` | Delivered in ingestion order; fusion rejects it as `sequence_ambiguous`. |

## 7. Guarantees and limitations

**Guarantees**

- Every stored event is processed exactly once, across restarts.
- Persisted results are durable (`synchronous=FULL`) and atomic per cycle.
  They are always identical to a verified replay of the journal.
- Processing order is deterministic given the journal, and the journal is
  the only input a replay needs besides the event store.
- Wrap-around of 32-bit sequence numbers is processed continuously, in the
  orderer and in fusion.

**Limitations**

1. **Recovery time grows with history.** Recovery replays the whole journal
   from step 1, at the rate measured in §8. Snapshots of engine state would
   bound it, but fusion deliberately offers none (fusion.md §11.4).
2. **Journal growth.** A tick step is journaled every `TickInterval`, about
   86 400 rows per day at the default, plus one row per event. No retention
   or compaction exists yet.
3. **Live order is not "ideal" order.** An event later than `Hold` is
   processed where it arrived, flagged `late`. A full re-sort of the store,
   which would yield the most complete result, is not offered, and no
   reconciliation of `requires_reconciliation` events exists yet.
4. **One processor per results database.** Ownership is fenced, but devices
   are not yet sharded across several processors.
5. **Cross-gateway commit skew** (§1) must stay below `Hold` for events of
   one session to reach fusion in sequence order. Beyond it they are
   flagged, and fusion's own reorder window is the second line of defence.
6. **The reference simulator is not wrap-safe for the event store.** It
   derives EventID from the sequence, so after a wrap its EventIDs repeat
   within a boot and the store classifies them as conflicts. Wrap-around
   end to end needs EventIDs unique per boot, as the tests use. Reaching a
   wrap takes about 45 years at 3 samples/s.
7. **Session diagnostics stay provisional** and per gateway. The orderer's
   flags are persisted; the gateways' registry diagnostics are not.

## 8. Benchmarks

**Conditions.**
- Medians of 5 runs, measured 2026-11-22.
- Intel Core i7-10750H, 32 GB RAM, Crucial P310 NVMe SSD, Windows 11 Pro,
  go1.27.0 windows/amd64, `modernc.org/sqlite` v1.38.2.
- Databases in the OS temp directory on that SSD, WAL, `synchronous=FULL`.
- Input: simulator traffic (10 vehicles, 3 sources).

Reproduce with:

```bash
go test -run='^$' -bench='Orderer|ProcessorThroughput|RecoveryReplay' -benchmem -count=5 -benchtime=20000x ./internal/chrono ./internal/processor
```

| Benchmark | What it measures | Result |
|-----------|------------------|--------|
| `OrdererInOrder` / `OrdererReordered` | `chrono.Orderer` Add + Release per event, 100 devices; a quarter of events one step early in the second case | 906 / 896 ns per event, 706 B, 5 allocs |
| `ProcessorThroughput` | Store read, ordering, fusion, and durable commit of journal, reports and diagnostics | **9 706 events/s** (103 µs/event), 8.7 KB and 118 allocs per event |
| `RecoveryReplay` | Restart recovery: journal read, event fetch by position, fusion replay, hash verification | **86 112 events/s** |
| `fusion Process/vehicles=1` | Engine cost per sample with the 64-bit sequence extension (`main` for comparison: 822 ns) | 840 ns/sample (+2%, near run-to-run noise) |
| `gateway PipelineUDP` (Task 003, re-run) | Ingestion end to end after the move to `internal/sqlitedb` (Task 003: 5 901) | 5 868 stored events/s, 0 lost |

**Reading the numbers.**

- **Throughput.** The processor (about 9 700 events/s) is faster than the
  event store's ingestion rate (about 6 000 events/s on this machine,
  ingestion.md). So it keeps up with the gateways, which share one store and
  therefore one write path.
- **Cost per event.** About 103 µs, dominated by inserting the journal row,
  the reports and their diagnostics. Fusion itself is under 1 µs and the
  orderer about 0.9 µs.
- **Recovery time is linear in the journal.** At about 86 000 events/s, a
  journal of 1 million events replays in about 12 s. One day at the
  store's maximum rate (about 500 million events) would take roughly
  100 minutes. This is limitation 1: snapshots or retention are needed
  before running at that load for long periods.

## 9. Running it

```bash
go run ./cmd/fleet-gateway   -listen :7400 -db events.db &
go run ./cmd/fleet-gateway   -listen :7401 -db events.db &
go run ./cmd/fleet-processor -events events.db -results results.db

# After a restart, check that the journal replays exactly:
go run ./cmd/fleet-processor -events events.db -results results.db -verify
```

Run `fleet-processor -h` for every flag.
