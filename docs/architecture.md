# FleetCore Architecture

This document records the architectural decisions behind FleetCore. Each
decision lists the context, the choice and its consequences, so later
changes can revisit them deliberately.

## Overview (v0.3.0)

```
cmd/fleet-sim ─────► internal/simulator ──────────────┐
cmd/fleet-fusion ──► internal/fusion ─────────────────┤
cmd/fleet-eval ────► internal/evaluation ─┬► fusion   │
                                          └► simulator├──► internal/telemetry
                     internal/protocol ───────────────┤
                           ▲                          │
cmd/fleet-gateway ─► internal/gateway ─┬► internal/eventstore (SQLite) ◄─┐
                                       └► internal/sessions              │
cmd/fleet-processor ─► internal/processor ─┬► internal/chrono            │
                                           ├► internal/fusion            │
                                           ├► internal/resultstore (SQLite)
                                           └► internal/eventstore ───────┘
```

The event store and the gateway also use `internal/protocol`; the session
registry uses only `internal/telemetry`. Both SQLite stores share
`internal/sqlitedb`.

| Package               | Responsibility | Depends on |
|-----------------------|----------------|------------|
| `internal/telemetry`  | Domain model: types, units, validation rules. No I/O. | stdlib |
| `internal/protocol`   | Binary wire format: framing, integrity, encode/decode. | telemetry |
| `internal/simulator`  | Deterministic synthetic fleet generator; test-only ground truth API. | telemetry |
| `internal/fusion`     | Telemetry fusion engine: ordering, sessions, distance estimation, diagnostics. | telemetry |
| `internal/evaluation` | Accuracy harness: simulator truth vs. engine vs. baselines. Tooling only. | fusion, simulator, telemetry |
| `internal/eventstore` | Durable event storage; authoritative identity and deduplication. | protocol, telemetry, `modernc.org/sqlite` |
| `internal/sessions`   | Bounded, in-memory session tracking and provisional ordering diagnostics. | telemetry |
| `internal/gateway`    | UDP receiver with bounded queues, batching writer, graceful shutdown. | protocol, eventstore, sessions, telemetry |
| `cmd/fleet-sim`       | CLI: simulator output as JSON Lines. | simulator, telemetry |
| `cmd/fleet-fusion`    | CLI: JSON Lines samples in, fusion reports out. | fusion, telemetry |
| `cmd/fleet-eval`      | CLI: prints the accuracy table. | evaluation |
| `internal/sqlitedb`   | SQLite configuration (WAL, FULL sync, single writer) and versioned migrations, shared by both stores. | `modernc.org/sqlite` |
| `internal/chrono`     | Deterministic chronological orderer: hybrid order, extended sequence numbers, holding, flags. | telemetry |
| `internal/resultstore`| Results database: processing journal, fusion reports, estimates, diagnostics. | chrono, fusion, sqlitedb, telemetry |
| `internal/processor`  | Event store → orderer → fusion → results, with verified journal replay. | chrono, eventstore, fusion, resultstore |
| `cmd/fleet-gateway`   | Executable running the ingestion pipeline. | gateway, eventstore, sessions |
| `cmd/fleet-processor` | Executable running the chronological processor. | processor, eventstore, resultstore, chrono |

`telemetry` is the core of the dependency graph and depends on nothing else
in the repository. `fusion` depends only on `telemetry`. A test
(`TestNoGroundTruthDependency`) fails if it ever imports the simulator, so
production estimation cannot see ground truth. The protocol, the simulator
and the fusion engine are independent of each other; they meet only in
tests, in the evaluation harness, and later in services that compose them.

The ingestion pipeline (`gateway`, `eventstore`, `sessions`) and `fusion` do
not import each other (`TestIngestionIsDecoupledFromFusion`). They are
connected only by `internal/processor`, which reads the event store and
drives the engine.

The fusion design is documented in [fusion.md](fusion.md), the ingestion
pipeline in [ingestion.md](ingestion.md), the end-to-end processing in
[processing.md](processing.md), and the remaining differences between the
subsystems in [integration.md](integration.md).

## ADR-1: One modular repository, one deployable unit (for now)

**Context.** The roadmap includes a gateway, a session registry, a fusion
engine, an event store, a command dispatcher and more — components that are
often deployed as separate services.

**Decision.** Start as a single Go module with internal packages and
explicit boundaries, not as multiple deployable services.

**Rationale.**

- Service boundaries are expensive to change once they are network APIs.
  The right boundaries are not yet known; package boundaries are cheap to
  move while the domain is still being discovered.
- Distribution adds failure modes (partial failure, retries, ordering,
  versioning between services) that would dominate the work before there is
  any domain logic worth distributing.
- A single module keeps the build, tests and CI trivial and makes
  cross-package refactors atomic, which matters for a codebase meant to be
  maintained and evaluated.
- `internal/` prevents accidental public API commitments.

**Consequences.** Packages must communicate through small, explicit Go APIs
(`telemetry.Sample`, `protocol.EncodeSample`, `simulator.Run`) so that any of
them can later sit behind a process or network boundary. A component will be
split out only when there is a concrete reason (independent scaling,
isolation, deployment cadence).

## ADR-2: Explicit absence instead of zero values

**Context.** In telemetry, 0 is a meaningful reading (a parked vehicle
travels 0 mm) and must not be confused with "no reading".

**Decision.** Missing measurements are represented with
`telemetry.Optional[T]`, encoded in JSON as `null` and on the wire with an
explicit presence bit. A sample carries a `Quality`, and validation enforces
*distance absent ⇔ quality unavailable*.

## ADR-3: Integer units

**Decision.** Distances are `int64` millimetres; device timestamps have
millisecond resolution. Integers avoid floating-point rounding, keep encoding
exact and make equality checks in tests meaningful.

## ADR-4: Source-specific distance semantics

**Decision.** Each source has one fixed distance kind: CAN odometers report
**cumulative** distance; GPS and pulse counters report **deltas** since their
previous reading. The rule lives in `Source.DistanceKind()` and is validated
on every sample. Reconciling the sources is the job of `internal/fusion`
(ADR-8).

## ADR-5: A custom binary protocol with fail-closed decoding

**Decision.** An original, versioned, big-endian protocol with a fixed
header, a length field, a CRC-32C trailer and a 512-byte maximum
(`docs/protocol.md`). Decoding is strict and canonical: anything not
explicitly valid is rejected, and accepted packets re-encode to identical
bytes. A fuzz test (`FuzzDecodeSample`) checks both properties.

**Rationale.** A hand-written format keeps the project dependency-free,
makes every byte reviewable, and provides a realistic surface for parsing
and robustness work. CRC-32C catches corruption only; authentication is a
transport/session concern for a later task.

## ADR-6: Deterministic simulation

**Decision.** The simulator's output depends only on its `Config`.

- All model arithmetic is integer, so results do not depend on floating-point
  behaviour (for example FMA fusion on some CPU architectures).
- Each vehicle and each sensor has its own PCG stream, seeded from
  `(seed, vehicle, stream)` through a splitmix64 mixer. Adding vehicles or
  disabling a source does not change the other streams. (Seeding PCG with
  raw, nearly equal words produced visibly correlated vehicles during
  development, which motivated the mixer.)
- A golden file (`internal/simulator/testdata/golden_seed42.jsonl`) pins the
  exact output, so model changes are always intentional.

## ADR-7: Standard library only

No third-party dependencies in v0.1.0. This minimises supply-chain risk and
keeps the code approachable. Dependencies will be added only when they bring
clear value (for example a metrics library for observability).

*Amended by ADR-13:* the event store depends on a SQLite driver. All other
packages remain standard-library only.

## ADR-8: Fusion as a synchronous, deterministic library

**Context.** Fusion must later be fed by a UDP gateway and by replay from a
persistent event store, and must stay testable without either.

**Decision.** `fusion.Engine` is a plain library with three calls:

- `Process(sample) []Report`
- `Tick(now) []Report`, a processing clock supplied by the caller that
  drains silent devices (see fusion.md §3.5)
- `Flush() []Report`

It has no goroutines, no I/O, no wall clock of its own and no logging. The
integration contract with the event pipeline is in fusion.md §11. Reports
(event dispositions and estimates, with structured diagnostics) are data
returned to the caller.

**Consequences.**

- Any transport can feed it: a gateway calls `Process` per decoded
  packet, a replay tool reads from the event store, and the CLI reads
  JSON Lines.
- The same input order always produces the same output, which is what
  makes replay-based recovery and golden tests possible.
- Concurrency is the caller's choice: one engine per goroutine, sharded
  by device.
- Diagnostics are part of the result, not log lines, so they can be
  stored, counted or alerted on.

## ADR-9: Order by sequence number, with a bounded reorder buffer

**Context.** Arrival order is not chronological. Device clocks skew and
jump, and the ingestion time reflects network delays.

**Decision.** Within a boot session, events are ordered by `Sequence`, the
only field the device assigns monotonically for this purpose.

- A per-session buffer of at most 256 events releases them in order.
- It waits at most a 30 s window, measured in **device time**, for a
  missing number before declaring a gap.
- Events later than that are reported as `requires_reconciliation` instead
  of being applied out of order.
- BootIDs are identifiers, not counters; session order is inferred from
  device time, with an explicit ambiguity outcome.

**Alternatives rejected.**

- *Immediate processing in arrival order* corrupts deltas under
  reordering.
- *Full ordered replay* needs unbounded memory, or a persistent log that
  does not exist yet. It becomes possible with the event store.

See fusion.md §3 and §5.

## ADR-10: Ground truth only through an explicit testing API

**Decision.** `simulator.RunWithGroundTruth` reports the true motion through
a callback, alongside the unchanged sample stream; observing it consumes no
randomness, so the Task 001 golden file is unchanged. Truth is not part of
`telemetry.Sample`, the wire protocol or `fleet-sim` output. Only tests and
`internal/evaluation` use it, and a test guards that `internal/fusion` does
not import the simulator.

## ADR-11: No fusion persistence yet

**Decision.** Fusion state lives in memory only. A process restart loses
it; a new engine re-establishes baselines and counts from there
(documented and tested). Durable recovery is deferred until the Persistent
Event Store exists, because recovery by deterministic replay (ADR-8) is
simpler and more robust than snapshotting internal estimator state.

## ADR-12: Durable identity is authoritative; session ordering is provisional

**Context.** Devices retransmit, UDP reorders and duplicates, devices reboot,
and the domain's "EventID is unique per device" is a convention that the
reference simulator does not keep across reboots (its EventID embeds the
per-boot sequence number).

**Decision.** Event identity is `(DeviceID, BootID, EventID)`, enforced by a
database `UNIQUE` constraint. A SHA-256 fingerprint of the canonical wire
encoding tells retransmissions from conflicts. A retransmission has the same
fingerprint and nothing is written. A conflict has a different fingerprint:
the stored event is kept and the other payload is recorded separately.
Including BootID means a reboot that restarts EventIDs creates new identities
instead of conflicts. The domain contract ("EventID unique per device") is
left unchanged. Violations of it are detected and reported (EventIDs reused
across boots are counted and queryable), never used to reject data.

Session analysis (gaps, reordering, reuse, boot transitions) lives in a
separate in-memory registry. It sees only newly stored events and produces
diagnostics, never filtering decisions. BootIDs are never ordered
numerically; ambiguous chronology is reported explicitly.

**Consequences.** Deduplication survives restarts and is shared by
concurrent writers. Late or out-of-order telemetry is always stored. Session
diagnostics are lost on restart and labelled provisional. The stored events
contain everything needed to replay session history, but no automatic replay
is implemented.

A device that reuses both a BootID and an EventID within it collides. A
different payload is then kept only in the conflict table. Fixing this needs
a wider boot identifier on the wire, which is a protocol change. The gateway
sends no ACKs, so senders get no delivery guarantee. Details in
[ingestion.md](ingestion.md).

## ADR-13: SQLite through `modernc.org/sqlite`

**Context.** The event store needs durable, transactional, constraint-checked
local storage without an external server. Go's standard library has
`database/sql` but no database engine.

**Decision.** SQLite, through `modernc.org/sqlite` **v1.38.2** (pinned in
`go.mod`, with checksums in `go.sum`).

**Rationale.**

- SQLite provides transactions, `UNIQUE`/`CHECK`/foreign-key constraints, WAL
  concurrency and crash safety in a single local file.
- `modernc.org/sqlite` is a pure-Go translation of SQLite. It needs no cgo,
  so the project still builds with `CGO_ENABLED=0`, cross-compiles, and builds
  on CI's Windows runners without a C toolchain. The cgo-based
  `mattn/go-sqlite3` would require one everywhere.
- v1.38.2 is the newest release whose own `go.mod` requires only Go 1.23,
  so adopting it does not raise the module's minimum Go version. Later
  releases require Go 1.24–1.26; upgrading is a separate, deliberate step.

**Costs.** A larger dependency tree (`modernc.org/libc` and helpers, all
indirect) and slower raw throughput than the C build, which is acceptable at
gateway batch sizes. Storage access stays behind the gateway's small
`Appender` interface, so the engine can be replaced.

## ADR-14: Hybrid chronology for the processing stream

**Context.** The processor must deliver stored events to fusion in a
chronological, deterministic order. Two rules conflict:

- "order by device time" is the requirement for chronology;
- fusion's ADR-9 orders a session by sequence, because device clocks jump.

Ordering a session by device time would hand fusion higher sequence numbers
first whenever the clock regresses. Fusion's reorder window is measured in
device time, so it would then declare spurious gaps.

**Decision.**

- Within a session (DeviceID + BootID): extended sequence order. A
  regressing clock produces a `clock_regression` diagnostic and never
  reorders events.
- Between sessions of a device: device time, when it is evidence. Heads
  within `SessionTolerance` of the earliest are ordered by the central
  ingestion order (store position), flagged `chronology_ambiguous`.
- Between devices: ingestion order. Clocks of different devices are not
  comparable, and fusion is per device.
- The store position is a deterministic tie-breaker, never treated as
  evidence of occurrence.
- Each device's next event is held for `Hold` of processing time so
  slightly late earlier events go first. Later ones are delivered flagged
  `late`, never dropped.

**Consequences.** With monotonic clocks this is exactly device-time order.
The rule is a function of the set of candidates, not of iteration order
(`TestSessionChoiceIsIterationIndependent`). Details:
processing.md §2.

## ADR-15: Determinism through a verified processing journal

**Context.** Live ordering depends on arrival timing, and fusion's idle drain
depends on when `Tick` is called. Neither can be recomputed from the stored
events, yet results must be reproducible after a restart.

**Decision.**

- Every `Engine.Process` and `Engine.Tick` call is a journal step,
  committed atomically with its outputs and the read cursor.
- Recovery replays the journal into a fresh engine. Each step's outputs are
  checked against a stored SHA-256, and each event against the event
  store's fingerprint.
- Unjournaled held events are re-read from the low watermark.
- The processor never calls `Flush`.

**Alternatives rejected.**

- *Recompute the order from the store on restart:* it would not reproduce
  live decisions.
- *Snapshot engine state:* fusion deliberately offers no snapshots
  (ADR-11), and a replay heals rather than freezes state.

**Consequences.** A restart never changes, duplicates or loses persisted
results, and every replay is verified rather than assumed. Recovery time
grows with the journal (about 86 000 events/s on the reference machine).
An intentional fusion change is applied with an explicit `-rebuild`, which
rewrites outputs and keeps the order.

## ADR-16: Sequence wrap-around by internal extension

**Context.** `telemetry.Sequence` "may wrap around after 2³²−1", but fusion
rejected everything after 2³²−1 (`sequence_exhausted`), contradicting the
domain.

**Decision.** Fusion and the orderer map each session's sequence numbers to
an internal 64-bit space by bounded serial arithmetic around the highest
number seen. A number 2³⁰ or more away is ambiguous: fusion rejects it
(`sequence_ambiguous`), and the orderer delivers it in ingestion order. The
public `uint32` contract, the wire format and reports are unchanged. The
two implementations are pinned together by a parity test so that fusion
keeps depending only on `telemetry`.

**Consequences.** This amends the fusion contract (fusion.md §9 item 6,
§11.1): `sequence_exhausted` is no longer produced and
`sequence_ambiguous` is new. The accuracy table is byte-identical, since
normal traffic is unaffected. The engine costs about 2% more per sample.

## ADR-17: A separate results database on a shared SQLite layer

**Decision.**

- The processor writes `results.db` (journal, reports, estimates,
  diagnostics) and only reads `events.db`.
- Both databases use `internal/sqlitedb`, extracted from the event store:
  one configuration, one migration mechanism.
- One processor owns a results database at a time; commits are fenced on
  ownership.

**Rationale.** The event store stays the system of record, written only by
gateways. The processor's commits do not compete with the gateways for the
event store's write lock. Results can be rebuilt from the journal without
touching events.

## ADR-18: Reliable communications (protocol v2)

Authenticated, acknowledged delivery and the command lifecycle are a
separate protocol version and set of packages (`internal/security`,
`internal/reliable`, `internal/device`, `internal/commandstore`,
`internal/dispatcher`, `internal/alerting`, `internal/simnet`), with their
own decision records ADR-R1 … ADR-R10 in [reliable.md](reliable.md). The
event store gained schema version 2 (additive); a v2 TELEMETRY event is
also a v1 event, so ADR-12 identity and ADR-15 processing apply unchanged.

## Reserved architectural space

These components are planned but **not** implemented; no placeholder
packages exist for them. The expected placement is listed so that current
APIs do not block them. (The fusion engine, UDP gateway, session registry
and event store, once listed here, are implemented as `internal/fusion`,
`internal/gateway`, `internal/sessions` and `internal/eventstore`; the
command dispatcher as `internal/dispatcher` with protocol v2, ADR-18.
Connecting ingestion to fusion is open work; see
[integration.md](integration.md).)

| Component | Expected placement | Builds on |
|-----------|--------------------|-----------|
| Diagnostics and Observability | `internal/obs` | typed `protocol.Error` codes as metric labels |
| Firmware Transfer Service | `internal/firmware` | new protocol message types (requires protocol v2) |
| Event Replicator | `internal/replicate` | event store |
