# FleetCore Architecture

This document records the architectural decisions behind FleetCore. Each
decision lists the context, the choice and its consequences, so later
changes can revisit them deliberately.

## Overview (v0.2.0)

```
cmd/fleet-sim ────► internal/simulator ─────────────┐
cmd/fleet-fusion ─► internal/fusion ────────────────┤
cmd/fleet-eval ───► internal/evaluation ─┬► fusion  ├──► internal/telemetry
                                         └► simulator│
                    internal/protocol ───────────────┘
```

| Package               | Responsibility | Depends on |
|-----------------------|----------------|------------|
| `internal/telemetry`  | Domain model: types, units, validation rules. No I/O. | stdlib |
| `internal/protocol`   | Binary wire format: framing, integrity, encode/decode. | telemetry |
| `internal/simulator`  | Deterministic synthetic fleet generator; test-only ground truth API. | telemetry |
| `internal/fusion`     | Telemetry fusion engine: ordering, sessions, distance estimation, diagnostics. | telemetry |
| `internal/evaluation` | Accuracy harness: simulator truth vs. engine vs. baselines. Tooling only. | fusion, simulator, telemetry |
| `cmd/fleet-sim`       | CLI: simulator output as JSON Lines. | simulator, telemetry |
| `cmd/fleet-fusion`    | CLI: JSON Lines samples in, fusion reports out. | fusion, telemetry |
| `cmd/fleet-eval`      | CLI: prints the accuracy table. | evaluation |

`telemetry` is the core of the dependency graph and depends on nothing else
in the repository. `fusion` depends only on `telemetry`. A test
(`TestNoGroundTruthDependency`) fails if it ever imports the simulator, so
production estimation cannot see ground truth. The protocol, the simulator
and the fusion engine are independent of each other; they meet only in
tests, in the evaluation harness, and later in services that compose them.

The fusion design is documented in [fusion.md](fusion.md).

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

## Reserved architectural space

These components are planned but **not** implemented; no placeholder
packages exist for them. The expected placement is listed so that current
APIs do not block them.

| Component | Expected placement | Builds on |
|-----------|--------------------|-----------|
| UDP Gateway | `internal/gateway`, `cmd/fleet-gateway` | `protocol.DecodeSample`; sets `IngestedAt`; feeds `fusion.Engine.Process` |
| Device Session Registry | `internal/session` | `BootID`, `Sequence`; could take over fusion's session tracking for all consumers |
| Persistent Event Store | `internal/store` | `EventID` uniqueness per device; enables replay-based fusion recovery and reconciliation of late events |
| Diagnostics and Observability | `internal/obs` | typed `protocol.Error` codes as metric labels |
| Firmware Transfer Service | `internal/firmware` | new protocol message types (requires protocol v2) |
| Command Dispatcher | `internal/command` | new protocol message types |
| Event Replicator | `internal/replicate` | event store |
