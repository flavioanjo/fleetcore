# FleetCore Architecture

This document records the architectural decisions behind FleetCore. Each
decision lists the context, the choice and its consequences, so later
changes can revisit them deliberately.

## Overview

```
cmd/fleet-sim ──► internal/simulator ──► internal/telemetry
                                              ▲
                  internal/protocol ──────────┤
                        ▲                     │
cmd/fleet-gateway ──► internal/gateway ──► internal/eventstore (SQLite)
                              └──────────► internal/sessions
```

| Package              | Responsibility | Depends on |
|----------------------|----------------|------------|
| `internal/telemetry` | Domain model: types, units, validation rules. No I/O. | stdlib |
| `internal/protocol`  | Binary wire format: framing, integrity, encode/decode. | telemetry |
| `internal/simulator` | Deterministic synthetic fleet generator. | telemetry |
| `cmd/fleet-sim`      | CLI that prints simulator output as JSON Lines. | simulator, telemetry |
| `internal/eventstore` | Durable event storage; authoritative identity and deduplication. | protocol, telemetry, `modernc.org/sqlite` |
| `internal/sessions`  | Bounded, in-memory session tracking and provisional ordering diagnostics. | telemetry |
| `internal/gateway`   | UDP receiver with bounded queues, batching writer, graceful shutdown. | protocol, eventstore, sessions, telemetry |
| `cmd/fleet-gateway`  | Executable running the ingestion pipeline. | gateway, eventstore, sessions |

The ingestion pipeline is described in [ingestion.md](ingestion.md).

`telemetry` is the core of the dependency graph and depends on nothing else
in the repository. The protocol and the simulator are independent of each
other; they meet only in tests (`internal/protocol/simulator_roundtrip_test.go`)
and, later, in services that compose them.

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
on every sample. Reconciling the sources (odometer fusion) is deliberately
out of scope until Task 002+.

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

*Amended by ADR-9:* the event store depends on a SQLite driver. All other
packages remain standard-library only.

## ADR-8: Durable identity is authoritative; session ordering is provisional

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

## ADR-9: SQLite through `modernc.org/sqlite`

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

## Reserved architectural space

These components are planned but **not** implemented; no placeholder
packages exist for them. (The UDP gateway, session registry and event store,
once listed here, are implemented as `internal/gateway`,
`internal/sessions` and `internal/eventstore`.) The expected placement is listed so that current
APIs do not block them.

| Component | Expected placement | Builds on |
|-----------|--------------------|-----------|
| Telemetry Fusion Engine | `internal/fusion` | source-specific `DistanceKind` and `Quality` |
| Diagnostics and Observability | `internal/obs` | typed `protocol.Error` codes as metric labels |
| Firmware Transfer Service | `internal/firmware` | new protocol message types (requires protocol v2) |
| Command Dispatcher | `internal/command` | new protocol message types |
| Event Replicator | `internal/replicate` | event store |
