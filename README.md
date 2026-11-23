# FleetCore

FleetCore is a fleet telemetry simulation platform written in Go. It models
how connected vehicles report distance travelled from several imperfect
sensors, defines an original binary protocol for transporting those
measurements, generates realistic but entirely synthetic telemetry, and
fuses the sensors into one explainable distance estimate.

> **Status:** v0.3.0 (in review). Domain model, wire protocol, simulator,
> telemetry fusion engine, event ingestion pipeline, and the end-to-end
> chronological processor that connects them and persists the results
> ([docs/processing.md](docs/processing.md)).

## Why it exists

FleetCore serves two purposes:

1. **An engineering portfolio.** It shows how a telemetry system can be built
   with explicit invariants, strict parsing, deterministic tests and
   documented design decisions.
2. **A realistic, maintainable codebase** for future software maintenance
   tasks and AI coding-agent evaluations: it has real domain rules, non-trivial
   edge cases and a test suite that makes regressions visible.

The project is inspired by common problems in fleet telemetry, distributed
systems and IoT infrastructure. All code, protocols and data are original;
all device identifiers and telemetry are synthetic.

## Features

- **Telemetry domain** (`internal/telemetry`)
  - Strongly typed samples: device, event, boot/session and sequence
    identifiers; device and ingestion timestamps; source (GPS, CAN, pulse
    counter); quality; distance.
  - Integer millimetre distances with explicit **cumulative** or **delta**
    semantics, fixed per source.
  - Explicit absence (`Optional[T]`) instead of zero-value sentinels.
  - Validation that reports every violated rule as a typed `FieldError`.
- **Binary protocol** (`internal/protocol`, [spec](docs/protocol.md))
  - Versioned, big-endian, length-prefixed frames with a CRC-32C trailer,
    512-byte maximum.
  - Strict, canonical decoding with typed errors (`errors.Is(err, protocol.ErrChecksumMismatch)`).
  - Golden-vector, table-driven, exhaustive truncation and bit-flip tests, plus a fuzz target.
- **Fleet simulator** (`internal/simulator`, `cmd/fleet-sim`)
  - Many vehicles with stop/urban/suburban/highway driving segments and
    bounded acceleration.
  - Three sensors with different error models: noisy GPS deltas with drift
    and dropouts, a 100 m-resolution CAN odometer, and a pulse counter with a
    per-vehicle calibration error.
  - Fully reproducible from a seed, on every platform; JSON Lines output.
  - Ground truth available only through an explicit testing API
    (`simulator.RunWithGroundTruth`).
- **Telemetry fusion engine** (`internal/fusion`, `cmd/fleet-fusion`, [design](docs/fusion.md))
  - Per-device distance estimation from GPS deltas, the 100 m CAN odometer
    and the pulse counter: inverse-variance fusion and CAN interval
    reconciliation.
  - Conservative pulse calibration (≥ 20 km CAN or GPS reference) and source
    health checks.
  - Explicit handling of missing measurements, including interpolation of
    short uncovered spans.
  - Bounded reordering by sequence number. Duplicates, conflicts, gaps,
    late events, reboots, stale sessions and clock problems each have a
    defined, reported outcome.
  - Structured, machine-readable diagnostics and uncertainty for every
    estimate.
  - Deterministic, integer arithmetic, finite memory (~5 KB per device).
- **Evaluation harness** (`internal/evaluation`, `cmd/fleet-eval`)
  - Compares fusion with simulator ground truth and simple baselines,
    across source combinations, seeds and delivery perturbations.

## Event ingestion

`cmd/fleet-gateway` receives protocol packets over UDP, validates them,
stamps their ingestion time, deduplicates them by durable event identity
and stores them in a local SQLite database, while tracking device sessions
(gaps, reordering, reboots). See [docs/ingestion.md](docs/ingestion.md).

```bash
go run ./cmd/fleet-gateway -listen 127.0.0.1:7400 -db fleetcore-events.db -log-format text
```

- **Identity:** `(DeviceID, BootID, EventID)` plus a SHA-256 fingerprint of
  the canonical packet. Retransmissions are idempotent. A different payload
  for a stored identity is kept as a conflict and never overwrites.
  EventIDs reused across boots are stored and reported.
- **Durability:** accepted events are committed in SQLite (WAL,
  `synchronous=FULL`) in atomic batches. Deduplication survives restarts.
- **No delivery guarantee to senders:** protocol v1 has no
  acknowledgements, so a device cannot know whether a packet was stored and
  must retransmit if it needs delivery.
- **Bounded resources:** fixed goroutines, bounded queues with one explicit
  drop point, and capped in-memory session tracking. Session diagnostics are
  provisional and in memory only.

Packets carry a CRC-32C, which detects corruption but does **not**
authenticate senders; run the gateway only on trusted networks.

## End-to-end processing

`cmd/fleet-processor` reads the events stored by one or more gateways,
orders them chronologically, feeds them to the fusion engine and persists
the journal, estimates and diagnostics in a results database. See
[docs/processing.md](docs/processing.md).

```bash
go run ./cmd/fleet-gateway   -listen :7400 -db events.db &
go run ./cmd/fleet-gateway   -listen :7401 -db events.db &   # a second gateway on the same store
go run ./cmd/fleet-processor -events events.db -results results.db
```

- **Order:** within a session by sequence number, wrap-around of the 32-bit
  sequence included. Between a device's sessions by device time, when it is
  evidence. Otherwise, and between devices, by the central ingestion order.
- **Deterministic recovery:** every call into the engine (events and
  `Tick`) is journaled with its outputs. A restart replays the journal and
  verifies every step against a stored hash before continuing.
- **Nothing dropped:** late, duplicate and conflicting events are preserved
  (in the event store, and as flags and fusion outcomes in the results).
- **Current results:** `SELECT * FROM latest_estimates` in the results
  database.

## Reliable communications and commands

Protocol v2 adds authenticated, acknowledged event delivery and an
end-to-end vehicle command lifecycle (BLOCK, UNBLOCK, OPEN_CARGO,
REQUEST_POSITION; PANIC, POSITION, BLOCK_STATE, CARGO_STATE and
COMMAND_RESULT events). See [docs/reliable.md](docs/reliable.md) (design,
guarantees, threat model) and [docs/protocol-v2.md](docs/protocol-v2.md)
(wire format). Version 1 is unchanged.

```bash
go run ./cmd/fleet-reliable-demo                       # narrated, deterministic simulated scenarios

go run ./cmd/fleet-command keygen -dir data -devices TRK-1
go run ./cmd/fleet-server -listen 127.0.0.1:7500 -data data -keys data/keys.json -authority data/authority.json &
go run ./cmd/fleet-device -id TRK-1 -keys data/keys.json -authority-pub data/authority.pub     -gateway 127.0.0.1:7500 -db data/device-TRK-1.db -panic-after 3s &
go run ./cmd/fleet-command request -data data -unverified-demo-identity -principal ops:alice -roles security_operator \
    -device TRK-1 -type BLOCK -reason "theft report"
go run ./cmd/fleet-command list -data data
```

- **ACK means durably stored.** A gateway acknowledges an event only after
  the event store committed it (or holds an identical one); devices keep
  every event until such an ACK. Retransmissions are stored and fused once.
- **Commands execute at most once** per CommandID, only if signed by the
  command authority (Ed25519, separate from the transport keys), and only
  under simulated safety interlocks. Outcomes are explicit: succeeded,
  failed, rejected, expired or **unknown** — never guessed.
- **A PANIC is handled only when an operator acknowledges it**; storage and
  notification are earlier, separate states.
- **Supervised server.** If a gateway, the dispatcher or the alerting
  consumer stops, `fleet-server` stops and exits 1 naming it; on
  SIGINT/SIGTERM it finishes work in progress and exits 0.
- **Simulated actuators only.** Real vehicle actuation needs independent
  safety engineering and hardware validation (see docs/reliable.md).

### Deployment blockers (demonstration-only behaviour)

This is not deployable as is. In particular:

- **Operator identities are not authenticated.** `fleet-command` takes the
  principal, roles and acknowledging operator from its flags; anyone who can
  run it can claim `security_operator`. It refuses `request` and `ack`
  unless `-unverified-demo-identity` is given, and labels every such
  identity as unverified. A signed command proves only that the request
  passed the policy check, not who made it.
- **No person is notified of a PANIC.** The only alert sink, `LogSink`,
  writes a log line. An alert in state `notified` means "the sink accepted
  it", which for `LogSink` is that log line.
- **Payloads are authenticated, not encrypted.** Positions and commands are
  readable on the network.
- **Keys are local files.** Device keys and the command authority's private
  key are files on disk (0600), not in a secure element or HSM.

The controls required before any real-world immobilizer or cargo actuation
(an identity provider and dual control, a real incident-notification
integration, confidentiality, HSM/secure-element key custody, independent
safety engineering and hardware validation) are listed in
[docs/reliable.md §10](docs/reliable.md#10-deployment-blockers-and-mandatory-controls).

## Requirements

- Go 1.23 or later.
- One third-party dependency: the pure-Go SQLite driver `modernc.org/sqlite`
  (used by the event and results stores; no cgo required). Everything else
  uses the standard library.

## Build and run

```bash
go build -o bin/fleet-sim ./cmd/fleet-sim

# A short stream: 2 vehicles, 3 rounds, 5 s apart, 3 sources each = 18 lines
go run ./cmd/fleet-sim -seed 7 -vehicles 2 -samples 3 -interval 5s
```

The sixth line of that command's output:

```json
{"device_id":"SIM-00002","event_id":8589934595,"boot_id":3247042027,"seq":3,"device_time":"2026-01-01T08:00:06.405Z","ingested_at":"2026-01-01T08:00:08.85Z","source":"pulse","quality":"good","distance":{"kind":"delta","value_mm":30000}}
```

Flags:

| Flag | Default | Meaning |
|------|---------|---------|
| `-seed` | `1` | Random seed. Identical flags produce identical output. |
| `-vehicles` | `3` | Number of vehicles (1–10000). |
| `-samples` | `5` | Sampling rounds per vehicle; each round emits one sample per source. |
| `-interval` | `1s` | Time between rounds (multiple of 100 ms, 100 ms–1 h). |
| `-start` | `2026-01-01T08:00:00Z` | Simulated start time (RFC 3339). |
| `-sources` | `gps,can,pulse` | Sources to emit, in output order. |

Samples are ordered by round, then by vehicle, then by source. A missing
measurement has `"quality":"unavailable"` and `"distance":null`.

### Fuse telemetry

```bash
# Simulate, fuse, and print one summary line per vehicle
go run ./cmd/fleet-sim -vehicles 2 -samples 360 -interval 10s \
  | go run ./cmd/fleet-fusion -events none -summary

# Keep everything: every event disposition and every estimate
go run ./cmd/fleet-fusion -in samples.jsonl -events all > reports.jsonl
```

`fleet-fusion` writes JSON Lines with a `type` of `event`, `estimate`,
`input_error` (an undecodable line: reported, never dropped; the exit
status is then 1) or `summary`. Flags: `-in`, `-events all|issues|none`,
`-summary`, `-strict`, `-window`, `-max-buffer`. The output format is
described in [docs/fusion.md](docs/fusion.md#6-output).

### Evaluate accuracy

```bash
go run ./cmd/fleet-eval -seeds 1,2,3,4,5 -perturb
```

This prints, for every source combination, the fused error against ground
truth next to the naive and CAN-difference baselines. Current results are
in [docs/fusion.md §8](docs/fusion.md#8-accuracy-evaluation).

## Tests

```bash
go test ./...
go test -race ./...      # requires cgo (a C toolchain) on Windows
go vet ./...

# Optional: fuzz the protocol decoder and the session registry
go test -run='^$' -fuzz=FuzzDecodeSample -fuzztime=60s ./internal/protocol
go test -run='^$' -fuzz=FuzzObserve -fuzztime=60s ./internal/sessions
go test -run='^$' -fuzz=FuzzOrderer -fuzztime=60s ./internal/chrono
go test -run='^$' -fuzz='^FuzzOpen$' -fuzztime=60s ./internal/reliable          # protocol v2 frames
go test -run='^$' -fuzz='^FuzzEventPayload$' -fuzztime=60s ./internal/reliable
go test -run='^$' -fuzz='^FuzzDownlink$' -fuzztime=60s ./internal/reliable

# Reliable communications: soak test (hours of simulated time) and benchmarks
FLEETCORE_SOAK=1 go test -run TestSoak -timeout 60m ./internal/simnet
go test -run '^$' -bench 'SealOpen|ReliableIngest|CommandDelivery' ./internal/reliable ./internal/gateway ./internal/dispatcher

# Regenerate the simulator golden file after an intentional model change
go test ./internal/simulator -run Golden -update

# Fusion throughput and memory benchmarks
go test -run '^$' -bench . ./internal/fusion

# Skip the 24-hour simulation test
go test -short ./...
```

CI (`.github/workflows/ci.yml`) runs:

- formatting, vet, build and tests;
- the race detector and a short fuzz session;
- a reproducibility smoke test;
- the accuracy evaluation, as part of the tests.

## Repository structure

```text
fleetcore/
├── cmd/
│   ├── fleet-sim/          CLI: synthetic telemetry as JSON Lines
│   ├── fleet-fusion/       CLI: fuse JSON Lines telemetry into estimates
│   ├── fleet-eval/         CLI: accuracy evaluation table
│   ├── fleet-gateway/      UDP ingestion gateway with SQLite storage
│   ├── fleet-processor/    chronological processor: store → fusion → results
│   ├── fleet-server/       protocol v2 gateways + command dispatcher + alerting
│   ├── fleet-device/       simulated v2 tracking device over UDP
│   ├── fleet-command/      keys, command requests, lifecycle inspection, alerts
│   └── fleet-reliable-demo/ narrated simulated scenarios
├── internal/
│   ├── telemetry/          domain model and validation
│   ├── protocol/           binary wire format
│   ├── simulator/          deterministic fleet simulator (+ testdata/ golden file, test-only ground truth)
│   ├── fusion/             telemetry fusion engine
│   ├── evaluation/         accuracy harness (tooling, not production)
│   ├── gateway/            UDP receiver, bounded pipeline, graceful shutdown
│   ├── sessions/           device session registry and ordering diagnostics
│   ├── eventstore/         durable, deduplicated event storage (SQLite)
│   ├── sqlitedb/           shared SQLite configuration and migrations
│   ├── chrono/             deterministic chronological orderer
│   ├── resultstore/        processing journal, estimates and diagnostics (SQLite)
│   ├── processor/          event store → orderer → fusion → results
│   ├── security/           HKDF message keys, key store, command authority, replay window
│   ├── reliable/           protocol v2 codec (events, ACKs, commands)
│   ├── device/             simulated device: durable outbox, command execution
│   ├── simhw/              simulated vehicle hardware (ground truth)
│   ├── commandstore/       durable command lifecycle and audit (SQLite)
│   ├── dispatcher/         command delivery and report application
│   ├── alerting/           critical-event boundary (PANIC, security violations)
│   ├── simnet/             deterministic simulated world and acceptance scenarios
│   └── e2e/                end-to-end tests over real UDP
├── docs/
│   ├── architecture.md     architectural decisions
│   ├── fusion.md           fusion engine design and evaluation
│   ├── ingestion.md        ingestion pipeline: identity, backpressure, storage
│   ├── integration.md      differences between ingestion and fusion
│   ├── processing.md       end-to-end processing: chronology, journal, recovery
│   ├── protocol.md         wire format specification (version 1)
│   ├── protocol-v2.md      reliable protocol specification (version 2)
│   └── reliable.md         reliable communications: design, guarantees, threat model
├── .github/workflows/ci.yml
├── COPYRIGHT
└── go.mod
```

## Roadmap

Done:

- ~~Telemetry Fusion Engine~~ ([docs/fusion.md](docs/fusion.md)).
- ~~UDP Gateway~~, ~~Device Session Registry~~ and ~~Persistent Event
  Store~~ ([docs/ingestion.md](docs/ingestion.md)).
- ~~End-to-end integration and event chronology~~
  ([docs/processing.md](docs/processing.md)).
- ~~Reliable communications and command delivery~~ (protocol v2, command
  dispatcher, simulated devices; [docs/reliable.md](docs/reliable.md)).

Planned, not implemented. See [docs/architecture.md](docs/architecture.md)
for where each piece is expected to fit.

1. **Reconciliation** of events fusion reports as `requires_reconciliation`,
   and bounded recovery time (journal retention or snapshots).
2. **Shared session tracking:** one session model for ingestion and fusion.
3. **Diagnostics and Observability:** metrics and structured logs, fed by
   fusion and ingestion diagnostics.
4. **Firmware Transfer Service** over the v2 downlink.
5. **Event Replicator:** replicate stored events to other consumers.
6. **Reliable communications hardening:** payload confidentiality (AEAD),
   keys in a secure element / HSM, an identity provider and dual control for
   actuations, retention and archival of the command and event audit trail.

## Copyright

Copyright (c) 2026 Flavio Anjo. All rights reserved. No license for reuse has
been granted yet; see [COPYRIGHT](COPYRIGHT).
