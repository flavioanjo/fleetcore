# FleetCore

FleetCore is a fleet telemetry simulation platform written in Go. It models
how connected vehicles report distance travelled from several imperfect
sensors, defines an original binary protocol for transporting those
measurements, and generates realistic but entirely synthetic telemetry.

> **Status:** v0.1.0 foundation (domain model, wire protocol, simulator)
> plus the event ingestion pipeline (UDP gateway, session registry, event store).

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

## Features in v0.1.0

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

## Requirements

- Go 1.23 or later.
- One third-party dependency: the pure-Go SQLite driver `modernc.org/sqlite`
  (used by the event store; no cgo required). Everything else uses the
  standard library.

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

## Tests

```bash
go test ./...
go test -race ./...      # requires cgo (a C toolchain) on Windows
go vet ./...

# Optional: fuzz the protocol decoder and the session registry
go test -run='^$' -fuzz=FuzzDecodeSample -fuzztime=60s ./internal/protocol
go test -run='^$' -fuzz=FuzzObserve -fuzztime=60s ./internal/sessions

# Regenerate the simulator golden file after an intentional model change
go test ./internal/simulator -run Golden -update
```

CI (`.github/workflows/ci.yml`) runs formatting, vet, build, tests, the race
detector, a short fuzz session and a reproducibility smoke test.

## Repository structure

```text
fleetcore/
├── cmd/
│   ├── fleet-sim/          CLI: synthetic telemetry as JSON Lines
│   └── fleet-gateway/      UDP ingestion gateway with SQLite storage
├── internal/
│   ├── telemetry/          domain model and validation
│   ├── protocol/           binary wire format
│   ├── simulator/          deterministic fleet simulator (+ testdata/ golden file)
│   ├── gateway/            UDP receiver, bounded pipeline, graceful shutdown
│   ├── sessions/           device session registry and ordering diagnostics
│   └── eventstore/         durable, deduplicated event storage (SQLite)
├── docs/
│   ├── architecture.md     architectural decisions
│   ├── ingestion.md        ingestion pipeline: identity, backpressure, storage
│   └── protocol.md         wire format specification
├── .github/workflows/ci.yml
├── COPYRIGHT
└── go.mod
```

## Roadmap

Planned, not implemented. See [docs/architecture.md](docs/architecture.md)
for where each piece is expected to fit.

1. **Telemetry Fusion Engine:** reconcile GPS, CAN and pulse distances into
   one trustworthy odometer.
2. **UDP Gateway:** receive protocol packets and stamp ingestion time.
   *Implemented ([docs/ingestion.md](docs/ingestion.md)).*
3. **Device Session Registry:** track boots and detect sequence gaps, replays
   and reordering. *Implemented.*
4. **Persistent Event Store:** durable, deduplicated storage by event ID.
   *Implemented (SQLite).*
5. **Diagnostics and Observability:** metrics and structured logs.
6. **Command Dispatcher** and **Firmware Transfer Service:** downlink
   messages (protocol v2).
7. **Event Replicator:** replicate stored events to other consumers.

## Copyright

Copyright (c) 2026 Flavio Anjo. All rights reserved. No license for reuse has
been granted yet; see [COPYRIGHT](COPYRIGHT).
