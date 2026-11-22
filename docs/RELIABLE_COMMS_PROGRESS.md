# Reliable Communications — progress log

Branch `feature/reliable-communications`, worktree `C:\Trabalho\Fleet\ReliableComms`,
based on `main` @ `f113d0c` (Task 004 merged). Local commits only: no push, no merge.

This file is updated at each milestone so the work can be resumed after an
interruption. The final state is summarised at the end.

## Plan (milestones)

| # | Milestone | Status |
|---|-----------|--------|
| 1 | `internal/security`: HKDF (RFC 5869), per-purpose session keys, key store with rotation states, Ed25519 command authority, replay window | done |
| 2 | `internal/reliable`: protocol v2 codec (frames, events, ACKs, commands), fail-closed decoding, golden + fuzz tests | done |
| 3 | `internal/eventstore` migration 2: reliable events, conflicts, device sessions; `AppendReliable` | pending |
| 4 | `internal/commandstore`: durable command lifecycle, authorization, transitions, audit history | pending |
| 5 | Gateway reliable v2 path (`internal/gateway/reliable*.go`): authenticate, persist, ACK after commit, downlink transport | pending |
| 6 | `internal/device`: durable outbox, retries, priority, command execution with simulated hardware | pending |
| 7 | `internal/dispatcher`: dispatch loop, expiry/unknown sweeper, COMMAND_RESULT/state applier | pending |
| 8 | `internal/alerting`: critical event consumer boundary (PANIC, security violations) | pending |
| 9 | `internal/simnet`: deterministic network, clock, fault injection; scenario tests | pending |
| 10 | Real UDP end-to-end tests with two gateways; CLI demos (`fleet-device`, `fleet-command`) | pending |
| 11 | Docs (protocol v2, state machines, threat model, guarantees, limits, ADRs, README/roadmap), benchmarks, final report | pending |

## Key decisions (summary; full rationale in docs/reliable.md ADRs)

- Protocol v2 is a separate codec (`internal/reliable`); the v1 codec and its golden
  tests are untouched. The gateway dispatches on the version byte.
- Authentication: HMAC-SHA256 (full 32-byte tag) with keys derived by HKDF-SHA256 per
  device key, direction, message type, key id and device session generation.
  CRC is not used in v2 (the MAC subsumes integrity).
- Actuation authority: Ed25519 signature by a command authority, verified by the device
  with a provisioned public key; independent from the transport key.
- Device → server command reports (received/accepted/rejected/succeeded/failed/unknown)
  travel as durable `COMMAND_RESULT` events; the "command result ACK" is the event ACK,
  which means "durably stored by the server".
- Replay defence: durable per-device session generation high-water mark (shared by all
  gateways) + per-gateway sliding counter window; all messages are content-bound and
  idempotent, so a replay that slips past the window cannot change state.
- Go 1.23 is the declared minimum: HKDF is implemented from crypto/hmac per RFC 5869
  (crypto/hkdf exists only from Go 1.24) and checked against the RFC test vectors.

## Log

- Worktree and branch created from `main` @ `f113d0c`.
- M1+M2 done. Golden v2 frame verified against an independent Python implementation
  (hashlib/hmac); this caught an off-by-one in the header size (a stray zero byte between
  device id and payload, consistent on both sides so round trips passed) — fixed before commit.
  Fuzz: FuzzOpen, FuzzEventPayload, FuzzDownlink ~25 s each, no failures.
