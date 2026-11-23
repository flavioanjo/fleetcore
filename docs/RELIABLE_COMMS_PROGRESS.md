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
| 3 | `internal/eventstore` migration 2: reliable events, conflicts, device sessions; `AppendReliable` | done |
| 4 | `internal/commandstore`: durable command lifecycle, authorization, transitions, audit history | done |
| 5 | Gateway reliable v2 path (`internal/gateway/reliable*.go`): authenticate, persist, ACK after commit, downlink transport | done |
| 6 | `internal/device`: durable outbox, retries, priority, command execution with simulated hardware | done |
| 7 | `internal/dispatcher`: dispatch loop, expiry/unknown sweeper, COMMAND_RESULT/state applier | done |
| 8 | `internal/alerting`: critical event consumer boundary (PANIC, security violations) | done |
| 9 | `internal/simnet`: deterministic network, clock, fault injection; scenario tests | done |
| 10 | Real UDP end-to-end tests with two gateways; CLI demos (`fleet-device`, `fleet-command`) | done |
| 11 | Docs (protocol v2, state machines, threat model, guarantees, limits, ADRs, README/roadmap), benchmarks, final report | done |

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
- M3 done: eventstore schema 2 (additive). Existing migration-mechanics tests were made
  version-relative (same assertions) because they hard-coded the test migration as v2.
- Architecture test extended to admit internal/reliable + internal/security for ingestion, with
  a new check that those packages import only the domain/v1 codec (commit 8ef21e2). Lesson: a
  `grep -v ok` pipeline masked a failing exit code once (5529b03); exit codes are now checked.
- M4 done: commandstore (lifecycle, write-ahead attempts, unknown vs expired, ranked reports).
- M5 done: gateway v2 path. A test exposed a cross-gateway stale-session window when the
  session was cached for 5 s; fixed by reading the durable session high-water mark per batch.
- M6 done: device (outbox, priority + anti-starvation, backoff+jitter, commands with
  write-ahead execution, boot recovery), simulated hardware in internal/simhw. Found and fixed:
  NULL args for argument-less commands violated a NOT NULL constraint.
- M7-M9 done: dispatcher, alerting, simnet with 11 acceptance scenarios. Found and fixed: commands
  created in the same millisecond were ordered by random id (UNBLOCK could overtake BLOCK;
  reproduced 17/30) -> explicit `seq` column.
- Mutation testing (24 deliberate defects in device, gateway, protocol, key derivation, event
  store, command store, alerting): 23 detected; the 24th was an equivalent mutant (a second
  layer still rejects stale sessions) - removing both layers is detected.
- M10 done: real UDP e2e (two gateways, crash-after-commit, failover, gateway restart, processor
  applies every sample once); fleet-server / fleet-device / fleet-command / fleet-reliable-demo;
  multi-process CLI flow run manually; logs checked for secrets.
- M11 done: docs/protocol-v2.md, docs/reliable.md, README/architecture/protocol updates, CI fuzz.
- Verification: go vet, gofmt, go test ./... (Go 1.27 and Go 1.23.0), go test -race ./... (two
  full clean runs; one earlier run hit the Windows TempDir cleanup issue #5 after all assertions
  passed), CGO_ENABLED=0 go test ./..., fuzz 6 targets x 45 s, soak 2 h simulated.
- Soak run 1 (2 h simulated) passed its invariants but its diagnostics showed 5 commands
  `unknown` after 44 transmitted attempts each that never reached the device: a gateway with a
  stale in-memory session kept sealing deliveries for the device's previous session. Fixed in
  the gateway (deliver only with the latest durable session / most recent gateway);
  deterministic regression test added (failed before, passes after).
- Soak run 2 (after the routing fix): 2 h simulated, 20 devices, 14 474 events all acknowledged and
  stored once, 37 commands: 24 succeeded, 13 failed (REQUEST_POSITION without a GNSS fix in the
  soak's vehicles), 0 unknown; 11 immobilizer drives.

## Final state

All milestones done and committed on `feature/reliable-communications` (not pushed, not merged).
See the final report in the session and docs/reliable.md for guarantees and known limitations.
