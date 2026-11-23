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

## Audit remediation (after the independent audit of 6a8a229)

Each confirmed defect has a regression test that failed before its fix (output recorded in the
commit messages) and passes after. Local commits only.

| Defect (audit item) | Cause | Fix | Commit |
|---|---|---|---|
| Replay window never terminates at `MaxUint64` (P1) | clearing loop `c <= ctr` wraps | iterate over the bounded offset | 7b139ec |
| Telemetry sequence advanced by rejected enqueues (P1) | increment inside the body builder, before capacity check / encode / commit | advance only after commit | 4c9ed40 |
| Session N event stored after another gateway recorded N+1 (P1) | admission and insert in separate transactions | session re-checked in the insert transaction; new outcome `StaleSession` | 33e51e8 |
| SQLite files held after shutdown (P0) | modernc.org/sqlite v1.38.2 leaks the statement of a query cancelled mid-step; the dropped connection becomes a zombie | driver wrapper: queries run without cancellation | 8321718 |
| Dispatcher failures invisible; stop leaves attempts "sending" (P0) | errors swallowed; stop cancelled the delivery in progress | `ErrFailed` after consecutive failures; iterations not interrupted | 5c41803 |
| Alerting had no supervised loop (P0) | Step called with the process context | `Consumer.Run` with the same semantics | 0cd5f67 |
| Server kept running with a stopped essential service (P0) | dispatcher result discarded | supervisor; exit 1 naming the service; stores closed after services | 7db15e4 |
| Orderly stop during actuation reported as interrupted (found here) | outcome commit used the caller's context | outcome recorded under `WithoutCancel`; fleet-device waits for its receiver | 3d4a216 |
| Blank / control-character principals accepted (found by the new matrix test) | only emptiness and length checked | printable UTF-8, no surrounding spaces | 6036f44 |
| Demo-only boundaries implicit (P1) | — | `-unverified-demo-identity`, LogSink wording, README blockers, reliable.md §10 | f33f6c9 |

Verification of f33f6c9 (Windows, Go 1.27; Go 1.23.0 where noted): gofmt, vet, `go test ./...`,
`CGO_ENABLED=0`, Go 1.23.0 pass; shutdown tests 10x (+3x with `-race`), sqlite cancellation 20x,
e2e 10x, simnet 5x, stop-safety 20x, session race 50x pass; six fuzz targets 30 s each (about
21 M executions) without failure; demo passes; soak: 2 h simulated, 14 474 events stored once,
37 commands (24 succeeded, 13 failed, 0 unknown). Full-suite `-race`: no data race; the v1 test
`TestConcurrentDevicesOverUDP` timed out under full parallel load in 3 of 3 runs on this branch and
in 1 of 2 runs on 6a8a229 (passes 10/10 alone on both): pre-existing, not fixed here.
Cost: batch-64 ingestion about 9% slower (one session lookup per event in the insert transaction).

## Second audit follow-up

**Query deadlines under the driver workaround** (b6f6891). Confirmed: through the wrapper a
read query executing before its first row ignores its deadline (the unwrapped driver interrupts
it); `TestQueryDeadlineDuringExecution` characterizes it. No modernc.org/sqlite release for
Go 1.23 fixes the leak (v1.39.0 still has it); v1.40.1, the first fixed release, requires Go 1.24.
Verified in a throwaway copy (v1.40.1, wrapper removed): leak test 10/10, deadline honoured,
suite and server shutdown tests pass, Go 1.23.0 refuses to build. Not applied (minimum Go
version is the maintainers' decision). Limitation, options and operational policy: docs/reliable.md
ADR-R12.

**TestConcurrentDevicesOverUDP** (v1 pipeline, 16 devices x 120 events). Instrumented throwaway
copies recorded settled events every 0.5 s past the former fixed 10 s deadline:

| Condition (`-race`) | HEAD | HEAD without wrapper | 6a8a229 |
|---|---|---|---|
| full suite, packages in parallel (3 runs each): settle time | 11.0 / 12.2 / 12.8 s | 12.3 / 11.9 / 12.4 s | 10.7 / 11.0 / 11.1 s |
| same: longest pause without progress | <= 0.51 s | <= 0.55 s | <= 0.48 s |
| full suite, one package at a time | 5.8 s | | |
| test alone (5 runs) | 5.80-5.87 s | | 5.86-5.90 s |
| test alone + CPU load (12 race-instrumented processes) | 14.7-19.0 s, pause <= 0.78 s | | |
| test alone + fsync load (6 processes of durable commits) | 6.3-6.5 s, pause <= 0.25 s | | |

Storage progresses continuously; the time is dominated by CPU contention on the
race-instrumented pipeline, not by disk or locks (StoreRetries and StoreFailed 0 in every run);
the wrapper adds nothing measurable (identical isolated times, no better without it). The fixed
10 s deadline was below the healthy settle time under the parallel `-race` suite on this machine
(all 9 instrumented runs needed more). The wait is now progress-based: fail after 10 s without
progress (the former total) or 120 s in total; the event count and all integrity assertions are
unchanged; the policy has its own deterministic test (`TestAwaitProgressPolicy`).
