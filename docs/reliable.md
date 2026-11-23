# Reliable communications and command delivery

FleetCore's reliable communications subsystem adds authenticated,
acknowledged event delivery from devices and an end-to-end vehicle command
lifecycle to the existing UDP ingestion, event store, chronological
processing and fusion pipeline. The wire format is
[protocol-v2.md](protocol-v2.md). Version 1 ingestion is unchanged.

> **Safety notice.** All actuators in this project are simulated
> (`internal/simhw`). Nothing here drives real relays, ignition circuits,
> immobilizers or locks. Production vehicle actuation requires independent
> safety engineering (e.g. ISO 26262 / ISO 25119-style hazard analysis),
> hardware validation, legal review and an authorization process this
> project does not provide. A device's report that a command succeeded is
> the device's account of a sensor reading; it is not an independent
> verification of the vehicle's physical state.

## 1. Architecture

```
 device (internal/device)                       server
 ┌──────────────────────────┐   UDP v2   ┌────────────────────────────────────────────────┐
 │ durable outbox (device.db)│──uplink──►│ gateway A ─┐                                    │
 │ priority + backoff        │           │ gateway B ─┼─► events.db (shared)                │
 │ command records           │◄─downlink─│            │    v1 events ──► processor ► fusion │
 │ simulated hardware (simhw)│           │            │    reliable_events                  │
 └──────────────────────────┘           │            │    device_sessions                   │
                                         │  dispatcher ◄──── COMMAND_RESULT / *_STATE        │
                                         │  │   ▲                                            │
                                         │  ▼   │ commands.db (lifecycle, attempts, history)  │
                                         │  alerting ◄──── PANIC / SECURITY_VIOLATION         │
                                         │       alerts.db (open → notified → acknowledged)   │
                                         └────────────────────────────────────────────────┘
```

| Package | Responsibility | Depends on |
|---|---|---|
| `internal/security` | HKDF, per-purpose message keys, key store and rotation states, Ed25519 command authority, replay window | stdlib |
| `internal/reliable` | v2 codec: frames, events, ACKs, heartbeats, command deliveries; no I/O, no state | telemetry, protocol (v1), security |
| `internal/eventstore` (schema 2) | `reliable_events`, `reliable_conflicts`, `device_sessions`; `AppendReliable` | reliable |
| `internal/gateway/reliable.go` | authenticate, session/replay checks, durable commit, signed ACKs; seal command deliveries | eventstore, reliable, security |
| `internal/device` | device logic: outbox, scheduling, ACK handling, command decision and execution | reliable, security, sqlitedb |
| `internal/simhw` | simulated vehicle: sensors, actuators, faults, ground-truth counters | device (interface), reliable |
| `internal/commandstore` | durable command lifecycle, authorization, attempts, history, device reports | reliable, security, sqlitedb |
| `internal/dispatcher` | promote, sweep deadlines, deliver, poll; apply device reports | commandstore, eventstore, reliable |
| `internal/alerting` | critical-event boundary towards incident response | eventstore, reliable |
| `internal/simnet` | deterministic simulated world and acceptance scenarios | everything above |
| `cmd/fleet-server`, `fleet-device`, `fleet-command`, `fleet-reliable-demo` | binaries | |

Ingestion stays decoupled from fusion (`TestIngestionIsDecoupledFromFusion`
admits the protocol packages and checks they import nothing beyond the
domain and the v1 codec). The gateway does not import the command store or
dispatcher; `fleet-server` composes them.

## 2. Event delivery

### Stages

| # | Stage | Where it is recorded |
|---|---|---|
| 1 | generated | device logic builds the typed body |
| 2 | durably queued | `outbox` row committed with a fresh UID (`Enqueue` returned nil) |
| 3 | transmitted | `attempts`, `first_sent`, `last_sent` (handed to the socket; not "delivered") |
| 4 | durably stored by the server | `reliable_events` row committed (plus `events` for TELEMETRY) |
| 5 | acknowledged | device `outbox.state = acked` after a matching success ACK |
| 6 | processed downstream | TELEMETRY: chronological processor journal; COMMAND_RESULT/BLOCK_STATE/CARGO_STATE: command-store applier cursor; PANIC/SECURITY_VIOLATION: alert opened, notified, acknowledged by an operator |

### Device state machine (one outbox row)

```
             Enqueue (committed)
                    │
                    ▼
        ┌───────► pending ──────────────┐
        │  retry   │   │  success ACK   │ permanent NACK
        │ (backoff)│   │ (UID+fingerprint, current session)
        └──────────┘   ▼                ▼
                     acked            failed     (kept; counted)
                    pending ── routine/diagnostic older than RoutineMaxAge ──► expired (kept; counted)
```

`AckRetry`, a lost ACK, an ACK of another session, an ACK whose fingerprint
differs, or any frame that fails authentication leave the row pending.

### Server classification (`AppendReliable`)

| Stored under (device, UID) | Result | ACK |
|---|---|---|
| nothing | Inserted | stored |
| same fingerprint | Duplicate (no write) | duplicate |
| other fingerprint | Conflict (payload kept in `reliable_conflicts`) | conflict |
| TELEMETRY whose v1 identity holds another payload | Conflict | conflict |
| batch failed to commit (after retries) | — | retry |

A TELEMETRY event whose identical sample is already in `events` (sent
earlier over v1) is linked, not inserted again: fusion applies it once.

### Guarantees and their assumptions

- **G1 Event durability** — An event durably queued on a device remains
  pending until a valid acknowledgement of durable server storage.
  *Requires*: the device store survives (no flash loss). A device that
  loses its store loses its queue; nothing can prevent that.
- **G2 ACK correctness** — A gateway issues a success ACK only after the
  transaction storing the event (or holding an identical one) committed.
  *Requires*: SQLite durability (`synchronous=FULL`, WAL) on the server's
  storage.
- **G3 Idempotence** — Retransmissions never create a second stored event
  or a second downstream application: identity (device, UID) is unique in
  the store; TELEMETRY enters the v1 table once; the processor journals
  each v1 position once.
- **G4 Crash consistency** — A crash before commit cannot produce a
  success ACK (none is generated before the commit returns). A crash after
  commit and before the ACK is recovered by the device's retransmission,
  answered `duplicate` by any gateway.
- **G5 Eventual delivery** — If the device store survives, the key is
  valid and the network eventually delivers a datagram and its ACK in the
  same session, a pending critical event is stored and acknowledged.
  Critical events never expire; their retry interval is capped (default
  15 s). UDP itself guarantees nothing; this is retransmission over it.
- Exactly-once *transport* delivery is **not** claimed. The server may
  receive an event many times; it stores and applies it once.

## 3. Commands

### Lifecycle (server, `commands.db`)

```
requested ──► eligible ──► sent ──► received ──► accepted ──► succeeded
    │             │          │          │            │      ├─► failed
    │             │          │          │            │      └─► unknown
    │             │          │          └────────────┴──► rejected
    └─────────────┴──► expired (deadline passed; provably never transmitted)
                  └──► unknown (deadline passed; an attempt may have been transmitted)
sent/received/accepted ──► unknown (result deadline passed without a final report)
unknown ──► succeeded | failed | rejected   (late final report; resolved_late=1)
```

| # | Requested stage | Recorded as |
|---|---|---|
| 1 | durably requested | `requested`, after authorization (denials go to `denied_requests`, never signed) |
| 2 | eligible for dispatch | `eligible`: not expired; actuations of one device serialized in request order |
| 3 | transmitted | attempt row `sending` written *before* the datagram, `sent` after; state `sent` |
| 4 | received and durably recorded by the device | COMMAND_RESULT received → `received` |
| 5 | accepted or rejected | COMMAND_RESULT accepted / rejected (+ reason) |
| 6 | executed or failed | COMMAND_RESULT succeeded / failed / unknown (+ reason, observed sensor state) |
| 7 | result durably recorded by the server | the COMMAND_RESULT event is ACKed only after commit; the applier records it in `command_reports` and the state atomically with its cursor |
| 8 | unknown or expired, needing attention | `unknown` / `expired` with `needs_attention = 1` |

Reports are ranked (received < accepted|rejected < succeeded|failed|unknown);
a report not above the recorded rank is stale and recorded as such. A
report from another device, of another type, for an unknown command, or
contradicting a final state is recorded with its verdict and never applied.

### Device handling (one delivery)

1. Authenticated for this device and its current session; ignored if past
   `attempt_not_after` (delayed/replayed attempt).
2. Known CommandID → duplicate delivery: **never executed again**; the
   current stage is re-reported at most every `StatusRepeat`.
3. Authority signature verified (Ed25519). Invalid → not recorded, not
   executed; rate-limited SECURITY_VIOLATION.
4. One transaction: record the command, queue COMMAND_RESULT received,
   decide (type, arguments, expiry, interlocks), queue the decision and, if
   accepted, set `exec_started` (write-ahead). If the reports cannot be
   queued (queue full), nothing is recorded and nothing executes.
5. Drive the actuator; read the sensor back.
6. One transaction: outcome, COMMAND_RESULT, BLOCK_STATE/CARGO_STATE.

A crash between 4 and 6 is found at the next boot (`exec_started` without
`exec_done`): reported **unknown / interrupted** with the sensor state, and
never re-executed.

### Commands and interlocks (simulated hardware)

| Command | Role (reference policy) | Interlocks on the device | Success means |
|---|---|---|---|
| BLOCK | security_operator, justification | vehicle stationary (speed 0); sensor readable | immobilizer sensor reads blocked |
| UNBLOCK | security_operator, justification | sensor readable | immobilizer sensor reads unblocked |
| OPEN_CARGO | cargo_operator, justification | stationary and ignition off | **door sensor** reads open (a released latch with a closed door is *unknown / not_confirmed_by_sensor*) |
| REQUEST_POSITION | fleet_viewer | — | a fresh valid fix exists; a POSITION (reason requested) is queued. Without one: *failed / position_unavailable*; nothing is fabricated |

Target state already held (sensor-confirmed) → succeeded /
already_in_state with no actuation.

### Command guarantees

- **C1 Uniqueness** — One CommandID drives an actuator at most once,
  whatever the number of deliveries (durable dedup on the device, write-ahead
  execution record). *Requires* the device store to survive.
- **C2 Uncertainty is explicit** — unknown, rejected, expired, failed and
  succeeded are distinct. `expired` is used only when no attempt can have
  been transmitted; otherwise the outcome is `unknown`.
- **C3 Expiry** — No first transmission after `expires_at`; the device
  refuses to start execution after `expires_at` (device clock) or to act on
  an attempt after `attempt_not_after`.
- **C4 Authority** — Only commands signed by the command authority execute;
  the transport key alone cannot authorize an actuation.

## 4. Authentication and threat model

| Threat | Defence | Residual |
|---|---|---|
| Spoofed device frames | HMAC-SHA256 with a per-device 256-bit key, keyed per direction/type/session (HKDF) | compromise of that device's key (below) |
| Forged ACK | ACKs are sealed with the device's downlink key and bound to UID + fingerprint + session; the device also checks the fingerprint | — |
| ACK for another event/device/command/session | key derivation includes device, key id, session and message type; payload binds UID and fingerprint | — |
| Replayed uplink frame | per-device durable session high-water mark (all gateways, restarts) + per-gateway sliding window | the *same* datagram replayed to a second gateway passes its window once; it is idempotent (duplicate) |
| Replayed downlink frame | accepted only in the device's current session; attempt freshness bound; CommandID dedup; ACK idempotent | within the freshness bound a replayed delivery is a duplicate (no effect) |
| Delayed command | `attempt_not_after` (≤ 5 min) and `expires_at`, device clock | a device clock far behind real time widens these windows (device time integrity assumed, e.g. GNSS-disciplined) |
| Tampering | every byte under the tag; canonical fail-closed decoding (fuzzed) | — |
| Duplicate commands | request-id idempotency (server), CommandID dedup (device), per-device actuation serialization | — |
| Unauthorized request | role policy at creation; denials audited; never signed | the principal's identity comes from the caller (identity provider out of scope) |
| Compromised gateway / transport key | cannot forge the Ed25519 authority signature: no actuation; can drop or delay traffic, forge ACKs for that device (making the device drop events it believes stored), read traffic (no encryption) | key per device limits the blast radius; detection via conflicts and SECURITY_VIOLATION |
| Compromised device key | impersonate that device only (keys are per device); revoke the key version | events forged before revocation are stored as that device's |
| Compromised authority key | can authorize commands for every device | keep it in an HSM/KMS; rotate via key ids; devices trust listed versions only |
| Flooding | unauthenticated datagrams are dropped before any state change and never answered; SECURITY_VIOLATION reports are rate-limited | CPU cost of HMAC verification per datagram |

Confidentiality is **not** provided (payloads are authenticated, not
encrypted). Secrets are typed (`security.Secret`), redacted in every text
form, loaded from 0600 files, never logged; tests generate keys at runtime;
no production credential exists in the repository.

**Key rotation** — provision a new key version on the device and in the
key store (state active, old one retired); the device starts a new session
with the new key id; revoke the old version once no device uses it.
**Restart behaviour** — session generations are durable on the device and
monotonic on the server; per-session counters restart with each session, so
no (key, session, counter) is ever reused. HMAC does not depend on nonce
uniqueness; the design stays safe if a future version adds an AEAD.

## 5. Failure and recovery (tested)

| Failure | Behaviour | Tests |
|---|---|---|
| Lost event / ACK / delivery | retransmission; duplicate classification; no re-execution | device, simnet `LostCommandAndResultAcks`, `LossyNetworkInvariants`, e2e |
| Duplicated / reordered / delayed datagrams | dedup; replay window; freshness bound | simnet lossy scenarios, gateway replay tests |
| Device crash before persistence | not queued (Enqueue error) | `TestCrashBeforePersistenceDoesNotQueue` |
| Device crash after persistence / reboot | queue survives; new session; old-session ACKs ignored | device, simnet `PanicOfflineRebootReconnect` |
| Device crash during execution | unknown / interrupted at boot; never repeated | `TestCrashDuringExecution…` |
| Gateway reboot | memory lost; sessions from the shared store; device fails over | simnet `ServerFaults`, e2e |
| Server crash between commit and ACK | retransmission answered `duplicate` (any gateway) | gateway, e2e (22 discarded ACKs recovered) |
| Database busy / write failure | bounded retries; `AckRetry`; never success | gateway, simnet `ServerFaults` |
| Dispatcher restart | all state in commands.db; write-ahead attempts | simnet `ServerFaults` |
| Command expiry / prolonged outage | expired vs unknown; never executed late | commandstore, simnet `ClockRegressionAndOutage` |
| Device clock regression | delivery unaffected (UIDs, sessions, counters are not time based) | simnet |
| v1 sequence wrap-around | UIDs are 64-bit and never wrap; v1 identities stay unique | `TestUIDsAndSessionsNeverRepeat` |
| Queue saturation | explicit ErrQueueFull; reserve for critical events and command reports; nothing evicted | device, simnet `PanicUnderQueuePressure` |

## 6. Operational limits and tuning

| Limit | Default | Where |
|---|---|---|
| Datagram | 512 B | protocol |
| Event body | 256 B | protocol |
| Device outbox | 10 000 pending, 10 % reserved for critical/control | `device.Config.Capacity`, `CriticalReserve` |
| Routine expiry | 7 days (critical/control never) | `RoutineMaxAge` |
| Device backoff | 2 s doubling to 2 min (critical 15 s), ±20 % jitter | `InitialBackoff`, `MaxBackoff`, `CriticalMaxBackoff`, `Jitter` |
| Sends per device tick | 32, ≥ 1 in 4 for routine traffic | `MaxSendsPerTick`, `RoutineShare` |
| Device retention | 1000 acked, 10 000 failed/expired rows | `RetainDone`, `RetainFailed` |
| Heartbeat | 30 s when idle | `HeartbeatInterval` |
| Gateway device state | 100 000 devices (LRU) | `ReliableConfig.MaxDevices` |
| Replay window | 1024 counters per device session | `security.WindowSize` |
| Gateway batch | 256 events or 10 ms; critical events flush at once | `gateway.Config` |
| Gateway commit retries | 3, 50 ms doubling | `ReliableConfig.StoreRetries` |
| Command TTL | 5 s … 15 min (BLOCK/UNBLOCK), 5 min (others) | `commandstore.Policy` |
| Result grace | 2 min after expiry | `Policy.ResultGrace` |
| Attempt validity | 30 s (≤ 5 min) | `dispatcher.Config.AttemptValidity` |
| Dispatcher backoff / poll | 2 s doubling to 30 s / 30 s | `dispatcher.Config` |
| Alert notify retry | 1 s doubling to 1 min | `alerting.Config` |

Each ACK is generated by the gateway's single reliable loop, at most one per
datagram of the batch being processed, and written synchronously: there is
no ACK queue to grow, and a failed send is counted and recovered by the
device's retransmission.

### Measured (Windows laptop, i7-10750H, NVMe, `synchronous=FULL`)

| Benchmark | Result |
|---|---|
| `BenchmarkSealOpen` (HKDF + HMAC verify, one event frame) | ~3.8 µs |
| `BenchmarkReliableIngest/batch64` | ~142 µs per event (~7000 events/s), ~8.8 ms per 64-event batch to ACK |
| `BenchmarkReliableIngest/batch1` | ~1.27 ms per event (one fsync per event) |
| `BenchmarkCommandDelivery` (create+sign, promote, write-ahead attempt) | ~4 ms per command |

The bottleneck is the durable commit (fsync), as intended: ACKs wait for
it. Batching amortizes it for events; commands pay ~3 commits each
(creation, attempt start, attempt end), acceptable for their rate.

## 7. Examples

Deterministic narrated demonstrations (simulated world):

```
go run ./cmd/fleet-reliable-demo                       # all scenarios
go run ./cmd/fleet-reliable-demo -scenario panic,lost-acks
```

Real processes over UDP:

```
fleet-command keygen -dir data -devices TRK-1
fleet-server -listen 127.0.0.1:7500,127.0.0.1:7501 -data data -keys data/keys.json -authority data/authority.json
fleet-device -id TRK-1 -keys data/keys.json -authority-pub data/authority.pub -gateway 127.0.0.1:7500 \
    -db data/device-TRK-1.db -fix=-23.55052,-46.63331 -panic-after 2s -offline-for 4s
fleet-command request -data data -unverified-demo-identity -principal ops:alice -roles security_operator -device TRK-1 -type BLOCK -reason "theft report"
fleet-command request -data data -unverified-demo-identity -principal ops:carol -roles fleet_viewer -device TRK-1 -type REQUEST_POSITION
fleet-command request -data data -unverified-demo-identity -principal ops:bob -roles cargo_operator -device TRK-1 -type OPEN_CARGO -reason delivery
fleet-command list -data data
fleet-command show -data data -id <command id>
fleet-command alerts -data data
fleet-command ack -data data -unverified-demo-identity -device TRK-1 -uid <panic uid> -operator ops:dave -note "driver contacted"
```

`fleet-command show` prints the full history, e.g.:

```
  requested -> eligible   [dispatcher] eligible for dispatch
  eligible  -> sent       [dispatcher] attempt 1 transmitted
  sent      -> received   [device] device reported received (none)
  received  -> accepted   [device] device reported accepted (none)
  accepted  -> succeeded  [device] device reported succeeded (none)
  attempt 1 at 00:12:49.107: sent via gw-1
```

## 8. Architecture decisions

- **ADR-R1 Separate v2 codec.** v2 is a new package and version byte; the
  v1 codec, its golden tests and v1 ingestion are untouched. *Alternative*:
  flags in v1 — rejected, v1 decoders would misinterpret them.
- **ADR-R2 HMAC-SHA256 with HKDF-derived per-purpose keys, no
  encryption.** Integrity and authenticity are the requirement; standard
  primitives from the Go standard library. Full 32-byte tag. *Alternative*:
  AEAD (ChaCha20-Poly1305) — would add confidentiality but needs nonce
  management and `x/crypto`; left to a future version (the session design
  already makes nonces unique).
- **ADR-R3 Ed25519 command authority separate from transport keys.**
  Limits a transport-key or gateway compromise to denial of service.
- **ADR-R4 Command reports as durable events.** received/accepted/rejected/
  succeeded/failed/unknown travel as COMMAND_RESULT events through the
  device outbox; the "command result ACK" is the event ACK, which means
  durable storage. One delivery mechanism, one guarantee; decisions and
  results are never lost by a crash. *Alternative*: dedicated result/ack
  message types — duplicates the outbox for no gain.
- **ADR-R5 Idempotent messages instead of a shared downlink counter.**
  Several server processes send downlink; a shared counter would need
  coordination. Every downlink message is idempotent and session-bound.
- **ADR-R6 Durable session high-water mark checked per batch.** A cache
  (first design) left a window in which a gateway that had not seen a
  reboot accepted old-session frames; the test that exposed it now passes
  without the window.
- **ADR-R6c Session re-checked inside the insert transaction.** A gateway
  admits a frame (session and replay checks) and stores its event in two
  transactions; another gateway can durably record a newer session in
  between. Reproduced deterministically
  (`TestReliableSessionAdvancedBetweenAdmitAndCommit`, gateway A's
  `BeforeCommit` hook makes gateway B record session N+1): before the fix the
  session-N event was stored and acknowledged after N+1 was recorded. The
  high-water mark itself was never lowered (the upsert only raises it) and
  gateway A already refused to seal commands for the superseded session
  (ADR-R6b), and the event was authentic and seen for the first time, so no
  state could be forged and no command authorized; but the documented rule
  "a frame of a lower generation is rejected by every gateway" did not hold
  at the persistence boundary. `AppendReliable` now compares each event with
  `device_sessions` in the same `BEGIN IMMEDIATE` transaction (the store has
  one writer, so the check and the insert are atomic with respect to
  `ObserveSession`); a superseded event yields `StaleSession`, nothing is
  written, the gateway answers nothing (stale_session) and the device
  retransmits the still-pending event in its current session. Cost: one
  primary-key lookup per event.
- **ADR-R6b Deliver only through the gateway with the latest view.** A
  gateway seals a delivery only if its remembered session is the device's
  current one in the shared store and no other gateway heard the device more
  recently. Found by the soak test: a device that moved to gateway B and
  rebooted there kept receiving deliveries sealed by gateway A for its
  previous session (dropped as stale); those commands ended `unknown`.
  Regression: `TestScenarioCommandAfterRebootBehindOtherGateway`.
- **ADR-R7 Write-ahead attempts and executions.** An attempt is recorded
  before transmission and an execution before actuation, so a crash can
  make an outcome unknown but never falsely expired or repeated.
- **ADR-R8 `unknown` is first-class.** Timeouts never become `failed`.
- **ADR-R9 Serialized actuations per device.** BLOCK and UNBLOCK for the
  same device never race; ordering is by an explicit request sequence
  (not timestamps: two requests in one millisecond kept random order).
- **ADR-R10 Alerts need a human.** Notification is at-least-once to a
  `Sink`; only an operator acknowledgement marks a critical event handled.
  No emergency dispatch is invented. "Notified" means what the sink
  guarantees: with the demonstration `LogSink` only that a log line was
  written (the server says so at startup and `fleet-command alerts` says so
  above its list).
- **ADR-R11 Supervised services; orderly stop finishes work in progress.**
  `fleet-server` runs the gateways, the dispatcher and the alerting consumer
  under a supervisor: any of them stopping on its own (error, panic, return
  without a stop request) stops the whole server with exit status 1 and the
  failed service named; a server never keeps running with command delivery
  or alert processing stopped. The dispatcher and the alerting consumer
  return `ErrFailed` after 25 consecutive failed iterations instead of
  logging forever. A stop request is honoured between iterations: a
  delivery between its write-ahead record and its outcome, a notification
  the sink accepted, and (on the device) an actuation's outcome commit are
  never interrupted, so an orderly stop cannot produce a false `unknown`.
  Stores are closed only after every service returned; a service that does
  not return within `-shutdown-grace` leaves the stores to the process exit
  (exit 1) rather than having them closed under it.
- **ADR-R12 Queries run without context cancellation.** modernc.org/sqlite
  v1.38.2 leaks a prepared statement when a query's context is cancelled
  while it steps to the first row; the interrupted connection is then
  dropped by `database/sql` and becomes a zombie that keeps the file and a
  read snapshot open (WAL checkpoints blocked; on Windows the file cannot be
  deleted). This was the cause of the intermittent `TestServerStartsAndStops`
  failures. `internal/sqlitedb` wraps the driver and passes queries a
  context without cancellation (a context cancelled before the query is
  still refused; row iteration still observes cancellation). Exec and
  transactions keep cancellation. Regression:
  `TestCancelledQueryReleasesConnection`. *Alternative*: upgrading the
  driver once a fixed release is verified; the wrapper stays harmless.

## 9. Known limitations

- No confidentiality (see ADR-R2). Positions and commands are readable on
  the wire.
- Principal authentication is delegated to the caller: `fleet-command`'s
  identity flags are unauthenticated claims (the tool refuses to use them
  without `-unverified-demo-identity`); no dual control for actuations.
- The only alert sink is the demonstration `LogSink`: no person is
  notified of a PANIC.
- Device time integrity is assumed for command expiry/freshness.
- A TELEMETRY event inherits the version-1 domain rule that device time may
  be at most 5 minutes after the ingestion time. A device whose clock runs
  more than 5 minutes ahead gets `AckInvalid` for its telemetry, which then
  stays `failed` (kept, counted) on the device rather than stored. Other
  event types do not have this rule.
- A device that loses its durable store loses its queue and its command
  dedup records (a re-delivered command id would then be new to it; the
  authority signature and expiry still bound what can execute).
- Device keys live in files and memory, not in a secure element; the
  authority key in a file, not an HSM.
- Server-side command delivery needs the dispatcher in the same process as
  a gateway that heard the device (`fleet-server`); several fleet-server
  processes work but each reaches only devices talking to its gateways.
- Retention of server-side reliable events, conflicts, attempts and history
  is unbounded (append-only audit); deployments need an archival policy.
- On Windows, a test's temporary-directory cleanup occasionally fails
  because a SQLite file is still held (issue #5, v1 packages). The failures
  seen in `cmd/fleet-server` during this work were not that issue but the
  driver defect of ADR-R12, now fixed and covered by tests.

## 10. Deployment blockers and mandatory controls

This implementation is a demonstration with simulated actuators. Before it
handles real data, and in any case before any real immobilizer or cargo
actuation is integrated, the following are mandatory; none of them is
implemented here.

| Blocker | Today | Required |
|---|---|---|
| Operator identity | `-principal`/`-roles`/`-operator` typed on the CLI; unauthenticated; refused without `-unverified-demo-identity` | authentication through an identity provider (MFA), roles from a managed directory, the authenticated identity bound into the audit record and the signed command |
| Authorization of actuations | one operator with the role suffices | dual control (two distinct authenticated operators) for BLOCK, UNBLOCK and OPEN_CARGO; rate limits; a break-glass procedure with review |
| Emergency notification | `LogSink`: a log line; nobody is paged | an incident-management integration with delivery confirmation, escalation and on-call coverage; monitoring of open, unacknowledged PANIC alerts |
| Confidentiality | payloads authenticated, not encrypted (positions and commands readable on the wire) | AEAD on the v2 payload (as a new protocol version) or a confidential transport |
| Key custody | device keys and the authority key in local files (0600) | device keys in a secure element; the authority key in an HSM with audited use; rotation and revocation procedures |
| Audit retention | unbounded, local | a retention and archival policy; tamper-evident export |
| Physical safety | simulated interlocks (speed, ignition, sensor confirmation) | independent safety engineering of the vehicle integration (hazard analysis, a functional-safety process, hardware interlocks that do not depend on this software), hardware-in-the-loop validation, a field-test authorization |
| Operations | process exit status and logs | a process supervisor that restarts on exit status 1; alerting on service failure and on growing queues |
