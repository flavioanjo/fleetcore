# Ingestion and Fusion — Integration Review

FleetCore now has two subsystems that consume the same telemetry:

- **Ingestion** ([ingestion.md](ingestion.md)): `internal/gateway`,
  `internal/eventstore`, `internal/sessions`. It receives, validates,
  deduplicates and durably stores events.
- **Fusion** ([fusion.md](fusion.md)): `internal/fusion`. It orders events
  and estimates distance travelled.

They were developed independently (Tasks 002 and 003). **Task 004 connects
them** through the chronological processor (`internal/chrono`,
`internal/processor`, `cmd/fleet-processor`; see
[processing.md](processing.md)). This document records where the two
subsystems' rules differ, what that implies, and which points Task 004
resolved. No protocol or domain contract was changed.

## Coupling

| Rule | Status |
|------|--------|
| `internal/fusion` imports only `internal/telemetry` (plus stdlib). | Holds; checked with `go list -deps`. `TestIngestionIsDecoupledFromFusion` fails if fusion imports an ingestion package. |
| `internal/fusion` does not import the simulator. | Enforced by `TestNoGroundTruthDependency`. |
| Ingestion packages do not import `internal/fusion` or `internal/evaluation`. | Holds; checked with `go list -deps` and enforced by `TestIngestionIsDecoupledFromFusion` (ingestion may import only telemetry, protocol and itself). |
| Only `internal/eventstore` depends on a third-party module (`modernc.org/sqlite`). | Holds; `fusion` and `cmd/fleet-fusion` binaries contain no SQLite code. |

## 1. Event identity

| | Event store | Fusion engine |
|---|---|---|
| Key | `(DeviceID, BootID, EventID)` | `(DeviceID, BootID, Sequence)` |
| Fingerprint | SHA-256 of the canonical packet (every wire field, including Sequence and EventID) | FNV-64 of EventID, device time, source, quality, distance |
| Horizon | Unlimited (database constraint) | Last 256 released events per session, plus the reorder buffer |
| Purpose | Storage identity: what is the same event, durably | Ordering slot: which position in the session's stream an event occupies |

These are **different concepts** and need not share a key. The store asks
"have I already stored this event?"; the engine asks "what fills sequence
slot *n* of this session?". Each is authoritative for its own question
(fusion.md §11.1): the store for storage, the engine for distance.

How the same input is classified by each layer, assuming the engine is fed
from the store's `events` table:

| Input | Store | Fusion (fed from `events`) |
|-------|-------|----------------------------|
| Retransmission | `Duplicate`, not stored again, so not re-delivered | Never sees it |
| Same `(BootID, Sequence)`, different EventIDs | Both inserted; registry reports `sequence_reuse` | First is applied; second is `rejected` / `sequence_conflict` |
| Same `(BootID, EventID)`, different Sequence or content | `Conflict`: first kept, second only in `event_conflicts` | Sees only the first. The second's sequence slot is never delivered and becomes a **`sequence_gap`** |
| Same EventID in a new boot (simulator reboot) | Inserted; counted as `EventIDReused` | Independent events, as intended |
| Reused BootID (device repeats a recent boot value) | New identities unless EventIDs also repeat, then `Conflict` | Events `rejected` / `stale_session` if the BootID is among the last 16 closed boots |

**Implications.**

- A storage conflict surfaces in fusion as a gap, not as a conflict. A
  consumer that correlates the two must look in `event_conflicts`.
- Feeding fusion from the `events` table in position order delivers each
  stored event exactly once, in a reproducible order. That is what
  fusion.md §11.2 asks for, and it makes the engine's 256-event duplicate
  horizon irrelevant for retransmissions.
- The engine's fingerprint excludes Sequence because Sequence is its key.
  The store's includes it because the key is EventID. Both are correct for
  their own key.

**Open decision.** None is required to connect the layers. If a single
authoritative identity is ever wanted across subsystems, it is a domain
decision (strengthening the per-boot EventID guarantee in
`internal/telemetry`), not something either layer should change on its own.

## 2. Sequence wrap-around

*Resolved in Task 004 (ADR-16).*

| | Rule |
|---|---|
| Domain (`telemetry.Sequence`) | "It may wrap around after 2³²−1." Unchanged. |
| Fusion engine | Internal 64-bit extension by bounded serial arithmetic. A wrap is continuous; a number 2³⁰ or more from the session's position is `rejected` / `sequence_ambiguous`. |
| Chronological processor | The same rule (`chrono.Extender`, pinned to fusion's by `TestExtenderParityWithChrono`); ambiguous numbers are flagged and delivered in ingestion order. |
| Session registry (gateway) | Serial arithmetic with a ±2³¹ range and no ambiguity zone. A jump in [2³⁰, 2³¹) is a gap or reordering there, but ambiguous in fusion. The registry is diagnostic only, so this affects diagnostics, not data; aligning it is a small follow-up. |
| Event store | Not affected: Sequence is not part of the identity. |
| Reference simulator | Derives EventID as `vehicle << 32 \| sequence`, so after a wrap its EventIDs repeat within a boot and the store classifies them as conflicts. End-to-end wrap needs a producer whose EventIDs stay unique per boot (as the tests use). Reaching a wrap takes about 45 years at 3 samples per second. |

The option rejected was declaring that a device must reboot before
exhausting the sequence space. It would have changed a Task 001 contract;
extending the comparison internally keeps the contract and needs no change
on devices.

## 3. Session chronology

| | Session registry (ingestion) | Fusion engine |
|---|---|---|
| Orders sessions by | First observation (ingestion time) | Device time, with `SessionSkewTolerance` (5 min) |
| Unknown boot that looks older | Tracked; `chronology_ambiguous` diagnostic | Events `rejected` / `session_order_ambiguous` until 32 consecutive events (`AdoptAfter`), then adopted |
| Late event of an earlier session | Tracked (`earlier_observed_session`, or `retired_session` if expired); always stored | `rejected` / `stale_session` once the session is closed |
| Effect on data | None. Advisory only; the store keeps everything | Determines what contributes to distance |
| Persistence | In memory, lost on restart | In memory, lost on restart |

**Implications.**

- The same stream can produce different stories. For example, a device
  that reboots with a reset clock is a plain `new_session` (plus
  `chronology_ambiguous` if the two overlap) in the registry. In fusion it
  produces up to 32 rejected events before adoption. Both are reported;
  neither is wrong under its own rule.
- Events the registry describes as late but valid are stored, while fusion
  may reject them as `stale_session` or `requires_reconciliation`. They
  remain available in the store for a future reconciliation step, which
  does not exist yet (fusion.md §9 item 2).
- Diagnostics share some names with different meanings. The registry's
  `sequence_gap` is reported when a jump is first seen and can later be
  filled (`out_of_order`). Fusion's `sequence_gap` is a final decision taken
  after the reorder window or an idle drain. A combined dashboard must not
  add them up.
- Neither ordering should override the other until a **shared session
  registry** exists (README roadmap). Fusion keeps its device-time rule,
  which suits distance estimation. Ingestion keeps its discovery order,
  which suits an advisory, never-discarding layer.

## 4. Other differences relevant to integration

| Topic | Ingestion | Fusion | Note |
|-------|-----------|--------|------|
| Time base | `IngestedAt` (gateway clock) for registry timeouts | Device time for the reorder window; caller-supplied `Tick(now)` for idle drains | A live integration must tick the engine (fusion.md §3.5). |
| Late-event horizon | 1024-sample bitmap per session (diagnostics only) | 30 s device-time window, 256-event buffer | Beyond fusion's window an event is `requires_reconciliation`. The store still has it. |
| Restart recovery | Store is durable; registry rebuild is possible by replay (tested) but not automatic | Deterministic replay from an append-only log (`TestReplayReproducesOutput`) | Both replays can read `Store.ReadAfter` from position 0. Neither is wired up. |
| Concurrency | One writer goroutine | One goroutine per engine, sharded by DeviceID | A consumer can read the store and dispatch by DeviceID. |
| Malformed input | Rejected before storage, counted by reason | Re-validates; invalid → `rejected` / `invalid_sample` | Stored events always pass fusion validation. |

## 5. How Task 004 connects them

| Need | Implementation |
|------|----------------|
| Read stored events in a fixed, reproducible order, each once | `fleet-processor` pages `Store.ReadAfter` by position. Every event it feeds to fusion is a journal step (`UNIQUE(position)`). |
| Chronological delivery | `chrono.Orderer`: sequence order within a session, device time between a device's sessions (ingestion order when device time is not evidence), ingestion order between devices (processing.md §2). |
| `Tick(now)` in live operation | Every `TickInterval`, journaled so replays tick at the same points. |
| Recovery | Verified replay of the journal (ticks included), then continuation from the low watermark (processing.md §3). |
| Persisted reports, estimates and diagnostics | `results.db` (`internal/resultstore`). |
| Sequence wrap | §2. |

Still open:

- reconciliation of `requires_reconciliation` events;
- sharding devices across several processors;
- a shared session model (§3), and aligning the gateway registry's serial
  range (§2);
- journal retention or snapshots, to bound recovery time.
