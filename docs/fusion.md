# Telemetry Fusion Engine — Design

`internal/fusion` estimates the distance a vehicle travelled from three
imperfect sources (GPS deltas, a 100 m CAN odometer, a pulse counter) and
explains every decision it makes. This document is the design reference:
algorithm, ordering, sessions, uncertainty, calibration, limits, and the
evaluation that backs the claims.

Nothing here is a production accuracy claim. All measurements come from the
FleetCore simulator and from hand-built scenarios.

## 1. Overview

```
samples (any order) ──► Engine ──► per-device ──► session reorder ──► estimator ──► Reports
                         │           device          buffer (seq)       (epochs)      event / estimate
                         └─ validation, device capacity, session routing
```

- **Engine**: validates input, routes samples to devices, owns statistics.
  Deterministic: no wall clock, no goroutines, map iteration never affects
  output.
- **Device**: tracks the current session (BootID) and recently closed ones.
- **Session**: a bounded reorder buffer that releases events strictly in
  sequence order.
- **Estimator**: groups released events into *epochs* and fuses them.

Every input sample gets exactly one final **event report**:

| Outcome | Meaning |
|---|---|
| `accepted` | Released in sequence order and given to the estimator. |
| `buffered` | Interim report: waiting in the reorder buffer. A final report follows. |
| `duplicate` | Identical copy of an event already seen (same boot, sequence and content). Ignored. |
| `rejected` | Not applied. `reason`: `invalid_sample`, `sequence_conflict` (same sequence, different content), `stale_session`, `device_capacity`, `sequence_ambiguous` (the sequence number is 2³⁰ or more from the session's position and cannot be placed; see §9 item 6). `sequence_exhausted` is no longer produced (Task 004). |
| `requires_reconciliation` | Plausible event that arrived after its sequence slot was finalised (`late_after_gap`, which includes numbers below the session's chosen first sequence), or older than duplicate tracking (`beyond_tracking`). Not applied. It is listed so a replay or reconciliation process can handle it. |

Each closed epoch produces one **estimate** (section 6).

## 2. Measurement semantics

These follow the Task 001 contracts (`internal/telemetry`).

| Source | Reports | Engine interpretation |
|---|---|---|
| GPS | delta since the previous GPS sample | Random error ~2% (good), ~10% (degraded). Distance during a no-fix reading is **lost** (`FoldsGaps=false`): the next delta covers only its own interval. |
| CAN | cumulative odometer, floored to 100 m | Unbiased but quantised. Two equal readings do **not** mean "stationary". An increment may represent travel over several epochs. |
| Pulse | delta at nominal calibration | Precise but biased by an unknown calibration factor. Pulses during an unavailable read are **folded** into the next delta (`FoldsGaps=true`). |

A missing measurement is never read as zero movement:

- an epoch with no usable measurement leaves the trip unchanged and reports
  `insufficient_evidence`;
- the travel is then either covered by a folded delta, bounded by CAN, or
  interpolated (section 4.5).

## 3. Time, ordering and the processing strategy

### 3.1 What each timestamp is used for

| Field | Used for | Not used for |
|---|---|---|
| `seq` (within a boot) | **Ordering.** It is the only field that is both monotonic and assigned by the device for exactly this purpose. | — |
| `boot_id` | Session identity. | Ordering sessions (it is random). |
| `device_time` | Grouping into epochs, elapsed time for speed checks, the reorder window, session-age heuristics. | Ordering within a session (the clock can jump). |
| `ingested_at` | Diagnostics only (`delayed_delivery`, `device_clock_ahead`). | Ordering: arrival order is not chronological order. |

### 3.2 Strategy: bounded reordering by sequence

The engine **buffers for bounded reordering**. It does not process blindly
in arrival order, and it does not keep unbounded history for replay.

- Each session holds up to `MaxBufferedPerSession` (256) events in a buffer
  keyed by sequence number.
- The buffer releases the next expected sequence number as soon as it is
  present, so in-order traffic is never delayed.
- When the next number is missing, the session waits until either
  - the oldest buffered event has waited `ReorderWindow` (30 s, measured in
    the session's own device time, not wall time); or
  - the buffer is full.
- It then declares the missing range a **gap** (`sequence_gap`) and
  continues.
- A new session does not know its first sequence number, so it buffers for
  up to one window and starts at the lowest number seen. This is the only
  latency the window adds.

Trade-offs:

- (+) Reordered events within the window are processed exactly as if they
  had arrived in order. A test asserts identical estimates.
- (+) Memory is finite and fixed per device, and the output is
  deterministic for a given input order.
- (−) Estimates are emitted when an epoch closes, at the next epoch's first
  event. A gap adds up to one window of latency.
- (−) An event later than the window is not applied. It is reported as
  `requires_reconciliation` / `late_after_gap`, never silently dropped.
  Applying it retroactively would require ordered replay, which is out of
  scope; see limitations.
- (−) Because the window is measured in device time, a device whose clock
  jumps far forward forces earlier gap decisions.

### 3.3 Duplicates and conflicts

Content fingerprints (FNV-64 of event ID, time, source, quality and
distance) are kept for the last `RecentEvents` (256) released events and for
everything buffered:

- same sequence and same fingerprint → `duplicate`;
- same sequence, different content → `rejected` / `sequence_conflict`;
- below the tracking horizon → `requires_reconciliation` /
  `beyond_tracking` (duplicate or late cannot be told apart).

### 3.4 Epochs and repeated timestamps

Released events with the same device time form one **epoch**, holding at
most one reading per source. That is how GPS, CAN and pulse readings taken
at the same instant are fused together.

- A second reading from the same source with the same timestamp closes the
  epoch and starts a zero-length one (`repeated_timestamp`).
- A device time earlier than the previous epoch is processed in sequence
  order, with elapsed-time checks skipped (`clock_regression`).

### 3.5 Device-time window vs. processing-time timeout

The two clocks bound different things.

| | `ReorderWindow` (30 s) | `IdleTimeout` (60 s) |
|---|---|---|
| Clock | Device time of the session: the latest `device_time` seen. | Processing time, supplied by the caller through `Engine.Tick(now)`. |
| Advances when | The device sends a newer event. | The caller ticks, whether or not anything arrives. |
| Decides | How long a missing sequence number is waited for **while the device keeps talking**. | When a **silent** device's pending work is released. |
| Effect | Declares a gap; later events continue. | Releases all buffered events (declaring gaps), closes the open epoch, adds `idle_flush`; the session stays open. |
| Deterministic for | The input order. | The input order together with the tick schedule. |

**Why both are needed.** Device time stops when the device stops. Take a
device that sends events 18 and 19 while 17 is lost, then goes quiet (it is
parked, out of coverage, or switched off). It never produces a later device
timestamp, so the reorder window never expires. Without `Tick`, events 18
and 19, and its last open epoch, would wait until `Flush`, which a live
pipeline never calls. The same holds for a device that sends less than one
window of data after a reboot, because session start-up buffering waits on
the same clock. `TestSilentDeviceAfterOutOfOrderEvents` and
`TestShortLivedDeviceIsReleasedByTick` show both behaviours.

**Tick semantics.**

- Each tick first records whether a device was active since the previous
  tick. A device with pending work that has been inactive for
  `IdleTimeout` since the last tick that saw it active is drained. Drain
  latency is therefore between `IdleTimeout` and `IdleTimeout` plus one
  tick period.
- Devices with nothing pending are skipped. Drained devices are processed
  in DeviceID order.
- An event that arrives after an idle drain for a slot that was declared
  missing is `requires_reconciliation` / `late_after_gap`. The next
  in-order events continue the session normally.
- Ticking never changes the result for devices that keep sending
  (`TestTickLeavesActiveDevicesAlone`).

**Replay.** Ticks are part of the input. A replay reproduces a live run
only if it ticks at the same points. The simplest rule is that live
consumers tick, and replays do not tick and call `Flush` at the end.
Consequently, a replay can apply events that the live run released early
as gaps, and the replay is the more complete result. `fleet-fusion` reads
a finite input and does not tick.

## 4. Fusion algorithm

All arithmetic is integer, in millimetres. Products that could exceed 64
bits use 128-bit intermediates (`math/bits`).

### 4.1 Delta sources: anchors instead of raw sums

Each delta source keeps an **anchor**: the fused trip estimate `E` at the
moment its current span began. A delta `d` (pulse: after calibration) is
turned into a proposal for the cumulative trip, `X = anchor + d`, and then
into a proposed increment `Δ = max(0, X − E)`.

This one rule handles several cases without special code:

- **Aligned sources** (the normal case): `anchor = E`, so `Δ = d`.
- **Folded pulse outage**: the anchor stays at the pre-outage `E`, so the
  double-length pulse delta is not double-counted against GPS, which
  covered the outage.
- **Unaligned sources**: each delta is still measured from where its own
  span started.

After the epoch every reporting source is re-anchored at the final `E`.

### 4.2 Plausibility and stationary handling

- **Speed check.** A delta above `MaxSpeed × span + slack` (70 m/s + 50 m)
  is excluded (`measurement_implausible`). Calibration is not updated from
  it.
- **Corroborated stationary drift.** A good pulse reading of 0 together
  with a GPS delta of at most 25 m: GPS is treated as positional drift
  (`gps_stationary_drift`).
- **GPS-only deadband.** A GPS delta below 2 m (good) or 10 m (degraded)
  is treated as noise (`gps_below_deadband`). The cost is under-counting
  very slow creep.

### 4.3 Inverse-variance fusion

Each proposal carries a variance, σ = rel × d + abs:

| Source | Good | Degraded | Extra term |
|---|---|---|---|
| GPS | 2% + 2 m | 10% + 10 m | — |
| Pulse | 0.5% + 1 m | 2% + 5 m | calibration uncertainty × d (initially 5%) |

Proposals are combined by inverse-variance weighting, not by an
unweighted average, and each source's weight is reported in
`contributions`.

- An uncalibrated pulse weighs less than GPS. A calibrated one weighs more.
- Degraded readings weigh less than good ones.

**Disagreement.** If the two delta sources differ by more than
`max(20 m, 4σ_combined)`, the engine still fuses them but reports
`sensor_disagreement`, and the estimate's confidence is set to `low`.
Without CAN nothing can arbitrate between them, so the engine does not
claim to know which source is right.

**Source health (with CAN).** Over each CAN span of at least 2 km, each
source's distance is compared with the odometer, with a tolerance of
2R + 15%. A source outside it becomes `source_suspect` and is excluded from
fusion until a later span agrees again (`source_recovered`).

Non-folding sources (GPS) are only checked when they covered at least 95%
of the epochs, because lost intervals would otherwise look like errors.

### 4.4 CAN reconciliation

Both CAN readings are floored, so the true distance `D` since the CAN
baseline satisfies

```
C − C₀ − R  <  D  <  C − C₀ + R        (R = 100 m)
```

At each CAN reading the engine moves the trip into this interval, using the
smallest correction that suffices, and reports it as `can_reconciled` and
`correction_mm`.

- In an epoch with no delta-source evidence, the trip is also raised to the
  expected value `C − C₀`. This is the whole CAN-only estimator.
- Identical readings never zero the delta-source travel. They only cap it
  at the upper bound (`can_unchanged_quantization`).
- An increment is treated as evidence about the whole span since the
  baseline, never as travel in the latest interval.

CAN anomalies:

| Situation | Behaviour |
|---|---|
| Reading below the previous one | Ignored (`can_decreased`), because it is stale or corrupt. |
| Jump larger than `MaxSpeed × time since last change + R` | Held (`can_jump_rejected`). If a later reading is consistent with the jump, the engine takes a new baseline without crediting the jump (`can_rebaselined`). |
| Unchanged while delta sources show more than R + 200 m of travel | CAN is declared stale (`can_stale`): travel removed by clamping since the last change is restored, and CAN is ignored until its value changes, which starts a new baseline. |
| No reading (unavailable) | Reported (`can_unavailable`). The next reading reconciles. |

### 4.5 Gap interpolation

When some device time was covered by no source (a GPS-only outage, or the
epoch after a sequence gap), the engine interpolates that span when the
next contributing epoch arrives: span × the mean of the speed before and
after. It reports this as `interpolated_mm` (`gap_interpolated`). It does
not interpolate in these cases:

- a contributing delta already spans the uncovered time (a folding pulse);
- CAN bounded the span;
- the span is longer than `MaxInterpolation` (60 s), which is reported as
  `gap_not_interpolated`.

### 4.6 Pulse calibration

Calibration is conservative and long-baseline:

- **CAN reference.** The engine accumulates pulse distance over a span
  starting at a CAN reading taken in the same epoch as a pulse reading.
  Once the odometer has advanced by at least **20 km**, the factor is
  `ΔCAN / Δpulse`, with uncertainty `2R / ΔCAN` (≤ 1%).
  - The factor is refined as the span grows, and replaced when a new span
    disagrees with it beyond both uncertainties.
  - Factors outside [0.9, 1.1] are rejected (`pulse_calibration_rejected`).
  - Changes of at least 500 ppm are reported (`pulse_calibration_updated`).
- **GPS reference** (only while there is no CAN calibration). The engine
  uses epochs where a good GPS delta and a good pulse delta cover the same
  interval and the vehicle moved, again after 20 km. The assumed systematic
  GPS uncertainty is 1%.
- **What resets the span.** Reboots, sequence gaps, implausible pulse
  readings and CAN re-baselines restart the span, but keep the current
  factor.

A single suspicious reading cannot calibrate the counter. It is either
excluded as implausible or diluted by a 20 km baseline.

## 5. Sessions and reboots

- A new BootID **does not reset** the trip. It restarts only what is
  per-boot: delta-source anchors (the first delta of a boot covers the time
  since boot), the calibration span and health tracking.
- The CAN baseline and the calibration factor are vehicle properties and
  carry over. The first CAN reading after a reboot is checked against the
  previous one: no decrease, and a jump no larger than the speed limit
  allows over the elapsed device time.
- Closed BootIDs (the last 16) are remembered. Their late events are
  `rejected` / `stale_session`, so a previous session cannot corrupt the
  current one.

**How session order is established.** BootIDs are random, so their numeric
order means nothing. When an unknown BootID appears:

- **Device time not older** than the current session's latest time minus
  `SessionSkewTolerance` (5 min): it is a **new session**. The current one
  is drained, with gaps declared, and closed.
- **Clearly older**: it is ambiguous. It could be a delayed earlier session
  never seen before, or a device whose clock reset at boot. Its events are
  rejected (`session_order_ambiguous`).
  - If it keeps sending `AdoptAfter` (32) consecutive events while the
    current session is silent, it is adopted (`session_adopted_despite_clock`).
  - Events rejected before adoption are reported, not applied.

**What remains ambiguous.** Without a monotonic boot counter or a trusted
clock, the engine cannot prove the order of two sessions. It relies on
device time and on which session keeps talking. A device that reboots with
a reset clock loses up to `AdoptAfter` events, and each one is reported.

## 6. Output

An estimate, one per closed epoch:

| Field | Meaning |
|---|---|
| `device_id`, `boot_id`, `first_seq`, `last_seq` | Which events the epoch holds. |
| `from`, `to` | Previous and current epoch device time (`from` is null for a session's first epoch). |
| `delta_mm` | Change of the trip: measured + `interpolated_mm` + `correction_mm`. It can be negative when CAN revises the trip downwards. |
| `interpolated_mm` | Travel estimated, not measured, for an uncovered span. |
| `correction_mm` | CAN reconciliation, possibly covering several earlier epochs. |
| `trip_mm` | Fused distance since the engine first saw the device. It survives reboots. |
| `uncertainty_mm` | Approximate 95% half-width of the trip error. |
| `confidence` | `none`, `low` (U > max(2% trip, 20 m) or unresolved disagreement), `medium` (U > max(0.5%, 5 m)) or `high`. |
| `state` | `bounded` when the trip was just checked against a CAN interval; otherwise `provisional`. |
| `contributions` | Per source: reported value, increment used, weight (ppm). |
| `diagnostics` | `{code, level, source?, seq?, value?, message}`. Codes are stable strings (`report.go`). |

**Provisional or final?** No estimate is final or certain.

- A `bounded` estimate is guaranteed, under the CAN quantisation model, to
  be within `uncertainty_mm` of the truth for the distance since the
  baseline.
- A `provisional` estimate may still be revised by later CAN readings,
  interpolation or reconciliation.

**Uncertainty model.**

- After a CAN bound: U = the distance from the estimate to the far end of
  the CAN interval. This is a guarantee, not a statistical figure.
- Between bounds, U grows by 2√(Σσ²) of the fused increments, plus a
  systematic term (pulse calibration uncertainty, or 1% for GPS), plus
  half of any interpolated distance.
- Without CAN, U accumulates from the start.

## 7. Complexity and memory bounds

Per device:

- reorder buffer ≤ 256 events;
- gap ranges ≤ 64;
- fingerprint ring ≤ 256 × 16 B;
- closed sessions ≤ 16;
- one open epoch (≤ 3 readings);
- constant-size estimator state.

Devices are capped by `MaxDevices` (100 000); beyond that, new devices are
`rejected` / `device_capacity`.

Measured with 10 000 devices: **~4.9 KB of retained heap per device**,
roughly 0.5 GB at the default device cap.

Time per sample:

- **in-order traffic:** O(1) amortised;
- **while a gap is open:** O(buffer) to find the lowest buffered sequence;
- **for an event below the next expected sequence:** O(256) to scan the
  fingerprint ring.

Measured (Go 1.27, i7-10750H, one goroutine, simulator traffic):

- about **0.9 µs per sample (~1.0–1.1 M samples/s)**;
- about 18 allocations and 1.8 KB allocated per sample, mostly report
  structures.

The CPU was shared during measurement; some slower runs (2–2.8 µs) were
observed.

## 8. Accuracy evaluation

`internal/evaluation` runs the simulator through
`simulator.RunWithGroundTruth`, a test-only API that leaves the samples
unchanged. It runs the result through the engine and compares the outcome
with the truth and with two baselines:

- **naive:** per epoch, the unweighted mean of whatever GPS and pulse
  deltas are present (nominal calibration, quality ignored), summed in
  arrival order with no ordering, duplicate or session handling. It uses
  the CAN difference when there are no delta sources.
- **CAN-diff:** the last CAN reading minus the first.

Each estimator is compared with the truth from the first instant it
observed. The *perturbed* runs deliver 2% of events swapped, 1% duplicated,
0.5% dropped and 0.2% two minutes late.

Setup:

- `fleet-eval -seeds 1,2,3,4,5 -perturb`;
- 10 vehicles × 2 000 rounds × 10 s, about 5.5 h and about 395 km per vehicle;
- aggregated over the five seeds (mean of per-seed means, max of maxima, minimum coverage).

| sources | perturbed | fused mean err m | fused max err m | fused mean rel % | naive mean err m | naive mean rel % | CAN-diff mean err m | track max m | min U coverage % | epoch err good / degraded m | ambiguous epochs | reconcile | duplicates |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| gps | no | 120.9 | 523.1 | 0.030 | 11871.7 | 3.012 | — | 770 | 99.9 | 7.22 / 10.25 | 3009 | 0 | 0 |
| gps | yes | 194.4 | 746.1 | 0.050 | 13655.3 | 3.467 | — | 1557 | 99.9 | 9.92 / 12.77 | 3640 | 195 | 883 |
| can | no | 40.7 | 183.9 | 0.010 | 40.7 | 0.010 | 40.7 | 410 | 99.5 | — | 555 | 0 | 0 |
| can | yes | 40.7 | 183.9 | 0.010 | 40.7 | 0.010 | 40.7 | 410 | 99.5 | — | 553 | 195 | 883 |
| pulse | no | 6652.2 | 13934.5 | 1.690 | 6652.2 | 1.690 | — | 13935 | 100.0 | 4.30 / 4.69 | 504 | 0 | 0 |
| pulse | yes | 6677.3 | 13943.3 | 1.696 | 6615.7 | 1.678 | — | 13943 | 100.0 | 6.92 / 6.60 | 1172 | 195 | 883 |
| gps+can | no | 31.9 | 81.1 | 0.008 | 11871.7 | 3.012 | 40.7 | 358 | 100.0 | 1.05 / 4.96 | 14 | 0 | 0 |
| gps+can | yes | 38.0 | 89.4 | 0.009 | 13824.7 | 3.505 | 43.4 | 363 | 99.9 | 1.06 / 4.96 | 58 | 372 | 1840 |
| gps+pulse | no | 120.5 | 294.7 | 0.031 | 3501.0 | 0.891 | — | 421 | 100.0 | 0.29 / 0.41 | 20 | 0 | 0 |
| gps+pulse | yes | 194.1 | 890.4 | 0.049 | 3484.3 | 0.887 | — | 1198 | 100.0 | 3.04 / 3.18 | 1391 | 372 | 1840 |
| can+pulse | no | 65.4 | 103.4 | 0.017 | 6652.2 | 1.690 | 40.7 | 330 | 100.0 | 0.54 / 0.84 | 6 | 0 | 0 |
| can+pulse | yes | 40.4 | 91.8 | 0.010 | 6553.0 | 1.664 | 40.7 | 624 | 99.5 | 2.18 / 2.34 | 692 | 372 | 1840 |
| gps+can+pulse | no | 68.9 | 100.3 | 0.017 | 3501.0 | 0.891 | 40.7 | 175 | 100.0 | 0.23 / 0.39 | 7 | 0 | 0 |
| gps+can+pulse | yes | 30.5 | 97.1 | 0.008 | 3485.5 | 0.887 | 40.7 | 360 | 99.9 | 0.40 / 0.63 | 698 | 555 | 2796 |

Column notes:

- **"track max"** is the largest |trip − truth| at any estimate during the
  trip.
- **"epoch err"** is the mean |measured increment − true increment| per
  epoch, split by whether the epoch contained a degraded reading.
- **"ambiguous epochs"** counts estimates with `insufficient_evidence` or
  `sensor_disagreement`.
- **Rejected events** were 0 in every run.

A 24-hour test run, `TestLongTrip` (2 vehicles, about 1 770 km each,
perturbed delivery), measured a maximum error of 397 m (0.022%) with GPS
only and 42 m (0.002%) with all three sources. The naive baseline's errors
were 63 km and 7.7 km.

### What the numbers show

- **Fusion beats the naive baseline by 1–3 orders of magnitude** whenever
  there is a delta source.
  - Naive GPS loses about 3% to no-fix intervals; fusion interpolates them.
  - Naive pulse inherits the calibration error; with GPS or CAN, fusion
    calibrates it.
  - Naive arithmetic counts duplicates.
- **With CAN, the final total is no better than the CAN difference.**
  - Over a full trip, the difference of two odometer readings is already
    within ±R, and fusion is held inside the same interval.
  - Fused final errors (30–69 m mean) are comparable to the CAN-diff
    baseline (41 m), and slightly worse when the uncalibrated pulse pushed
    the estimate to an edge of the interval early in the trip.
  - Fusion's benefit with CAN is **during the trip**: a track max of
    175 m with all sources against 410 m for CAN alone. It also has
    sub-metre per-epoch error, and survives CAN outages, staleness and
    jumps.
- **A pulse counter alone cannot be calibrated.** Its error (up to 3.5%) is
  the simulator's per-vehicle calibration error. This is reported, not
  hidden; it requires a second source.
- **The uncertainty indicator is conservative.** At least 99.5% of
  estimates fall inside it in every configuration. That is wide for
  delta-only cases, where it grows with the calibration uncertainty.

### Acceptance thresholds

These are enforced by `internal/evaluation/evaluation_test.go` over seeds 1–5.

| Threshold | Reasoning | Observed |
|---|---|---|
| With CAN: every final error < 2R = 200 m | The trip is kept inside the CAN interval, which contains the truth. | max 103 m |
| GPS only: mean rel ≤ 0.5%, max ≤ 1% | Random errors average out; what remains is interpolation and the deadband. | mean ≤ 0.05%, max 746 m (≈ 0.19%) |
| Pulse only: max rel ≤ 3.5% | Uncorrectable calibration (simulator ±3%) plus quantisation. | within limit; worst 13.9 km on a ~400 km trip |
| GPS+pulse: mean rel ≤ 0.5% | GPS-limited, after calibration against GPS. | ≤ 0.05% |
| ≥ 2 sources: fused mean error < naive | Fusion must earn its complexity. | Always, by 18–430× |
| Uncertainty coverage ≥ 90% | The indicator must not be overconfident. | ≥ 99.5% |
| Delivery perturbations cause no `rejected` events | Late, dropped and duplicated events have their own outcomes. | 0 |

The thresholds follow from the sensor model and the algorithm's
guarantees, not from tuning: the defaults in `DefaultConfig` are generic
sensor figures. The test also uses hand-made scenarios with other error
patterns (`internal/fusion/*_test.go`), such as 10% GPS bias, a broken
pulse counter, a frozen odometer and 3% pulse miscalibration, each with an
exactly known expected outcome.

## 9. Known limitations

1. **No persistence.** A restarted process loses all fusion state. The new
   engine re-establishes baselines and counts only what it sees afterwards
   (`TestRestartLosesInMemoryState`). Durable recovery needs a state store
   or replay from a persistent event log.
2. **Late events beyond the window are not applied.** They are reported as
   `requires_reconciliation`; no reconciliation process exists yet.
3. **Lost events make the next delta of each source ambiguous.**
   - The engine cannot tell which source an event belonged to, so both
     sources only re-anchor, and the span is interpolated.
   - With alternating stops and starts, the interpolation can be off by up
     to one epoch's travel.
   - **Persistent loss** of an event in every epoch (for example a gateway
     dropping all packets of one source) leaves no epoch with a usable
     delta. Without CAN the trip then stops growing. This is fully
     reported (`sequence_gap` and `insufficient_evidence` on every epoch)
     but not estimated. Identifying the lost source, for instance from a
     per-source sequence, would need a protocol change.
4. **CAN phase is not estimated.** The CAN baseline's quantisation phase is
   unknown, so with CAN the final error is bounded by ±R (worst case
   almost 2R from the trip's own start), not better. Estimating the phase
   from low-speed odometer transitions could tighten this (a Task 003
   candidate).
5. **Assumed aligned sampling.** Source semantics assume sensors sample at
   the same instants, as the simulator and typical devices do. Unaligned
   sources are handled by anchors, but calibration pairing and stationary
   corroboration then rarely apply.
6. **Sequence wrap-around is supported** (Task 004, ADR-16). Within a
   session, sequence numbers are mapped to an internal 64-bit space by
   bounded serial arithmetic around the highest number seen, so 2³²−1 is
   followed by 0. The public `uint32` contract is unchanged, and reports
   carry the wire values. A number 2³⁰ or more away from the session's
   position, in either direction, is **ambiguous**. It could be far in the
   past or the future, so it is rejected (`sequence_ambiguous`) rather than
   guessed. Earlier versions rejected everything after 2³²−1
   (`sequence_exhausted`).
7. **Session order is heuristic**; see section 5.
8. **GPS deadband** under-counts creeping below 0.2 m/s (2 m per 10 s).
9. **One goroutine per engine.** Scale out by sharding devices across
   engines.
10. **Error figures come from a simulator** with stationary error models.
    Real sensors have correlated errors (multipath, tyre-wear drift) that
    this evaluation does not cover.

## 10. Configuration reference

All knobs are fields of `fusion.Config`, documented in `config.go`. The
defaults are generic (MaxSpeed 70 m/s, CAN resolution 100 m, 30 s reorder
window, 60 s idle timeout) and are validated by `Config.Validate`. The CLI
exposes the window and the buffer size; other knobs are code-level for now.

## 11. Integration contract with the event pipeline

This section states what the engine expects from an upstream ingestion
pipeline (the UDP gateway and persistent event store of Task 003, connected
to the engine by the chronological processor of Task 004; see
processing.md)
and what that pipeline can expect in return. It is written against the
domain types only. The engine does not depend on any pipeline package, and
`internal/fusion` will continue to import only `internal/telemetry`.

### 11.1 Identity invariants

| Field | What the engine assumes | What it does **not** assume |
|---|---|---|
| `DeviceID` | Stable per vehicle. All of a vehicle's state is keyed by it. | Any structure beyond the telemetry format rules. |
| `BootID` | Non-zero, stays the same for one power cycle, and differs from the previous boot's. Previous values are remembered (16 per device), so reusing a recent BootID makes its events `stale_session`. | Any order. BootIDs are opaque and never compared numerically. |
| `Sequence` | Increases by one per sample within a boot, across all sources, and restarts with each boot. It is the ordering key, compared by bounded serial arithmetic (§9 item 6). | Starting at 0 or 1 (the first number is learned). Not wrapping: 2³²−1 is followed by 0 within the same session. |
| `EventID` | Nothing. It is part of the content fingerprint only. | Uniqueness. The reference producer derives it from the per-boot sequence, so it repeats across reboots (`TestEventIDReuseAcrossBootsIsNormal`). |

The engine's **event identity** is `(DeviceID, BootID, Sequence)`. A content
fingerprint covers EventID, device time, source, quality and distance:

| Seen by the engine | Outcome |
|---|---|
| Same identity, same content (retransmission, any `IngestedAt`) | `duplicate` |
| Same identity, different content, including a different EventID | `rejected` / `sequence_conflict`. The first one released wins. |
| Same EventID in two different `(BootID, Sequence)` slots | Two independent events. |

A durable store keyed by `(DeviceID, BootID, EventID)` classifies these
cases differently:

- it stores both events of a reused `(BootID, Sequence)` with different
  EventIDs, and the engine then rejects the second as a sequence conflict;
- it treats the same EventID in two sequence slots as a conflict and
  delivers only the first.

Each layer reports what it rejects, so nothing is lost silently. A
consumer that needs a single answer should treat the store's
classification as authoritative for storage and the engine's as
authoritative for distance.

### 11.2 Input the engine expects

1. **Samples as `telemetry.Sample`**, with `IngestedAt` set by the
   receiver. The engine re-validates every sample (invalid →
   `rejected` / `invalid_sample`) and uses `IngestedAt` only for
   diagnostics.
2. **Each stored event delivered once, in a fixed, reproducible order**,
   for example the store's append order. Arrival order need not be
   chronological; the engine reorders within each session. Re-delivering
   events is harmless (`duplicate`) but wastes the fingerprint window
   (256 events), beyond which a re-delivery is classified
   `beyond_tracking`.
3. **One engine per goroutine.** `Process` is synchronous, at about
   1 µs/sample, and does no I/O. To scale out, shard devices by DeviceID
   across engines; a device must always reach the same engine.
4. **`Tick(now)` periodically** in live operation, for example once per
   second with the processing clock; see §3.5. `Flush` is for the end of a
   finite input or for an orderly shutdown, after which the engine must not
   be reused.

### 11.3 What the engine returns

- One final **event report** per input sample. The outcome tells a
  pipeline what fusion did with a stored event. `requires_reconciliation`
  events are the ones a later reconciliation or replay step should
  revisit.
- **Estimates** per epoch, with `trip_mm`, `uncertainty_mm`, `state` and
  diagnostics. They are emitted when an epoch closes: at the next epoch's
  first event, at a gap, at an idle drain, or at `Flush`.
- Reports are plain data. The engine logs nothing. Persisting, counting or
  alerting on diagnostics is the caller's job.

### 11.4 Recovery

Fusion state is in memory only (§9). With an append-only store, recovery
is **deterministic replay**:

- feed the stored events, in store order, into a fresh engine, without
  ticks, then `Flush` (`TestReplayReproducesOutput`);
- the replay output is reproducible bit for bit;
- it can differ from what a live, ticking engine emitted only where idle
  drains released events early (§3.5).

Snapshotting estimator state is deliberately not offered. Replay is
simpler to get right, and it heals idle-drain artefacts.

### 11.5 Open points for the integration review

- **Sequence wrap-around.** *Resolved in Task 004:* the engine and the
  chronological processor extend sequence numbers with the same rule
  (`TestExtenderParityWithChrono`); see processing.md §2.2.
- **Duplicate horizon.** The engine remembers 256 released events per
  session. A store that guarantees once-only delivery makes this
  irrelevant; at-least-once delivery with long delays would turn late
  re-deliveries into `beyond_tracking`.
- **Session ordering.** The engine orders unknown boots by device time,
  adopting after 32 consecutive events. A pipeline-side session registry
  may order them differently (for example by first observation). Fusion
  keeps its own rule. A shared session registry, as planned, would remove
  the duplication.
