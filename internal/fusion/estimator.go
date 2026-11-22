package fusion

import (
	"fmt"
	"time"

	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

const ppm = 1_000_000

// Calibration reference names (diagnostic messages only).
const (
	calNominal = "nominal"
	calGPS     = "gps"
	calCAN     = "can"
)

// srcState is the per-session state of one delta source (GPS or pulse).
type srcState struct {
	// The anchor is the trip estimate at the end of the span the source's
	// next delta will cover. anchored is false when that span is unknown
	// (after a sequence gap); the next reading then only re-anchors.
	anchored   bool
	anchorTrip telemetry.Millimetres
	anchorTime time.Time
	timeKnown  bool // anchorTime is a real reading time (false right after boot)

	suspect      bool                  // excluded from fusion by the health check
	healthSum    telemetry.Millimetres // calibrated distance since the health checkpoint
	healthEpochs int64                 // epochs with a contribution since the checkpoint
}

type canState struct {
	valid          bool                  // a baseline exists and CAN is trusted
	base           telemetry.Millimetres // CAN value at the baseline
	baseTrip       telemetry.Millimetres // trip estimate at the baseline
	last           telemetry.Millimetres // last accepted CAN value
	lastChangeTime time.Time             // device time when last changed
	lastTimeKnown  bool

	fineSinceChange    telemetry.Millimetres // fused delta-source travel since the last CAN change
	clampedSinceChange telemetry.Millimetres // travel removed by upper-bound clamping since the last change

	pending      bool // an implausible jump awaits confirmation
	pendingValue telemetry.Millimetres
	pendingTime  time.Time

	stale      bool // CAN froze; ignore it until the value changes
	staleValue telemetry.Millimetres
}

type calState struct {
	ppm    int64  // pulse calibration factor, ppm (1e6 = nominal)
	uncPPM int64  // its 1-sigma uncertainty
	ref    string // calNominal, calGPS or calCAN

	// CAN-referenced span: pulse distance accumulated since a CAN reading.
	spanActive bool
	spanStart  telemetry.Millimetres
	spanPulse  int64

	// GPS-referenced pairs: epochs where a good GPS delta and a good pulse
	// delta covered exactly the same interval.
	pairGPS, pairPulse int64
}

func (c *calState) apply(d telemetry.Millimetres) telemetry.Millimetres {
	return telemetry.Millimetres(mulDiv(int64(d), c.ppm, ppm))
}

func (c *calState) resetSpan() { c.spanActive, c.spanPulse = false, 0 }

type epoch struct {
	open              bool
	time              time.Time
	readings          [4]*telemetry.Sample // indexed by telemetry.Source
	firstSeq, lastSeq telemetry.Sequence
	clockRegressed    bool
}

// estimator holds the distance estimate of one device. It receives events
// strictly in sequence order, one session at a time, and groups them into
// epochs (events of one session sharing a device timestamp, at most one
// per source).
type estimator struct {
	cfg  *Config
	id   telemetry.DeviceID
	boot telemetry.BootID

	trip     telemetry.Millimetres // fused trip distance; survives reboots
	evidence bool                  // some measurement has contributed

	src [4]srcState
	can canState
	cal calState

	// Uncertainty model: base (after a CAN bound) + 2*sqrt(var) + systematic.
	uncBase, uncVar, uncSys int64

	healthActive     bool
	healthCheckpoint telemetry.Millimetres
	healthEpochs     int64

	// Gap interpolation: device time no source covered since the last
	// contributing epoch, and the speed measured in that epoch.
	uncovered    time.Duration
	uncoveredEnd time.Time
	rate         int64 // mm/s
	haveRate     bool

	ep        epoch
	lastEpoch time.Time
	haveLast  bool
	pending   []Diagnostic // attached to the next estimate
}

func newEstimator(cfg *Config, id telemetry.DeviceID) *estimator {
	return &estimator{
		cfg: cfg,
		id:  id,
		cal: calState{ppm: ppm, uncPPM: cfg.PulseInitialUncPPM, ref: calNominal},
	}
}

// startSession prepares for a new boot. Accumulated trip, CAN baseline and
// calibration are kept; delta-source baselines restart from the boot.
func (e *estimator) startSession(boot telemetry.BootID) {
	e.boot = boot
	for _, s := range []telemetry.Source{telemetry.SourceGPS, telemetry.SourcePulse} {
		st := &e.src[s]
		st.anchored, st.anchorTrip, st.timeKnown = true, e.trip, false
	}
	e.haveLast = false
	e.uncovered, e.haveRate = 0, false
	e.cal.resetSpan()
	e.resetHealth()
}

// endSession closes the open epoch, if any.
func (e *estimator) endSession() []Estimate {
	if !e.ep.open {
		return nil
	}
	return []Estimate{e.closeEpoch()}
}

// gap records that sequence numbers g.from..g.to were never received.
func (e *estimator) gap(g seqRange) []Estimate {
	out := e.endSession()
	for _, s := range []telemetry.Source{telemetry.SourceGPS, telemetry.SourcePulse} {
		e.src[s].anchored = false
	}
	e.cal.resetSpan()
	e.resetHealth()
	e.pending = append(e.pending, diag(CodeSequenceGap, LevelWarning,
		fmt.Sprintf("sequence %d-%d never received; delta-source spans are ambiguous until re-anchored", g.fromSeq(), g.toSeq())).
		withSeq(g.fromSeq()).withValue(g.count()))
	return out
}

// ingest adds one in-order event. It returns the estimate of the previous
// epoch if this event starts a new one.
func (e *estimator) ingest(s telemetry.Sample) []Estimate {
	var out []Estimate
	if e.ep.open && (!s.DeviceTime.Equal(e.ep.time) || e.ep.readings[s.Source] != nil) {
		if s.DeviceTime.Equal(e.ep.time) {
			e.pending = append(e.pending, diag(CodeRepeatedTimestamp, LevelInfo,
				"second reading from the same source with the same device time; treated as a new zero-length epoch").
				withSource(s.Source).withSeq(s.Sequence))
		}
		out = append(out, e.closeEpoch())
	}
	if !e.ep.open {
		e.ep = epoch{open: true, time: s.DeviceTime, firstSeq: s.Sequence}
		if e.haveLast && s.DeviceTime.Before(e.lastEpoch) {
			e.ep.clockRegressed = true
			e.pending = append(e.pending, diag(CodeClockRegression, LevelWarning,
				"device time went backwards within a session; elapsed-time checks skipped for this epoch").
				withSeq(s.Sequence).withValue(e.lastEpoch.Sub(s.DeviceTime).Milliseconds()))
		}
	}
	cp := s
	e.ep.readings[s.Source] = &cp
	e.ep.lastSeq = s.Sequence
	return out
}

type contrib struct {
	src        telemetry.Source
	quality    telemetry.Quality
	seq        telemetry.Sequence
	raw        telemetry.Millimetres // as reported
	used       telemetry.Millimetres // calibrated span distance
	delta      int64                 // proposed trip increment
	variance   int64
	sysPPM     int64
	spanStart  time.Time
	spanKnown  bool
	suppressed bool
	weight     int64
}

func (e *estimator) closeEpoch() Estimate {
	cfg, t := e.cfg, e.ep.time
	est := Estimate{
		DeviceID: e.id, BootID: e.boot, FirstSeq: e.ep.firstSeq, LastSeq: e.ep.lastSeq,
		To: t, State: StateProvisional, Contributions: []Contribution{},
	}
	if e.haveLast {
		est.From = telemetry.Some(e.lastEpoch)
	}
	diags := e.pending
	e.pending = nil
	add := func(d Diagnostic) { diags = append(diags, d) }
	startTrip := e.trip
	reanchor := map[telemetry.Source]bool{}

	// 1. Turn each delta reading into a proposed trip increment.
	var cs []*contrib
	for _, src := range []telemetry.Source{telemetry.SourceGPS, telemetry.SourcePulse} {
		r := e.ep.readings[src]
		if r == nil {
			continue
		}
		st, model := &e.src[src], cfg.model(src)
		d, ok := r.Distance.Get()
		if !ok {
			add(diag(CodeSourceUnavailable, LevelInfo, "no measurement in this reading").withSource(src).withSeq(r.Sequence))
			if !model.FoldsGaps {
				reanchor[src] = true // distance during this interval is lost to this source
			}
			continue
		}
		if !st.anchored {
			add(diag(CodeSpanAmbiguous, LevelInfo, "start of this delta's span is unknown after a sequence gap; used only to re-anchor").
				withSource(src).withSeq(r.Sequence))
			reanchor[src] = true
			continue
		}
		used := d.Value
		if src == telemetry.SourcePulse {
			used = e.cal.apply(d.Value)
		}
		c := &contrib{src: src, quality: r.Quality, seq: r.Sequence, raw: d.Value, used: used,
			spanStart: st.anchorTime, spanKnown: st.timeKnown && !e.ep.clockRegressed && !t.Before(st.anchorTime)}
		if c.spanKnown {
			limit := telemetry.Millimetres(mulDiv(int64(cfg.MaxSpeed), t.Sub(st.anchorTime).Milliseconds(), 1000)) + cfg.PlausibilitySlack
			if used > limit {
				add(diag(CodeMeasurementImplausible, LevelWarning,
					fmt.Sprintf("%d mm in %s exceeds the %d mm speed limit; excluded", used, t.Sub(st.anchorTime), limit)).
					withSource(src).withSeq(r.Sequence).withValue(int64(used)))
				reanchor[src] = true
				if src == telemetry.SourcePulse {
					e.cal.resetSpan()
				}
				continue
			}
		}
		c.delta = max(0, int64(st.anchorTrip+used-startTrip))
		rel, abs := model.GoodRelPPM, model.GoodAbs
		if r.Quality == telemetry.QualityDegraded {
			rel, abs = model.DegradedRelPPM, model.DegradedAbs
		}
		sigma := mulDiv(int64(used), rel, ppm) + int64(abs)
		c.variance = sigmaVariance(sigma)
		c.sysPPM = model.SystematicPPM
		if src == telemetry.SourcePulse {
			c.variance += sigmaVariance(mulDiv(int64(used), e.cal.uncPPM, ppm))
			c.sysPPM = e.cal.uncPPM
		}
		cs = append(cs, c)
	}

	// 2. Stationary handling for GPS.
	var gps, pulse *contrib
	for _, c := range cs {
		if c.src == telemetry.SourceGPS {
			gps = c
		} else {
			pulse = c
		}
	}
	// Only a healthy pulse counter may corroborate "stationary".
	trustedPulse := pulse
	if pulse != nil && e.src[telemetry.SourcePulse].suspect {
		trustedPulse = nil
	}
	if gps != nil {
		switch {
		case trustedPulse != nil && pulse.raw == 0 && pulse.quality == telemetry.QualityGood && gps.used <= cfg.GPSStationaryCorroborated:
			if gps.used > 0 {
				add(diag(CodeGPSStationaryDrift, LevelInfo, "pulse counter reports no movement; GPS delta treated as positional drift").
					withSource(telemetry.SourceGPS).withSeq(gps.seq).withValue(int64(gps.used)))
			}
			gps.delta, gps.suppressed = 0, true
		case trustedPulse == nil:
			band := cfg.GPSDeadbandGood
			if gps.quality == telemetry.QualityDegraded {
				band = cfg.GPSDeadbandDegraded
			}
			if gps.used > 0 && gps.used < band {
				add(diag(CodeGPSBelowDeadband, LevelInfo, "GPS delta below the stationary noise band; treated as no movement").
					withSource(telemetry.SourceGPS).withSeq(gps.seq).withValue(int64(gps.used)))
				gps.delta, gps.suppressed = 0, true
			}
		}
	}

	// 3. Health: exclude sources the CAN cross-check marked suspect.
	var use []*contrib
	for _, c := range cs {
		if e.src[c.src].suspect {
			add(diag(CodeSourceExcluded, LevelWarning, "source is suspect after disagreeing with the CAN odometer").
				withSource(c.src).withSeq(c.seq))
			continue
		}
		use = append(use, c)
	}

	// 4. Disagreement between the remaining delta sources.
	disagree := false
	if len(use) == 2 && !use[0].suppressed && !use[1].suppressed {
		diff := absDiff(use[0].delta, use[1].delta)
		thr := max(int64(cfg.DisagreementFloor), cfg.DisagreementSigma*isqrt(use[0].variance+use[1].variance))
		if diff > thr {
			disagree = true
			add(diag(CodeSensorDisagreement, LevelWarning,
				fmt.Sprintf("GPS proposes %d mm, pulse %d mm (threshold %d mm); fused by inverse variance", gpsOf(use).delta, pulseOf(use).delta, thr)).
				withValue(diff))
		}
	}

	// 5. Inverse-variance fusion of the proposed increments.
	var fused, fusedVar, maxSys int64
	if len(use) > 0 {
		fused, fusedVar = use[0].delta, use[0].variance
		for _, c := range use[1:] {
			fused, fusedVar = combine(fused, fusedVar, c.delta, c.variance)
		}
		for _, c := range use {
			c.weight = mulDiv(fusedVar, ppm, c.variance)
			maxSys = max(maxSys, c.sysPPM)
			est.Contributions = append(est.Contributions, Contribution{
				Source: c.src, ReportedMM: c.raw, UsedMM: telemetry.Millimetres(c.delta), WeightPPM: c.weight})
		}
		e.evidence = true
	}
	elapsedKnown := e.haveLast && !e.ep.clockRegressed && !t.Before(e.lastEpoch)
	elapsedMs := int64(0)
	if elapsedKnown {
		elapsedMs = t.Sub(e.lastEpoch).Milliseconds()
	}
	var interp int64
	if len(use) > 0 {
		if e.uncovered > 0 {
			interp = e.interpolate(use, fused, elapsedMs, add)
		}
		if elapsedMs > 0 {
			e.rate, e.haveRate = fused*1000/elapsedMs, true
		}
	}
	e.trip += telemetry.Millimetres(fused + interp)
	e.uncVar = min(e.uncVar+fusedVar, 1<<60) // saturate; only reachable without CAN over absurd spans
	e.uncSys += mulDiv(fused, maxSys, ppm)
	e.can.fineSinceChange += telemetry.Millimetres(fused + interp)

	// 6. Bookkeeping for calibration and health.
	e.healthEpochs++
	for _, c := range cs {
		st := &e.src[c.src]
		st.healthEpochs++
		// Health compares what the source itself reported, even when the
		// value was suppressed as drift: otherwise a broken corroborating
		// source would make a healthy one look wrong too.
		st.healthSum += c.used
		reanchor[c.src] = true
	}
	if pulse != nil {
		e.cal.spanPulse += int64(pulse.raw)
		if gps != nil && e.cal.ref != calCAN {
			e.pairWithGPS(gps, pulse, add)
		}
	}

	// 7. CAN reconciliation.
	if r := e.ep.readings[telemetry.SourceCAN]; r != nil {
		if e.processCAN(r, len(use) > 0, pulse != nil, add) {
			est.State = StateBounded
			e.evidence = true
		}
	}

	// 8. Re-anchor delta sources at the final estimate.
	for src := range reanchor {
		st := &e.src[src]
		st.anchored, st.anchorTrip, st.anchorTime, st.timeKnown = true, e.trip, t, true
	}

	switch {
	case len(use) > 0 || est.State == StateBounded:
		e.uncovered = 0 // measured, or accounted for by the CAN bound
	default:
		add(diag(CodeInsufficientEvidence, LevelInfo,
			"no usable measurement in this epoch; trip unchanged for now (not assumed stationary)"))
		if elapsedMs > 0 {
			e.uncovered += time.Duration(elapsedMs) * time.Millisecond
			e.uncoveredEnd = t
		}
	}
	est.DeltaMM = e.trip - startTrip
	est.InterpolatedMM = telemetry.Millimetres(interp)
	est.CorrectionMM = est.DeltaMM - telemetry.Millimetres(fused+interp)
	est.TripMM = e.trip
	est.UncertaintyMM = telemetry.Millimetres(e.uncBase + 2*isqrt(e.uncVar) + e.uncSys)
	est.Confidence = e.confidence(est.UncertaintyMM, disagree)
	est.Diagnostics = diags

	e.lastEpoch, e.haveLast = t, true
	e.ep = epoch{}
	return est
}

// interpolate estimates travel during the uncovered span that ended at
// uncoveredEnd, as the mean of the speed before and after it times its
// duration. It does nothing when a contributing delta already spans the
// uncovered time (a pulse counter that kept counting), when the span is
// longer than MaxInterpolation, or when either speed is unknown.
func (e *estimator) interpolate(use []*contrib, fused, elapsedMs int64, add func(Diagnostic)) int64 {
	gap := e.uncovered
	e.uncovered = 0
	for _, c := range use {
		if !c.spanKnown || c.spanStart.Before(e.uncoveredEnd) {
			return 0 // this delta covers the uncovered span itself
		}
	}
	if gap > e.cfg.MaxInterpolation || !e.haveRate || elapsedMs <= 0 {
		add(diag(CodeGapNotInterpolated, LevelWarning,
			fmt.Sprintf("%s without any measurement could not be interpolated; that travel is missing from the trip", gap)).
			withValue(gap.Milliseconds()))
		return 0
	}
	after := fused * 1000 / elapsedMs
	interp := (e.rate + after) * gap.Milliseconds() / 2000
	interp = min(interp, mulDiv(int64(e.cfg.MaxSpeed), gap.Milliseconds(), 1000))
	e.uncSys += interp / 2 // interpolation error is not random noise: add it linearly
	add(diag(CodeGapInterpolated, LevelInfo,
		fmt.Sprintf("%s without measurements interpolated from %d and %d mm/s", gap, e.rate, after)).withValue(interp))
	return interp
}

func gpsOf(cs []*contrib) *contrib   { return findSrc(cs, telemetry.SourceGPS) }
func pulseOf(cs []*contrib) *contrib { return findSrc(cs, telemetry.SourcePulse) }
func findSrc(cs []*contrib, s telemetry.Source) *contrib {
	for _, c := range cs {
		if c.src == s {
			return c
		}
	}
	return &contrib{}
}

func (e *estimator) confidence(u telemetry.Millimetres, disagree bool) Confidence {
	switch {
	case !e.evidence:
		return ConfidenceNone
	case disagree || u > max(e.trip/50, 20*telemetry.Metre):
		return ConfidenceLow
	case u > max(e.trip/200, 5*telemetry.Metre):
		return ConfidenceMedium
	default:
		return ConfidenceHigh
	}
}

// processCAN applies one CAN reading. It returns true when the trip
// estimate was checked against the CAN-implied interval.
func (e *estimator) processCAN(r *telemetry.Sample, fineEvidence, pulseInEpoch bool, add func(Diagnostic)) bool {
	cfg, t, can := e.cfg, r.DeviceTime, &e.can
	d, ok := r.Distance.Get()
	if !ok {
		add(diag(CodeCANUnavailable, LevelInfo, "no odometer value in this reading").withSource(telemetry.SourceCAN).withSeq(r.Sequence))
		return false
	}
	c := d.Value
	canDiag := func(code Code, level Level, msg string) Diagnostic {
		return diag(code, level, msg).withSource(telemetry.SourceCAN).withSeq(r.Sequence)
	}

	if can.stale && c == can.staleValue {
		return false // still frozen; already reported
	}
	if can.stale && c < can.staleValue {
		add(canDiag(CodeCANDecreased, LevelWarning,
			fmt.Sprintf("odometer %d mm is below the frozen value %d mm; ignored", c, can.staleValue)).withValue(int64(c - can.staleValue)))
		return false
	}
	if !can.valid {
		code, msg := CodeCANBaseline, "first usable odometer reading; later readings bound the trip estimate"
		if can.stale {
			code, msg = CodeCANRebaselined, "odometer changed after being stale; new baseline"
		}
		e.canBaseline(c, t, pulseInEpoch)
		add(canDiag(code, LevelInfo, msg).withValue(int64(c)))
		return false
	}
	if c < can.last {
		add(canDiag(CodeCANDecreased, LevelWarning,
			fmt.Sprintf("odometer %d mm is below the previous %d mm (stale or corrupt); ignored", c, can.last)).withValue(int64(c - can.last)))
		return false
	}
	if can.lastTimeKnown && !t.Before(can.lastChangeTime) {
		limit := telemetry.Millimetres(mulDiv(int64(cfg.MaxSpeed), t.Sub(can.lastChangeTime).Milliseconds(), 1000)) +
			cfg.CANResolution + cfg.PlausibilitySlack
		if c-can.last > limit {
			if can.pending && c >= can.pendingValue && !t.Before(can.pendingTime) &&
				c-can.pendingValue <= telemetry.Millimetres(mulDiv(int64(cfg.MaxSpeed), t.Sub(can.pendingTime).Milliseconds(), 1000))+cfg.CANResolution+cfg.PlausibilitySlack {
				e.canBaseline(c, t, pulseInEpoch)
				add(canDiag(CodeCANRebaselined, LevelWarning, "implausible odometer jump confirmed by a consistent follow-up reading; new baseline, jump not credited").
					withValue(int64(c)))
				return false
			}
			can.pending, can.pendingValue, can.pendingTime = true, c, t
			add(canDiag(CodeCANJumpRejected, LevelWarning,
				fmt.Sprintf("odometer advanced %d mm, more than the %d mm possible since its last change; held pending confirmation", c-can.last, limit)).
				withValue(int64(c - can.last)))
			return false
		}
	}
	can.pending = false

	if c == can.last {
		if can.fineSinceChange > cfg.CANResolution+cfg.CANStaleTolerance {
			restored := can.clampedSinceChange
			e.trip += restored // the bound that removed this travel came from a frozen value
			can.valid, can.stale, can.staleValue = false, true, c
			e.cal.resetSpan()
			e.resetHealth()
			add(canDiag(CodeCANStale, LevelWarning,
				fmt.Sprintf("delta sources report %d mm of travel while the odometer stayed at %d mm; CAN ignored until it changes, %d mm of earlier clamping restored",
					can.fineSinceChange, c, restored)).
				withValue(int64(restored)))
			return false
		}
		if can.fineSinceChange > 0 {
			add(canDiag(CodeCANUnchanged, LevelInfo, "odometer unchanged while delta sources report movement; consistent with 100 m quantisation").
				withValue(int64(can.fineSinceChange)))
		}
	} else {
		can.last, can.lastChangeTime, can.lastTimeKnown = c, t, true
		can.fineSinceChange, can.clampedSinceChange = 0, 0
	}

	// The true distance since the baseline lies strictly within
	// (c-base-R, c-base+R) because both readings are floor-quantised.
	res := cfg.CANResolution
	lo := can.baseTrip + c - can.base - (res - 1)
	hi := can.baseTrip + c - can.base + (res - 1)
	target := e.trip
	if !fineEvidence {
		// No delta source this epoch: move to the expected value given CAN alone.
		target = max(target, can.baseTrip+c-can.base)
	}
	target = min(max(target, lo), hi)
	if corr := target - e.trip; corr != 0 {
		if corr < 0 {
			can.clampedSinceChange -= corr
		}
		add(canDiag(CodeCANReconciled, LevelInfo,
			"trip estimate moved into the interval implied by the odometer; correction may cover several earlier epochs").withValue(int64(corr)))
	}
	e.trip = target
	// The truth lies in (lo, hi): the farther end bounds the error.
	e.uncBase, e.uncVar, e.uncSys = int64(max(target-lo, hi-target)), 0, 0

	if pulseInEpoch {
		e.calibrateFromCAN(c, add)
	}
	e.checkHealth(c, add)
	return true
}

func (e *estimator) canBaseline(c telemetry.Millimetres, t time.Time, pulseInEpoch bool) {
	e.can = canState{valid: true, base: c, baseTrip: e.trip, last: c, lastChangeTime: t, lastTimeKnown: true}
	e.cal.resetSpan()
	if pulseInEpoch {
		e.cal.spanActive, e.cal.spanStart = true, c
	}
	e.resetHealth()
	e.healthActive, e.healthCheckpoint = true, c
}

// calibrateFromCAN refines the pulse calibration against the odometer.
// The span always starts at a CAN reading taken in the same epoch as a
// pulse reading, so both cover the same interval; its uncertainty is
// dominated by the two CAN quantisation errors, 2R / span.
func (e *estimator) calibrateFromCAN(c telemetry.Millimetres, add func(Diagnostic)) {
	cal := &e.cal
	if !cal.spanActive {
		cal.spanActive, cal.spanStart, cal.spanPulse = true, c, 0
		return
	}
	span := c - cal.spanStart
	if span < e.cfg.CalibrationMinSpan || cal.spanPulse <= 0 {
		return
	}
	est := mulDiv(int64(span), ppm, cal.spanPulse)
	unc := mulDiv(2*int64(e.cfg.CANResolution), ppm, int64(span))
	switch {
	case cal.ref != calCAN,
		absDiff(est, cal.ppm) > unc+cal.uncPPM, // the calibration has really changed
		unc < cal.uncPPM:                       // a longer span refines it
		e.setCalibration(est, unc, calCAN, add)
	}
}

func (e *estimator) pairWithGPS(gps, pulse *contrib, add func(Diagnostic)) {
	aligned := gps.spanKnown && pulse.spanKnown && gps.spanStart.Equal(pulse.spanStart)
	if !aligned || gps.quality != telemetry.QualityGood || pulse.quality != telemetry.QualityGood ||
		gps.suppressed || gps.raw <= 0 || pulse.raw <= 0 {
		return
	}
	e.cal.pairGPS += int64(gps.raw)
	e.cal.pairPulse += int64(pulse.raw)
	if e.cal.pairGPS < int64(e.cfg.CalibrationMinSpan) {
		return
	}
	est := mulDiv(e.cal.pairGPS, ppm, e.cal.pairPulse)
	if e.cal.ref == calNominal || absDiff(est, e.cal.ppm) >= e.cfg.CalibrationReportPPM {
		e.setCalibration(est, e.cfg.GPSCalibrationUncPPM, calGPS, add)
	} else {
		e.cal.ppm = est
	}
}

func (e *estimator) setCalibration(est, unc int64, ref string, add func(Diagnostic)) {
	cfg, cal := e.cfg, &e.cal
	if est < cfg.CalibrationMinPPM || est > cfg.CalibrationMaxPPM {
		add(diag(CodeCalibrationReject, LevelWarning,
			fmt.Sprintf("%s reference implies pulse factor %d ppm, outside [%d, %d]; calibration unchanged", ref, est, cfg.CalibrationMinPPM, cfg.CalibrationMaxPPM)).
			withSource(telemetry.SourcePulse).withValue(est))
		return
	}
	if cal.ref != ref || absDiff(est, cal.ppm) >= cfg.CalibrationReportPPM {
		add(diag(CodeCalibrationUpdated, LevelInfo,
			fmt.Sprintf("pulse factor %d -> %d ppm (reference %s, uncertainty %d ppm)", cal.ppm, est, ref, unc)).
			withSource(telemetry.SourcePulse).withValue(est))
	}
	cal.ppm, cal.uncPPM, cal.ref = est, unc, ref
}

func (e *estimator) resetHealth() {
	e.healthActive, e.healthEpochs = false, 0
	for i := range e.src {
		e.src[i].healthSum, e.src[i].healthEpochs = 0, 0
	}
}

// checkHealth compares each delta source with the odometer over spans of
// at least HealthMinSpan. A source far outside the expected agreement is
// marked suspect and excluded from fusion until a later span agrees again.
func (e *estimator) checkHealth(c telemetry.Millimetres, add func(Diagnostic)) {
	cfg := e.cfg
	if !e.healthActive {
		e.resetHealth() // sums since the reset predate this checkpoint
		e.healthActive, e.healthCheckpoint = true, c
		return
	}
	span := c - e.healthCheckpoint
	if span < cfg.HealthMinSpan {
		return
	}
	tol := 2*cfg.CANResolution + telemetry.Millimetres(mulDiv(int64(span), cfg.HealthTolerancePPM, ppm))
	for _, src := range []telemetry.Source{telemetry.SourceGPS, telemetry.SourcePulse} {
		st := &e.src[src]
		if st.healthEpochs == 0 {
			continue
		}
		if !cfg.model(src).FoldsGaps && st.healthEpochs*ppm < e.healthEpochs*cfg.HealthMinCoveragePPM {
			continue // too many lost intervals to compare a non-folding source
		}
		off := st.healthSum - span
		switch bad := absDiff(int64(st.healthSum), int64(span)) > int64(tol); {
		case bad && !st.suspect:
			st.suspect = true
			add(diag(CodeSourceSuspect, LevelWarning,
				fmt.Sprintf("reported %d mm over a %d mm odometer span; excluded from fusion", st.healthSum, span)).
				withSource(src).withValue(int64(off)))
		case !bad && st.suspect:
			st.suspect = false
			add(diag(CodeSourceRecovered, LevelInfo, "agrees with the odometer again; re-included").withSource(src).withValue(int64(off)))
		}
	}
	e.resetHealth()
	e.healthActive, e.healthCheckpoint = true, c
}
