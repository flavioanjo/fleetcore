package simulator

import (
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// Motion model parameters. Speeds are mm/s, accelerations mm/s².
const (
	stepDuration  = 100 * time.Millisecond
	stepsPerSec   = int64(time.Second / stepDuration)
	maxAccel      = 2_500
	maxDecel      = 3_500
	cruiseJitter  = 150 // max random speed change per step while cruising
	nominalPulse  = 400 // mm of travel per pulse assumed by the device
	canResolution = 100 * telemetry.Metre
	maxBacklog    = 300 * time.Second
)

type segment struct {
	perMille       int64 // probability of picking this segment
	minSpeed       int64 // mm/s
	maxSpeed       int64
	minDur, maxDur int64 // seconds
}

// Segment table: stop, urban, suburban, highway.
var segments = []segment{
	{200, 0, 0, 10, 120},
	{350, 8_000, 14_000, 30, 300},
	{250, 14_000, 22_000, 60, 600},
	{200, 25_000, 33_000, 120, 900},
}

type vehicle struct {
	id   telemetry.DeviceID
	boot telemetry.BootID
	idx  int

	motion, gps, can, pulse, latency *rand.Rand

	clockOffset time.Duration // per-vehicle phase within the interval

	speed      int64 // mm/s
	target     int64 // mm/s
	stepsLeft  int64 // steps until the next segment
	stepRemMM  int64 // sub-millimetre remainder (in mm*steps) of integration
	odometer   telemetry.Millimetres
	lastDelta  telemetry.Millimetres // true distance in the last interval
	pulseSpace int64                 // true mm per pulse (calibration error)
	pulseCarry int64                 // true mm travelled but not yet reported as pulses
	seq        telemetry.Sequence
}

func newVehicle(cfg Config, idx int) *vehicle {
	setup := newRand(cfg.Seed, idx, streamSetup)
	v := &vehicle{
		id:      telemetry.DeviceID(fmt.Sprintf("%s%05d", DeviceIDPrefix, idx+1)),
		boot:    telemetry.BootID(setup.Uint32N(math.MaxUint32) + 1), // never zero
		idx:     idx,
		motion:  newRand(cfg.Seed, idx, streamMotion),
		gps:     newRand(cfg.Seed, idx, streamGPS),
		can:     newRand(cfg.Seed, idx, streamCAN),
		pulse:   newRand(cfg.Seed, idx, streamPulse),
		latency: newRand(cfg.Seed, idx, streamLatency),
		// Odometer between 5,000 and 250,000 km.
		odometer: telemetry.Millimetres(randRange(setup, 5_000_000, 250_000_000)) * telemetry.Metre,
		// True pulse spacing within ±3% of nominal.
		pulseSpace:  randRange(setup, nominalPulse*970/1000, nominalPulse*1030/1000),
		clockOffset: time.Duration(setup.Int64N(int64(cfg.Interval/time.Millisecond))) * time.Millisecond,
	}
	return v
}

// advance simulates one interval of motion and records the true distance.
func (v *vehicle) advance(interval time.Duration) {
	var travelled int64
	for i := int64(0); i < int64(interval/stepDuration); i++ {
		if v.stepsLeft == 0 {
			v.pickSegment()
		}
		v.stepsLeft--
		v.updateSpeed()
		// distance = speed * 0.1 s, keeping the remainder exact.
		v.stepRemMM += v.speed
		travelled += v.stepRemMM / stepsPerSec
		v.stepRemMM %= stepsPerSec
	}
	v.lastDelta = telemetry.Millimetres(travelled)
	v.odometer += v.lastDelta
	v.pulseCarry += travelled
}

func (v *vehicle) pickSegment() {
	roll := v.motion.Int64N(1000)
	seg := segments[len(segments)-1]
	for _, s := range segments {
		if roll < s.perMille {
			seg = s
			break
		}
		roll -= s.perMille
	}
	v.target = randRange(v.motion, seg.minSpeed, seg.maxSpeed)
	v.stepsLeft = randRange(v.motion, seg.minDur, seg.maxDur) * stepsPerSec
}

func (v *vehicle) updateSpeed() {
	up, down := int64(maxAccel/stepsPerSec), int64(maxDecel/stepsPerSec)
	switch diff := v.target - v.speed; {
	case diff > up:
		v.speed += up
	case diff < -down:
		v.speed -= down
	case v.target == 0:
		v.speed = 0
	default:
		v.speed = v.target + randRange(v.motion, -cruiseJitter, cruiseJitter)
		if v.speed < 0 {
			v.speed = 0
		}
	}
}

func (v *vehicle) sample(src telemetry.Source, deviceTime time.Time) telemetry.Sample {
	v.seq++
	s := telemetry.Sample{
		DeviceID:   v.id,
		EventID:    telemetry.EventID(uint64(v.idx+1)<<32 | uint64(v.seq)),
		BootID:     v.boot,
		Sequence:   v.seq,
		DeviceTime: deviceTime,
		IngestedAt: telemetry.Some(deviceTime.Add(v.ingestLatency())),
		Source:     src,
	}
	var d telemetry.Optional[telemetry.Millimetres]
	switch src {
	case telemetry.SourceGPS:
		s.Quality, d = v.readGPS()
	case telemetry.SourceCAN:
		s.Quality, d = v.readCAN()
	case telemetry.SourcePulse:
		s.Quality, d = v.readPulse()
	}
	if val, ok := d.Get(); ok {
		s.Distance = telemetry.Some(telemetry.Distance{Kind: src.DistanceKind(), Value: val})
	}
	return s
}

func (v *vehicle) ingestLatency() time.Duration {
	ms := randRange(v.latency, 80, 2_500)
	if chance(v.latency, 10) { // 1%: delivered from a backlog
		ms += randRange(v.latency, 30_000, int64(maxBacklog/time.Millisecond)-2_500)
	}
	return time.Duration(ms) * time.Millisecond
}

func (v *vehicle) readGPS() (telemetry.Quality, telemetry.Optional[telemetry.Millimetres]) {
	r := v.gps
	switch roll := r.Int64N(1000); {
	case roll < 30: // 3% no fix
		return telemetry.QualityUnavailable, telemetry.None[telemetry.Millimetres]()
	case roll < 100: // 7% degraded fix: ±5% error, larger drift
		return telemetry.QualityDegraded, telemetry.Some(gpsDistance(r, int64(v.lastDelta), 50, 8_000))
	default: // good fix: ±1% error
		return telemetry.QualityGood, telemetry.Some(gpsDistance(r, int64(v.lastDelta), 10, 1_500))
	}
}

// gpsDistance applies a proportional error of up to ±errPerMille and, when the
// vehicle is stationary, a positional drift of up to maxDrift mm.
func gpsDistance(r *rand.Rand, trueMM, errPerMille, maxDrift int64) telemetry.Millimetres {
	if trueMM == 0 {
		return telemetry.Millimetres(r.Int64N(maxDrift + 1))
	}
	d := trueMM + trueMM*randRange(r, -errPerMille, errPerMille)/1000
	if d < 0 {
		d = 0
	}
	return telemetry.Millimetres(min(d, int64(telemetry.MaxDeltaDistance)))
}

func (v *vehicle) readCAN() (telemetry.Quality, telemetry.Optional[telemetry.Millimetres]) {
	if chance(v.can, 5) { // 0.5% bus frame missing
		return telemetry.QualityUnavailable, telemetry.None[telemetry.Millimetres]()
	}
	return telemetry.QualityGood, telemetry.Some(v.odometer / canResolution * canResolution)
}

func (v *vehicle) readPulse() (telemetry.Quality, telemetry.Optional[telemetry.Millimetres]) {
	r := v.pulse
	pulses := v.pulseCarry / v.pulseSpace
	v.pulseCarry %= v.pulseSpace
	switch roll := r.Int64N(1000); {
	case roll < 5: // 0.5% counter read failure; pulses accumulate for next read
		v.pulseCarry += pulses * v.pulseSpace
		return telemetry.QualityUnavailable, telemetry.None[telemetry.Millimetres]()
	case roll < 15: // 1% electrical noise: a few spurious or missed pulses
		pulses = max(0, pulses+randRange(r, -2, 2))
		return telemetry.QualityDegraded, telemetry.Some(telemetry.Millimetres(pulses * nominalPulse))
	default:
		return telemetry.QualityGood, telemetry.Some(telemetry.Millimetres(pulses * nominalPulse))
	}
}
