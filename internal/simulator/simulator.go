// Package simulator generates deterministic, synthetic vehicle telemetry.
//
// Every vehicle drives a simple speed model (stops, urban, suburban and
// highway segments with bounded acceleration) and carries three simulated
// sensors that observe the same true motion with different error models:
//
//   - GPS: distance deltas with proportional noise, stationary drift,
//     occasional degraded fixes and dropouts.
//   - CAN: a cumulative odometer quantised to 100 m, with rare dropouts.
//   - Pulse counter: distance deltas derived from whole wheel pulses using a
//     nominal pulse spacing, while the true spacing differs per vehicle
//     (calibration error).
//
// Determinism: output depends only on Config. All arithmetic is integer, so
// results are identical across platforms, and every vehicle and sensor has
// its own random stream derived from (Seed, vehicle index, stream), so adding
// vehicles or disabling a source does not change the other streams.
//
// All identifiers and data are fictional.
package simulator

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// Config limits.
const (
	MaxVehicles    = 10_000
	MaxSamples     = 1_000_000
	MinInterval    = 100 * time.Millisecond
	MaxInterval    = time.Hour
	IntervalStep   = 100 * time.Millisecond
	DeviceIDPrefix = "SIM-"
)

// Config controls a simulation run.
type Config struct {
	// Seed selects the random streams. Equal configs produce equal output.
	Seed uint64
	// Vehicles is the number of simulated vehicles, 1..MaxVehicles.
	Vehicles int
	// Samples is the number of sampling rounds per vehicle, 1..MaxSamples.
	// Each round emits one sample per enabled source.
	Samples int
	// Interval is the time between rounds: a multiple of 100 ms within
	// [MinInterval, MaxInterval].
	Interval time.Duration
	// Start is the simulated wall-clock time of round zero (UTC).
	Start time.Time
	// Sources are the enabled measurement sources, emitted in this order.
	// Must be non-empty and free of duplicates.
	Sources []telemetry.Source
}

// DefaultConfig returns a small, fully specified configuration.
func DefaultConfig() Config {
	return Config{
		Seed:     1,
		Vehicles: 3,
		Samples:  5,
		Interval: time.Second,
		Start:    time.Date(2026, time.January, 1, 8, 0, 0, 0, time.UTC),
		Sources:  telemetry.Sources(),
	}
}

// Validate reports the first configuration error, if any.
func (c Config) Validate() error {
	switch {
	case c.Vehicles < 1 || c.Vehicles > MaxVehicles:
		return fmt.Errorf("simulator: vehicles %d outside [1, %d]", c.Vehicles, MaxVehicles)
	case c.Samples < 1 || c.Samples > MaxSamples:
		return fmt.Errorf("simulator: samples %d outside [1, %d]", c.Samples, MaxSamples)
	case c.Interval < MinInterval || c.Interval > MaxInterval:
		return fmt.Errorf("simulator: interval %s outside [%s, %s]", c.Interval, MinInterval, MaxInterval)
	case c.Interval%IntervalStep != 0:
		return fmt.Errorf("simulator: interval %s is not a multiple of %s", c.Interval, IntervalStep)
	case c.Start.Location() != time.UTC || c.Start.Nanosecond()%int(time.Millisecond) != 0:
		return errors.New("simulator: start must be UTC with millisecond resolution")
	case c.Start.Before(telemetry.MinTimestamp):
		return fmt.Errorf("simulator: start before %s", telemetry.MinTimestamp.Format(time.RFC3339))
	case len(c.Sources) == 0:
		return errors.New("simulator: no sources enabled")
	}
	// The last sample (plus latency) must stay inside the valid time range.
	end := c.Start.Add(time.Duration(c.Samples)*c.Interval + c.Interval + maxBacklog)
	if !end.Before(telemetry.MaxTimestamp) {
		return fmt.Errorf("simulator: run would end after %s", telemetry.MaxTimestamp.Format(time.RFC3339))
	}
	seen := map[telemetry.Source]bool{}
	for _, s := range c.Sources {
		if !s.Valid() {
			return fmt.Errorf("simulator: invalid source %s", s)
		}
		if seen[s] {
			return fmt.Errorf("simulator: duplicate source %s", s)
		}
		seen[s] = true
	}
	return nil
}

// EmitFunc receives each generated sample. Returning an error stops the run.
type EmitFunc func(telemetry.Sample) error

// Run generates the telemetry described by cfg and passes each sample to
// emit, in order: round by round, vehicle by vehicle, source by source.
// Every emitted sample satisfies telemetry.Sample.Validate.
func Run(cfg Config, emit EmitFunc) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	vehicles := make([]*vehicle, cfg.Vehicles)
	for i := range vehicles {
		vehicles[i] = newVehicle(cfg, i)
	}
	for round := 0; round < cfg.Samples; round++ {
		for _, v := range vehicles {
			// A round's samples are taken at the end of its interval and
			// describe the motion during that interval.
			v.advance(cfg.Interval)
			deviceTime := cfg.Start.Add(v.clockOffset + time.Duration(round+1)*cfg.Interval)
			for _, src := range cfg.Sources {
				s := v.sample(src, deviceTime)
				if err := s.Validate(); err != nil {
					// A bug in the simulator, not a user error.
					return fmt.Errorf("simulator: generated invalid sample: %w", err)
				}
				if err := emit(s); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Random stream identifiers (mixed into the PCG stream selector).
const (
	streamSetup uint64 = iota + 1
	streamMotion
	streamGPS
	streamCAN
	streamPulse
	streamLatency
)

// newRand returns the random stream for (seed, vehicle, stream). The inputs
// are passed through splitmix64 so that nearby inputs (vehicle 1 vs 2) give
// unrelated PCG states; seeding PCG with raw, nearly equal words produces
// visibly correlated first outputs.
func newRand(seed uint64, vehicle int, stream uint64) *rand.Rand {
	a := splitmix64(seed ^ splitmix64(uint64(vehicle)))
	b := splitmix64(a ^ splitmix64(stream))
	return rand.New(rand.NewPCG(a, b))
}

// splitmix64 is the finaliser of the SplitMix64 generator, a bijective mix.
func splitmix64(x uint64) uint64 {
	x += 0x9E3779B97F4A7C15
	x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9
	x = (x ^ (x >> 27)) * 0x94D049BB133111EB
	return x ^ (x >> 31)
}

// randRange returns a uniform integer in [lo, hi].
func randRange(r *rand.Rand, lo, hi int64) int64 {
	return lo + r.Int64N(hi-lo+1)
}

// chance reports true with probability perMille/1000.
func chance(r *rand.Rand, perMille int64) bool {
	return r.Int64N(1000) < perMille
}
