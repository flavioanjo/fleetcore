package fusion

import (
	"errors"
	"fmt"
	"time"

	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// SourceModel describes the error model the engine assumes for a delta
// source. All error figures are 1-sigma.
type SourceModel struct {
	// GoodRelPPM and DegradedRelPPM are random errors proportional to the
	// reported distance, in parts per million.
	GoodRelPPM, DegradedRelPPM int64
	// GoodAbs and DegradedAbs are absolute random error floors.
	GoodAbs, DegradedAbs telemetry.Millimetres
	// SystematicPPM is the bias that does not average out over many
	// readings (used for the uncertainty indicator only).
	SystematicPPM int64
	// FoldsGaps reports whether distance travelled during an unavailable
	// reading is included in the source's next available delta (true for a
	// pulse counter, which keeps counting), or lost (false for GPS, which
	// cannot measure without a fix).
	FoldsGaps bool
}

// Config holds every tunable of the engine. Use DefaultConfig and override
// fields as needed; New validates the result.
type Config struct {
	// Ordering and memory bounds.
	ReorderWindow time.Duration // device-time lateness tolerated before a sequence gap is declared
	// IdleTimeout is a processing-time limit, applied only by Engine.Tick:
	// a device with buffered events or an open epoch that receives nothing
	// for this long is drained (gaps declared, epoch closed). ReorderWindow
	// alone cannot do this, because device time only advances when the
	// device sends something.
	IdleTimeout           time.Duration
	MaxBufferedPerSession int // reorder buffer capacity per device session
	MaxTrackedGaps        int // declared-missing sequence ranges remembered per session
	RecentEvents          int // processed-event fingerprints remembered per session (duplicate detection)
	MaxClosedSessions     int // previous BootIDs remembered per device
	MaxDevices            int // devices tracked by one engine

	// Session handling.
	SessionSkewTolerance time.Duration // an unknown BootID older than the current session by more than this is treated as stale
	AdoptAfter           int           // consecutive events from such an "older" BootID after which it is adopted anyway

	// Plausibility.
	MaxSpeed          telemetry.Millimetres // maximum plausible speed, mm per second
	PlausibilitySlack telemetry.Millimetres // absolute allowance added to speed-based limits

	// CAN odometer.
	CANResolution     telemetry.Millimetres // quantisation step of the CAN odometer
	CANStaleTolerance telemetry.Millimetres // fine-source travel beyond one CAN step, with no CAN change, that marks CAN as stale

	// Delta source error models.
	GPS, Pulse SourceModel

	// GPS stationary handling.
	GPSDeadbandGood, GPSDeadbandDegraded telemetry.Millimetres // GPS-only deltas below these are treated as positional noise
	GPSStationaryCorroborated            telemetry.Millimetres // max GPS delta discarded when a good pulse reading reports zero

	// Disagreement detection between delta sources.
	DisagreementSigma int64                 // threshold in combined standard deviations
	DisagreementFloor telemetry.Millimetres // never report differences smaller than this

	// Gap interpolation: when no source covered some time span (GPS outage,
	// lost events) and the next contributing delta does not cover it either,
	// the travel is estimated from the speeds before and after, for spans of
	// at most MaxInterpolation.
	MaxInterpolation time.Duration

	// Pulse calibration.
	CalibrationMinSpan    telemetry.Millimetres // reference distance required before calibrating
	CalibrationMinPPM     int64                 // plausible calibration factor range
	CalibrationMaxPPM     int64
	CalibrationReportPPM  int64 // report calibration changes at least this large
	PulseInitialUncPPM    int64 // calibration uncertainty before any calibration
	GPSCalibrationUncPPM  int64 // assumed systematic GPS error when GPS is the reference
	HealthMinSpan         telemetry.Millimetres
	HealthTolerancePPM    int64 // relative disagreement with CAN that marks a source suspect
	HealthMinCoveragePPM  int64 // minimum share of epochs a non-folding source must cover to be health-checked
	DelayedDeliveryNotice time.Duration
	ClockAheadNotice      time.Duration
}

// DefaultConfig returns conservative defaults. They are generic vehicle and
// sensor figures (consumer GNSS, wheel pulse counters, 100 m CAN odometers),
// not values fitted to the FleetCore simulator.
func DefaultConfig() Config {
	return Config{
		ReorderWindow:         30 * time.Second,
		IdleTimeout:           time.Minute,
		MaxBufferedPerSession: 256,
		MaxTrackedGaps:        64,
		RecentEvents:          256,
		MaxClosedSessions:     16,
		MaxDevices:            100_000,

		SessionSkewTolerance: 5 * time.Minute,
		AdoptAfter:           32,

		MaxSpeed:          70 * telemetry.Metre, // 252 km/h
		PlausibilitySlack: 50 * telemetry.Metre,

		CANResolution:     100 * telemetry.Metre,
		CANStaleTolerance: 200 * telemetry.Metre,

		GPS: SourceModel{
			GoodRelPPM: 20_000, DegradedRelPPM: 100_000,
			GoodAbs: 2 * telemetry.Metre, DegradedAbs: 10 * telemetry.Metre,
			SystematicPPM: 10_000,
			FoldsGaps:     false,
		},
		Pulse: SourceModel{
			GoodRelPPM: 5_000, DegradedRelPPM: 20_000,
			GoodAbs: 1 * telemetry.Metre, DegradedAbs: 5 * telemetry.Metre,
			SystematicPPM: 0, // calibration uncertainty is tracked separately
			FoldsGaps:     true,
		},

		GPSDeadbandGood:           2 * telemetry.Metre,
		GPSDeadbandDegraded:       10 * telemetry.Metre,
		GPSStationaryCorroborated: 25 * telemetry.Metre,

		DisagreementSigma: 4,
		DisagreementFloor: 20 * telemetry.Metre,

		MaxInterpolation: time.Minute,

		CalibrationMinSpan:    20 * telemetry.Kilometre,
		CalibrationMinPPM:     900_000,
		CalibrationMaxPPM:     1_100_000,
		CalibrationReportPPM:  500,
		PulseInitialUncPPM:    50_000,
		GPSCalibrationUncPPM:  10_000,
		HealthMinSpan:         2 * telemetry.Kilometre,
		HealthTolerancePPM:    150_000,
		HealthMinCoveragePPM:  950_000,
		DelayedDeliveryNotice: time.Minute,
		ClockAheadNotice:      2 * time.Second,
	}
}

// Validate reports the first invalid setting.
func (c Config) Validate() error {
	pos := []struct {
		name string
		v    int64
	}{
		{"ReorderWindow", int64(c.ReorderWindow)},
		{"IdleTimeout", int64(c.IdleTimeout)},
		{"MaxBufferedPerSession", int64(c.MaxBufferedPerSession)},
		{"MaxTrackedGaps", int64(c.MaxTrackedGaps)},
		{"RecentEvents", int64(c.RecentEvents)},
		{"MaxClosedSessions", int64(c.MaxClosedSessions)},
		{"MaxDevices", int64(c.MaxDevices)},
		{"SessionSkewTolerance", int64(c.SessionSkewTolerance)},
		{"AdoptAfter", int64(c.AdoptAfter)},
		{"MaxSpeed", int64(c.MaxSpeed)},
		{"CANResolution", int64(c.CANResolution)},
		{"CANStaleTolerance", int64(c.CANStaleTolerance)},
		{"DisagreementSigma", c.DisagreementSigma},
		{"CalibrationMinSpan", int64(c.CalibrationMinSpan)},
		{"CalibrationMinPPM", c.CalibrationMinPPM},
		{"PulseInitialUncPPM", c.PulseInitialUncPPM},
		{"GPSCalibrationUncPPM", c.GPSCalibrationUncPPM},
		{"HealthMinSpan", int64(c.HealthMinSpan)},
		{"HealthTolerancePPM", c.HealthTolerancePPM},
	}
	for _, p := range pos {
		if p.v <= 0 {
			return fmt.Errorf("fusion: config %s must be positive", p.name)
		}
	}
	nonNeg := []struct {
		name string
		v    int64
	}{
		{"PlausibilitySlack", int64(c.PlausibilitySlack)},
		{"GPSDeadbandGood", int64(c.GPSDeadbandGood)},
		{"GPSDeadbandDegraded", int64(c.GPSDeadbandDegraded)},
		{"GPSStationaryCorroborated", int64(c.GPSStationaryCorroborated)},
		{"DisagreementFloor", int64(c.DisagreementFloor)},
		{"CalibrationReportPPM", c.CalibrationReportPPM},
		{"HealthMinCoveragePPM", c.HealthMinCoveragePPM},
		{"MaxInterpolation", int64(c.MaxInterpolation)},
		{"DelayedDeliveryNotice", int64(c.DelayedDeliveryNotice)},
		{"ClockAheadNotice", int64(c.ClockAheadNotice)},
	}
	for _, p := range nonNeg {
		if p.v < 0 {
			return fmt.Errorf("fusion: config %s must not be negative", p.name)
		}
	}
	if c.CalibrationMaxPPM <= c.CalibrationMinPPM {
		return errors.New("fusion: config CalibrationMaxPPM must exceed CalibrationMinPPM")
	}
	if c.MaxSpeed > 1000*telemetry.Metre {
		return errors.New("fusion: config MaxSpeed above 1 km/s is not supported")
	}
	for name, m := range map[string]SourceModel{"GPS": c.GPS, "Pulse": c.Pulse} {
		if m.GoodRelPPM < 0 || m.DegradedRelPPM < 0 || m.GoodAbs <= 0 || m.DegradedAbs <= 0 || m.SystematicPPM < 0 {
			return fmt.Errorf("fusion: config %s model: errors must be non-negative and absolute floors positive", name)
		}
	}
	return nil
}

func (c *Config) model(src telemetry.Source) SourceModel {
	if src == telemetry.SourcePulse {
		return c.Pulse
	}
	return c.GPS
}
