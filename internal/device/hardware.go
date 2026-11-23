package device

import "github.com/flavioanjo/fleetcore/internal/reliable"

// Hardware is the device's view of the (simulated) vehicle: sensors it can
// read and actuators it can drive. The device logic never assumes an
// actuation worked; it reads the corresponding sensor back.
//
// This interface is implemented only by simulators (internal/simnet). No
// implementation drives real relays, ignition circuits, immobilizers or
// locks: production vehicle actuation requires independent safety
// engineering, hardware validation and authorization that this project
// does not provide.
type Hardware interface {
	// Vehicle returns the vehicle's motion state; ok is false when the
	// sensors cannot be read (safety interlocks then refuse to actuate).
	Vehicle() (v VehicleState, ok bool)
	// Fix returns the current GNSS fix; ok is false without a valid fix.
	Fix() (reliable.PositionFix, bool)

	// SetImmobilizer drives the immobilizer actuator.
	SetImmobilizer(engage bool) error
	// ImmobilizerState reads the immobilizer position sensor.
	ImmobilizerState() reliable.BlockStateValue

	// ReleaseCargoLatch drives the cargo latch release actuator.
	ReleaseCargoLatch(compartment uint8) error
	// CargoLatch reads the latch position sensor.
	CargoLatch(compartment uint8) reliable.LatchState
	// CargoDoor reads the door sensor (independent of the latch).
	CargoDoor(compartment uint8) reliable.DoorState
}

// VehicleState is what the safety interlocks evaluate.
type VehicleState struct {
	SpeedKmhX10 uint16
	IgnitionOn  bool
}

// Stationary reports whether the vehicle is not moving.
func (v VehicleState) Stationary() bool { return v.SpeedKmhX10 == 0 }
