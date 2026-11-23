// Package simhw simulates a vehicle's sensors and actuators for the
// device simulator (internal/device). It is the simulation's ground truth:
// tests read the physical state and the number of physical operations
// from it, never from the device's own records, so device decision logic
// and simulation truth stay separate.
//
// Nothing here drives real hardware. The immobilizer and cargo latch are
// state variables with configurable faults.
package simhw

import (
	"errors"
	"sync"
	"time"

	"github.com/flavioanjo/fleetcore/internal/device"
	"github.com/flavioanjo/fleetcore/internal/reliable"
)

// Vehicle is a simulated vehicle. It is safe for concurrent use. A Vehicle
// outlives device process restarts (it is the hardware).
type Vehicle struct {
	mu sync.Mutex

	speedKmhX10 uint16
	ignition    bool
	sensorsOK   bool

	fix    reliable.PositionFix
	hasFix bool
	now    func() time.Time

	blocked           bool
	immobilizerFault  bool // actuator refuses to move
	immobilizerSensor bool // false: position sensor unreadable
	immobilizerDrives int
	immobilizerMoves  int

	latchReleased   map[uint8]bool
	doorOpen        map[uint8]bool
	latchFault      bool
	doorOpensOnLift bool // someone opens the door when the latch releases
	latchReleases   int
}

// New returns a parked vehicle (stationary, ignition off, unblocked,
// cargo closed and latched, sensors working, no GNSS fix). now is the
// simulation clock used for fix times.
func New(now func() time.Time) *Vehicle {
	return &Vehicle{sensorsOK: true, immobilizerSensor: true, now: now, doorOpensOnLift: true,
		latchReleased: map[uint8]bool{}, doorOpen: map[uint8]bool{}}
}

// --- device.Hardware ---

// Vehicle implements device.Hardware: speed and ignition; ok is false
// when the sensors fail.
func (v *Vehicle) Vehicle() (device.VehicleState, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return device.VehicleState{SpeedKmhX10: v.speedKmhX10, IgnitionOn: v.ignition}, v.sensorsOK
}

var _ device.Hardware = (*Vehicle)(nil)

// Fix implements device.Hardware.
func (v *Vehicle) Fix() (reliable.PositionFix, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if !v.hasFix {
		return reliable.PositionFix{}, false
	}
	f := v.fix
	f.FixTime = time.UnixMilli(v.now().UnixMilli()).UTC()
	f.SpeedKmhX10 = v.speedKmhX10
	return f, true
}

// SetImmobilizer implements device.Hardware.
func (v *Vehicle) SetImmobilizer(engage bool) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.immobilizerDrives++
	if v.immobilizerFault {
		return errors.New("simhw: immobilizer actuator fault")
	}
	if v.blocked != engage {
		v.immobilizerMoves++
	}
	v.blocked = engage
	return nil
}

// ImmobilizerState implements device.Hardware.
func (v *Vehicle) ImmobilizerState() reliable.BlockStateValue {
	v.mu.Lock()
	defer v.mu.Unlock()
	switch {
	case !v.immobilizerSensor:
		return reliable.BlockUnknown
	case v.blocked:
		return reliable.Blocked
	}
	return reliable.Unblocked
}

// ReleaseCargoLatch implements device.Hardware.
func (v *Vehicle) ReleaseCargoLatch(c uint8) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.latchReleases++
	if v.latchFault {
		return errors.New("simhw: latch actuator fault")
	}
	v.latchReleased[c] = true
	if v.doorOpensOnLift {
		v.doorOpen[c] = true
	}
	return nil
}

// CargoLatch implements device.Hardware.
func (v *Vehicle) CargoLatch(c uint8) reliable.LatchState {
	v.mu.Lock()
	defer v.mu.Unlock()
	if !v.sensorsOK {
		return reliable.LatchUnknown
	}
	if v.latchReleased[c] {
		return reliable.LatchReleased
	}
	return reliable.LatchLocked
}

// CargoDoor implements device.Hardware.
func (v *Vehicle) CargoDoor(c uint8) reliable.DoorState {
	v.mu.Lock()
	defer v.mu.Unlock()
	if !v.sensorsOK {
		return reliable.DoorUnknown
	}
	if v.doorOpen[c] {
		return reliable.DoorOpen
	}
	return reliable.DoorClosed
}

// --- simulation controls ---

// Drive sets the vehicle in motion (or stops it with 0).
func (v *Vehicle) Drive(speedKmhX10 uint16, ignition bool) {
	v.mu.Lock()
	v.speedKmhX10, v.ignition = speedKmhX10, ignition
	v.mu.Unlock()
}

// SetFix sets the GNSS fix (ok=false: no fix). FixTime and speed are
// filled from the clock and the motion state when read.
func (v *Vehicle) SetFix(f reliable.PositionFix, ok bool) {
	v.mu.Lock()
	v.fix, v.hasFix = f, ok
	v.mu.Unlock()
}

// Faults configures failures.
type Faults struct {
	SensorsUnavailable   bool // motion, latch and door sensors unreadable
	ImmobilizerFault     bool // immobilizer actuator does not move
	ImmobilizerSensorOff bool // immobilizer position sensor unreadable
	LatchFault           bool // latch actuator does not move
	DoorStaysClosed      bool // the latch releases but nobody opens the door
}

// SetFaults applies a fault configuration.
func (v *Vehicle) SetFaults(f Faults) {
	v.mu.Lock()
	v.sensorsOK = !f.SensorsUnavailable
	v.immobilizerFault, v.immobilizerSensor = f.ImmobilizerFault, !f.ImmobilizerSensorOff
	v.latchFault, v.doorOpensOnLift = f.LatchFault, !f.DoorStaysClosed
	v.mu.Unlock()
}

// CloseCargo closes and latches compartment c (a person did it).
func (v *Vehicle) CloseCargo(c uint8) {
	v.mu.Lock()
	v.doorOpen[c], v.latchReleased[c] = false, false
	v.mu.Unlock()
}

// Truth is the physical state and operation counts.
type Truth struct {
	Blocked           bool
	ImmobilizerDrives int // actuator commands received (including no-op and faulted)
	ImmobilizerMoves  int // actual state changes
	LatchReleases     int
	CargoOpen         bool
}

// Truth returns the ground truth.
func (v *Vehicle) Truth() Truth {
	v.mu.Lock()
	defer v.mu.Unlock()
	return Truth{Blocked: v.blocked, ImmobilizerDrives: v.immobilizerDrives, ImmobilizerMoves: v.immobilizerMoves,
		LatchReleases: v.latchReleases, CargoOpen: v.doorOpen[1]}
}
