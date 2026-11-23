package reliable

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/flavioanjo/fleetcore/internal/protocol"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// EventType identifies the body schema of an uploaded event.
type EventType uint8

// Event types.
const (
	EventTelemetry         EventType = 0x01 // a version-1 telemetry sample (GPS/CAN/pulse distance)
	EventPosition          EventType = 0x02 // GNSS position report
	EventPanic             EventType = 0x03 // panic button activation (critical)
	EventSecurityViolation EventType = 0x04 // tamper or authentication anomaly (critical)
	EventDiagnostic        EventType = 0x05 // operational diagnostic
	EventBlockState        EventType = 0x06 // simulated immobilizer state
	EventCargoState        EventType = 0x07 // simulated cargo compartment state
	EventCommandResult     EventType = 0x08 // command lifecycle report, correlated by CommandID
)

var eventTypeNames = map[EventType]string{
	EventTelemetry: "TELEMETRY", EventPosition: "POSITION", EventPanic: "PANIC",
	EventSecurityViolation: "SECURITY_VIOLATION", EventDiagnostic: "DIAGNOSTIC",
	EventBlockState: "BLOCK_STATE", EventCargoState: "CARGO_STATE", EventCommandResult: "COMMAND_RESULT",
}

func (t EventType) String() string {
	if n, ok := eventTypeNames[t]; ok {
		return n
	}
	return fmt.Sprintf("event_type(%d)", uint8(t))
}

// Valid reports whether t is defined.
func (t EventType) Valid() bool { _, ok := eventTypeNames[t]; return ok }

// EventTypes lists every event type.
func EventTypes() []EventType {
	return []EventType{EventTelemetry, EventPosition, EventPanic, EventSecurityViolation,
		EventDiagnostic, EventBlockState, EventCargoState, EventCommandResult}
}

// Class is the urgency class of an event. Lower values are scheduled first
// by devices; see device.Config for the anti-starvation rule.
type Class uint8

// Classes.
const (
	ClassEmergency  Class = 1 // PANIC
	ClassSecurity   Class = 2 // SECURITY_VIOLATION
	ClassControl    Class = 3 // COMMAND_RESULT, BLOCK_STATE, CARGO_STATE
	ClassRoutine    Class = 4 // TELEMETRY, POSITION
	ClassDiagnostic Class = 5 // DIAGNOSTIC
)

func (c Class) String() string {
	switch c {
	case ClassEmergency:
		return "emergency"
	case ClassSecurity:
		return "security"
	case ClassControl:
		return "control"
	case ClassRoutine:
		return "routine"
	case ClassDiagnostic:
		return "diagnostic"
	}
	return fmt.Sprintf("class(%d)", uint8(c))
}

// Critical reports whether events of the class are critical: never expired
// by devices, allowed into the reserved queue capacity, and forwarded to
// the alerting boundary.
func (c Class) Critical() bool { return c == ClassEmergency || c == ClassSecurity }

// Class returns the urgency class of the event type.
func (t EventType) Class() Class {
	switch t {
	case EventPanic:
		return ClassEmergency
	case EventSecurityViolation:
		return ClassSecurity
	case EventCommandResult, EventBlockState, EventCargoState:
		return ClassControl
	case EventDiagnostic:
		return ClassDiagnostic
	}
	return ClassRoutine
}

// MaxEventBody bounds an event body.
const MaxEventBody = 256

// EventUpload is the payload of a TypeEventUpload frame.
//
//	event_uid(8) event_type(1) device_time(8) body_len(2) body(body_len)
type EventUpload struct {
	// UID is the event's durable identity on its device: a 64-bit counter
	// the device persists and never reuses (not reset by reboots, and 2^64
	// is never reached in practice, so it does not wrap).
	UID        uint64
	Type       EventType
	DeviceTime time.Time // generation time, device clock
	Body       []byte    // canonical encoding of the typed body
}

// Encode returns the canonical payload.
func (e EventUpload) Encode() ([]byte, error) {
	if e.UID == 0 || e.UID > MaxIdentity || !e.Type.Valid() || !inRange(e.DeviceTime) || len(e.Body) > MaxEventBody {
		return nil, errf(CodeInvalidEvent, "uid, type, time or body size invalid")
	}
	b := make([]byte, 0, 19+len(e.Body))
	b = binary.BigEndian.AppendUint64(b, e.UID)
	b = append(b, byte(e.Type))
	b = putMillis(b, e.DeviceTime)
	b = binary.BigEndian.AppendUint16(b, uint16(len(e.Body)))
	return append(b, e.Body...), nil
}

// DecodeEventUpload decodes the payload structure (not the typed body).
func DecodeEventUpload(p []byte) (EventUpload, error) {
	r := reader{b: p}
	e := EventUpload{UID: r.u64(), Type: EventType(r.u8()), DeviceTime: r.millis("device_time")}
	n := int(r.u16())
	if n > MaxEventBody {
		r.fail(CodeTooLarge, "body %d bytes", n)
	}
	e.Body = r.bytes(n)
	if err := r.done(); err != nil {
		return EventUpload{}, err
	}
	if e.UID == 0 || e.UID > MaxIdentity {
		return EventUpload{}, errf(CodeInvalidEvent, "event uid 0 or above 2^63-1")
	}
	if !e.Type.Valid() {
		return EventUpload{}, errf(CodeInvalidEvent, "unknown event type %d", uint8(e.Type))
	}
	return e, nil
}

// Fingerprint identifies an event's content: SHA-256 over a fixed context,
// the device id and the canonical EventUpload payload. It excludes the
// envelope (key id, session, counter, tag), so a retransmission in another
// session has the same fingerprint, and two events differ in fingerprint
// exactly when a transmitted field differs.
type Fingerprint [sha256.Size]byte

func (f Fingerprint) String() string { return hex.EncodeToString(f[:]) }

// EventFingerprint computes the fingerprint of an encoded event payload.
func EventFingerprint(deviceID string, payload []byte) Fingerprint {
	h := sha256.New()
	h.Write([]byte("fleetcore/v2/event\x00"))
	h.Write([]byte{byte(len(deviceID))})
	h.Write([]byte(deviceID))
	h.Write(payload)
	var f Fingerprint
	copy(f[:], h.Sum(nil))
	return f
}

// CommandID is the immutable 128-bit identity of a command, chosen at
// random by the command store.
type CommandID [16]byte

func (c CommandID) String() string { return hex.EncodeToString(c[:]) }

// IsZero reports whether the id is unset.
func (c CommandID) IsZero() bool { return c == CommandID{} }

// ParseCommandID decodes a 32-hex-digit command id.
func ParseCommandID(s string) (CommandID, error) {
	var c CommandID
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != len(c) {
		return c, fmt.Errorf("reliable: a command id is 32 hex digits")
	}
	copy(c[:], b)
	if c.IsZero() {
		return c, fmt.Errorf("reliable: zero command id")
	}
	return c, nil
}

// ---- Typed bodies ----

// Body is a typed event body.
type Body interface {
	EventType() EventType
	encode() ([]byte, error)
}

// EncodeBody validates and encodes a typed body.
func EncodeBody(b Body) ([]byte, error) { return b.encode() }

// Telemetry carries a version-1 telemetry sample unchanged (its canonical
// v1 packet is the body). Its EventID must equal the event UID, its
// DeviceID the frame's device and its DeviceTime the event's device time,
// so the v1 identity (DeviceID, BootID, EventID) stored by the event store
// is unique across reboots.
type Telemetry struct{ Sample telemetry.Sample }

func (Telemetry) EventType() EventType { return EventTelemetry }
func (t Telemetry) encode() ([]byte, error) {
	s := t.Sample
	s.IngestedAt = telemetry.None[time.Time]()
	b, err := protocol.EncodeSample(s)
	if err != nil {
		return nil, &Error{Code: CodeInvalidEvent, Err: err}
	}
	return b, nil
}

// Fix is the GNSS fix quality.
type Fix uint8

// Fix values. FixNone means no position is available; a FixNone position
// carries no coordinates (all zero), so a missing fix is never presented as
// a location.
const (
	FixNone Fix = 1
	Fix2D   Fix = 2
	Fix3D   Fix = 3
)

func (f Fix) String() string {
	switch f {
	case FixNone:
		return "none"
	case Fix2D:
		return "2d"
	case Fix3D:
		return "3d"
	}
	return fmt.Sprintf("fix(%d)", uint8(f))
}

// PositionFix is a GNSS fix (24 bytes on the wire):
//
//	fix(1) lat_e7(4) lon_e7(4) speed_kmh_x10(2) heading_cdeg(2) hdop_x10(2) satellites(1) fix_time(8)
type PositionFix struct {
	Fix         Fix
	LatE7       int32  // degrees × 1e7, [-90°, 90°]
	LonE7       int32  // degrees × 1e7, [-180°, 180°]
	SpeedKmhX10 uint16 // km/h × 10, [0, 500 km/h]
	HeadingCdeg uint16 // centidegrees from true north, [0, 36000)
	HDOPx10     uint16 // horizontal dilution of precision × 10, [1, 999]
	Satellites  uint8  // satellites used, [0, 64]
	FixTime     time.Time
}

const fixSize = 24

// Valid reports whether the fix carries a usable position.
func (p PositionFix) Valid() bool { return p.Fix == Fix2D || p.Fix == Fix3D }

func (p PositionFix) check() error {
	switch p.Fix {
	case FixNone:
		if p != (PositionFix{Fix: FixNone}) {
			return errf(CodeInvalidEvent, "a position without fix must carry no coordinates")
		}
		return nil
	case Fix2D, Fix3D:
	default:
		return errf(CodeInvalidEvent, "fix %d", p.Fix)
	}
	minSats := uint8(3)
	if p.Fix == Fix3D {
		minSats = 4
	}
	switch {
	case p.LatE7 < -900_000_000 || p.LatE7 > 900_000_000:
		return errf(CodeInvalidEvent, "latitude out of range")
	case p.LonE7 < -1_800_000_000 || p.LonE7 > 1_800_000_000:
		return errf(CodeInvalidEvent, "longitude out of range")
	case p.SpeedKmhX10 > 5000:
		return errf(CodeInvalidEvent, "speed out of range")
	case p.HeadingCdeg >= 36000:
		return errf(CodeInvalidEvent, "heading out of range")
	case p.HDOPx10 < 1 || p.HDOPx10 > 999:
		return errf(CodeInvalidEvent, "hdop out of range")
	case p.Satellites < minSats || p.Satellites > 64:
		return errf(CodeInvalidEvent, "%d satellites for a %s fix", p.Satellites, p.Fix)
	case !inRange(p.FixTime) || !p.FixTime.Equal(canonicalTime(p.FixTime)):
		return errf(CodeInvalidEvent, "fix time out of range or below millisecond resolution")
	}
	return nil
}

func (p PositionFix) append(b []byte) []byte {
	b = append(b, byte(p.Fix))
	b = binary.BigEndian.AppendUint32(b, uint32(p.LatE7))
	b = binary.BigEndian.AppendUint32(b, uint32(p.LonE7))
	b = binary.BigEndian.AppendUint16(b, p.SpeedKmhX10)
	b = binary.BigEndian.AppendUint16(b, p.HeadingCdeg)
	b = binary.BigEndian.AppendUint16(b, p.HDOPx10)
	b = append(b, p.Satellites)
	if p.Fix == FixNone {
		return binary.BigEndian.AppendUint64(b, 0)
	}
	return putMillis(b, p.FixTime)
}

func readFix(r *reader) PositionFix {
	p := PositionFix{Fix: Fix(r.u8()), LatE7: int32(r.u32()), LonE7: int32(r.u32()),
		SpeedKmhX10: r.u16(), HeadingCdeg: r.u16(), HDOPx10: r.u16(), Satellites: r.u8()}
	if p.Fix == FixNone {
		if r.u64() != 0 {
			r.fail(CodeInvalidEvent, "a position without fix has no fix time")
		}
		return p
	}
	p.FixTime = r.millis("fix_time")
	return p
}

// PositionReason says why a position was reported.
type PositionReason uint8

// Position reasons.
const (
	PositionPeriodic  PositionReason = 1
	PositionRequested PositionReason = 2 // answer to REQUEST_POSITION; RequestID set
)

// Position is a POSITION event (41 bytes): fix(24) reason(1) request_id(16).
// A POSITION event always carries a real fix: "no position available" is
// reported as a REQUEST_POSITION command result, never as a POSITION.
type Position struct {
	PositionFix
	Reason    PositionReason
	RequestID CommandID // the REQUEST_POSITION command answered; zero otherwise
}

func (Position) EventType() EventType { return EventPosition }
func (p Position) check() error {
	if !p.PositionFix.Valid() {
		return errf(CodeInvalidEvent, "a POSITION event needs a valid fix")
	}
	if err := p.PositionFix.check(); err != nil {
		return err
	}
	switch p.Reason {
	case PositionPeriodic:
		if !p.RequestID.IsZero() {
			return errf(CodeInvalidEvent, "periodic position with a request id")
		}
	case PositionRequested:
		if p.RequestID.IsZero() {
			return errf(CodeInvalidEvent, "requested position without request id")
		}
	default:
		return errf(CodeInvalidEvent, "position reason %d", p.Reason)
	}
	return nil
}
func (p Position) encode() ([]byte, error) {
	if err := p.check(); err != nil {
		return nil, err
	}
	b := p.PositionFix.append(make([]byte, 0, 41))
	b = append(b, byte(p.Reason))
	return append(b, p.RequestID[:]...), nil
}

// Panic is a PANIC event (30 bytes): trigger(1) has_position(1) fix(24)
// position_age_ms(4). The position is the most recent valid fix, if any,
// and its age at the moment of the panic; it is optional because a panic
// must be reported even without a fix.
type Panic struct {
	Trigger     uint8 // 1 = panic button
	HasPosition bool
	Position    PositionFix   // valid fix when HasPosition; zero value otherwise
	PositionAge time.Duration // ms resolution, [0, 24h] when HasPosition; 0 otherwise
}

// PanicButton is the only defined panic trigger.
const PanicButton uint8 = 1

func (Panic) EventType() EventType { return EventPanic }
func (p Panic) check() error {
	if p.Trigger != PanicButton {
		return errf(CodeInvalidEvent, "panic trigger %d", p.Trigger)
	}
	if !p.HasPosition {
		if p.Position != (PositionFix{}) || p.PositionAge != 0 {
			return errf(CodeInvalidEvent, "panic without position carries position data")
		}
		return nil
	}
	if !p.Position.Valid() {
		return errf(CodeInvalidEvent, "panic position needs a valid fix")
	}
	if p.PositionAge < 0 || p.PositionAge > 24*time.Hour || p.PositionAge%time.Millisecond != 0 {
		return errf(CodeInvalidEvent, "position age out of range")
	}
	return p.Position.check()
}
func (p Panic) encode() ([]byte, error) {
	if err := p.check(); err != nil {
		return nil, err
	}
	b := []byte{p.Trigger, 0}
	if p.HasPosition {
		b[1] = 1
		b = p.Position.append(b)
	} else {
		b = append(b, make([]byte, fixSize)...)
	}
	return binary.BigEndian.AppendUint32(b, uint32(p.PositionAge/time.Millisecond)), nil
}

// SecurityCode classifies a SECURITY_VIOLATION.
type SecurityCode uint8

// Security codes.
const (
	SecurityTamper              SecurityCode = 1 // simulated enclosure/tamper switch
	SecurityInvalidAuthority    SecurityCode = 2 // authenticated command with an invalid authority signature
	SecurityDownlinkAuthFailure SecurityCode = 3 // burst of downlink frames failing authentication
)

// SecurityViolation is a SECURITY_VIOLATION event (5 bytes): code(1) detail(4).
type SecurityViolation struct {
	Code   SecurityCode
	Detail uint32
}

func (SecurityViolation) EventType() EventType { return EventSecurityViolation }
func (s SecurityViolation) encode() ([]byte, error) {
	if s.Code < SecurityTamper || s.Code > SecurityDownlinkAuthFailure {
		return nil, errf(CodeInvalidEvent, "security code %d", s.Code)
	}
	return binary.BigEndian.AppendUint32([]byte{byte(s.Code)}, s.Detail), nil
}

// Diagnostic is a DIAGNOSTIC event (10 bytes): code(2) value(8).
type Diagnostic struct {
	Code  uint16
	Value int64
}

func (Diagnostic) EventType() EventType { return EventDiagnostic }
func (d Diagnostic) encode() ([]byte, error) {
	if d.Code == 0 {
		return nil, errf(CodeInvalidEvent, "zero diagnostic code")
	}
	return binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint16(nil, d.Code), uint64(d.Value)), nil
}

// BlockStateValue is the immobilizer state read back from the simulated
// actuator's position sensor.
type BlockStateValue uint8

// Immobilizer states.
const (
	Blocked      BlockStateValue = 1
	Unblocked    BlockStateValue = 2
	BlockUnknown BlockStateValue = 3 // sensor unavailable or inconsistent
)

func (s BlockStateValue) String() string {
	switch s {
	case Blocked:
		return "blocked"
	case Unblocked:
		return "unblocked"
	case BlockUnknown:
		return "unknown"
	}
	return fmt.Sprintf("block_state(%d)", uint8(s))
}

// StateCause says why a state was reported.
type StateCause uint8

// State causes.
const (
	CauseCommand     StateCause = 1 // after executing a command (CommandID set)
	CauseBoot        StateCause = 2 // read at device boot
	CauseChange      StateCause = 3 // observed change without a command
	CauseInterrupted StateCause = 4 // read after an interrupted execution (CommandID set)
)

func (c StateCause) valid() bool { return c >= CauseCommand && c <= CauseInterrupted }
func (c StateCause) needsCommand() bool {
	return c == CauseCommand || c == CauseInterrupted
}

// BlockState is a BLOCK_STATE event (18 bytes): state(1) cause(1) command_id(16).
type BlockState struct {
	State     BlockStateValue
	Cause     StateCause
	CommandID CommandID
}

func (BlockState) EventType() EventType { return EventBlockState }
func (s BlockState) check() error {
	if s.State < Blocked || s.State > BlockUnknown || !s.Cause.valid() || s.Cause.needsCommand() == s.CommandID.IsZero() {
		return errf(CodeInvalidEvent, "block state %d cause %d", s.State, s.Cause)
	}
	return nil
}
func (s BlockState) encode() ([]byte, error) {
	if err := s.check(); err != nil {
		return nil, err
	}
	return append([]byte{byte(s.State), byte(s.Cause)}, s.CommandID[:]...), nil
}

// DoorState is the cargo door sensor reading.
type DoorState uint8

// Door states.
const (
	DoorOpen    DoorState = 1
	DoorClosed  DoorState = 2
	DoorUnknown DoorState = 3
)

func (d DoorState) String() string {
	switch d {
	case DoorOpen:
		return "open"
	case DoorClosed:
		return "closed"
	case DoorUnknown:
		return "unknown"
	}
	return fmt.Sprintf("door(%d)", uint8(d))
}

// LatchState is the cargo latch position sensor reading.
type LatchState uint8

// Latch states.
const (
	LatchLocked   LatchState = 1
	LatchReleased LatchState = 2
	LatchUnknown  LatchState = 3
)

func (l LatchState) String() string {
	switch l {
	case LatchLocked:
		return "locked"
	case LatchReleased:
		return "released"
	case LatchUnknown:
		return "unknown"
	}
	return fmt.Sprintf("latch(%d)", uint8(l))
}

// CargoState is a CARGO_STATE event (19 bytes): door(1) latch(1) cause(1)
// command_id(16). Door and latch are separate sensors: a released latch
// does not mean the door opened.
type CargoState struct {
	Door      DoorState
	Latch     LatchState
	Cause     StateCause
	CommandID CommandID
}

func (CargoState) EventType() EventType { return EventCargoState }
func (s CargoState) check() error {
	if s.Door < DoorOpen || s.Door > DoorUnknown || s.Latch < LatchLocked || s.Latch > LatchUnknown ||
		!s.Cause.valid() || s.Cause.needsCommand() == s.CommandID.IsZero() {
		return errf(CodeInvalidEvent, "cargo state door %d latch %d cause %d", s.Door, s.Latch, s.Cause)
	}
	return nil
}
func (s CargoState) encode() ([]byte, error) {
	if err := s.check(); err != nil {
		return nil, err
	}
	return append([]byte{byte(s.Door), byte(s.Latch), byte(s.Cause)}, s.CommandID[:]...), nil
}

// CommandResult is a COMMAND_RESULT event (21 bytes): command_id(16)
// command_type(1) stage(1) reason(2) observed(1). It reports one stage of a
// command's lifecycle on the device; a command produces one report per
// stage it reaches (received, then accepted or rejected, then a final
// outcome).
type CommandResult struct {
	CommandID   CommandID
	CommandType CommandType
	Stage       Stage
	Reason      Reason
	// Observed is the actuator state read back after the stage, in the
	// command's domain (BlockStateValue for BLOCK/UNBLOCK, DoorState for
	// OPEN_CARGO), or 0.
	Observed uint8
}

func (CommandResult) EventType() EventType { return EventCommandResult }
func (c CommandResult) check() error {
	// The command type is echoed as received: a device reports a type it
	// does not implement (ReasonUnsupported) with that type's code, so any
	// non-zero code is valid here.
	if c.CommandID.IsZero() || c.CommandType == 0 || !c.Stage.valid() || !c.Reason.valid() {
		return errf(CodeInvalidEvent, "command result for %s stage %d reason %d", c.CommandType, c.Stage, c.Reason)
	}
	if c.Observed > 3 {
		return errf(CodeInvalidEvent, "observed state %d", c.Observed)
	}
	return nil
}
func (c CommandResult) encode() ([]byte, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	b := append(c.CommandID[:0:0], c.CommandID[:]...)
	b = append(b, byte(c.CommandType), byte(c.Stage))
	b = binary.BigEndian.AppendUint16(b, uint16(c.Reason))
	return append(b, c.Observed), nil
}

// DecodeBody decodes and validates an event body in the context of its
// upload: device id, UID and device time constrain the TELEMETRY body.
func DecodeBody(deviceID string, e EventUpload) (Body, error) {
	r := reader{b: e.Body}
	var out Body
	switch e.Type {
	case EventTelemetry:
		s, err := protocol.DecodeSample(e.Body)
		if err != nil {
			return nil, &Error{Code: CodeInvalidEvent, Err: err}
		}
		if string(s.DeviceID) != deviceID || uint64(s.EventID) != e.UID || !s.DeviceTime.Equal(e.DeviceTime) {
			return nil, errf(CodeInvalidEvent, "telemetry sample identity or time differs from the event envelope")
		}
		return Telemetry{Sample: s}, nil
	case EventPosition:
		p := Position{PositionFix: readFix(&r), Reason: PositionReason(r.u8())}
		r.fixed(p.RequestID[:])
		if r.err == nil {
			if err := p.check(); err != nil {
				r.err = err.(*Error)
			}
		}
		out = p
	case EventPanic:
		p := Panic{Trigger: r.u8()}
		has := r.u8()
		fx := readFixRaw(&r)
		age := time.Duration(r.u32()) * time.Millisecond
		switch has {
		case 0:
			if fx != (rawFix{}) {
				r.fail(CodeInvalidEvent, "panic without position carries position data")
			}
			p.PositionAge = age
		case 1:
			p.HasPosition = true
			pf, err := fx.decode()
			if err != nil {
				r.fail(CodeInvalidEvent, "%v", err)
			}
			p.Position, p.PositionAge = pf, age
		default:
			r.fail(CodeInvalidEvent, "has_position %d", has)
		}
		if r.err == nil {
			if err := p.check(); err != nil {
				r.err = err.(*Error)
			}
		}
		out = p
	case EventSecurityViolation:
		s := SecurityViolation{Code: SecurityCode(r.u8()), Detail: r.u32()}
		if r.err == nil && (s.Code < SecurityTamper || s.Code > SecurityDownlinkAuthFailure) {
			r.fail(CodeInvalidEvent, "security code %d", s.Code)
		}
		out = s
	case EventDiagnostic:
		d := Diagnostic{Code: r.u16(), Value: int64(r.u64())}
		if r.err == nil && d.Code == 0 {
			r.fail(CodeInvalidEvent, "zero diagnostic code")
		}
		out = d
	case EventBlockState:
		s := BlockState{State: BlockStateValue(r.u8()), Cause: StateCause(r.u8())}
		r.fixed(s.CommandID[:])
		if r.err == nil {
			if err := s.check(); err != nil {
				r.err = err.(*Error)
			}
		}
		out = s
	case EventCargoState:
		s := CargoState{Door: DoorState(r.u8()), Latch: LatchState(r.u8()), Cause: StateCause(r.u8())}
		r.fixed(s.CommandID[:])
		if r.err == nil {
			if err := s.check(); err != nil {
				r.err = err.(*Error)
			}
		}
		out = s
	case EventCommandResult:
		var c CommandResult
		r.fixed(c.CommandID[:])
		c.CommandType, c.Stage, c.Reason, c.Observed = CommandType(r.u8()), Stage(r.u8()), Reason(r.u16()), r.u8()
		if r.err == nil {
			if err := c.check(); err != nil {
				r.err = err.(*Error)
			}
		}
		out = c
	default:
		return nil, errf(CodeInvalidEvent, "unknown event type %d", uint8(e.Type))
	}
	if err := r.done(); err != nil {
		if CodeOf(err) == CodeMalformedPayload || CodeOf(err) == CodeLengthMismatch {
			return nil, &Error{Code: CodeInvalidEvent, Detail: e.Type.String() + " body", Err: err}
		}
		return nil, err
	}
	return out, nil
}

// rawFix is a fix as read from the wire, before validation (used where an
// all-zero block means "absent").
type rawFix struct {
	b [fixSize]byte
}

func readFixRaw(r *reader) rawFix {
	var f rawFix
	r.fixed(f.b[:])
	return f
}

func (f rawFix) decode() (PositionFix, error) {
	r := reader{b: f.b[:]}
	p := readFix(&r)
	if err := r.done(); err != nil {
		return PositionFix{}, err
	}
	return p, nil
}

// NewEvent builds an upload from a typed body.
func NewEvent(uid uint64, deviceTime time.Time, b Body) (EventUpload, error) {
	body, err := b.encode()
	if err != nil {
		return EventUpload{}, err
	}
	return EventUpload{UID: uid, Type: b.EventType(), DeviceTime: canonicalTime(deviceTime), Body: body}, nil
}
