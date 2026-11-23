package reliable

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/flavioanjo/fleetcore/internal/security"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// CommandType identifies a vehicle command.
type CommandType uint8

// Command types (command schema version 1).
const (
	CommandBlock           CommandType = 1 // request safe immobilization
	CommandUnblock         CommandType = 2 // request removal of immobilization
	CommandOpenCargo       CommandType = 3 // request opening of the cargo compartment
	CommandRequestPosition CommandType = 4 // request an immediate position report
)

// CommandSchemaVersion is the version of the command argument schemas.
const CommandSchemaVersion uint8 = 1

var commandNames = map[CommandType]string{
	CommandBlock: "BLOCK", CommandUnblock: "UNBLOCK", CommandOpenCargo: "OPEN_CARGO", CommandRequestPosition: "REQUEST_POSITION",
}

func (c CommandType) String() string {
	if n, ok := commandNames[c]; ok {
		return n
	}
	return fmt.Sprintf("command(%d)", uint8(c))
}

// Valid reports whether c is defined.
func (c CommandType) Valid() bool { _, ok := commandNames[c]; return ok }

// Actuating reports whether the command moves a (simulated) actuator.
// Actuating commands of one device are dispatched one at a time.
func (c CommandType) Actuating() bool {
	return c == CommandBlock || c == CommandUnblock || c == CommandOpenCargo
}

// ParseCommandType parses a command name such as "BLOCK".
func ParseCommandType(s string) (CommandType, error) {
	for c, n := range commandNames {
		if n == s {
			return c, nil
		}
	}
	return 0, fmt.Errorf("reliable: unknown command %q", s)
}

// Stage is the lifecycle stage a device reports in a COMMAND_RESULT.
type Stage uint8

// Stages, in lifecycle order. Received < Accepted|Rejected <
// Succeeded|Failed|Unknown.
const (
	StageReceived  Stage = 1 // durably recorded by the device
	StageAccepted  Stage = 2 // authorization and safety checks passed; execution will be attempted
	StageRejected  Stage = 3 // not executed (Reason says why); final
	StageSucceeded Stage = 4 // executed and the outcome was confirmed by a sensor; final
	StageFailed    Stage = 5 // execution attempted and not achieved; final
	StageUnknown   Stage = 6 // execution started but its outcome cannot be established; final
)

var stageNames = map[Stage]string{
	StageReceived: "received", StageAccepted: "accepted", StageRejected: "rejected",
	StageSucceeded: "succeeded", StageFailed: "failed", StageUnknown: "unknown",
}

func (s Stage) String() string {
	if n, ok := stageNames[s]; ok {
		return n
	}
	return fmt.Sprintf("stage(%d)", uint8(s))
}

func (s Stage) valid() bool { _, ok := stageNames[s]; return ok }

// Rank orders stages: a report of a lower rank than the recorded one is
// stale.
func (s Stage) Rank() int {
	switch s {
	case StageReceived:
		return 1
	case StageAccepted, StageRejected:
		return 2
	case StageSucceeded, StageFailed, StageUnknown:
		return 3
	}
	return 0
}

// Final reports whether no later stage can follow.
func (s Stage) Final() bool { return s == StageRejected || s.Rank() == 3 }

// Reason qualifies a stage.
type Reason uint16

// Reasons.
const (
	ReasonNone                Reason = 0
	ReasonExpired             Reason = 1  // the command's execution deadline passed before it could start
	ReasonUnsupported         Reason = 2  // unknown command type or schema version
	ReasonBadArguments        Reason = 3  // arguments invalid for the type
	ReasonVehicleMoving       Reason = 4  // safety interlock: vehicle not stationary
	ReasonIgnitionOn          Reason = 5  // safety interlock: ignition on
	ReasonActuatorFault       Reason = 6  // the actuator reported a fault
	ReasonNotConfirmed        Reason = 7  // actuation done but the sensor did not confirm the effect
	ReasonPositionUnavailable Reason = 8  // no valid GNSS fix: no position is fabricated
	ReasonInterrupted         Reason = 9  // device restarted during execution; outcome unknown
	ReasonAlreadyInState      Reason = 10 // target state already held (confirmed by sensor); nothing actuated
	ReasonSensorUnavailable   Reason = 11 // the state sensor could not be read
	ReasonAttemptStale        Reason = 12 // this delivery attempt was older than its freshness bound
	ReasonBusy                Reason = 13 // another actuation is in progress on the device
)

var reasonNames = map[Reason]string{
	ReasonNone: "none", ReasonExpired: "expired", ReasonUnsupported: "unsupported", ReasonBadArguments: "bad_arguments",
	ReasonVehicleMoving: "vehicle_moving", ReasonIgnitionOn: "ignition_on", ReasonActuatorFault: "actuator_fault",
	ReasonNotConfirmed: "not_confirmed_by_sensor", ReasonPositionUnavailable: "position_unavailable",
	ReasonInterrupted: "interrupted", ReasonAlreadyInState: "already_in_state", ReasonSensorUnavailable: "sensor_unavailable",
	ReasonAttemptStale: "attempt_stale", ReasonBusy: "busy",
}

func (r Reason) String() string {
	if n, ok := reasonNames[r]; ok {
		return n
	}
	return fmt.Sprintf("reason(%d)", uint16(r))
}

func (r Reason) valid() bool { _, ok := reasonNames[r]; return ok }

// Command is the authority-signed part of a command: what to do, to which
// device, by whom, and until when. It never changes after creation.
type Command struct {
	ID        CommandID
	DeviceID  string
	Type      CommandType
	Args      []byte
	IssuedAt  time.Time // server clock
	ExpiresAt time.Time // last instant at which execution may start (server clock)
	Principal string    // authorized origin (operator identity), 1..64 bytes
}

// MaxPrincipal bounds the principal.
const MaxPrincipal = 64

// Command argument helpers. BLOCK and UNBLOCK take no arguments.
// OPEN_CARGO takes the compartment number (1 byte, 1..4). REQUEST_POSITION
// takes the maximum acceptable fix age in seconds (2 bytes, 1..3600).

// CargoArgs encodes OPEN_CARGO arguments.
func CargoArgs(compartment uint8) []byte { return []byte{compartment} }

// PositionArgs encodes REQUEST_POSITION arguments.
func PositionArgs(maxAge time.Duration) []byte {
	return binary.BigEndian.AppendUint16(nil, uint16(maxAge/time.Second))
}

// ParseCargoArgs decodes OPEN_CARGO arguments.
func ParseCargoArgs(a []byte) (uint8, bool) {
	if len(a) != 1 || a[0] < 1 || a[0] > 4 {
		return 0, false
	}
	return a[0], true
}

// ParsePositionArgs decodes REQUEST_POSITION arguments.
func ParsePositionArgs(a []byte) (time.Duration, bool) {
	if len(a) != 2 {
		return 0, false
	}
	s := binary.BigEndian.Uint16(a)
	if s < 1 || s > 3600 {
		return 0, false
	}
	return time.Duration(s) * time.Second, true
}

// ValidArgs reports whether args are valid for the command type.
func ValidArgs(t CommandType, args []byte) bool {
	switch t {
	case CommandBlock, CommandUnblock:
		return len(args) == 0
	case CommandOpenCargo:
		_, ok := ParseCargoArgs(args)
		return ok
	case CommandRequestPosition:
		_, ok := ParsePositionArgs(args)
		return ok
	}
	return false
}

// Check validates the command's static fields, including that its type is
// known and its arguments fit the type.
func (c Command) Check() error {
	if err := c.checkEnvelope(); err != nil {
		return err
	}
	switch {
	case !c.Type.Valid():
		return errf(CodeInvalidCommand, "unknown command type %d", c.Type)
	case !ValidArgs(c.Type, c.Args):
		return errf(CodeInvalidCommand, "invalid arguments for %s", c.Type)
	}
	return nil
}

// checkEnvelope validates everything except the type and its arguments, so
// a device can decode (and report as unsupported) a command type it does
// not implement.
func (c Command) checkEnvelope() error {
	switch {
	case c.ID.IsZero():
		return errf(CodeInvalidCommand, "zero command id")
	case telemetry.DeviceID(c.DeviceID).Validate() != nil:
		return errf(CodeInvalidCommand, "device id")
	case c.Type == 0:
		return errf(CodeInvalidCommand, "command type 0 is reserved")
	case !inRange(c.IssuedAt) || !inRange(c.ExpiresAt) || !c.ExpiresAt.After(c.IssuedAt):
		return errf(CodeInvalidCommand, "issue/expiry times")
	case len(c.Principal) == 0 || len(c.Principal) > MaxPrincipal:
		return errf(CodeInvalidCommand, "principal length")
	}
	return nil
}

// Statement is the canonical byte string the command authority signs:
//
//	"FCCMD1" command_id(16) device_id_len(1) device_id type(1) schema_version(1)
//	args_len(1) args issued_at(8) expires_at(8) principal_len(1) principal
//
// It binds every field that determines what may be executed, where and
// until when. Delivery-attempt fields are not signed (they change per
// attempt) and are covered by the transport tag instead.
func (c Command) Statement() []byte {
	b := make([]byte, 0, 64+len(c.DeviceID)+len(c.Args)+len(c.Principal))
	b = append(b, "FCCMD1"...)
	b = append(b, c.ID[:]...)
	b = append(b, byte(len(c.DeviceID)))
	b = append(b, c.DeviceID...)
	b = append(b, byte(c.Type), CommandSchemaVersion, byte(len(c.Args)))
	b = append(b, c.Args...)
	b = putMillis(b, c.IssuedAt)
	b = putMillis(b, c.ExpiresAt)
	b = append(b, byte(len(c.Principal)))
	return append(b, c.Principal...)
}

// CommandDelivery is the payload of a TypeCommandDelivery frame:
//
//	command_id(16) type(1) schema_version(1) args_len(1) args
//	issued_at(8) expires_at(8) principal_len(1) principal
//	attempt(4) attempt_sent_at(8) attempt_not_after(8)
//	authority_key_id(4) authority_signature(64)
//
// The device id is the frame's.
type CommandDelivery struct {
	Command
	Attempt         uint32    // 1-based delivery attempt number
	AttemptSentAt   time.Time // server clock when this attempt was sent
	AttemptNotAfter time.Time // the device ignores this attempt after this instant (delayed or replayed datagram)
	AuthorityKeyID  uint32
	Signature       []byte // Ed25519 over Statement()
}

// MaxAttemptValidity bounds AttemptNotAfter - AttemptSentAt.
const MaxAttemptValidity = 5 * time.Minute

// Encode returns the canonical payload. Servers only encode commands that
// pass Command.Check.
func (d CommandDelivery) Encode() ([]byte, error) {
	if err := d.check(); err != nil {
		return nil, err
	}
	if len(d.Args) > 255 || len(d.Principal) > MaxPrincipal {
		return nil, errf(CodeInvalidCommand, "field too long")
	}
	c := d.Command
	b := make([]byte, 0, 128+len(c.Args)+len(c.Principal))
	b = append(b, c.ID[:]...)
	b = append(b, byte(c.Type), CommandSchemaVersion, byte(len(c.Args)))
	b = append(b, c.Args...)
	b = putMillis(b, c.IssuedAt)
	b = putMillis(b, c.ExpiresAt)
	b = append(b, byte(len(c.Principal)))
	b = append(b, c.Principal...)
	b = binary.BigEndian.AppendUint32(b, d.Attempt)
	b = putMillis(b, d.AttemptSentAt)
	b = putMillis(b, d.AttemptNotAfter)
	b = binary.BigEndian.AppendUint32(b, d.AuthorityKeyID)
	return append(b, d.Signature...), nil
}

func (d CommandDelivery) check() error {
	if err := d.Command.checkEnvelope(); err != nil {
		return err
	}
	switch {
	case d.Attempt == 0:
		return errf(CodeInvalidCommand, "attempt 0")
	case !inRange(d.AttemptSentAt) || !d.AttemptNotAfter.After(d.AttemptSentAt) ||
		d.AttemptNotAfter.Sub(d.AttemptSentAt) > MaxAttemptValidity:
		return errf(CodeInvalidCommand, "attempt validity window")
	case d.AuthorityKeyID == 0 || len(d.Signature) != security.SignatureSize:
		return errf(CodeInvalidCommand, "authority key id or signature size")
	}
	return nil
}

// DecodeCommandDelivery decodes a delivery for device deviceID. A delivery
// of an unknown command type or with invalid arguments still decodes, so
// the device can reject it with ReasonUnsupported/ReasonBadArguments;
// Command.Check reports the problem. Everything else fails closed.
func DecodeCommandDelivery(deviceID string, p []byte) (CommandDelivery, error) {
	r := reader{b: p}
	var d CommandDelivery
	r.fixed(d.ID[:])
	d.DeviceID = deviceID
	d.Type = CommandType(r.u8())
	if v := r.u8(); r.err == nil && v != CommandSchemaVersion {
		r.fail(CodeInvalidCommand, "command schema version %d", v)
	}
	d.Args = r.bytes(int(r.u8()))
	d.IssuedAt = r.millis("issued_at")
	d.ExpiresAt = r.millis("expires_at")
	d.Principal = string(r.bytes(int(r.u8())))
	d.Attempt = r.u32()
	d.AttemptSentAt = r.millis("attempt_sent_at")
	d.AttemptNotAfter = r.millis("attempt_not_after")
	d.AuthorityKeyID = r.u32()
	d.Signature = r.bytes(security.SignatureSize)
	if err := r.done(); err != nil {
		return CommandDelivery{}, err
	}
	if err := d.check(); err != nil {
		return CommandDelivery{}, err
	}
	return d, nil
}

// AckStatus is the server's verdict on an uploaded event.
type AckStatus uint8

// ACK statuses. Only AckStored and AckDuplicate are success: both mean an
// event with this device, UID and fingerprint is durably committed to the
// event store. The negative statuses are equally authenticated and bound
// to the event, so a device can stop retrying a permanently rejected event
// without deleting it.
const (
	AckStored    AckStatus = 1 // committed now
	AckDuplicate AckStatus = 2 // an identical event was already committed
	AckConflict  AckStatus = 3 // this UID is committed with a different fingerprint; permanent
	AckInvalid   AckStatus = 4 // authenticated but invalid body; permanent
	AckRetry     AckStatus = 5 // temporary server failure (storage busy or failing); retry later
)

func (s AckStatus) String() string {
	switch s {
	case AckStored:
		return "stored"
	case AckDuplicate:
		return "duplicate"
	case AckConflict:
		return "conflict"
	case AckInvalid:
		return "invalid"
	case AckRetry:
		return "retry"
	}
	return fmt.Sprintf("ack_status(%d)", uint8(s))
}

// Success reports whether the status confirms durable storage.
func (s AckStatus) Success() bool { return s == AckStored || s == AckDuplicate }

// Permanent reports whether the event will never be accepted.
func (s AckStatus) Permanent() bool { return s == AckConflict || s == AckInvalid }

// EventAck is the payload of a TypeEventAck frame (49 bytes):
//
//	event_uid(8) fingerprint(32) status(1) server_position(8)
//
// The device id and session are the frame's. A device accepts an ACK only
// if its UID and fingerprint match a queued event of its own.
type EventAck struct {
	UID         uint64
	Fingerprint Fingerprint
	Status      AckStatus
	// Position is the server's reliable-event position for success statuses
	// (diagnostic), 0 otherwise.
	Position int64
}

// Encode returns the canonical payload.
func (a EventAck) Encode() ([]byte, error) {
	if a.UID == 0 || a.Status < AckStored || a.Status > AckRetry || a.Position < 0 ||
		(a.Status.Success() != (a.Position > 0)) {
		return nil, errf(CodeMalformedPayload, "ack fields")
	}
	b := binary.BigEndian.AppendUint64(make([]byte, 0, 49), a.UID)
	b = append(b, a.Fingerprint[:]...)
	b = append(b, byte(a.Status))
	return binary.BigEndian.AppendUint64(b, uint64(a.Position)), nil
}

// DecodeEventAck decodes an ACK payload.
func DecodeEventAck(p []byte) (EventAck, error) {
	r := reader{b: p}
	a := EventAck{UID: r.u64()}
	r.fixed(a.Fingerprint[:])
	a.Status = AckStatus(r.u8())
	a.Position = int64(r.u64())
	if err := r.done(); err != nil {
		return EventAck{}, err
	}
	if _, err := a.Encode(); err != nil {
		return EventAck{}, err
	}
	return a, nil
}

// Heartbeat is the payload of a TypeHeartbeat frame (12 bytes):
// device_time(8) pending_events(4). It announces the device's session (so
// the server can address commands to it) when it has no events to send.
type Heartbeat struct {
	DeviceTime time.Time
	Pending    uint32
}

// Encode returns the canonical payload.
func (h Heartbeat) Encode() ([]byte, error) {
	if !inRange(h.DeviceTime) {
		return nil, errf(CodeMalformedPayload, "heartbeat time")
	}
	return binary.BigEndian.AppendUint32(putMillis(nil, h.DeviceTime), h.Pending), nil
}

// DecodeHeartbeat decodes a heartbeat payload.
func DecodeHeartbeat(p []byte) (Heartbeat, error) {
	r := reader{b: p}
	h := Heartbeat{DeviceTime: r.millis("device_time"), Pending: r.u32()}
	if err := r.done(); err != nil {
		return Heartbeat{}, err
	}
	return h, nil
}
