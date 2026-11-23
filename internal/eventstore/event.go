// Package eventstore persists accepted telemetry durably and is the
// authoritative deduplication point of the ingestion pipeline.
//
// # Event identity
//
// The durable identity of an event is the triple (DeviceID, BootID, EventID).
//
// The domain documents EventID as unique per device. The store relies on a
// weaker assumption, uniqueness per device *and boot*, because the reference
// simulator derives EventID from the per-boot sequence number, which restarts
// on every boot: keyed by (DeviceID, EventID) alone, every post-reboot event
// would collide with a pre-reboot one. Producers that honour the stronger
// domain contract are unaffected, and violations of it are still reported,
// not hidden: an insert whose (DeviceID, EventID) already exists under other
// BootIDs lists them in Result.EventIDAlsoInBoots (and ByEventID finds them).
//
// Each event also carries a payload fingerprint: SHA-256 of the sample's
// canonical version-1 wire encoding (protocol.EncodeSample). The protocol is
// canonical, so two samples have the same fingerprint exactly when every
// transmitted field is equal. IngestedAt is receiver metadata, not part of
// the wire format, and is deliberately excluded: a retransmission received
// later is still the same event.
//
// Classification of an incoming event against the store:
//
//   - identity unknown                      → [Inserted]
//   - identity known, fingerprint equal     → [Duplicate] (retransmission; no write)
//   - identity known, fingerprint different → [Conflict]  (stored event is kept
//     unchanged; the conflicting payload is recorded in the conflict table)
//
// Consequences:
//
//   - The same EventID on two devices, or in two boots of one device, is two
//     distinct identities.
//   - Identical measurements with different EventIDs are distinct events.
//     (A repeated (BootID, Sequence) is a session anomaly reported by the
//     session registry, not a storage-level duplicate.)
//   - An event arriving after a session transition is classified by identity
//     only; lateness never causes it to be dropped.
//
// Limitation: BootID is a 32-bit identifier chosen by the device. If a
// device reuses a BootID and, within it, an EventID, the two events share an
// identity. A different payload is then a Conflict: preserved in the conflict
// table, but not in the event table. An identical payload is a Duplicate,
// which loses no information because every transmitted field is equal.
// Without a larger or monotonic boot identifier on the wire, no receiver can
// tell those cases from retransmission or corruption.
//
// Correctness relies on the UNIQUE(device_id, boot_id, event_id) constraint
// inside a transaction, so it holds across process restarts and concurrent
// writers.
package eventstore

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/flavioanjo/fleetcore/internal/protocol"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// Fingerprint is the SHA-256 of a sample's canonical wire encoding.
type Fingerprint [sha256.Size]byte

func (f Fingerprint) String() string { return fmt.Sprintf("%x", f[:]) }

// Key is the durable identity of an event.
type Key struct {
	DeviceID telemetry.DeviceID
	BootID   telemetry.BootID
	EventID  telemetry.EventID
}

func (k Key) String() string { return fmt.Sprintf("%s/%d/%d", k.DeviceID, k.BootID, k.EventID) }

// Event is a validated sample ready to be stored. Build it with NewEvent.
type Event struct {
	Sample      telemetry.Sample
	Packet      []byte // canonical version-1 encoding of Sample
	Fingerprint Fingerprint
}

// Key returns the event's durable identity.
func (e Event) Key() Key {
	return Key{DeviceID: e.Sample.DeviceID, BootID: e.Sample.BootID, EventID: e.Sample.EventID}
}

// ErrNoIngestionTime is returned by NewEvent for a sample without IngestedAt.
var ErrNoIngestionTime = errors.New("eventstore: sample has no ingestion time")

// NewEvent validates s, which must carry an ingestion time, and computes its
// canonical encoding and fingerprint.
func NewEvent(s telemetry.Sample) (Event, error) {
	if !s.IngestedAt.IsPresent() {
		return Event{}, ErrNoIngestionTime
	}
	if err := s.Validate(); err != nil {
		return Event{}, fmt.Errorf("eventstore: %w", err)
	}
	pkt, err := protocol.EncodeSample(s)
	if err != nil {
		return Event{}, fmt.Errorf("eventstore: %w", err)
	}
	return Event{Sample: s, Packet: pkt, Fingerprint: sha256.Sum256(pkt)}, nil
}

// Outcome is the result of appending one event.
type Outcome uint8

// Append outcomes.
const (
	Inserted  Outcome = iota + 1 // new identity, stored
	Duplicate                    // identical event already stored
	Conflict                     // identity already stored with a different payload
	// StaleSession (v2 events only): the event's session is older than the
	// device's durable session high-water mark at commit time; nothing is
	// written. See [Store.AppendReliable].
	StaleSession
)

func (o Outcome) String() string {
	switch o {
	case Inserted:
		return "inserted"
	case Duplicate:
		return "duplicate"
	case Conflict:
		return "conflict"
	case StaleSession:
		return "stale_session"
	default:
		return fmt.Sprintf("outcome(%d)", uint8(o))
	}
}

// Result describes what happened to one appended event.
type Result struct {
	Outcome Outcome
	// Position is the store position of the stored event with this identity
	// (the new row for Inserted, the existing one otherwise). Positions
	// increase with insertion order and are never reused.
	Position int64
	// Existing is the fingerprint of the stored event (equal to the
	// appended one unless Outcome is Conflict).
	Existing Fingerprint
	// EventIDAlsoInBoots is set for Inserted events whose (DeviceID,
	// EventID) is already stored under other BootIDs (up to 8, oldest
	// first): the producer violates the domain's "EventID unique per
	// device" contract. The event is stored regardless.
	EventIDAlsoInBoots []telemetry.BootID
}

// Record is a stored event.
type Record struct {
	Position    int64
	Sample      telemetry.Sample // IngestedAt is the first receipt time
	Fingerprint Fingerprint
	StoredAt    time.Time
}

// ConflictRecord is a payload that reused an already stored identity.
type ConflictRecord struct {
	Key         Key
	Fingerprint Fingerprint
	Packet      []byte    // canonical encoding of the conflicting sample
	FirstSeenAt time.Time // ingestion time of its first receipt
	LastSeenAt  time.Time // ingestion time of its latest receipt
	Count       int64     // number of receipts
}
