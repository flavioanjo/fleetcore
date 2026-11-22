package security

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
)

// SecretSize is the size of a device master key in bytes (256 bits).
const SecretSize = 32

// Secret is a device master key. Its text forms are redacted so it cannot
// leak through logs, fmt or JSON by accident; use Bytes for the key itself.
type Secret [SecretSize]byte

const redacted = "[REDACTED]"

func (Secret) String() string               { return redacted }
func (Secret) GoString() string             { return redacted }
func (Secret) MarshalJSON() ([]byte, error) { return json.Marshal(redacted) }
func (Secret) MarshalText() ([]byte, error) { return []byte(redacted), nil }
func (s *Secret) Bytes() []byte             { return s[:] }
func (s Secret) Equal(o Secret) bool        { return subtle.ConstantTimeCompare(s[:], o[:]) == 1 }
func (s Secret) IsZero() bool               { return s == Secret{} }
func (s Secret) Format(f fmt.State, _ rune) { fmt.Fprint(f, redacted) }
func (s *Secret) UnmarshalJSON([]byte) error {
	return errors.New("security: secrets are not decoded from JSON directly")
}
func (s *Secret) UnmarshalText(b []byte) error { return s.UnmarshalJSON(b) }

// GenerateSecret returns a fresh random device master key.
func GenerateSecret() (Secret, error) {
	var s Secret
	if _, err := rand.Read(s[:]); err != nil {
		return Secret{}, fmt.Errorf("security: generate secret: %w", err)
	}
	return s, nil
}

// ParseSecretHex decodes a 64-hex-digit secret.
func ParseSecretHex(h string) (Secret, error) {
	b, err := hex.DecodeString(h)
	if err != nil || len(b) != SecretSize {
		return Secret{}, fmt.Errorf("security: a secret is %d hex-encoded bytes", SecretSize)
	}
	var s Secret
	copy(s[:], b)
	if s.IsZero() {
		return Secret{}, errors.New("security: all-zero secret rejected")
	}
	return s, nil
}

// Direction is the direction of a protocol message. Keys are separated by
// direction, so a message cannot be reflected back to its sender.
type Direction uint8

// Directions.
const (
	Uplink   Direction = 1 // device → server
	Downlink Direction = 2 // server → device
)

// macSalt is the fixed HKDF salt of protocol v2 message keys.
var macSalt = []byte("fleetcore/v2/hkdf-salt")

// MessageKey derives the HMAC-SHA256 key for one message purpose of one
// device session:
//
//	HKDF-SHA256(IKM = device master key, salt = "fleetcore/v2/hkdf-salt",
//	  info = "fleetcore/v2/mac" 0x00 direction purpose key_id(4) session_gen(8) len(device_id) device_id)
//
// purpose is the protocol message type. Every input that distinguishes two
// contexts is in info, so a key (and therefore a tag) of one device,
// direction, message type, key version or session never verifies a message
// of another.
func MessageKey(master *Secret, dir Direction, purpose uint8, deviceID string, keyID uint32, sessionGen uint64) [32]byte {
	info := make([]byte, 0, 32+len(deviceID))
	info = append(info, "fleetcore/v2/mac"...)
	info = append(info, 0, byte(dir), purpose)
	info = binary.BigEndian.AppendUint32(info, keyID)
	info = binary.BigEndian.AppendUint64(info, sessionGen)
	info = append(info, byte(len(deviceID)))
	info = append(info, deviceID...)
	out, err := HKDF(master[:], macSalt, info, 32)
	if err != nil {
		panic(err) // 32 bytes is always a valid HKDF length
	}
	var k [32]byte
	copy(k[:], out)
	return k
}

// KeyState is the lifecycle state of a device key version.
type KeyState uint8

// Key states. A device starts sessions with its newest key; during a
// rotation the server keeps accepting the previous version (Retired) until
// it is revoked.
const (
	KeyActive  KeyState = 1 // accepted; the version new sessions should use
	KeyRetired KeyState = 2 // still accepted for existing devices during rotation
	KeyRevoked KeyState = 3 // rejected
)

func (s KeyState) String() string {
	switch s {
	case KeyActive:
		return "active"
	case KeyRetired:
		return "retired"
	case KeyRevoked:
		return "revoked"
	}
	return fmt.Sprintf("key_state(%d)", uint8(s))
}

// DeviceKey is one version of a device's master key.
type DeviceKey struct {
	DeviceID string
	KeyID    uint32
	Secret   Secret
	State    KeyState
}

// Key store errors.
var (
	ErrUnknownKey = errors.New("security: unknown device key")
	ErrKeyRevoked = errors.New("security: device key revoked")
)

// KeyStore resolves device key versions. Implementations must be safe for
// concurrent use.
type KeyStore interface {
	// Lookup returns the key, ErrUnknownKey or ErrKeyRevoked.
	Lookup(deviceID string, keyID uint32) (DeviceKey, error)
}

// MemoryKeyStore is an in-memory KeyStore. Production deployments would
// back KeyStore with an HSM or KMS; this type serves tests, the simulator
// and key files.
type MemoryKeyStore struct {
	mu   sync.RWMutex
	keys map[keyRef]DeviceKey
}

type keyRef struct {
	dev string
	id  uint32
}

// NewMemoryKeyStore returns an empty key store.
func NewMemoryKeyStore() *MemoryKeyStore { return &MemoryKeyStore{keys: map[keyRef]DeviceKey{}} }

// Put adds or replaces a key version.
func (m *MemoryKeyStore) Put(k DeviceKey) error {
	if k.DeviceID == "" || k.KeyID == 0 || k.Secret.IsZero() {
		return errors.New("security: key needs a device id, a non-zero key id and a non-zero secret")
	}
	if k.State == 0 {
		k.State = KeyActive
	}
	m.mu.Lock()
	m.keys[keyRef{k.DeviceID, k.KeyID}] = k
	m.mu.Unlock()
	return nil
}

// SetState changes the state of an existing key version (rotation,
// revocation).
func (m *MemoryKeyStore) SetState(deviceID string, keyID uint32, st KeyState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k, ok := m.keys[keyRef{deviceID, keyID}]
	if !ok {
		return ErrUnknownKey
	}
	k.State = st
	m.keys[keyRef{deviceID, keyID}] = k
	return nil
}

// Lookup implements KeyStore.
func (m *MemoryKeyStore) Lookup(deviceID string, keyID uint32) (DeviceKey, error) {
	m.mu.RLock()
	k, ok := m.keys[keyRef{deviceID, keyID}]
	m.mu.RUnlock()
	switch {
	case !ok:
		return DeviceKey{}, ErrUnknownKey
	case k.State == KeyRevoked:
		return DeviceKey{}, ErrKeyRevoked
	}
	return k, nil
}

// keyFile is the on-disk format of a device key file:
//
//	{"devices": [{"device_id": "SIM-1", "key_id": 1, "secret_hex": "<64 hex>", "state": "active"}]}
//
// Key files are secrets: keep them out of version control and readable
// only by the service account.
type keyFile struct {
	Devices []struct {
		DeviceID  string `json:"device_id"`
		KeyID     uint32 `json:"key_id"`
		SecretHex string `json:"secret_hex"`
		State     string `json:"state"`
	} `json:"devices"`
}

// LoadKeyFile reads a device key file into a new MemoryKeyStore.
func LoadKeyFile(path string) (*MemoryKeyStore, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("security: read key file: %w", err)
	}
	var f keyFile
	if err := json.Unmarshal(b, &f); err != nil {
		return nil, errors.New("security: key file is not valid JSON") // never echo the content
	}
	ks := NewMemoryKeyStore()
	for i, d := range f.Devices {
		sec, err := ParseSecretHex(d.SecretHex)
		if err != nil {
			return nil, fmt.Errorf("security: key file entry %d (%s/%d): %w", i, d.DeviceID, d.KeyID, err)
		}
		st := KeyActive
		switch d.State {
		case "", "active":
		case "retired":
			st = KeyRetired
		case "revoked":
			st = KeyRevoked
		default:
			return nil, fmt.Errorf("security: key file entry %d: unknown state %q", i, d.State)
		}
		if err := ks.Put(DeviceKey{DeviceID: d.DeviceID, KeyID: d.KeyID, Secret: sec, State: st}); err != nil {
			return nil, fmt.Errorf("security: key file entry %d: %w", i, err)
		}
	}
	return ks, nil
}

// WriteKeyFile writes keys in the key file format with 0600 permissions.
// It is used by the simulator and demos to provision test devices.
func WriteKeyFile(path string, keys []DeviceKey) error {
	var f keyFile
	for _, k := range keys {
		f.Devices = append(f.Devices, struct {
			DeviceID  string `json:"device_id"`
			KeyID     uint32 `json:"key_id"`
			SecretHex string `json:"secret_hex"`
			State     string `json:"state"`
		}{k.DeviceID, k.KeyID, hex.EncodeToString(k.Secret[:]), k.State.String()})
	}
	b, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o600)
}
