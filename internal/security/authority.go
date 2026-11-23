package security

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
)

// The command authority signs every vehicle command. Devices verify the
// signature with a provisioned public key before acting, so the transport
// key (shared by the device and the gateways) is not enough to authorize an
// actuation: an attacker who extracts a device's transport key, or controls
// a gateway, can disrupt delivery but cannot make a device execute a
// command the authority did not sign.
//
// Ed25519 (RFC 8032, crypto/ed25519) is used as-is. Signed statements are
// prefixed with a fixed context string so an authority signature can never
// be valid for another protocol or purpose.

// authorityContext is prepended to every signed statement.
const authorityContext = "fleetcore/v2/command-authorization\x00"

// SignatureSize is the size of an authority signature.
const SignatureSize = ed25519.SignatureSize

// Authority errors.
var (
	ErrUnknownAuthorityKey = errors.New("security: unknown command authority key")
	ErrBadSignature        = errors.New("security: command authority signature invalid")
)

// Authority holds a command authority signing key. Its private key is never
// formatted or logged.
type Authority struct {
	keyID uint32
	priv  ed25519.PrivateKey
}

// NewAuthority generates a fresh authority key with the given version.
func NewAuthority(keyID uint32) (*Authority, error) {
	if keyID == 0 {
		return nil, errors.New("security: authority key id must be non-zero")
	}
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("security: generate authority key: %w", err)
	}
	return &Authority{keyID: keyID, priv: priv}, nil
}

// KeyID is the authority key version carried with each signature.
func (a *Authority) KeyID() uint32 { return a.keyID }

// Public returns the verification key.
func (a *Authority) Public() ed25519.PublicKey { return a.priv.Public().(ed25519.PublicKey) }

// Sign signs a command authorization statement.
func (a *Authority) Sign(statement []byte) []byte {
	return ed25519.Sign(a.priv, withContext(statement))
}

func (a *Authority) String() string {
	return fmt.Sprintf("authority(key %d, private key %s)", a.keyID, redacted)
}
func (a *Authority) GoString() string { return a.String() }

func withContext(stmt []byte) []byte {
	m := make([]byte, 0, len(authorityContext)+len(stmt))
	return append(append(m, authorityContext...), stmt...)
}

// Verifier holds the authority public keys a device trusts, by version.
// Rotation adds a version; revocation removes one.
type Verifier struct {
	mu   sync.RWMutex
	keys map[uint32]ed25519.PublicKey
}

// NewVerifier returns a verifier trusting the given keys.
func NewVerifier(keys map[uint32]ed25519.PublicKey) *Verifier {
	v := &Verifier{keys: map[uint32]ed25519.PublicKey{}}
	for id, k := range keys {
		v.keys[id] = append(ed25519.PublicKey(nil), k...)
	}
	return v
}

// Revoke stops trusting a key version.
func (v *Verifier) Revoke(keyID uint32) {
	v.mu.Lock()
	delete(v.keys, keyID)
	v.mu.Unlock()
}

// Verify checks sig over statement with key version keyID.
func (v *Verifier) Verify(keyID uint32, statement, sig []byte) error {
	v.mu.RLock()
	pub, ok := v.keys[keyID]
	v.mu.RUnlock()
	if !ok {
		return ErrUnknownAuthorityKey
	}
	if len(sig) != SignatureSize || !ed25519.Verify(pub, withContext(statement), sig) {
		return ErrBadSignature
	}
	return nil
}

// authorityFile is the on-disk format of an authority private key:
//
//	{"key_id": 1, "seed_hex": "<64 hex>"}
type authorityFile struct {
	KeyID   uint32 `json:"key_id"`
	SeedHex string `json:"seed_hex"`
}

// WriteAuthorityFile writes the private key (0600) and returns the public
// key in hex for provisioning devices.
func WriteAuthorityFile(path string, a *Authority) (string, error) {
	b, err := json.Marshal(authorityFile{KeyID: a.keyID, SeedHex: hex.EncodeToString(a.priv.Seed())})
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, b, 0o600); err != nil {
		return "", err
	}
	return hex.EncodeToString(a.Public()), nil
}

// LoadAuthorityFile reads an authority private key file.
func LoadAuthorityFile(path string) (*Authority, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("security: read authority file: %w", err)
	}
	var f authorityFile
	if err := json.Unmarshal(b, &f); err != nil {
		return nil, errors.New("security: authority file is not valid JSON")
	}
	seed, err := hex.DecodeString(f.SeedHex)
	if err != nil || len(seed) != ed25519.SeedSize || f.KeyID == 0 {
		return nil, errors.New("security: authority file needs a non-zero key_id and a 32-byte seed")
	}
	return &Authority{keyID: f.KeyID, priv: ed25519.NewKeyFromSeed(seed)}, nil
}

// ParsePublicKeyHex decodes a hex Ed25519 public key.
func ParsePublicKeyHex(h string) (ed25519.PublicKey, error) {
	b, err := hex.DecodeString(h)
	if err != nil || len(b) != ed25519.PublicKeySize {
		return nil, errors.New("security: an authority public key is 32 hex-encoded bytes")
	}
	return ed25519.PublicKey(b), nil
}
