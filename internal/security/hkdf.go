// Package security holds the cryptographic building blocks of the FleetCore
// reliable communications protocol (protocol v2, internal/reliable):
//
//   - HKDF-SHA256 key derivation (RFC 5869), used to derive a separate
//     message-authentication key per device key, direction, message type,
//     key version and device session.
//   - Device key material and a key store with rotation states.
//   - The command authority: Ed25519 signatures that authorize vehicle
//     commands independently of the transport keys.
//   - A sliding replay window for per-session message counters.
//
// Nothing here is new cryptography. The primitives are the Go standard
// library's crypto/hmac, crypto/sha256 and crypto/ed25519; HKDF is the RFC
// 5869 construction over HMAC (crypto/hkdf only exists from Go 1.24, and
// the module's declared minimum is Go 1.23), verified against the RFC test
// vectors.
//
// Secrets are never logged: Secret's String, GoString and MarshalJSON
// return a redacted placeholder.
package security

import (
	"crypto/hmac"
	"crypto/sha256"
	"errors"
)

// hkdfExtract is HKDF-Extract(salt, IKM) = HMAC-SHA256(salt, IKM).
func hkdfExtract(salt, ikm []byte) []byte {
	if len(salt) == 0 {
		salt = make([]byte, sha256.Size)
	}
	m := hmac.New(sha256.New, salt)
	m.Write(ikm)
	return m.Sum(nil)
}

// errHKDFLength is returned for an output longer than 255 hash blocks.
var errHKDFLength = errors.New("security: HKDF output too long")

// hkdfExpand is HKDF-Expand(PRK, info, L).
func hkdfExpand(prk, info []byte, length int) ([]byte, error) {
	if length < 0 || length > 255*sha256.Size {
		return nil, errHKDFLength
	}
	out := make([]byte, 0, length)
	var t []byte
	m := hmac.New(sha256.New, prk)
	for i := byte(1); len(out) < length; i++ {
		m.Reset()
		m.Write(t)
		m.Write(info)
		m.Write([]byte{i})
		t = m.Sum(nil)
		out = append(out, t...)
	}
	return out[:length], nil
}

// HKDF derives length bytes from ikm with HKDF-SHA256 (RFC 5869).
func HKDF(ikm, salt, info []byte, length int) ([]byte, error) {
	return hkdfExpand(hkdfExtract(salt, ikm), info, length)
}
