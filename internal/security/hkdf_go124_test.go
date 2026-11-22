//go:build go1.24

package security

import (
	"bytes"
	"crypto/hkdf"
	"crypto/sha256"
	"testing"
)

// When built with Go 1.24 or later, the local RFC 5869 implementation is
// cross-checked against the standard library's crypto/hkdf.
func TestHKDFMatchesStandardLibrary(t *testing.T) {
	for i := 0; i < 64; i++ {
		ikm := bytes.Repeat([]byte{byte(i)}, i+1)
		salt := bytes.Repeat([]byte{byte(255 - i)}, i%40)
		info := bytes.Repeat([]byte{byte(i * 3)}, i%17)
		n := 1 + i*7
		got, err := HKDF(ikm, salt, info, n)
		if err != nil {
			t.Fatal(err)
		}
		want, err := hkdf.Key(sha256.New, ikm, salt, string(info), n)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("case %d: local HKDF differs from crypto/hkdf", i)
		}
	}
}
