package security

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// RFC 5869 Appendix A, test cases 1 and 3.
func TestHKDFRFC5869Vectors(t *testing.T) {
	ikm := bytes.Repeat([]byte{0x0b}, 22)
	cases := []struct {
		name             string
		salt, info, want string
	}{
		{"A.1", "000102030405060708090a0b0c", "f0f1f2f3f4f5f6f7f8f9",
			"3cb25f25faacd57a90434f64d0362f2a2d2d0a90cf1a5a4c5db02d56ecc4c5bf34007208d5b887185865"},
		{"A.3", "", "",
			"8da4e775a563c18f715f802a063c5a31b8a11f5c5ee1879ec3454e5f3c738d2d9d201395faa4b61a96c8"},
	}
	for _, c := range cases {
		got, err := HKDF(ikm, mustHex(t, c.salt), mustHex(t, c.info), 42)
		if err != nil {
			t.Fatal(err)
		}
		if hex.EncodeToString(got) != c.want {
			t.Errorf("%s: got %x", c.name, got)
		}
	}
	if _, err := HKDF(ikm, nil, nil, 255*32+1); err == nil {
		t.Error("oversize HKDF output accepted")
	}
}

func TestMessageKeySeparation(t *testing.T) {
	var m1, m2 Secret
	m1[0], m2[0] = 1, 2
	base := MessageKey(&m1, Uplink, 0x10, "DEV-1", 1, 7)
	if base != MessageKey(&m1, Uplink, 0x10, "DEV-1", 1, 7) {
		t.Fatal("derivation is not deterministic")
	}
	variants := map[string][32]byte{
		"master":    MessageKey(&m2, Uplink, 0x10, "DEV-1", 1, 7),
		"direction": MessageKey(&m1, Downlink, 0x10, "DEV-1", 1, 7),
		"purpose":   MessageKey(&m1, Uplink, 0x11, "DEV-1", 1, 7),
		"device":    MessageKey(&m1, Uplink, 0x10, "DEV-2", 1, 7),
		"key id":    MessageKey(&m1, Uplink, 0x10, "DEV-1", 2, 7),
		"session":   MessageKey(&m1, Uplink, 0x10, "DEV-1", 1, 8),
		// Length-prefixing the device id keeps ("DEV-1", gen) and a device id
		// that absorbs gen bytes distinct.
		"device prefix": MessageKey(&m1, Uplink, 0x10, "DEV-", 1, 7),
	}
	for name, k := range variants {
		if k == base {
			t.Errorf("changing the %s does not change the key", name)
		}
	}
}

func TestSecretIsRedacted(t *testing.T) {
	s, err := GenerateSecret()
	if err != nil {
		t.Fatal(err)
	}
	h := hex.EncodeToString(s[:])
	js, _ := json.Marshal(struct{ K Secret }{s})
	for _, out := range []string{fmt.Sprint(s), fmt.Sprintf("%v %+v %#v %s %x %q", s, s, s, s, s, s), string(js),
		fmt.Sprint(DeviceKey{DeviceID: "d", KeyID: 1, Secret: s})} {
		if strings.Contains(out, h) || strings.Contains(out, h[:16]) {
			t.Fatalf("secret leaked: %s", out)
		}
		if !strings.Contains(out, redacted) {
			t.Fatalf("no redaction marker in %q", out)
		}
	}
	var back Secret
	if err := json.Unmarshal(js, &struct{ K *Secret }{&back}); err == nil {
		t.Fatal("secret decoded from JSON")
	}
}

func TestKeyStoreStatesAndFile(t *testing.T) {
	s1, _ := GenerateSecret()
	s2, _ := GenerateSecret()
	path := filepath.Join(t.TempDir(), "keys.json")
	if err := WriteKeyFile(path, []DeviceKey{
		{DeviceID: "D1", KeyID: 1, Secret: s1, State: KeyRetired},
		{DeviceID: "D1", KeyID: 2, Secret: s2, State: KeyActive},
	}); err != nil {
		t.Fatal(err)
	}
	ks, err := LoadKeyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	k, err := ks.Lookup("D1", 1)
	if err != nil || !k.Secret.Equal(s1) || k.State != KeyRetired {
		t.Fatalf("retired key: %v %v", k.State, err)
	}
	if _, err := ks.Lookup("D1", 3); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("unknown key: %v", err)
	}
	if _, err := ks.Lookup("D2", 2); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("other device: %v", err)
	}
	if err := ks.SetState("D1", 1, KeyRevoked); err != nil {
		t.Fatal(err)
	}
	if _, err := ks.Lookup("D1", 1); !errors.Is(err, ErrKeyRevoked) {
		t.Fatalf("revoked key: %v", err)
	}
	// Malformed files fail without echoing their content.
	os.WriteFile(path, []byte(`{"devices":[{"device_id":"D","key_id":1,"secret_hex":"abcd"}]}`), 0o600)
	if _, err := LoadKeyFile(path); err == nil || strings.Contains(err.Error(), "abcd") {
		t.Fatalf("short secret: %v", err)
	}
	os.WriteFile(path, []byte(`not json 0123456789abcdef`), 0o600)
	if _, err := LoadKeyFile(path); err == nil || strings.Contains(err.Error(), "0123456789abcdef") {
		t.Fatalf("bad json: %v", err)
	}
	if _, err := ParseSecretHex(strings.Repeat("00", 32)); err == nil {
		t.Fatal("zero secret accepted")
	}
}

func TestAuthoritySignVerify(t *testing.T) {
	a, err := NewAuthority(1)
	if err != nil {
		t.Fatal(err)
	}
	other, _ := NewAuthority(1)
	v := NewVerifier(map[uint32]ed25519.PublicKey{1: a.Public()})
	stmt := []byte("command statement")
	sig := a.Sign(stmt)
	if err := v.Verify(1, stmt, sig); err != nil {
		t.Fatal(err)
	}
	if err := v.Verify(1, []byte("command statemenT"), sig); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("tampered statement: %v", err)
	}
	bad := append([]byte(nil), sig...)
	bad[10] ^= 1
	if err := v.Verify(1, stmt, bad); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("tampered signature: %v", err)
	}
	if err := v.Verify(1, stmt, other.Sign(stmt)); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("other authority: %v", err)
	}
	if err := v.Verify(2, stmt, sig); !errors.Is(err, ErrUnknownAuthorityKey) {
		t.Fatalf("unknown key id: %v", err)
	}
	if err := v.Verify(1, stmt, sig[:10]); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("short signature: %v", err)
	}
	v.Revoke(1)
	if err := v.Verify(1, stmt, sig); !errors.Is(err, ErrUnknownAuthorityKey) {
		t.Fatalf("revoked key: %v", err)
	}
	// File round trip; the private key never appears in String.
	path := filepath.Join(t.TempDir(), "authority.json")
	pubHex, err := WriteAuthorityFile(path, a)
	if err != nil {
		t.Fatal(err)
	}
	b, err := LoadAuthorityFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pub, _ := ParsePublicKeyHex(pubHex)
	if err := NewVerifier(map[uint32]ed25519.PublicKey{1: pub}).Verify(1, stmt, b.Sign(stmt)); err != nil {
		t.Fatal(err)
	}
	if s := fmt.Sprintf("%v %#v", a, a); strings.Contains(s, hex.EncodeToString(a.priv.Seed())) || !strings.Contains(s, redacted) {
		t.Fatalf("authority formatting: %s", s)
	}
}

func TestReplayWindow(t *testing.T) {
	var w ReplayWindow
	if w.Accept(0) {
		t.Fatal("counter 0 accepted")
	}
	for _, c := range []uint64{1, 2, 5, 3} { // reordered
		if !w.Accept(c) {
			t.Fatalf("fresh counter %d rejected", c)
		}
	}
	for _, c := range []uint64{1, 2, 3, 5} {
		if w.Accept(c) {
			t.Fatalf("replayed counter %d accepted", c)
		}
	}
	if !w.Accept(4) {
		t.Fatal("late but fresh counter rejected")
	}
	if !w.Accept(5 + WindowSize - 1) {
		t.Fatal("jump rejected")
	}
	if w.Check(5) || w.Check(4) {
		t.Fatal("counter now outside the window accepted")
	}
	if !w.Check(6 + 1) {
		t.Fatal("unseen counter inside the window rejected")
	}
	if !w.Accept(1 << 40) {
		t.Fatal("large jump rejected")
	}
	if w.Check(5+WindowSize-1) || !w.Check(1<<40-1) || w.Check(1<<40) {
		t.Fatal("window not reset correctly after a large jump")
	}
	// Check never mutates.
	var x ReplayWindow
	x.Check(9)
	if x.Top() != 0 {
		t.Fatal("Check mutated the window")
	}
}

// Every counter is accepted exactly once, whatever the arrival order, as
// long as it stays inside the window.
func TestReplayWindowExhaustive(t *testing.T) {
	var w ReplayWindow
	seen := map[uint64]bool{}
	ctr := uint64(0)
	for i := 0; i < 20000; i++ {
		ctr += uint64(i%7) + 1
		c := ctr
		if i%5 == 0 && c > 600 {
			c -= 600 // reordered, inside the window
		}
		got := w.Accept(c)
		if got == seen[c] {
			t.Fatalf("counter %d: accept=%v, seen before=%v", c, got, seen[c])
		}
		seen[c] = true
	}
}
