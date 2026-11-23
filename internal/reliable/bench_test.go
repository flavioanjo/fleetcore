package reliable

import (
	"testing"

	"github.com/flavioanjo/fleetcore/internal/security"
)

// BenchmarkSealOpen measures framing + HKDF key derivation + HMAC on both
// sides for one event upload.
func BenchmarkSealOpen(b *testing.B) {
	ks := keyStore(b, "DEV-1")
	pkt := uploadFrame(b, ks, "DEV-1", 5, 9, sampleBodies()[0])
	b.ReportAllocs()
	b.SetBytes(int64(len(pkt)))
	for i := 0; i < b.N; i++ {
		if _, err := Open(pkt, security.Uplink, ks); err != nil {
			b.Fatal(err)
		}
	}
}
