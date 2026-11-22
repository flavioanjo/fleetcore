package chrono

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// FuzzOrderer feeds arbitrary arrival patterns (several devices and boots,
// sequence numbers near the wrap point, clock jumps, repeats, interleaved
// releases) and checks the orderer's invariants:
//   - every added event is released exactly once (after a final force);
//   - within a session, events not flagged late, reused or ambiguous leave
//     in strictly increasing extended sequence order;
//   - the output is a deterministic function of the input.
func FuzzOrderer(f *testing.F) {
	f.Add([]byte{0, 0, 255, 255, 255, 250, 1, 0, 0, 255, 255, 255, 255, 2, 0, 0, 0, 0, 0, 0, 3})
	f.Add([]byte{1, 1, 0, 0, 0, 5, 9, 0, 2, 0, 0, 0, 4, 1, 0, 1, 0, 0, 0, 6, 200})
	f.Fuzz(func(t *testing.T, data []byte) {
		// 9 sessions are possible (3 devices x 3 boots). With room for all
		// of them, session state is never evicted and the ordering
		// invariant must hold; with 4, eviction happens and only delivery
		// and determinism are checked (an evicted session restarts its
		// ordering state, which is documented).
		for _, maxSessions := range []int{16, 4} {
			a, added := runOrderer(data, maxSessions)
			b, _ := runOrderer(data, maxSessions)
			if fmt.Sprint(a) != fmt.Sprint(b) {
				t.Fatalf("MaxSessions %d: orderer is not deterministic", maxSessions)
			}
			seen := map[int64]int{}
			last := map[sessionKey]uint64{}
			for _, r := range a {
				seen[r.Position]++
				k := sessionKey{r.Sample.DeviceID, r.Sample.BootID}
				if maxSessions < 9 || r.Flags&(FlagLate|FlagSequenceReused|FlagSequenceAmbiguous) != 0 {
					continue
				}
				if prev, ok := last[k]; ok && r.ExtSequence <= prev {
					t.Fatalf("session %v: ext %d released after %d without a late/reused flag", k, r.ExtSequence, prev)
				}
				last[k] = r.ExtSequence
			}
			if len(seen) != added {
				t.Fatalf("MaxSessions %d: %d distinct events released, %d added", maxSessions, len(seen), added)
			}
			for p, n := range seen {
				if n != 1 {
					t.Fatalf("MaxSessions %d: position %d released %d times", maxSessions, p, n)
				}
			}
		}
	})
}

// runOrderer interprets data as records of 7 bytes:
// device(1) boot(1) seq(4) deviceTimeDelta(1, signed seconds); a record
// whose device byte has the top bit set triggers a Release instead.
func runOrderer(data []byte, maxSessions int) ([]Released, int) {
	o := New(Config{Hold: 2 * time.Second, MaxHeld: 16, MaxSessions: maxSessions})
	var out []Released
	now := t0
	devTime := t0
	pos := int64(0)
	added := 0
	for len(data) >= 7 {
		r := data[:7]
		data = data[7:]
		now = now.Add(500 * time.Millisecond)
		if r[0]&0x80 != 0 {
			out = append(out, o.Release(now.Add(time.Duration(r[1])*100*time.Millisecond), false)...)
			continue
		}
		devTime = devTime.Add(time.Duration(int8(r[6])) * time.Second)
		pos++
		added++
		o.Add(Item{Position: pos, SeenAt: now, Sample: telemetry.Sample{
			DeviceID:   telemetry.DeviceID([]string{"a", "b", "c"}[r[0]%3]),
			BootID:     telemetry.BootID(r[1]%3 + 1),
			Sequence:   telemetry.Sequence(binary.BigEndian.Uint32(r[2:6])),
			DeviceTime: devTime,
		}})
	}
	out = append(out, o.Release(now, true)...)
	if o.Held() != 0 {
		panic("force left events held")
	}
	return out, added
}
