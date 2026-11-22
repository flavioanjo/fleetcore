package sessions

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

var t0 = time.Date(2026, 3, 14, 9, 0, 0, 0, time.UTC)

// smp builds a sample whose device time is t0+devSec and ingestion time
// t0+ingSec.
func smp(dev string, boot telemetry.BootID, seq telemetry.Sequence, devSec, ingSec int) telemetry.Sample {
	return telemetry.Sample{
		DeviceID:   telemetry.DeviceID(dev),
		EventID:    telemetry.EventID(uint64(boot)<<32 | uint64(seq) | 1<<63),
		BootID:     boot,
		Sequence:   seq,
		DeviceTime: t0.Add(time.Duration(devSec) * time.Second),
		IngestedAt: telemetry.Some(t0.Add(time.Duration(ingSec) * time.Second)),
		Source:     telemetry.SourceGPS,
		Quality:    telemetry.QualityUnavailable,
	}
}

func observe(t *testing.T, r *Registry, s telemetry.Sample) Observation {
	t.Helper()
	o, err := r.Observe(s)
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	return o
}

func kinds(o Observation) []Kind {
	var ks []Kind
	for _, d := range o.Diagnostics {
		ks = append(ks, d.Kind)
	}
	return ks
}

func expectKinds(t *testing.T, o Observation, want ...Kind) {
	t.Helper()
	got := kinds(o)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("diagnostics %v, want %v (%+v)", got, want, o.Diagnostics)
	}
}

func TestInOrderStream(t *testing.T) {
	r := New(Config{})
	expectKinds(t, observe(t, r, smp("d", 7, 1, 0, 0)), NewSession)
	for i := 2; i <= 100; i++ {
		expectKinds(t, observe(t, r, smp("d", 7, telemetry.Sequence(i), i, i)))
	}
	st := r.Sessions("d")[0]
	if st.Received != 100 || st.Missing != 0 || st.Lowest != 1 || st.Highest != 100 {
		t.Fatalf("state %+v", st)
	}
}

func TestGapThenLateFill(t *testing.T) {
	r := New(Config{})
	observe(t, r, smp("d", 7, 10, 0, 0))
	o := observe(t, r, smp("d", 7, 14, 4, 4))
	expectKinds(t, o, SequenceGap)
	if o.Diagnostics[0].Count != 3 || o.Diagnostics[0].Expected != 11 || o.Session.Missing != 3 {
		t.Fatalf("gap %+v, missing %d", o.Diagnostics[0], o.Session.Missing)
	}
	o = observe(t, r, smp("d", 7, 12, 2, 5))
	expectKinds(t, o, OutOfOrder)
	if o.Session.Missing != 2 {
		t.Fatalf("missing %d after fill, want 2", o.Session.Missing)
	}
	// Before the first observed sample: extends the session start.
	o = observe(t, r, smp("d", 7, 8, -2, 6))
	expectKinds(t, o, OutOfOrder)
	if o.Session.Lowest != 8 || o.Session.Missing != 3 { // 9, 11, 13
		t.Fatalf("state %+v", o.Session)
	}
}

func TestSequenceReuse(t *testing.T) {
	r := New(Config{})
	observe(t, r, smp("d", 7, 1, 0, 0))
	observe(t, r, smp("d", 7, 2, 1, 1))
	expectKinds(t, observe(t, r, smp("d", 7, 2, 1, 2)), SequenceReuse)
	expectKinds(t, observe(t, r, smp("d", 7, 1, 0, 3)), SequenceReuse)
	if st := r.Sessions("d")[0]; st.Received != 2 || st.Reused != 2 {
		t.Fatalf("state %+v", st)
	}
}

func TestSequenceWindowAndWraparound(t *testing.T) {
	r := New(Config{SequenceWindow: 64})
	observe(t, r, smp("d", 7, math.MaxUint32-1, 0, 0))
	expectKinds(t, observe(t, r, smp("d", 7, math.MaxUint32, 1, 1)))
	expectKinds(t, observe(t, r, smp("d", 7, 0, 2, 2))) // wraps: in order
	expectKinds(t, observe(t, r, smp("d", 7, 1, 3, 3)))
	for i := 2; i < 200; i++ {
		observe(t, r, smp("d", 7, telemetry.Sequence(i), i+2, i+2))
	}
	// Sequence 5 is far outside the 64-sample window.
	expectKinds(t, observe(t, r, smp("d", 7, 5, 7, 300)), SequenceUnverifiable)
	// Inside the window, a repeat is still detected.
	expectKinds(t, observe(t, r, smp("d", 7, 190, 192, 301)), SequenceReuse)
}

func TestNewBootIsNotOrderedByBootID(t *testing.T) {
	r := New(Config{})
	observe(t, r, smp("d", 900, 1, 0, 0))
	observe(t, r, smp("d", 900, 2, 1, 1))
	// A numerically smaller BootID observed later, with later device time
	// and after the overlap window: a plain new session, not "older".
	o := observe(t, r, smp("d", 5, 1, 600, 600))
	expectKinds(t, o, NewSession)
	if o.Diagnostics[0].OtherBoot != 900 {
		t.Fatalf("NewSession.OtherBoot = %d", o.Diagnostics[0].OtherBoot)
	}
	// Delayed tail of the earlier session is kept and described, not
	// rejected or treated as a new session.
	o = observe(t, r, smp("d", 900, 3, 2, 601))
	expectKinds(t, o, EarlierObservedSession)
	if o.Diagnostics[0].OtherBoot != 5 || o.Session.Received != 3 {
		t.Fatalf("observation %+v", o)
	}
}

func TestAmbiguousChronology(t *testing.T) {
	t.Run("overlapping activity", func(t *testing.T) {
		r := New(Config{OverlapWindow: time.Minute})
		observe(t, r, smp("d", 1, 1, 0, 0))
		o := observe(t, r, smp("d", 2, 1, 10, 10))
		expectKinds(t, o, NewSession, ChronologyAmbiguous)
		if o.Diagnostics[1].OtherBoot != 1 {
			t.Fatalf("%+v", o.Diagnostics[1])
		}
	})
	t.Run("device clock contradicts arrival order", func(t *testing.T) {
		r := New(Config{OverlapWindow: time.Minute})
		observe(t, r, smp("d", 1, 1, 1000, 0))
		// Observed 10 min later, but its device time is earlier: either a
		// clock reset on boot or a delayed older session. Not decidable.
		expectKinds(t, observe(t, r, smp("d", 2, 1, 500, 600)), NewSession, ChronologyAmbiguous)
	})
}

func TestRetiredSessionLateEvent(t *testing.T) {
	r := New(Config{SessionIdleTimeout: time.Hour})
	observe(t, r, smp("d", 1, 1, 0, 0))
	observe(t, r, smp("d", 2, 1, 7200, 7200)) // session 1 idle > 1h: retired
	if n := len(r.Sessions("d")); n != 1 {
		t.Fatalf("%d sessions tracked, want 1", n)
	}
	o := observe(t, r, smp("d", 1, 2, 1, 7300))
	expectKinds(t, o, RetiredSession)
	if !o.Session.Reopened || o.Diagnostics[0].OtherBoot != 2 {
		t.Fatalf("observation %+v", o)
	}
	// Still the earlier-observed one afterwards.
	expectKinds(t, observe(t, r, smp("d", 1, 3, 2, 7301)), EarlierObservedSession)
}

func TestBoundedMemory(t *testing.T) {
	r := New(Config{MaxDevices: 3, MaxSessionsPerDevice: 2, MaxRetiredPerDevice: 2})
	for b := telemetry.BootID(1); b <= 5; b++ {
		observe(t, r, smp("d", b, 1, int(b)*1000, int(b)*1000))
	}
	if n := len(r.Sessions("d")); n != 2 {
		t.Fatalf("%d sessions, want 2", n)
	}
	// Boots 1..3 retired; ring keeps 2 and 3. Boot 1 is forgotten entirely.
	if k := kinds(observe(t, r, smp("d", 3, 2, 3001, 6000))); k[0] != RetiredSession {
		t.Fatalf("boot 3: %v, want retired_session first", k)
	}
	if k := kinds(observe(t, r, smp("d", 1, 2, 1001, 6001))); k[0] != NewSession {
		t.Fatalf("boot 1: %v, want new_session first", k)
	}

	for i := 0; i < 10; i++ {
		observe(t, r, smp(fmt.Sprintf("x%d", i), 1, 1, 7000+i, 7000+i))
	}
	st := r.Stats()
	if st.Devices != 3 || st.DevicesEvicted != 8 {
		t.Fatalf("stats %+v", st)
	}
	if st.Sessions != 3 {
		t.Fatalf("sessions %d, want 3", st.Sessions)
	}
}

func TestExpire(t *testing.T) {
	r := New(Config{SessionIdleTimeout: time.Hour, DeviceIdleTimeout: 2 * time.Hour})
	observe(t, r, smp("a", 1, 1, 0, 0))
	observe(t, r, smp("b", 1, 1, 3600, 3600))
	if n := r.Expire(t0.Add(2*time.Hour + time.Second)); n != 1 {
		t.Fatalf("expired %d, want 1", n)
	}
	if r.Sessions("a") != nil || r.Sessions("b") == nil {
		t.Fatal("wrong device expired")
	}
	if st := r.Stats(); st.Devices != 1 || st.Sessions != 1 || st.DevicesExpired != 1 {
		t.Fatalf("stats %+v", st)
	}
}

func TestIndependentDevices(t *testing.T) {
	r := New(Config{})
	observe(t, r, smp("a", 7, 1, 0, 0))
	// Same BootID and sequence on another device is unrelated.
	expectKinds(t, observe(t, r, smp("b", 7, 1, 0, 0)), NewSession)
	expectKinds(t, observe(t, r, smp("a", 7, 2, 1, 1)))
	expectKinds(t, observe(t, r, smp("b", 7, 3, 2, 2)), SequenceGap)
}

func TestObserveRequiresIngestionTime(t *testing.T) {
	s := smp("d", 1, 1, 0, 0)
	s.IngestedAt = telemetry.None[time.Time]()
	if _, err := New(Config{}).Observe(s); err == nil {
		t.Fatal("sample without IngestedAt accepted")
	}
}
