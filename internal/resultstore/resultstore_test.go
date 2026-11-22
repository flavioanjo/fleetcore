package resultstore

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/flavioanjo/fleetcore/internal/chrono"
	"github.com/flavioanjo/fleetcore/internal/fusion"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

var t0 = time.Date(2026, 6, 1, 8, 0, 0, 0, time.UTC)

func open(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "results.db")
	s, err := Open(context.Background(), path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, path
}

func estimate(dev string, trip telemetry.Millimetres) fusion.Report {
	return fusion.Report{Type: fusion.ReportEstimate, Estimate: &fusion.Estimate{
		DeviceID: telemetry.DeviceID(dev), BootID: 1, To: t0, TripMM: trip,
		Confidence: fusion.ConfidenceHigh, State: fusion.StateBounded,
		Diagnostics: []fusion.Diagnostic{{Code: fusion.CodeCANReconciled, Level: fusion.LevelInfo, Message: "x"}},
	}}
}

func eventStep(step, pos int64, dev string, flags chrono.Flag, rs ...fusion.Report) Step {
	sum, _, _ := HashReports(rs)
	return Step{Entry: Entry{Step: step, Kind: KindEvent, Position: pos, DeviceID: telemetry.DeviceID(dev), BootID: 1,
		Sequence: telemetry.Sequence(pos), ExtSequence: uint64(pos), DeviceTime: t0, Flags: flags, OutputHash: sum}, Reports: rs}
}

func TestCommitJournalAndQuery(t *testing.T) {
	ctx := context.Background()
	s, _ := open(t)
	if _, err := s.Claim(ctx, "me", "cfg", false); err != nil {
		t.Fatal(err)
	}
	steps := []Step{
		eventStep(1, 1, "a", 0, estimate("a", 100)),
		eventStep(2, 2, "b", chrono.FlagLate, estimate("b", 7)),
		eventStep(3, 3, "a", 0, estimate("a", 250)),
		{Entry: Entry{Step: 4, Kind: KindTick, TickAt: t0}},
	}
	steps[3].Entry.OutputHash, _, _ = HashReports(nil)
	if err := s.Commit(ctx, "me", steps, 3, 3); err != nil {
		t.Fatal(err)
	}
	latest, err := s.LatestEstimates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(latest) != 2 || latest[0].DeviceID != "a" || latest[0].TripMM != 250 || latest[1].TripMM != 7 {
		t.Fatalf("latest %+v", latest)
	}
	c, err := s.Count(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if c.Steps != 4 || c.EventSteps != 3 || c.TickSteps != 1 || c.Estimates != 3 ||
		c.Diagnostics["chronology:late"] != 1 || c.Diagnostics["fusion:can_reconciled"] != 3 {
		t.Fatalf("counts %+v", c)
	}
	es, err := s.Journal(ctx, 1, 10)
	if err != nil || len(es) != 3 || es[0].Flags != chrono.FlagLate || es[2].Kind != KindTick || !es[2].TickAt.Equal(t0) {
		t.Fatalf("journal %+v %v", es, err)
	}
	st, _ := s.Claim(ctx, "me", "cfg", false)
	if st.NextStep != 5 || st.LowPosition != 3 {
		t.Fatalf("state %+v", st)
	}
}

func TestCommitRejectsGapsAndForeignOwner(t *testing.T) {
	ctx := context.Background()
	s, _ := open(t)
	s.Claim(ctx, "me", "cfg", false)
	if err := s.Commit(ctx, "me", []Step{eventStep(2, 1, "a", 0)}, 0, 1); err == nil {
		t.Fatal("step 2 accepted as the first step")
	}
	if err := s.Commit(ctx, "someone-else", []Step{eventStep(1, 1, "a", 0)}, 0, 1); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("foreign owner: %v", err)
	}
	if err := s.Commit(ctx, "me", []Step{eventStep(1, 1, "a", 0)}, 0, 1); err != nil {
		t.Fatal(err)
	}
	// The same event position cannot be journaled twice.
	if err := s.Commit(ctx, "me", []Step{eventStep(2, 1, "a", 0)}, 0, 1); err == nil {
		t.Fatal("a position was journaled twice")
	}
}

func TestJournalIsImmutable(t *testing.T) {
	ctx := context.Background()
	s, path := open(t)
	s.Claim(ctx, "me", "cfg", false)
	if err := s.Commit(ctx, "me", []Step{eventStep(1, 1, "a", 0, estimate("a", 1))}, 1, 1); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, q := range []string{`UPDATE journal SET position = 9`, `UPDATE journal SET flags = 1`, `DELETE FROM journal`} {
		if _, err := db.Exec(q); err == nil {
			t.Errorf("%s succeeded", q)
		}
	}
	if _, err := db.Exec(`UPDATE journal SET output_hash = zeroblob(32)`); err != nil {
		t.Errorf("output_hash must stay rewritable for rebuilds: %v", err)
	}
}

func TestConfigChangeNeedsRebuild(t *testing.T) {
	ctx := context.Background()
	s, _ := open(t)
	s.Claim(ctx, "me", "cfg-1", false)
	s.Commit(ctx, "me", []Step{eventStep(1, 1, "a", 0)}, 1, 1)
	if _, err := s.Claim(ctx, "me", "cfg-2", false); !errors.Is(err, ErrConfigChanged) {
		t.Fatalf("claim with another config: %v", err)
	}
	if _, err := s.Claim(ctx, "me", "cfg-2", true); err != nil {
		t.Fatalf("claim for rebuild: %v", err)
	}
}
