package buffer

import (
	"testing"

	"github.com/LIVCK/agent/internal/platform/platformtest"
	"github.com/LIVCK/agent/pkg/wire"
)

func TestSpoolRoundTrip(t *testing.T) {
	clk := testClock()
	fs := platformtest.NewMemFS()
	path := "/state/spool.pb"

	src := New(clk, nil, DefaultMaxBytes, DefaultHorizon)
	now := clk.Now().UnixMilli()
	for i := int64(1); i <= 5; i++ {
		src.Add(report(now+i, 3))
	}
	events := []*wire.Event{
		{EventId: "e1", Type: wire.EventType_EVENT_TYPE_BOOT, OccurredAtUnixMs: now},
	}

	sp := NewSpool(fs, path, DefaultMaxBytes)
	if err := sp.Save(src.Snapshot(), events); err != nil {
		t.Fatalf("save: %v", err)
	}
	if perm, ok := fs.Perm(path); !ok || perm != spoolPerm {
		t.Fatalf("spool file perm = %v (ok=%v), want 0600", perm, ok)
	}

	reports, gotEvents, err := sp.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(reports) != 5 {
		t.Fatalf("want 5 spooled reports, got %d", len(reports))
	}
	if len(gotEvents) != 1 || gotEvents[0].GetEventId() != "e1" {
		t.Fatalf("want event e1 restored, got %+v", gotEvents)
	}

	// Consume-once: a second load finds nothing.
	reports2, events2, err := sp.Load()
	if err != nil {
		t.Fatalf("second load: %v", err)
	}
	if len(reports2) != 0 || len(events2) != 0 {
		t.Fatal("spool was not consumed on load")
	}

	// Restore into a fresh ring preserves the reports.
	dst := New(clk, nil, DefaultMaxBytes, DefaultHorizon)
	dst.Restore(reports)
	if dst.Len() != 5 {
		t.Fatalf("want 5 reports after restore, got %d", dst.Len())
	}
}

func TestSpoolEmptyRemovesStaleFile(t *testing.T) {
	fs := platformtest.NewMemFS()
	path := "/state/spool.pb"
	_ = fs.WriteFileAtomic(path, []byte("stale"), 0o600)

	sp := NewSpool(fs, path, DefaultMaxBytes)
	if err := sp.Save(nil, nil); err != nil {
		t.Fatalf("save empty: %v", err)
	}
	if _, ok := fs.Perm(path); ok {
		t.Fatal("empty save should have removed the stale spool file")
	}
}

func TestSpoolMissingFileNoError(t *testing.T) {
	fs := platformtest.NewMemFS()
	sp := NewSpool(fs, "/state/absent.pb", DefaultMaxBytes)
	reports, events, err := sp.Load()
	if err != nil {
		t.Fatalf("missing spool should not error: %v", err)
	}
	if reports != nil || events != nil {
		t.Fatal("missing spool should return no data")
	}
}
