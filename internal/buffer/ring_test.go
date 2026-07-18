package buffer

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/LIVCK/agent/internal/event"
	"github.com/LIVCK/agent/internal/platform/platformtest"
	"github.com/LIVCK/agent/pkg/wire"
)

func testClock() *platformtest.Clock {
	return platformtest.NewClock(time.Unix(1_700_000_000, 0))
}

func report(sampledAtMs int64, keys int) *wire.Report {
	m := make(map[string]float64, keys)
	for i := 0; i < keys; i++ {
		m["sys.disk.mount"+string(rune('a'+i%26))+".used_pct"] = float64(i)
	}
	return &wire.Report{SampledAtUnixMs: sampledAtMs, Metrics: m}
}

func TestRingNewestFirstReplay(t *testing.T) {
	clk := testClock()
	r := New(clk, nil, DefaultMaxBytes, DefaultHorizon)
	now := clk.Now().UnixMilli()
	r.Add(report(now+1, 2))
	r.Add(report(now+2, 2))
	r.Add(report(now+3, 2))

	reports, seqs := r.TakeBatch(2, 0)
	if len(reports) != 2 {
		t.Fatalf("want 2 reports, got %d", len(reports))
	}
	if reports[0].SampledAtUnixMs != now+3 || reports[1].SampledAtUnixMs != now+2 {
		t.Fatalf("want newest-first [now+3, now+2], got [%d, %d]",
			reports[0].SampledAtUnixMs, reports[1].SampledAtUnixMs)
	}
	if len(seqs) != 2 {
		t.Fatalf("want 2 seqs, got %d", len(seqs))
	}
}

func TestRingRemoveOnAck(t *testing.T) {
	clk := testClock()
	r := New(clk, nil, DefaultMaxBytes, DefaultHorizon)
	now := clk.Now().UnixMilli()
	for i := int64(1); i <= 3; i++ {
		r.Add(report(now+i, 2))
	}
	reports, seqs := r.TakeBatch(3, 0)
	if len(reports) != 3 {
		t.Fatalf("want 3, got %d", len(reports))
	}
	r.Remove(seqs)
	if r.Len() != 0 {
		t.Fatalf("want empty after ack, got %d", r.Len())
	}
	if r.Bytes() != 0 {
		t.Fatalf("want 0 bytes after ack, got %d", r.Bytes())
	}
	// A second take yields nothing.
	if reps, _ := r.TakeBatch(3, 0); len(reps) != 0 {
		t.Fatalf("want no reports after full ack, got %d", len(reps))
	}
}

func TestRingDropOldestCountsAndEvents(t *testing.T) {
	clk := testClock()
	q := event.NewQueue(clk, seqIDGen(), 64)
	// Tiny cap forces eviction after the second report.
	r := New(clk, q, 400, DefaultHorizon)
	now := clk.Now().UnixMilli()
	for i := int64(1); i <= 20; i++ {
		r.Add(report(now+i, 10))
	}
	if r.Bytes() > 400 {
		t.Fatalf("ring exceeded byte cap: %d > 400", r.Bytes())
	}
	if r.DroppedTotal() == 0 {
		t.Fatal("expected drops under a tiny cap")
	}
	if q.Len() == 0 {
		t.Fatal("expected buffer_overflow events emitted")
	}
	for _, e := range q.Peek(0) {
		if e.Type != wire.EventType_EVENT_TYPE_BUFFER_OVERFLOW {
			t.Fatalf("want buffer_overflow event, got %v", e.Type)
		}
		if e.Meta["dropped_count"] == "" {
			t.Fatal("buffer_overflow event missing dropped_count meta")
		}
	}
}

func TestRingTimeHorizonPurge(t *testing.T) {
	clk := testClock()
	r := New(clk, nil, DefaultMaxBytes, 60*time.Minute)
	base := clk.Now().UnixMilli()
	r.Add(report(base, 2)) // now
	// Advance past the horizon and add a fresh report; the old one is purged.
	clk.Advance(61 * time.Minute)
	r.Add(report(clk.Now().UnixMilli(), 2))
	if r.Len() != 1 {
		t.Fatalf("want 1 report after horizon purge, got %d", r.Len())
	}
	if r.DroppedTotal() != 1 {
		t.Fatalf("want 1 dropped by horizon, got %d", r.DroppedTotal())
	}
}

// TestRingByteCapProperty is the property test: after any sequence of Adds the
// buffered bytes never exceed the cap (given each report is smaller than the
// cap).
func TestRingByteCapProperty(t *testing.T) {
	clk := testClock()
	const cap = 8 * 1024
	r := New(clk, nil, cap, DefaultHorizon)
	rng := rand.New(rand.NewPCG(1, 2))
	now := clk.Now().UnixMilli()
	for i := 0; i < 2000; i++ {
		keys := 1 + rng.IntN(20) // bounded so a single report stays well under cap
		r.Add(report(now+int64(i), keys))
		if r.Bytes() > cap {
			t.Fatalf("iteration %d: bytes %d exceed cap %d", i, r.Bytes(), cap)
		}
	}
	if r.DroppedTotal() == 0 {
		t.Fatal("expected the property workload to force at least one drop")
	}
}

// seqIDGen returns a deterministic id generator for tests.
func seqIDGen() event.IDGen {
	n := 0
	return func() string {
		n++
		return "id-" + string(rune('0'+n%10)) + string(rune('a'+n/10%26))
	}
}
