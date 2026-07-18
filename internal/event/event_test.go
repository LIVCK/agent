package event

import (
	"testing"
	"time"

	"github.com/LIVCK/agent/internal/platform/platformtest"
	"github.com/LIVCK/agent/pkg/wire"
)

func newQueue(capacity int) *Queue {
	clk := platformtest.NewClock(time.Unix(1_700_000_000, 0))
	n := 0
	return NewQueue(clk, func() string { n++; return "id-" + string(rune('a'+n%26)) + string(rune('0'+n/26%10)) }, capacity)
}

func TestQueueEmitPeekRemove(t *testing.T) {
	q := newQueue(16)
	q.Emit(wire.EventType_EVENT_TYPE_BOOT, map[string]string{"boot_id": "x"})
	q.Emit(wire.EventType_EVENT_TYPE_OOM_KILL, map[string]string{"count": "1"})
	if q.Len() != 2 {
		t.Fatalf("want 2 pending, got %d", q.Len())
	}
	peek := q.Peek(0)
	if len(peek) != 2 || peek[0].Type != wire.EventType_EVENT_TYPE_BOOT {
		t.Fatalf("peek order wrong: %+v", peek)
	}
	if peek[0].ID == "" || peek[0].OccurredAtUnixMs == 0 {
		t.Fatal("emitted event missing id or timestamp")
	}
	q.Remove([]string{peek[0].ID})
	if q.Len() != 1 {
		t.Fatalf("want 1 after remove, got %d", q.Len())
	}
	if q.Peek(0)[0].Type != wire.EventType_EVENT_TYPE_OOM_KILL {
		t.Fatal("wrong event remained after remove")
	}
}

func TestQueueOverflowDropsOldest(t *testing.T) {
	q := newQueue(3)
	for i := 0; i < 5; i++ {
		q.Emit(wire.EventType_EVENT_TYPE_BOOT, nil)
	}
	if q.Len() != 3 {
		t.Fatalf("want cap 3, got %d", q.Len())
	}
}

func TestQueueSnapshotRestore(t *testing.T) {
	q := newQueue(16)
	q.Emit(wire.EventType_EVENT_TYPE_CLEAN_SHUTDOWN, map[string]string{"type": "poweroff"})
	snap := q.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("want 1 snapshot event, got %d", len(snap))
	}

	q2 := newQueue(16)
	q2.Restore(snap)
	if q2.Len() != 1 {
		t.Fatalf("restore lost events, len=%d", q2.Len())
	}
	if q2.Peek(0)[0].Meta["type"] != "poweroff" {
		t.Fatal("restore lost meta")
	}
}

func TestWireRoundTrip(t *testing.T) {
	e := Event{ID: "e1", Type: wire.EventType_EVENT_TYPE_BOOT, OccurredAtUnixMs: 123, Meta: map[string]string{"k": "v"}}
	w := e.ToWire()
	back := FromWire(w)
	if back.ID != e.ID || back.Type != e.Type || back.OccurredAtUnixMs != e.OccurredAtUnixMs || back.Meta["k"] != "v" {
		t.Fatalf("wire round trip lost data: %+v", back)
	}
}
