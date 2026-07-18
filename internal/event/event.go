// Package event holds the agent's lifecycle event queue and the Emitter
// interface that other foundation packages (config, buffer) push into. Events
// are the stronger delivery class: their dedupe anchor is event_id and pulse
// only marks an event delivered after a successful produce, so the agent keeps
// an event queued until the server acknowledges the batch that carried it
// (Peek/Remove, mirroring the report ring). The full event budget and
// coalescing rules (max 30/h, per-type caps) live in the lifecycle collectors
// that emit events; this package just provides a bounded queue with drop-oldest
// so a runaway emitter cannot grow memory without bound.
package event

import (
	"sync"

	"github.com/LIVCK/agent/internal/platform"
	"github.com/LIVCK/agent/pkg/wire"
)

// DefaultCapacity bounds the number of pending events held in memory. It is a
// safety cap, not the event budget: at steady state far fewer events exist.
const DefaultCapacity = 256

// Event is a pending lifecycle event before wire serialization. ID is a UUIDv4
// dedupe anchor; OccurredAtUnixMs is wall-clock at occurrence; Meta is the
// type-validated metadata map.
type Event struct {
	ID               string
	Type             wire.EventType
	OccurredAtUnixMs int64
	Meta             map[string]string
}

// Emitter is the narrow interface foundation packages use to raise a lifecycle
// event. config raises config_applied/config_error; buffer raises
// buffer_overflow; the sender raises clock_skew_detected.
type Emitter interface {
	Emit(t wire.EventType, meta map[string]string)
}

// IDGen returns a fresh UUIDv4. It is injected so tests get deterministic ids.
type IDGen func() string

// Queue is a bounded, FIFO event queue. It is safe for concurrent use. Emit
// assigns an id and occurrence time; Peek returns the oldest events without
// removing them; Remove drops acknowledged events by id. On overflow the oldest
// pending event is dropped (the newest lifecycle signal is the most relevant).
type Queue struct {
	mu    sync.Mutex
	clock platform.Clock
	newID IDGen
	cap   int
	items []Event
}

// NewQueue builds a queue with the given capacity (<=0 uses DefaultCapacity).
func NewQueue(clock platform.Clock, newID IDGen, capacity int) *Queue {
	if capacity <= 0 {
		capacity = DefaultCapacity
	}
	return &Queue{clock: clock, newID: newID, cap: capacity}
}

// Emit enqueues an event of type t with meta. The meta map is copied. If the
// queue is at capacity the oldest event is dropped first.
func (q *Queue) Emit(t wire.EventType, meta map[string]string) {
	e := Event{
		ID:               q.newID(),
		Type:             t,
		OccurredAtUnixMs: q.clock.Now().UnixMilli(),
		Meta:             copyMeta(meta),
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) >= q.cap {
		// Drop oldest.
		q.items = q.items[1:]
	}
	q.items = append(q.items, e)
}

// Peek returns up to max oldest events without removing them, in occurrence
// order. The caller sends them and calls Remove with the delivered ids.
func (q *Queue) Peek(max int) []Event {
	q.mu.Lock()
	defer q.mu.Unlock()
	if max <= 0 || max > len(q.items) {
		max = len(q.items)
	}
	out := make([]Event, max)
	copy(out, q.items[:max])
	return out
}

// Remove drops the events with the given ids (typically after a 202). Ids not
// present are ignored.
func (q *Queue) Remove(ids []string) {
	if len(ids) == 0 {
		return
	}
	drop := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		drop[id] = struct{}{}
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	kept := q.items[:0]
	for _, e := range q.items {
		if _, ok := drop[e.ID]; ok {
			continue
		}
		kept = append(kept, e)
	}
	q.items = kept
}

// Len returns the number of pending events.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

// Snapshot returns a copy of all pending events for the exit spool.
func (q *Queue) Snapshot() []Event {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]Event, len(q.items))
	copy(out, q.items)
	return out
}

// Restore replaces the queue contents with the given events (from the spool on
// start), capped at capacity keeping the newest.
func (q *Queue) Restore(events []Event) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(events) > q.cap {
		events = events[len(events)-q.cap:]
	}
	q.items = make([]Event, len(events))
	copy(q.items, events)
}

// FromWire converts a wire event (from the exit spool) to a pending event.
func FromWire(w *wire.Event) Event {
	return Event{
		ID:               w.GetEventId(),
		Type:             w.GetType(),
		OccurredAtUnixMs: w.GetOccurredAtUnixMs(),
		Meta:             copyMeta(w.GetMeta()),
	}
}

// ToWire converts a pending event to its wire form.
func (e Event) ToWire() *wire.Event {
	return &wire.Event{
		EventId:          e.ID,
		OccurredAtUnixMs: e.OccurredAtUnixMs,
		Type:             e.Type,
		Meta:             copyMeta(e.Meta),
	}
}

func copyMeta(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
