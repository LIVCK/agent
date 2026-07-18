// Package buffer holds the agent's report ring: a RAM buffer bounded by both a
// byte cap (15 MB) and a time horizon (60 min), with drop-oldest on overflow. It
// is at-least-once: a report stays until the server acknowledges the batch that
// carried it with a 202 (remove-on-202), so a failed send loses nothing. Replay
// is newest-first so live data reaches the charts immediately and a backlog
// heals backwards. On a clean exit the ring is spooled to the state directory
// and reloaded on start, so a restart costs no data point and steady-state disk
// IO is zero.
package buffer

import (
	"strconv"
	"sync"
	"time"

	"github.com/LIVCK/agent/internal/platform"
	"github.com/LIVCK/agent/pkg/wire"
	"google.golang.org/protobuf/proto"
)

// Defaults for the ring bounds.
const (
	DefaultMaxBytes = 15 * 1024 * 1024
	DefaultHorizon  = 60 * time.Minute
)

// Emitter raises a buffer_overflow event when the ring drops reports. It matches
// internal/event.Emitter without importing it.
type Emitter interface {
	Emit(t wire.EventType, meta map[string]string)
}

type entry struct {
	seq    uint64
	size   int
	report *wire.Report
}

// Ring is the bounded report buffer. It is safe for concurrent use: the collect
// loop Adds while the sender TakeBatch/Removes.
type Ring struct {
	mu       sync.Mutex
	clock    platform.Clock
	emitter  Emitter
	maxBytes int
	horizon  time.Duration

	entries []entry
	bytes   int
	nextSeq uint64
	dropped uint64 // cumulative, feeds sys.agent.dropped_reports
}

// New builds a ring. maxBytes <= 0 uses DefaultMaxBytes; horizon <= 0 uses
// DefaultHorizon. emitter may be nil (overflow is then counted but not evented).
func New(clock platform.Clock, emitter Emitter, maxBytes int, horizon time.Duration) *Ring {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxBytes
	}
	if horizon <= 0 {
		horizon = DefaultHorizon
	}
	return &Ring{clock: clock, emitter: emitter, maxBytes: maxBytes, horizon: horizon}
}

// Add appends a report as the newest entry, then evicts the oldest entries that
// fall outside the time horizon or push the ring over the byte cap. Evictions
// are counted into the cumulative dropped total and, if any occurred, raise one
// buffer_overflow event with the number dropped.
func (r *Ring) Add(report *wire.Report) {
	size := proto.Size(report)
	r.mu.Lock()
	r.purgeExpiredLocked()
	r.entries = append(r.entries, entry{seq: r.nextSeq, size: size, report: report})
	r.nextSeq++
	r.bytes += size
	dropped := r.evictOverflowLocked()
	total := r.dropped
	r.mu.Unlock()

	if dropped > 0 && r.emitter != nil {
		r.emitter.Emit(wire.EventType_EVENT_TYPE_BUFFER_OVERFLOW, map[string]string{
			"dropped_count": strconv.FormatUint(dropped, 10),
			"dropped_total": strconv.FormatUint(total, 10),
		})
	}
}

// evictOverflowLocked drops oldest entries while over the byte cap, keeping at
// least one entry. Returns how many it dropped this call.
func (r *Ring) evictOverflowLocked() uint64 {
	var n uint64
	for r.bytes > r.maxBytes && len(r.entries) > 1 {
		r.bytes -= r.entries[0].size
		r.entries = r.entries[1:]
		r.dropped++
		n++
	}
	return n
}

// purgeExpiredLocked drops entries whose sample time is older than the horizon.
func (r *Ring) purgeExpiredLocked() {
	cutoff := r.clock.Now().Add(-r.horizon).UnixMilli()
	i := 0
	for i < len(r.entries) && r.entries[i].report.GetSampledAtUnixMs() < cutoff {
		r.bytes -= r.entries[i].size
		r.dropped++
		i++
	}
	if i > 0 {
		r.entries = r.entries[i:]
	}
}

// TakeBatch returns up to maxReports of the NEWEST reports whose combined proto
// size does not exceed maxBytes, newest-first, without removing them. The
// returned seqs identify the batch for Remove after a 202. A batch always
// contains at least the single newest report even if it alone exceeds maxBytes.
func (r *Ring) TakeBatch(maxReports, maxBytes int) (reports []*wire.Report, seqs []uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.purgeExpiredLocked()
	if len(r.entries) == 0 {
		return nil, nil
	}
	budget := 0
	for i := len(r.entries) - 1; i >= 0; i-- {
		e := r.entries[i]
		if len(reports) >= maxReports {
			break
		}
		if len(reports) > 0 && maxBytes > 0 && budget+e.size > maxBytes {
			break
		}
		reports = append(reports, e.report)
		seqs = append(seqs, e.seq)
		budget += e.size
	}
	return reports, seqs
}

// Remove drops the entries with the given seqs (after a 202 or a terminal 4xx
// discard). Unknown seqs are ignored.
func (r *Ring) Remove(seqs []uint64) {
	if len(seqs) == 0 {
		return
	}
	drop := make(map[uint64]struct{}, len(seqs))
	for _, s := range seqs {
		drop[s] = struct{}{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	kept := r.entries[:0]
	for _, e := range r.entries {
		if _, ok := drop[e.seq]; ok {
			r.bytes -= e.size
			continue
		}
		kept = append(kept, e)
	}
	r.entries = kept
}

// DroppedTotal returns the cumulative number of reports dropped since start. It
// feeds the sys.agent.dropped_reports metric; the UI computes deltas.
func (r *Ring) DroppedTotal() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.dropped
}

// FillPct returns the ring fill as a percentage of the byte cap, for
// sys.agent.buffer_fill_pct.
func (r *Ring) FillPct() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return float64(r.bytes) / float64(r.maxBytes) * 100
}

// Snapshot returns all buffered reports oldest-first, for the exit spool.
func (r *Ring) Snapshot() []*wire.Report {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*wire.Report, len(r.entries))
	for i, e := range r.entries {
		out[i] = e.report
	}
	return out
}

// Restore appends spooled reports (oldest-first) on start, enforcing the byte
// cap and horizon silently: a restore is not an overflow, so it raises no
// buffer_overflow event.
func (r *Ring) Restore(reports []*wire.Report) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, rep := range reports {
		size := proto.Size(rep)
		r.entries = append(r.entries, entry{seq: r.nextSeq, size: size, report: rep})
		r.nextSeq++
		r.bytes += size
	}
	r.purgeExpiredLocked()
	for r.bytes > r.maxBytes && len(r.entries) > 1 {
		r.bytes -= r.entries[0].size
		r.entries = r.entries[1:]
		r.dropped++
	}
}

// Len returns the number of buffered reports.
func (r *Ring) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.entries)
}

// Bytes returns the current buffered byte total.
func (r *Ring) Bytes() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.bytes
}
