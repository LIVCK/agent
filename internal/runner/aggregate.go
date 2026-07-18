// This file is the agent-local aggregation layer, which spans two aggregation
// levels: the runner sub-samples the cheap collectors every
// sample_interval and folds each Sample into an aggregator; at the end of the
// report interval the aggregator collapses every key's sub-samples into one
// report value using the catalog's agg mode for that key. The catalog is the
// single source of truth for the mode (pkg/wire): nothing here hardcodes which
// key averages, peaks or carries a companion series.
//
// The five modes (pkg/wire Agg* constants):
//
//	avg     arithmetic mean of the sub-samples.
//	avg+max the mean AND an extra "<key>.max" companion series holding the peak,
//	        so a transient spike stays visible instead of being averaged away.
//	        The companion is emitted only for avg+max keys, never for any other.
//	max     the window peak (a single series; distinct from avg+max: no mean, no
//	        companion). Used for transient counters like sys.agent.stuck_mounts.
//	last    the most recent sub-sample (cumulative counters, static gauges).
//	delta   the sum of the sub-deltas over the interval. Each sub-sample value is
//	        already a max(0,.)-protected forward delta produced by the collector,
//	        so the interval delta is their sum. No catalog key uses delta today
//	        (it is reserved); the mode is implemented so a future key works.
package runner

import (
	"sort"
	"strings"

	"github.com/LIVCK/agent/internal/collector"
	"github.com/LIVCK/agent/pkg/wire"
)

// maxSuffix is appended to an avg+max key to name its peak companion series.
const maxSuffix = ".max"

// keyAcc accumulates the sub-samples of one metric key over a report interval.
// A key that is absent from some sub-samples (a rate key with no predecessor, a
// mount that vanished) simply contributes fewer observations; the mean is over
// the observed count, never zero-filled.
type keyAcc struct {
	agg   string
	sum   float64
	max   float64
	last  float64
	count int
}

func (a *keyAcc) observe(v float64) {
	if a.count == 0 || v > a.max {
		a.max = v
	}
	a.sum += v
	a.last = v
	a.count++
}

// aggregator folds Samples across sub-samples and collapses them per key.
type aggregator struct {
	keys map[string]*keyAcc
}

func newAggregator() *aggregator {
	return &aggregator{keys: make(map[string]*keyAcc, 64)}
}

// add folds one collector Sample into the accumulator for its key, resolving the
// aggregation mode from the catalog on first sight of the key.
func (a *aggregator) add(s collector.Sample) {
	acc, ok := a.keys[s.Key]
	if !ok {
		acc = &keyAcc{agg: aggFor(s.Key)}
		a.keys[s.Key] = acc
	}
	acc.observe(s.Value)
}

// collapse produces the report metrics from the accumulated sub-samples. An
// avg+max key yields both its mean and a "<key>.max" companion; every other key
// yields exactly one value. Keys with no observations are dropped (no zero-fake).
func (a *aggregator) collapse() map[string]float64 {
	out := make(map[string]float64, len(a.keys)+4)
	for key, acc := range a.keys {
		if acc.count == 0 {
			continue
		}
		switch acc.agg {
		case wire.AggAvg:
			out[key] = acc.sum / float64(acc.count)
		case wire.AggAvgMax:
			out[key] = acc.sum / float64(acc.count)
			out[key+maxSuffix] = acc.max
		case wire.AggMax:
			out[key] = acc.max
		case wire.AggDelta:
			out[key] = acc.sum
		case wire.AggLast:
			out[key] = acc.last
		default:
			// Defensive: an unknown mode keeps the latest value rather than
			// inventing a mean or a companion series.
			out[key] = acc.last
		}
	}
	return out
}

// aggFor returns the catalog aggregation mode for a concrete metric key. Exact
// (non-wildcard) keys resolve directly; per-device keys resolve against their
// wildcard template. An unknown key defaults to AggLast so a stray value is
// never averaged or given a spurious companion.
func aggFor(key string) string {
	if e, ok := wire.Lookup(key); ok {
		return e.Agg
	}
	if agg, ok := resolveWildcardAgg(key); ok {
		return agg
	}
	return wire.AggLast
}

// wildMatcher matches a concrete per-device key against one wildcard catalog
// template. The template "sys.disk.{mount}.used_pct" becomes prefix
// "sys.disk." and suffix ".used_pct"; a concrete key matches when it carries
// both around a non-empty middle segment.
type wildMatcher struct {
	prefix string
	suffix string
	agg    string
}

// wildMatchers holds every wildcard template, ordered most-specific first so an
// ambiguous concrete key (e.g. "...used_pct" also being a suffix of
// "...inodes_used_pct") resolves to the longest, most specific template.
var wildMatchers = buildWildMatchers()

func buildWildMatchers() []wildMatcher {
	var ms []wildMatcher
	for _, e := range wire.Catalog() {
		if e.Wildcard == "" {
			continue
		}
		ph := "{" + e.Wildcard + "}"
		i := strings.Index(e.Key, ph)
		if i < 0 {
			continue
		}
		ms = append(ms, wildMatcher{
			prefix: e.Key[:i],
			suffix: e.Key[i+len(ph):],
			agg:    e.Agg,
		})
	}
	sort.SliceStable(ms, func(a, b int) bool {
		if len(ms[a].suffix) != len(ms[b].suffix) {
			return len(ms[a].suffix) > len(ms[b].suffix)
		}
		return len(ms[a].prefix) > len(ms[b].prefix)
	})
	return ms
}

// resolveWildcardAgg resolves a concrete per-device key to its template's agg
// mode. The middle segment (the normalized device/mount/iface) may itself
// contain dots (a PCI address like 0000_01_00.0, a mount path), so matching is
// by prefix+suffix, not by segment count.
func resolveWildcardAgg(key string) (string, bool) {
	for _, m := range wildMatchers {
		if len(key) > len(m.prefix)+len(m.suffix) &&
			strings.HasPrefix(key, m.prefix) &&
			strings.HasSuffix(key, m.suffix) {
			return m.agg, true
		}
	}
	return "", false
}
