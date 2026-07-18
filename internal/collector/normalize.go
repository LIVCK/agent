package collector

import (
	"sort"
	"strings"
)

// maxSegmentLen bounds a normalized wildcard segment (mount, device, iface) so a
// pathological mount path cannot bloat a metric key. It matches the catalog
// normalization rule.
const maxSegmentLen = 40

// NormalizeSegment turns a raw mount path, device name or interface name into a
// safe metric-key segment. It lowercases, maps "/" to "_", names the root mount
// "_root", keeps only [a-z0-9_.-] (any other byte becomes "_"), and truncates to
// maxSegmentLen. This is the agent-side wildcard normalization the sys.* catalog
// pins: the same input must always yield the same segment across
// reports, and collisions after normalization are resolved by the caller (see
// DedupeSegment).
func NormalizeSegment(raw string) string {
	s := strings.ToLower(raw)
	s = strings.ReplaceAll(s, "/", "_")
	if s == "_" {
		s = "_root"
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '_', c == '.', c == '-':
			b.WriteByte(c)
		default:
			b.WriteByte('_')
		}
	}
	s = b.String()
	if len(s) > maxSegmentLen {
		s = s[:maxSegmentLen]
	}
	if s == "" {
		s = "_"
	}
	return s
}

// DedupeSegment returns seg if it is not yet in used, otherwise seg with a
// "-2", "-3", ... suffix until it is unique. used is updated with the chosen
// segment. Two different raw names can normalize to the same segment (for
// example two mounts differing only in a stripped character); this keeps their
// series identities distinct within one report.
func DedupeSegment(seg string, used map[string]bool) string {
	if !used[seg] {
		used[seg] = true
		return seg
	}
	for n := 2; ; n++ {
		cand := seg + "-" + itoa(n)
		if !used[cand] {
			used[cand] = true
			return cand
		}
	}
}

// itoa is a tiny base-10 formatter kept local so normalize.go has no strconv
// dependency for a single small integer.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// CapBySize keeps at most max entries, choosing the largest by the size the
// caller supplies (biggest filesystems, busiest devices/interfaces) so the
// selection is deterministic and stable report to report. Ties break on the raw
// name so the set does not flap. The excess is dropped rather than folded into a
// synthetic "_other" bucket: aggregating percentage gauges across mounts would
// be meaningless, and the caps are a rare safety ceiling on realistic hosts.
func CapBySize[T any](items []T, max int, name func(T) string, size func(T) float64) []T {
	if max <= 0 || len(items) <= max {
		return items
	}
	idx := make([]int, len(items))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool {
		ia, ib := idx[a], idx[b]
		sa, sb := size(items[ia]), size(items[ib])
		if sa != sb {
			return sa > sb
		}
		return name(items[ia]) < name(items[ib])
	})
	out := make([]T, 0, max)
	for _, i := range idx[:max] {
		out = append(out, items[i])
	}
	return out
}
