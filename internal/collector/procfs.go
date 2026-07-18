package collector

import (
	"strconv"
	"strings"
)

// ParseMeminfo parses /proc/meminfo into a key -> bytes map. Meminfo reports
// most fields in kB; a "kB" unit is normalized to bytes here so callers work in
// a single unit. Fields without a kB unit are kept as their raw integer. A
// malformed line is skipped rather than failing the whole parse: a single odd
// line must not blind the mem collector.
func ParseMeminfo(data []byte) map[string]uint64 {
	out := make(map[string]uint64, 48)
	for _, line := range strings.Split(string(data), "\n") {
		f := strings.Fields(line)
		if len(f) < 2 {
			continue
		}
		key := strings.TrimSuffix(f[0], ":")
		v, err := strconv.ParseUint(f[1], 10, 64)
		if err != nil {
			continue
		}
		if len(f) >= 3 && f[2] == "kB" {
			v *= 1024
		}
		out[key] = v
	}
	return out
}

// ParseVmstatKey returns a single named counter from /proc/vmstat, e.g.
// "oom_kill". ok is false when the key is absent (an older kernel without the
// oom_kill counter) so the caller can omit the metric instead of faking a zero.
func ParseVmstatKey(data []byte, key string) (val uint64, ok bool) {
	for _, line := range strings.Split(string(data), "\n") {
		f := strings.Fields(line)
		if len(f) < 2 || f[0] != key {
			continue
		}
		v, err := strconv.ParseUint(f[1], 10, 64)
		if err != nil {
			return 0, false
		}
		return v, true
	}
	return 0, false
}
