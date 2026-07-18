// soak_test runs the full run loop against a discarding 202 ingest for a bounded
// duration and asserts memory stays flat: no monotonic climb in the live Go heap
// over sustained operation. It targets the run loop, buffer, sender and the
// per-interval aggregator allocation churn - NOT the real collectors' RSS (the
// 80MB-vs-MemoryMax question is validated separately against a live agent). The
// default duration is short so `go test ./...` stays fast; set LIVCK_SOAK_DURATION
// (e.g. 5m, 24h) for the release-gate soak. ASCII only.
package runner

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestSoakMemoryFlat is the hermetic memory-stability soak.
func TestSoakMemoryFlat(t *testing.T) {
	dur := soakDuration(t)

	// Discard-count ingest (202 by default, retention off so the recorder itself
	// cannot grow and masquerade as an agent leak).
	retain := false
	f := newE2E(t, e2eOpts{
		budget:   -1, // unlimited: the loop free-runs on the instant clock
		applyCfg: smallInterval,
		retain:   &retain,
	})
	start := time.Now()
	cancel := f.start(t)

	// Sample the live heap (and RSS, best-effort on Linux) across the run. A GC
	// before each read makes HeapAlloc the live set, so a leak shows as a rising
	// floor rather than allocator noise.
	sampleEvery := dur / 20
	if sampleEvery < 100*time.Millisecond {
		sampleEvery = 100 * time.Millisecond
	}
	deadline := time.Now().Add(dur)
	var heap []uint64
	var rss []uint64
	for time.Now().Before(deadline) {
		time.Sleep(sampleEvery)
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		heap = append(heap, ms.HeapAlloc)
		if r, ok := processRSSBytes(); ok {
			rss = append(rss, r)
		}
		t.Logf("t+%-8s heap_alloc=%s rss=%s delivered_batches~%d",
			time.Since(start).Round(time.Millisecond),
			humanBytes(ms.HeapAlloc), rssStr(rss), f.ing.attempts.Load())
	}
	cancel()

	if f.ing.attempts.Load() == 0 {
		t.Fatal("soak delivered nothing; the run loop did not exercise the sender")
	}
	if len(heap) < 4 {
		t.Fatalf("too few heap samples (%d) to judge flatness", len(heap))
	}

	// Flatness: drop a warmup prefix, then require the late peak to stay within a
	// generous multiple of the early baseline (real leaks grow without bound over
	// the release-gate durations; this catches them without flaking on GC jitter).
	warm := len(heap) / 4
	baseline := minU(heap[warm:])
	peak := maxU(heap[warm:])
	limit := baseline*3 + 8*1024*1024 // 3x + 8 MiB headroom
	t.Logf("soak %s: heap baseline=%s peak=%s limit=%s samples=%d",
		dur, humanBytes(baseline), humanBytes(peak), humanBytes(limit), len(heap))
	if peak > limit {
		t.Fatalf("heap grew beyond the flatness limit: peak=%s baseline=%s limit=%s (possible leak)",
			humanBytes(peak), humanBytes(baseline), humanBytes(limit))
	}
}

// soakDuration is the LIVCK_SOAK_DURATION override, else a short default (shorter
// still under -short) so the routine suite stays fast.
func soakDuration(t *testing.T) time.Duration {
	t.Helper()
	if v := strings.TrimSpace(os.Getenv("LIVCK_SOAK_DURATION")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("invalid LIVCK_SOAK_DURATION %q: %v", v, err)
		}
		return d
	}
	if testing.Short() {
		return 500 * time.Millisecond
	}
	return 2 * time.Second
}

// processRSSBytes reads the resident set size from /proc/self/statm (Linux). It
// returns false where the file is unavailable (non-Linux), so the soak still
// runs its portable heap assertion everywhere.
func processRSSBytes() (uint64, bool) {
	raw, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, false
	}
	fields := strings.Fields(string(raw))
	if len(fields) < 2 {
		return 0, false
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0, false
	}
	return pages * uint64(os.Getpagesize()), true
}

func minU(xs []uint64) uint64 {
	m := xs[0]
	for _, x := range xs[1:] {
		if x < m {
			m = x
		}
	}
	return m
}

func maxU(xs []uint64) uint64 {
	m := xs[0]
	for _, x := range xs[1:] {
		if x > m {
			m = x
		}
	}
	return m
}

func humanBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return strconv.FormatUint(b, 10) + "B"
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return strconv.FormatFloat(float64(b)/float64(div), 'f', 1, 64) + string("KMGT"[exp]) + "iB"
}

func rssStr(rss []uint64) string {
	if len(rss) == 0 {
		return "n/a"
	}
	return humanBytes(rss[len(rss)-1])
}
