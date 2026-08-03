package runner

import (
	"testing"
	"time"

	"github.com/LIVCK/agent/internal/platform"
	"github.com/LIVCK/agent/internal/platform/platformtest"
)

func profileRunner(t *testing.T, interval time.Duration, calls *int) (*Runner, *platformtest.Clock) {
	t.Helper()
	clk := platformtest.NewClock(time.Unix(1_700_000_000, 0))
	r := New(Options{
		Platform:        platform.Platform{Clock: clk, FS: platformtest.NewMemFS()},
		ProfileInterval: interval,
		ProfileRefresh:  func() { *calls++ },
	})
	r.lastProfileAt = clk.Now()
	return r, clk
}

// TestProfileRefreshHonoursItsOwnCadence pins the whole point of the throttle: the
// report interval is configurable down to 30s, and re-reading /proc/cpuinfo and
// /proc/stat that often to catch a monthly distro upgrade is pure waste.
func TestProfileRefreshHonoursItsOwnCadence(t *testing.T) {
	calls := 0
	r, clk := profileRunner(t, 5*time.Minute, &calls)

	// Nine 30s report intervals: still inside the window, so no host read at all.
	for i := 0; i < 9; i++ {
		clk.Advance(30 * time.Second)
		r.refreshProfile()
	}
	if calls != 0 {
		t.Fatalf("no refresh may run before the interval elapsed, got %d", calls)
	}

	clk.Advance(30 * time.Second) // now exactly 5 min since the seed
	r.refreshProfile()
	if calls != 1 {
		t.Fatalf("want exactly 1 refresh at the interval boundary, got %d", calls)
	}

	// The clock is reset on every refresh, so the next one is a further interval out.
	clk.Advance(4 * time.Minute)
	r.refreshProfile()
	if calls != 1 {
		t.Fatalf("want no second refresh inside the next window, got %d", calls)
	}
	clk.Advance(time.Minute)
	r.refreshProfile()
	if calls != 2 {
		t.Fatalf("want a second refresh once the next window elapsed, got %d", calls)
	}
}

func TestProfileRefreshDefaultsToConfigPollCadence(t *testing.T) {
	calls := 0
	r, clk := profileRunner(t, 0, &calls)

	clk.Advance(DefaultProfileInterval - time.Second)
	r.refreshProfile()
	if calls != 0 {
		t.Fatalf("zero interval must fall back to the default, got %d calls too early", calls)
	}
	clk.Advance(time.Second)
	r.refreshProfile()
	if calls != 1 {
		t.Fatalf("want 1 refresh after the default interval, got %d", calls)
	}
}

// A runner wired without the hook (every existing test, and any embedder that does
// not want host reads) must not panic on the loop's new call site.
func TestProfileRefreshIsOptional(t *testing.T) {
	clk := platformtest.NewClock(time.Unix(1_700_000_000, 0))
	r := New(Options{Platform: platform.Platform{Clock: clk, FS: platformtest.NewMemFS()}})
	clk.Advance(time.Hour)
	r.refreshProfile()
}
