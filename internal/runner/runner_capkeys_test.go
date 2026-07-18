package runner

import (
	"context"
	"net/http"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func TestCapKeysKeepsCoreShedsWildcards(t *testing.T) {
	m := map[string]float64{
		"sys.cpu.total_pct":   1,
		"sys.mem.used_pct":    2,
		"sys.agent.rss_bytes": 3,
	}
	for i := 0; i < 20; i++ {
		m["sys.disk.d"+strconv.Itoa(i)+".used_pct"] = float64(i)
	}

	out := capKeys(m, 5)
	if len(out) != 5 {
		t.Fatalf("capped map size = %d, want 5", len(out))
	}
	// The fixed base keys must survive; per-device keys are shed to fit.
	for _, k := range []string{"sys.cpu.total_pct", "sys.mem.used_pct", "sys.agent.rss_bytes"} {
		if _, ok := out[k]; !ok {
			t.Fatalf("core key %q was dropped", k)
		}
	}
}

// TestCapKeysShedsProbeKeysBeforeBase pins the fix for the probe-key crowd-out:
// sys.probe.* is a per-target wildcard family and must be shed before the fixed
// base metrics that sort AFTER "sys.probe" alphabetically (procs/psi/swap/uptime),
// not treated as a core key that fills the budget ahead of them.
func TestCapKeysShedsProbeKeysBeforeBase(t *testing.T) {
	base := []string{"sys.procs.running", "sys.psi.cpu_some_avg10", "sys.swap.used_pct", "sys.uptime_seconds"}
	m := map[string]float64{"sys.cpu.total_pct": 1}
	for _, k := range base {
		m[k] = 1
	}
	// 15 targets * 7 keys = 105 probe keys, far over the budget.
	for i := 0; i < 15; i++ {
		p := "sys.probe.t" + strconv.Itoa(i) + "."
		for _, s := range []string{"up", "rtt_avg_ms", "rtt_min_ms", "rtt_max_ms", "loss_pct", "http_status", "cert_expiry_hours"} {
			m[p+s] = 1
		}
	}

	out := capKeys(m, 5)
	if len(out) != 5 {
		t.Fatalf("capped map size = %d, want 5", len(out))
	}
	for _, k := range append([]string{"sys.cpu.total_pct"}, base...) {
		if _, ok := out[k]; !ok {
			t.Fatalf("fixed base key %q was dropped in favour of a probe key", k)
		}
	}
	for k := range out {
		if len(k) >= 10 && k[:10] == "sys.probe." {
			t.Fatalf("probe wildcard key %q survived over a fixed base key", k)
		}
	}
}

func TestCapKeysNoopUnderCap(t *testing.T) {
	m := map[string]float64{"sys.cpu.total_pct": 1}
	if got := capKeys(m, 120); len(got) != 1 {
		t.Fatalf("under-cap map must be unchanged, got %d", len(got))
	}
	if got := capKeys(m, 0); len(got) != 1 {
		t.Fatalf("zero cap disables capping, got %d", len(got))
	}
}

// lifecycleSpy records that the runner drove the lifecycle hooks.
type lifecycleSpy struct {
	startup  atomic.Bool
	ticks    atomic.Int64
	shutdown atomic.Bool
}

func (s *lifecycleSpy) Startup(context.Context)  { s.startup.Store(true) }
func (s *lifecycleSpy) Tick(context.Context)     { s.ticks.Add(1) }
func (s *lifecycleSpy) Shutdown(context.Context) { s.shutdown.Store(true) }

func TestRunnerDrivesLifecycleHooks(t *testing.T) {
	f := newRunnerFixture(t, http.StatusAccepted)
	spy := &lifecycleSpy{}
	f.runner.opt.Lifecycle = spy

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = f.runner.Run(ctx); close(done) }()

	waitFor(t, func() bool { return spy.startup.Load() }, "lifecycle Startup called before shutdown")

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not shut down on cancel")
	}
	if !spy.shutdown.Load() {
		t.Fatal("lifecycle Shutdown must run on cancellation")
	}
}
