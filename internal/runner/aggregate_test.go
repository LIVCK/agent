package runner

import (
	"context"
	"io"
	"log/slog"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/LIVCK/agent/internal/collector"
	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/platform"
	"github.com/LIVCK/agent/internal/platform/platformtest"
	"github.com/LIVCK/agent/pkg/wire"
)

const eps = 1e-9

func feed(agg *aggregator, key string, vals ...float64) {
	for _, v := range vals {
		agg.add(collector.Sample{Key: key, Value: v})
	}
}

func TestAggregatorAvg(t *testing.T) {
	// sys.mem.used_pct is agg=avg in the catalog.
	agg := newAggregator()
	feed(agg, "sys.mem.used_pct", 10, 20, 30)
	out := agg.collapse()
	if math.Abs(out["sys.mem.used_pct"]-20) > eps {
		t.Fatalf("avg = %v, want 20", out["sys.mem.used_pct"])
	}
	if _, ok := out["sys.mem.used_pct.max"]; ok {
		t.Fatal("avg key must not emit a .max companion")
	}
}

func TestAggregatorAvgMaxEmitsBaseAndCompanion(t *testing.T) {
	// The core of the rework: an avg+max key emits its mean as the base key and
	// its peak as a "<key>.max" companion so a transient spike stays visible.
	// sys.cpu.total_pct is agg=avg+max. Samples [5,5,100,5,5,5] over 30s.
	agg := newAggregator()
	feed(agg, "sys.cpu.total_pct", 5, 5, 100, 5, 5, 5)
	out := agg.collapse()

	wantBase := 125.0 / 6.0 // 20.833...
	if math.Abs(out["sys.cpu.total_pct"]-wantBase) > 1e-6 {
		t.Fatalf("avg+max base = %v, want %v", out["sys.cpu.total_pct"], wantBase)
	}
	if out["sys.cpu.total_pct.max"] != 100 {
		t.Fatalf("avg+max companion = %v, want 100", out["sys.cpu.total_pct.max"])
	}
}

func TestAggregatorLast(t *testing.T) {
	// sys.mem.total_bytes is agg=last.
	agg := newAggregator()
	feed(agg, "sys.mem.total_bytes", 1, 2, 3)
	out := agg.collapse()
	if out["sys.mem.total_bytes"] != 3 {
		t.Fatalf("last = %v, want 3", out["sys.mem.total_bytes"])
	}
}

func TestAggregatorMax(t *testing.T) {
	// sys.agent.stuck_mounts is agg=max (a single peak series, no companion).
	agg := newAggregator()
	feed(agg, "sys.agent.stuck_mounts", 0, 2, 1)
	out := agg.collapse()
	if out["sys.agent.stuck_mounts"] != 2 {
		t.Fatalf("max = %v, want 2", out["sys.agent.stuck_mounts"])
	}
	if _, ok := out["sys.agent.stuck_mounts.max"]; ok {
		t.Fatal("max key must not emit a .max companion")
	}
}

func TestAggregatorDeltaSumsSubDeltasWithReset(t *testing.T) {
	// No catalog key uses delta today (reserved), so exercise the collapse path
	// directly. Each sub-sample value is an already max(0,.)-protected forward
	// delta; a counter reset shows up as a 0 sub-sample, so the interval delta is
	// the plain sum and never goes negative.
	acc := &keyAcc{agg: wire.AggDelta}
	for _, v := range []float64{2, 3, 0, 5} {
		acc.observe(v)
	}
	agg := &aggregator{keys: map[string]*keyAcc{"sys.future.delta": acc}}
	out := agg.collapse()
	if out["sys.future.delta"] != 10 {
		t.Fatalf("delta = %v, want 10", out["sys.future.delta"])
	}
}

func TestAggregatorDropsEmptyKeys(t *testing.T) {
	// A key that was observed zero times (never added) produces nothing.
	agg := newAggregator()
	if len(agg.collapse()) != 0 {
		t.Fatal("empty aggregator must collapse to no metrics")
	}
}

func TestAggForResolvesWildcardTemplates(t *testing.T) {
	cases := map[string]string{
		"sys.cpu.total_pct":              wire.AggAvgMax,
		"sys.mem.used_pct":               wire.AggAvg,
		"sys.mem.total_bytes":            wire.AggLast,
		"sys.agent.stuck_mounts":         wire.AggMax,
		"sys.disk._root.used_pct":        wire.AggAvg,
		"sys.disk._root.inodes_used_pct": wire.AggAvg,
		"sys.disk._root.total_bytes":     wire.AggLast,
		"sys.diskio.sda.read_bps":        wire.AggAvg,
		"sys.net.eth0.rx_bps":            wire.AggAvg,
		// PCI-address GPU segment carries a dot, proving prefix+suffix matching
		// (not segment counting) resolves the template.
		"sys.gpu.0000_01_00.0.util_pct":        wire.AggAvgMax,
		"sys.gpu.0000_01_00.0.mem_total_bytes": wire.AggLast,
		"sys.smart.sda.temp_c":                 wire.AggAvgMax,
		"sys.smart.sda.health_ok":              wire.AggLast,
	}
	for key, want := range cases {
		if got := aggFor(key); got != want {
			t.Errorf("aggFor(%q) = %q, want %q", key, got, want)
		}
	}
	// An unknown key defaults to last so it is never averaged or given a companion.
	if got := aggFor("sys.made.up.key"); got != wire.AggLast {
		t.Errorf("aggFor(unknown) = %q, want %q", got, wire.AggLast)
	}
}

// TestEmittedKeysAreCatalogKeysOrCompanions asserts the contract the rework must
// hold: every emitted key is either a catalog key (exact or wildcard-resolved)
// or a ".max" companion of an avg+max key, and nothing else.
func TestEmittedKeysAreCatalogKeysOrCompanions(t *testing.T) {
	agg := newAggregator()
	feed(agg, "sys.cpu.total_pct", 5, 100)           // avg+max -> base + .max
	feed(agg, "sys.mem.used_pct", 40, 60)            // avg
	feed(agg, "sys.mem.total_bytes", 1<<30)          // last
	feed(agg, "sys.agent.stuck_mounts", 0, 1)        // max
	feed(agg, "sys.disk._root.used_pct", 50, 70)     // wildcard avg
	feed(agg, "sys.diskio.sda.write_bps", 1e6)       // wildcard avg
	feed(agg, "sys.net.eth0.tx_bps", 2e6)            // wildcard avg
	feed(agg, "sys.gpu.0000_01_00.0.temp_c", 40, 90) // wildcard avg+max -> base + .max
	feed(agg, "sys.smart.sda.temp_c", 35, 38)        // wildcard avg+max -> base + .max

	out := agg.collapse()
	sawCompanion := false
	for key := range out {
		if !emittedKeyValid(key) {
			t.Errorf("emitted key %q is neither a catalog key nor a valid .max companion", key)
		}
		if strings.HasSuffix(key, maxSuffix) {
			sawCompanion = true
		}
	}
	if !sawCompanion {
		t.Fatal("expected at least one .max companion in the output")
	}
	// Every avg+max key must have produced its companion.
	for _, base := range []string{"sys.cpu.total_pct", "sys.gpu.0000_01_00.0.temp_c", "sys.smart.sda.temp_c"} {
		if _, ok := out[base+maxSuffix]; !ok {
			t.Errorf("avg+max key %q missing its %s companion", base, maxSuffix)
		}
	}
}

// emittedKeyValid is the test-side oracle for the key contract.
func emittedKeyValid(key string) bool {
	if _, ok := wire.Lookup(key); ok {
		return true
	}
	if _, ok := resolveWildcardAgg(key); ok {
		return true
	}
	if strings.HasSuffix(key, maxSuffix) {
		base := strings.TrimSuffix(key, maxSuffix)
		return aggFor(base) == wire.AggAvgMax
	}
	return false
}

// countingCollector counts Collect calls and emits one fixed key. interval marks
// it as exec-based (interval-sampled) via the collector.IntervalSampled marker.
type countingCollector struct {
	name     string
	key      string
	interval bool
	calls    int
}

func (c *countingCollector) Name() string          { return c.name }
func (c *countingCollector) Available() bool       { return true }
func (c *countingCollector) IntervalSampled() bool { return c.interval }
func (c *countingCollector) Collect(context.Context) ([]collector.Sample, error) {
	c.calls++
	return []collector.Sample{{Key: c.key, Value: float64(c.calls)}}, nil
}

// sleepCollector blocks for d (real time) on each Collect, simulating a slow exec
// collector (e.g. probe waiting on black-holed targets).
type sleepCollector struct {
	d        time.Duration
	interval bool
}

func (c *sleepCollector) Name() string          { return "slow" }
func (c *sleepCollector) Available() bool       { return true }
func (c *sleepCollector) IntervalSampled() bool { return c.interval }
func (c *sleepCollector) Collect(ctx context.Context) ([]collector.Sample, error) {
	select {
	case <-time.After(c.d):
	case <-ctx.Done():
	}
	return nil, nil
}

// deadlineCollector records how much budget its ctx still had when it ran.
type deadlineCollector struct {
	interval  bool
	remaining time.Duration
}

func (c *deadlineCollector) Name() string          { return "recorder" }
func (c *deadlineCollector) Available() bool       { return true }
func (c *deadlineCollector) IntervalSampled() bool { return c.interval }
func (c *deadlineCollector) Collect(ctx context.Context) ([]collector.Sample, error) {
	if dl, ok := ctx.Deadline(); ok {
		c.remaining = time.Until(dl)
	}
	return nil, nil
}

// TestIntervalCollectorsGetIndependentBudget pins the HIGH fix: exec collectors
// run under IntervalCollectTimeout (not the short CollectTimeout) AND each gets
// its OWN budget, so a slow probe cannot shrink the next collector's deadline
// into a false "down". A slow interval collector runs first; the recorder that
// runs after it must still see ~the full IntervalCollectTimeout.
func TestIntervalCollectorsGetIndependentBudget(t *testing.T) {
	slow := &sleepCollector{d: 250 * time.Millisecond, interval: true}
	rec := &deadlineCollector{interval: true}

	reg := collector.NewRegistry()
	reg.Register(slow) // registered (and iterated) first
	reg.Register(rec)

	r := New(Options{
		Registry:               reg,
		Log:                    slog.New(slog.NewTextHandler(io.Discard, nil)),
		CollectTimeout:         100 * time.Millisecond,
		IntervalCollectTimeout: 500 * time.Millisecond,
	})

	r.collectInto(context.Background(), newAggregator(), true)

	// One shared budget would leave the recorder ~250ms (or less than the 100ms
	// CollectTimeout); an independent IntervalCollectTimeout budget gives it ~500ms.
	if rec.remaining < 400*time.Millisecond {
		t.Fatalf("recorder saw %v budget after a slow collector, want ~500ms (independent IntervalCollectTimeout)", rec.remaining)
	}
}

// TestCollectIntervalSubSamplesCheapPerInterval proves the cadence split: over a
// 30s interval at a 5s cadence the cheap collector is read 6 times while the
// exec (interval-sampled) collector is read exactly once.
func TestCollectIntervalSubSamplesCheapPerInterval(t *testing.T) {
	clock := platformtest.NewClock(time.Unix(0, 0))
	cheap := &countingCollector{name: "cheap", key: "sys.cpu.total_pct"}
	exec := &countingCollector{name: "exec", key: "sys.smart.sda.temp_c", interval: true}

	reg := collector.NewRegistry()
	reg.Register(cheap)
	reg.Register(exec)

	cfg := config.NewManager(nil, nil, nil, "", slog.New(slog.NewTextHandler(io.Discard, nil)))
	cfg.Apply([]byte(`{"config_version":2,"interval_seconds":30,"sample_interval_seconds":5}`))
	if cfg.Current().SampleIntervalSeconds != 5 {
		t.Fatalf("sample_interval_seconds not applied: %d", cfg.Current().SampleIntervalSeconds)
	}

	r := New(Options{
		Platform: platform.Platform{Clock: clock},
		Config:   cfg,
		Registry: reg,
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	rep := r.collectInterval(context.Background())
	if rep == nil {
		t.Fatal("collectInterval returned nil report")
	}
	if cheap.calls != 6 {
		t.Errorf("cheap collector collected %d times, want 6", cheap.calls)
	}
	if exec.calls != 1 {
		t.Errorf("exec collector collected %d times, want 1", exec.calls)
	}
	// sampled_at is the window end: 6 * 5s = 30s after the fake clock start.
	if rep.SampledAtUnixMs != int64(30*time.Second/time.Millisecond) {
		t.Errorf("sampled_at = %d ms, want %d ms", rep.SampledAtUnixMs, 30*time.Second/time.Millisecond)
	}
	// The cheap collector's avg+max key must carry its companion.
	if _, ok := rep.Metrics["sys.cpu.total_pct.max"]; !ok {
		t.Error("expected sys.cpu.total_pct.max companion in the report")
	}
}

func TestSubSampleCountRounding(t *testing.T) {
	cases := []struct{ interval, sample, want int }{
		{30, 5, 6}, {60, 5, 12}, {120, 5, 24},
		{30, 3, 10}, {30, 10, 3},
		{30, 7, 4}, // 30/7 = 4.28 -> 4
		{30, 4, 8}, // 30/4 = 7.5 -> 8
		{0, 5, 1},  // never below 1
		{30, 0, 6}, // zero cadence falls back to default 5
	}
	for _, c := range cases {
		if got := subSampleCount(c.interval, c.sample); got != c.want {
			t.Errorf("subSampleCount(%d,%d) = %d, want %d", c.interval, c.sample, got, c.want)
		}
	}
}
