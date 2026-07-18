package probe

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/LIVCK/agent/internal/collector"
	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/platform/platformtest"
)

type fakeProber struct {
	mu     sync.Mutex
	calls  int
	byType map[string]Result
}

func (f *fakeProber) Probe(_ context.Context, t config.ProbeTarget) Result {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	return f.byType[t.Type]
}

func cfgWith(targets []config.ProbeTarget, interval int) func() *config.Config {
	c := config.Defaults()
	c.Collectors.Probe = config.ProbeConfig{IntervalSeconds: interval, Targets: targets}
	return func() *config.Config { return c }
}

func toMap(s []collector.Sample) map[string]float64 {
	m := map[string]float64{}
	for _, x := range s {
		m[x.Key] = x.Value
	}
	return m
}

func TestProbeEmitsPerTargetKeys(t *testing.T) {
	certExp := 240 * time.Hour
	prober := &fakeProber{byType: map[string]Result{
		"tcp":   {Up: true, Attempts: 3, RTTs: []time.Duration{10 * time.Millisecond, 30 * time.Millisecond, 20 * time.Millisecond}},
		"https": {Up: true, Attempts: 1, RTTs: []time.Duration{50 * time.Millisecond}, Status: 200, CertExpiry: &certExp},
		"dns":   {Up: false, Attempts: 2},
	}}
	c := &Collector{
		cfg: cfgWith([]config.ProbeTarget{
			{ID: "db01", Type: "tcp", Target: "10.0.0.5", Port: 5432},
			{ID: "reg1", Type: "https", Target: "registry.internal", Port: 443, Path: "/v2/"},
			{ID: "dns1", Type: "dns", Target: "svc.internal", Resolver: "10.0.0.53"},
		}, 60),
		prober: prober,
		clock:  platformtest.NewClock(time.Unix(1000, 0)),
	}

	out, err := c.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	m := toMap(out)

	// TCP: reachable, min/avg/max over 3 attempts, no loss.
	eq(t, m, "sys.probe.db01.up", 1)
	eq(t, m, "sys.probe.db01.rtt_min_ms", 10)
	eq(t, m, "sys.probe.db01.rtt_max_ms", 30)
	eq(t, m, "sys.probe.db01.rtt_avg_ms", 20)
	eq(t, m, "sys.probe.db01.loss_pct", 0)

	// HTTPS: reachable, status + cert expiry.
	eq(t, m, "sys.probe.reg1.up", 1)
	eq(t, m, "sys.probe.reg1.http_status", 200)
	eq(t, m, "sys.probe.reg1.cert_expiry_hours", 240)

	// DNS: both attempts failed → down, 100% loss, and NO rtt keys (no zero-fake).
	eq(t, m, "sys.probe.dns1.up", 0)
	eq(t, m, "sys.probe.dns1.loss_pct", 100)
	if _, ok := m["sys.probe.dns1.rtt_avg_ms"]; ok {
		t.Error("a down target must not emit an rtt")
	}
}

func TestProbeSelfThrottlesToCadence(t *testing.T) {
	clk := platformtest.NewClock(time.Unix(1000, 0))
	prober := &fakeProber{byType: map[string]Result{"tcp": {Up: true, Attempts: 1, RTTs: []time.Duration{time.Millisecond}}}}
	c := &Collector{
		cfg:    cfgWith([]config.ProbeTarget{{ID: "a", Type: "tcp", Target: "h", Port: 1}}, 60),
		prober: prober,
		clock:  clk,
	}

	_, _ = c.Collect(context.Background()) // probes once
	_, _ = c.Collect(context.Background()) // within 60s cadence → cached, must NOT re-probe
	if prober.calls != 1 {
		t.Fatalf("prober calls = %d, want 1 (throttled within cadence)", prober.calls)
	}

	clk.Advance(61 * time.Second)
	out, _ := c.Collect(context.Background()) // cadence elapsed → re-probe
	if prober.calls != 2 {
		t.Fatalf("prober calls after cadence = %d, want 2", prober.calls)
	}
	if toMap(out)["sys.probe.a.up"] != 1 {
		t.Fatal("re-probe should still emit samples")
	}
}

func TestProbeUnavailableWithoutTargets(t *testing.T) {
	c := New(func() *config.Config { return config.Defaults() }, platformtest.NewClock(time.Unix(1, 0)))
	if c.Available() {
		t.Fatal("probe collector must be unavailable when no targets are configured")
	}
}

func TestProbeIsIntervalSampled(t *testing.T) {
	if !collector.IsIntervalSampled(New(func() *config.Config { return config.Defaults() }, platformtest.NewClock(time.Unix(1, 0)))) {
		t.Fatal("probe must be interval-sampled (never on the 2s live path)")
	}
}

func eq(t *testing.T, m map[string]float64, key string, want float64) {
	t.Helper()
	if got, ok := m[key]; !ok || got != want {
		t.Errorf("%s = %v (present=%v), want %v", key, got, ok, want)
	}
}
