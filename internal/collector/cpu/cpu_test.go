package cpu

import (
	"context"
	"testing"

	"github.com/LIVCK/agent/internal/collector"
	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/platform/platformtest"
)

func write(fs *platformtest.MemFS, body string) {
	_ = fs.WriteFileAtomic(statPath, []byte(body), 0o644)
}

func toMap(s []collector.Sample) map[string]float64 {
	m := map[string]float64{}
	for _, x := range s {
		m[x.Key] = x.Value
	}
	return m
}

func TestCPUSeedsThenComputesDelta(t *testing.T) {
	fs := platformtest.NewMemFS()
	write(fs, "cpu  100 0 50 1000 0 0 0 0 0 0\ncpu0 100 0 50 1000 0 0 0 0 0 0\n")
	c := New(fs, config.Defaults)

	first, err := c.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 0 {
		t.Fatalf("first sample must seed and emit nothing, got %v", first)
	}

	// dTotal=750, didle=600 -> busy=150 -> total_pct=20; duser=100 -> 13.33.
	write(fs, "cpu  200 0 100 1600 0 0 0 0 0 0\ncpu0 200 0 100 1600 0 0 0 0 0 0\n")
	got, err := c.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	m := toMap(got)
	if near(m["sys.cpu.total_pct"], 20) == false {
		t.Fatalf("total_pct = %v, want 20", m["sys.cpu.total_pct"])
	}
	if !near(m["sys.cpu.user_pct"], 13.333) {
		t.Fatalf("user_pct = %v, want ~13.33", m["sys.cpu.user_pct"])
	}
	if !near(m["sys.cpu.system_pct"], 6.667) {
		t.Fatalf("system_pct = %v, want ~6.67", m["sys.cpu.system_pct"])
	}
}

func TestCPUCounterResetReBaselines(t *testing.T) {
	fs := platformtest.NewMemFS()
	write(fs, "cpu  1000 0 1000 10000 0 0 0 0 0 0\n")
	c := New(fs, config.Defaults)
	_, _ = c.Collect(context.Background())

	// Counters drop below previous (reboot): re-baseline, emit nothing.
	write(fs, "cpu  10 0 10 100 0 0 0 0 0 0\n")
	got, _ := c.Collect(context.Background())
	if len(got) != 0 {
		t.Fatalf("counter reset must emit nothing, got %v", got)
	}
}

func TestCPUDisabledByConfig(t *testing.T) {
	fs := platformtest.NewMemFS()
	write(fs, "cpu  1 0 1 1 0 0 0 0 0 0\n")
	cfg := func() *config.Config {
		c := config.Defaults()
		c.Collectors.CPU = false
		return c
	}
	c := New(fs, cfg)
	if c.Available() {
		t.Fatal("cpu must be unavailable when disabled in config")
	}
}

func near(v, want float64) bool { return v >= want-0.01 && v <= want+0.01 }
