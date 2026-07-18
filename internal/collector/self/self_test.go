package self

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/LIVCK/agent/internal/collector"
	"github.com/LIVCK/agent/internal/platform/platformtest"
)

func TestSelfReportsRSSAndSeedsCPU(t *testing.T) {
	fs := platformtest.NewMemFS()
	// resident = 5 pages (statm field 2).
	_ = fs.WriteFileAtomic(statmPath, []byte("1000 5 3 1 0 100 0\n"), 0o644)
	_ = fs.WriteFileAtomic(statPath, []byte("42 (livck-agent) S 1 42 42 0 -1 0 0 0 0 0 100 50 0 0 20 0 1 0 0\n"), 0o644)

	clock := platformtest.NewClock(time.Unix(1000, 0))
	c := New(fs, clock)
	if c.Name() != "self" || !c.Available() {
		t.Fatal("self must be named self and always available")
	}

	got, err := c.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	m := toMap(got)
	wantRSS := float64(5 * os.Getpagesize())
	if m["sys.agent.rss_bytes"] != wantRSS {
		t.Fatalf("rss = %v, want %v", m["sys.agent.rss_bytes"], wantRSS)
	}
	if _, ok := m["sys.agent.cpu_pct"]; ok {
		t.Fatal("first sample must not emit cpu_pct (no predecessor)")
	}
}

func TestSelfCPUDelta(t *testing.T) {
	fs := platformtest.NewMemFS()
	_ = fs.WriteFileAtomic(statmPath, []byte("1000 5 3 1 0 100 0\n"), 0o644)
	// utime=100 stime=50 -> 150 jiffies.
	_ = fs.WriteFileAtomic(statPath, []byte("42 (livck-agent) S 1 42 42 0 -1 0 0 0 0 0 100 50 0 0 20 0 1 0 0\n"), 0o644)

	clock := platformtest.NewClock(time.Unix(1000, 0))
	c := New(fs, clock)
	if _, err := c.Collect(context.Background()); err != nil {
		t.Fatal(err)
	}

	// 10s later: +200 jiffies over 100 ticks/s = 2 cpu-seconds over 10s = 20%.
	clock.Advance(10 * time.Second)
	_ = fs.WriteFileAtomic(statPath, []byte("42 (livck-agent) S 1 42 42 0 -1 0 0 0 0 0 250 100 0 0 20 0 1 0 0\n"), 0o644)
	got, err := c.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	m := toMap(got)
	if v := m["sys.agent.cpu_pct"]; v < 19.9 || v > 20.1 {
		t.Fatalf("cpu_pct = %v, want ~20", v)
	}
}

func toMap(s []collector.Sample) map[string]float64 {
	m := map[string]float64{}
	for _, x := range s {
		m[x.Key] = x.Value
	}
	return m
}
