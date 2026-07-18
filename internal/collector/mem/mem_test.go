package mem

import (
	"context"
	"testing"

	"github.com/LIVCK/agent/internal/collector"
	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/platform/platformtest"
)

const k = 1024

func toMap(s []collector.Sample) map[string]float64 {
	m := map[string]float64{}
	for _, x := range s {
		m[x.Key] = x.Value
	}
	return m
}

func TestMemUsesMemAvailableAndOOM(t *testing.T) {
	fs := platformtest.NewMemFS()
	_ = fs.WriteFileAtomic(meminfoPath, []byte(
		"MemTotal:       1000 kB\nMemFree:         100 kB\nMemAvailable:    400 kB\n"+
			"Buffers:          50 kB\nCached:          200 kB\n"), 0o644)
	_ = fs.WriteFileAtomic(vmstatPath, []byte("nr_free_pages 123\noom_kill 7\n"), 0o644)

	got, err := New(fs, config.Defaults).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	m := toMap(got)
	if m["sys.mem.total_bytes"] != 1000*k {
		t.Fatalf("total = %v", m["sys.mem.total_bytes"])
	}
	if m["sys.mem.available_bytes"] != 400*k {
		t.Fatalf("available = %v", m["sys.mem.available_bytes"])
	}
	if m["sys.mem.used_bytes"] != 600*k {
		t.Fatalf("used = %v", m["sys.mem.used_bytes"])
	}
	if m["sys.mem.used_pct"] != 60 {
		t.Fatalf("used_pct = %v", m["sys.mem.used_pct"])
	}
	if m["sys.mem.cached_bytes"] != 200*k || m["sys.mem.buffers_bytes"] != 50*k {
		t.Fatalf("cached/buffers wrong: %v", m)
	}
	if m["sys.mem.oom_kills"] != 7 {
		t.Fatalf("oom_kills = %v, want 7", m["sys.mem.oom_kills"])
	}
}

func TestMemFallsBackWhenMemAvailableMissing(t *testing.T) {
	fs := platformtest.NewMemFS()
	_ = fs.WriteFileAtomic(meminfoPath, []byte(
		"MemTotal:       1000 kB\nMemFree:         100 kB\nBuffers:          50 kB\nCached:          200 kB\n"), 0o644)

	got, err := New(fs, config.Defaults).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	m := toMap(got)
	// available ~= free + buffers + cached = 350; no oom key without vmstat.
	if m["sys.mem.available_bytes"] != 350*k {
		t.Fatalf("fallback available = %v, want 350k", m["sys.mem.available_bytes"])
	}
	if _, ok := m["sys.mem.oom_kills"]; ok {
		t.Fatal("oom_kills must be omitted without a vmstat counter")
	}
}
