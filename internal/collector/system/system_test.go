package system

import (
	"context"
	"testing"

	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/platform/platformtest"
)

func TestSystemUptimeAndProcCounts(t *testing.T) {
	fs := platformtest.NewMemFS()
	_ = fs.WriteFileAtomic(uptimePath, []byte("12345.67 8000.00\n"), 0o644)
	_ = fs.WriteFileAtomic("/proc/1/stat", []byte("1 (systemd) S 0 1 1 0 -1\n"), 0o644)
	_ = fs.WriteFileAtomic("/proc/2/stat", []byte("2 (kthreadd) S 0 0 0 0 -1\n"), 0o644)
	// A zombie whose comm contains a space, to exercise the last-')' parse.
	_ = fs.WriteFileAtomic("/proc/3/stat", []byte("3 (zombie proc) Z 1 3 3 0 -1\n"), 0o644)
	// Non-numeric entry must be ignored.
	_ = fs.WriteFileAtomic("/proc/self/stat", []byte("9 (self) R 1 9 9 0 -1\n"), 0o644)

	got, err := New(fs, config.Defaults).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]float64{}
	for _, s := range got {
		m[s.Key] = s.Value
	}
	if m["sys.uptime_seconds"] != 12345.67 {
		t.Fatalf("uptime = %v", m["sys.uptime_seconds"])
	}
	if m["sys.procs.total"] != 3 {
		t.Fatalf("procs.total = %v, want 3", m["sys.procs.total"])
	}
	if m["sys.procs.zombies"] != 1 {
		t.Fatalf("procs.zombies = %v, want 1", m["sys.procs.zombies"])
	}
}

func TestSystemUptimeReportedWithNoProcesses(t *testing.T) {
	fs := platformtest.NewMemFS()
	_ = fs.WriteFileAtomic(uptimePath, []byte("42.0 10.0\n"), 0o644)
	// No /proc/<pid> entries: the scan finds no processes but uptime is still
	// reported (uptime never depends on the process scan).
	got, err := New(fs, config.Defaults).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]float64{}
	for _, s := range got {
		m[s.Key] = s.Value
	}
	if m["sys.uptime_seconds"] != 42.0 {
		t.Fatalf("uptime = %v", m["sys.uptime_seconds"])
	}
}
