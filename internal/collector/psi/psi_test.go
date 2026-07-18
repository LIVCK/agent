package psi

import (
	"context"
	"testing"

	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/platform/platformtest"
)

const (
	cpuBody = "some avg10=1.50 avg60=0.80 avg300=0.20 total=12345\n"
	ioBody  = "some avg10=2.25 avg60=1.00 avg300=0.50 total=6789\nfull avg10=1.00 avg60=0.50 avg300=0.25 total=3456\n"
	memBody = "some avg10=0.10 avg60=0.05 avg300=0.01 total=111\nfull avg10=0.05 avg60=0.02 avg300=0.00 total=55\n"
)

func TestPSIParsesSomeAvg10(t *testing.T) {
	fs := platformtest.NewMemFS()
	_ = fs.WriteFileAtomic(cpuFile, []byte(cpuBody), 0o644)
	_ = fs.WriteFileAtomic(ioFile, []byte(ioBody), 0o644)
	_ = fs.WriteFileAtomic(memoryFile, []byte(memBody), 0o644)

	c := New(fs, config.Defaults)
	if !c.Available() {
		t.Fatal("psi must be available when /proc/pressure/cpu exists")
	}
	got, err := c.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]float64{}
	for _, s := range got {
		m[s.Key] = s.Value
	}
	if m["sys.psi.cpu_some_pct"] != 1.50 || m["sys.psi.io_some_pct"] != 2.25 || m["sys.psi.mem_some_pct"] != 0.10 {
		t.Fatalf("psi values wrong: %v", m)
	}
}

func TestPSIUnavailableWithoutFiles(t *testing.T) {
	c := New(platformtest.NewMemFS(), config.Defaults)
	if c.Available() {
		t.Fatal("psi must be unavailable when the kernel exposes no pressure files")
	}
}

func TestPSIMissingSubsystemDropsOnlyThatKey(t *testing.T) {
	fs := platformtest.NewMemFS()
	_ = fs.WriteFileAtomic(cpuFile, []byte(cpuBody), 0o644)
	// io and memory files absent (kernel variant): only cpu key emitted.
	got, err := New(fs, config.Defaults).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Key != "sys.psi.cpu_some_pct" {
		t.Fatalf("want only cpu key, got %v", got)
	}
}
