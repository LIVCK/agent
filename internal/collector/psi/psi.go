// Package psi reports Pressure Stall Information from /proc/pressure/{cpu,io,
// memory}. PSI needs kernel 4.20+ and, on RHEL 9, the psi=1 boot argument; when
// it is absent the files do not exist and the collector reports itself
// unavailable rather than emitting fake zeroes. A single missing subsystem file
// drops only that key.
package psi

import (
	"context"
	"strconv"
	"strings"

	"github.com/LIVCK/agent/internal/collector"
	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/platform"
)

// The per-subsystem files. cpu is the probe for availability: every PSI-enabled
// kernel publishes it.
const (
	cpuFile      = "/proc/pressure/cpu"
	ioFile       = "/proc/pressure/io"
	memoryFile   = "/proc/pressure/memory"
	avg10Prefix  = "avg10="
	someLinePref = "some"
)

// Collector reads PSI some-avg10 pressure.
type Collector struct {
	fs  platform.FS
	cfg func() *config.Config
}

// New builds a psi collector.
func New(fs platform.FS, cfg func() *config.Config) *Collector {
	return &Collector{fs: fs, cfg: cfg}
}

// Name returns "psi".
func (*Collector) Name() string { return "psi" }

// Available reports whether PSI is enabled in config and present on this host.
// It probes /proc/pressure/cpu: if the kernel does not expose PSI the whole
// source is skipped without an error.
func (c *Collector) Available() bool {
	if !c.cfg().Collectors.PSI {
		return false
	}
	_, err := c.fs.Stat(cpuFile)
	return err == nil
}

// Collect reads the some-avg10 pressure for cpu, io and memory. A subsystem file
// that cannot be read is skipped, not zero-filled.
func (c *Collector) Collect(context.Context) ([]collector.Sample, error) {
	var samples []collector.Sample
	for _, s := range []struct {
		key  string
		path string
	}{
		{"sys.psi.cpu_some_pct", cpuFile},
		{"sys.psi.io_some_pct", ioFile},
		{"sys.psi.mem_some_pct", memoryFile},
	} {
		data, err := c.fs.ReadFile(s.path)
		if err != nil {
			continue
		}
		if v, ok := parseSomeAvg10(data); ok {
			samples = append(samples, collector.Sample{Key: s.key, Value: collector.ClampPercent(v)})
		}
	}
	return samples, nil
}

// parseSomeAvg10 extracts avg10 from the "some" line of a pressure file.
func parseSomeAvg10(data []byte) (float64, bool) {
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, someLinePref) {
			continue
		}
		for _, f := range strings.Fields(line) {
			if !strings.HasPrefix(f, avg10Prefix) {
				continue
			}
			v, err := strconv.ParseFloat(strings.TrimPrefix(f, avg10Prefix), 64)
			if err != nil {
				return 0, false
			}
			return v, true
		}
	}
	return 0, false
}

var _ collector.Collector = (*Collector)(nil)
