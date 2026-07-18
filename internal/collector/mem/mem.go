// Package mem reports memory usage from /proc/meminfo plus the cumulative
// oom_kill counter from /proc/vmstat. All values are gauges or a cumulative
// counter, so there is no rate state here: the oom_kill counter is reported raw
// (agg=last); the lifecycle package owns turning its delta into an oom_kill
// event.
package mem

import (
	"context"
	"errors"

	"github.com/LIVCK/agent/internal/collector"
	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/platform"
)

const (
	meminfoPath = "/proc/meminfo"
	vmstatPath  = "/proc/vmstat"
)

var errNoMemTotal = errors.New("mem: /proc/meminfo has no MemTotal")

// Collector reads memory usage.
type Collector struct {
	fs  platform.FS
	cfg func() *config.Config
}

// New builds a mem collector.
func New(fs platform.FS, cfg func() *config.Config) *Collector {
	return &Collector{fs: fs, cfg: cfg}
}

// Name returns "mem".
func (*Collector) Name() string { return "mem" }

// Available reports whether the mem collector is enabled in the current config.
func (c *Collector) Available() bool { return c.cfg().Collectors.Mem }

// Collect parses meminfo into the sys.mem.* keys. MemAvailable is preferred; on
// a kernel too old to publish it (pre-3.14) it is approximated from free +
// buffers + cached rather than dropped.
func (c *Collector) Collect(context.Context) ([]collector.Sample, error) {
	data, err := c.fs.ReadFile(meminfoPath)
	if err != nil {
		return nil, err
	}
	mi := collector.ParseMeminfo(data)
	total, ok := mi["MemTotal"]
	if !ok || total == 0 {
		return nil, errNoMemTotal
	}
	avail, hasAvail := mi["MemAvailable"]
	if !hasAvail {
		avail = mi["MemFree"] + mi["Buffers"] + mi["Cached"]
	}
	if avail > total {
		avail = total
	}
	used := total - avail

	samples := []collector.Sample{
		{Key: "sys.mem.total_bytes", Value: float64(total)},
		{Key: "sys.mem.used_bytes", Value: float64(used)},
		{Key: "sys.mem.available_bytes", Value: float64(avail)},
		{Key: "sys.mem.used_pct", Value: collector.RatioPercent(float64(used), float64(total))},
		{Key: "sys.mem.cached_bytes", Value: float64(mi["Cached"])},
		{Key: "sys.mem.buffers_bytes", Value: float64(mi["Buffers"])},
	}

	// oom_kills is cumulative and reported raw; omit it entirely on kernels
	// without the counter (< 4.13) rather than faking a zero.
	if vm, err := c.fs.ReadFile(vmstatPath); err == nil {
		if oom, ok := collector.ParseVmstatKey(vm, "oom_kill"); ok {
			samples = append(samples, collector.Sample{
				Key:   "sys.mem.oom_kills",
				Value: float64(oom),
			})
		}
	}
	return samples, nil
}

var _ collector.Collector = (*Collector)(nil)
