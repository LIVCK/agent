// Package swap reports swap usage from /proc/meminfo. A host with no swap
// reports a real total of zero (not a fake): the zero-fake rule only bars
// emitting rate keys with no predecessor, not a gauge whose true value is zero.
package swap

import (
	"context"
	"errors"

	"github.com/LIVCK/agent/internal/collector"
	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/platform"
)

const meminfoPath = "/proc/meminfo"

var errNoSwapTotal = errors.New("swap: /proc/meminfo has no SwapTotal")

// Collector reads swap usage.
type Collector struct {
	fs  platform.FS
	cfg func() *config.Config
}

// New builds a swap collector.
func New(fs platform.FS, cfg func() *config.Config) *Collector {
	return &Collector{fs: fs, cfg: cfg}
}

// Name returns "swap".
func (*Collector) Name() string { return "swap" }

// Available reports whether the swap collector is enabled in the current config.
func (c *Collector) Available() bool { return c.cfg().Collectors.Swap }

// Collect parses swap total/used from meminfo.
func (c *Collector) Collect(context.Context) ([]collector.Sample, error) {
	data, err := c.fs.ReadFile(meminfoPath)
	if err != nil {
		return nil, err
	}
	mi := collector.ParseMeminfo(data)
	total, ok := mi["SwapTotal"]
	if !ok {
		return nil, errNoSwapTotal
	}
	free := mi["SwapFree"]
	if free > total {
		free = total
	}
	used := total - free
	return []collector.Sample{
		{Key: "sys.swap.total_bytes", Value: float64(total)},
		{Key: "sys.swap.used_bytes", Value: float64(used)},
		{Key: "sys.swap.used_pct", Value: collector.RatioPercent(float64(used), float64(total))},
	}, nil
}

var _ collector.Collector = (*Collector)(nil)
