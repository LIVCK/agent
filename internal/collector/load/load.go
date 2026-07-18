// Package load reports the 1/5/15-minute load averages from /proc/loadavg.
// These are instantaneous gauges, not counters, so there is no delta state and
// every enabled sample emits.
package load

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/LIVCK/agent/internal/collector"
	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/platform"
)

const loadavgPath = "/proc/loadavg"

var errShortLoadavg = errors.New("load: /proc/loadavg has fewer than three fields")

// Collector reads the load averages.
type Collector struct {
	fs  platform.FS
	cfg func() *config.Config
}

// New builds a load collector.
func New(fs platform.FS, cfg func() *config.Config) *Collector {
	return &Collector{fs: fs, cfg: cfg}
}

// Name returns "load".
func (*Collector) Name() string { return "load" }

// Available reports whether the load collector is enabled in the current config.
func (c *Collector) Available() bool { return c.cfg().Collectors.Load }

// Collect parses the three load averages.
func (c *Collector) Collect(context.Context) ([]collector.Sample, error) {
	data, err := c.fs.ReadFile(loadavgPath)
	if err != nil {
		return nil, err
	}
	f := strings.Fields(string(data))
	if len(f) < 3 {
		return nil, errShortLoadavg
	}
	one, err1 := strconv.ParseFloat(f[0], 64)
	five, err5 := strconv.ParseFloat(f[1], 64)
	fifteen, err15 := strconv.ParseFloat(f[2], 64)
	if err1 != nil || err5 != nil || err15 != nil {
		return nil, errShortLoadavg
	}
	return []collector.Sample{
		{Key: "sys.load.1", Value: one},
		{Key: "sys.load.5", Value: five},
		{Key: "sys.load.15", Value: fifteen},
	}, nil
}

var _ collector.Collector = (*Collector)(nil)
