// Package self reports the agent's own resource use for overhead transparency
// (sys.agent.rss_bytes, sys.agent.cpu_pct). RSS comes from /proc/self/statm;
// cpu_pct is a delta of the agent's own utime+stime from /proc/self/stat over
// the elapsed wall time, so the first sample seeds the baseline and emits rss
// only. The buffer-derived self keys (dropped_reports, buffer_fill_pct) and the
// disk-derived stuck_mounts are emitted elsewhere; self never fakes them.
package self

import (
	"context"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/LIVCK/agent/internal/collector"
	"github.com/LIVCK/agent/internal/platform"
)

const (
	statmPath = "/proc/self/statm"
	statPath  = "/proc/self/stat"
	// clockTicks is USER_HZ. It is 100 on every supported distro/arch; reading
	// sysconf(_SC_CLK_TCK) would require cgo, which the agent forbids.
	clockTicks = 100
)

// Collector reports the agent's own RSS and CPU.
type Collector struct {
	fs    platform.FS
	clock platform.Clock

	prevJiffies uint64
	prevTime    time.Time
	seeded      bool
}

// New builds a self collector.
func New(fs platform.FS, clock platform.Clock) *Collector {
	return &Collector{fs: fs, clock: clock}
}

// Name returns "self".
func (*Collector) Name() string { return "self" }

// Available is always true: the agent can always observe itself.
func (*Collector) Available() bool { return true }

// Collect reports rss_bytes every time and cpu_pct once a predecessor sample
// exists. A missing /proc/self/statm falls back to the Go runtime's OS memory so
// rss is never dropped.
func (c *Collector) Collect(context.Context) ([]collector.Sample, error) {
	samples := []collector.Sample{{Key: "sys.agent.rss_bytes", Value: c.rss()}}

	now := c.clock.Now()
	jiffies, ok := c.selfJiffies()
	if !ok {
		return samples, nil
	}
	if !c.seeded {
		c.prevJiffies = jiffies
		c.prevTime = now
		c.seeded = true
		return samples, nil
	}
	dj, reset := collector.CounterDelta(jiffies, c.prevJiffies)
	dt := now.Sub(c.prevTime)
	c.prevJiffies = jiffies
	c.prevTime = now
	if reset || dt <= 0 {
		return samples, nil
	}
	cpuSeconds := float64(dj) / clockTicks
	pct := collector.ClampPercent(collector.Rate(cpuSeconds, dt) * 100)
	samples = append(samples, collector.Sample{Key: "sys.agent.cpu_pct", Value: pct})
	return samples, nil
}

// rss returns resident set size in bytes from /proc/self/statm, falling back to
// the Go runtime's reported OS memory when statm is unreadable.
func (c *Collector) rss() float64 {
	if data, err := c.fs.ReadFile(statmPath); err == nil {
		f := strings.Fields(string(data))
		if len(f) >= 2 {
			if resident, err := strconv.ParseUint(f[1], 10, 64); err == nil {
				return float64(resident) * float64(os.Getpagesize())
			}
		}
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return float64(ms.Sys)
}

// selfJiffies returns utime+stime for the agent process from /proc/self/stat.
// The comm field is parenthesised and may contain spaces, so the fields are
// read after the last ')'.
func (c *Collector) selfJiffies() (uint64, bool) {
	data, err := c.fs.ReadFile(statPath)
	if err != nil {
		return 0, false
	}
	s := string(data)
	i := strings.LastIndexByte(s, ')')
	if i < 0 {
		return 0, false
	}
	f := strings.Fields(s[i+1:])
	// After ')' the fields are stat field 3 onwards: utime is field 14
	// (index 11 here) and stime field 15 (index 12).
	if len(f) < 13 {
		return 0, false
	}
	utime, err1 := strconv.ParseUint(f[11], 10, 64)
	stime, err2 := strconv.ParseUint(f[12], 10, 64)
	if err1 != nil || err2 != nil {
		return 0, false
	}
	return utime + stime, true
}

var _ collector.Collector = (*Collector)(nil)
