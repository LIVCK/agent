// Package system reports host uptime and a process/zombie count. Uptime comes
// from /proc/uptime. The counts come from a single directory listing of /proc:
// each numeric entry is a process, and a process whose /proc/<pid>/stat state is
// 'Z' is a zombie. This is a state-only scan, not a per-process resource walk
// (the agent deliberately collects no per-process metrics); it reads one small
// stat file per pid once per report interval and never samples per-process cpu/mem.
package system

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/LIVCK/agent/internal/collector"
	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/platform"
)

const (
	uptimePath = "/proc/uptime"
	procDir    = "/proc"
)

var errShortUptime = errors.New("system: /proc/uptime is empty")

// Collector reports uptime and process counts.
type Collector struct {
	fs  platform.FS
	cfg func() *config.Config
}

// New builds a system collector.
func New(fs platform.FS, cfg func() *config.Config) *Collector {
	return &Collector{fs: fs, cfg: cfg}
}

// Name returns "system".
func (*Collector) Name() string { return "system" }

// Available reports whether the system collector is enabled in the current
// config.
func (c *Collector) Available() bool { return c.cfg().Collectors.System }

// Collect reports uptime and, best-effort, the process and zombie counts. A
// failure to list /proc drops the two process keys but still reports uptime.
func (c *Collector) Collect(context.Context) ([]collector.Sample, error) {
	data, err := c.fs.ReadFile(uptimePath)
	if err != nil {
		return nil, err
	}
	f := strings.Fields(string(data))
	if len(f) < 1 {
		return nil, errShortUptime
	}
	up, err := strconv.ParseFloat(f[0], 64)
	if err != nil {
		return nil, errShortUptime
	}
	samples := []collector.Sample{{Key: "sys.uptime_seconds", Value: up}}

	if total, zombies, ok := c.scanProcs(); ok {
		samples = append(samples,
			collector.Sample{Key: "sys.procs.total", Value: float64(total)},
			collector.Sample{Key: "sys.procs.zombies", Value: float64(zombies)},
		)
	}
	return samples, nil
}

// scanProcs lists /proc once and counts processes and zombies. A pid directory
// that vanishes between listing and stat read (a process exiting) is skipped.
func (c *Collector) scanProcs() (total, zombies int, ok bool) {
	entries, err := c.fs.ReadDir(procDir)
	if err != nil {
		return 0, 0, false
	}
	for _, e := range entries {
		if !e.IsDir() || !isPID(e.Name()) {
			continue
		}
		total++
		stat, err := c.fs.ReadFile(procDir + "/" + e.Name() + "/stat")
		if err != nil {
			continue
		}
		if procState(stat) == 'Z' {
			zombies++
		}
	}
	return total, zombies, true
}

// isPID reports whether name is all decimal digits.
func isPID(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		if name[i] < '0' || name[i] > '9' {
			return false
		}
	}
	return true
}

// procState returns the state char from a /proc/<pid>/stat line. The comm field
// (in parens) can contain spaces and parens, so the state is read as the first
// non-space byte after the last ')'.
func procState(data []byte) byte {
	s := string(data)
	i := strings.LastIndexByte(s, ')')
	if i < 0 || i+1 >= len(s) {
		return 0
	}
	rest := strings.TrimLeft(s[i+1:], " ")
	if rest == "" {
		return 0
	}
	return rest[0]
}

var _ collector.Collector = (*Collector)(nil)
