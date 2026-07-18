// Package cpu reports aggregate CPU utilisation from /proc/stat. Every key is a
// delta between two reads of the kernel's cumulative jiffie counters, so the
// first sample after start seeds the baseline and emits nothing (no zero-fake).
// A counter that moves backwards (a reboot or a VM migration resetting the
// steal counter) re-baselines and emits nothing for that sample.
package cpu

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/LIVCK/agent/internal/collector"
	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/platform"
)

const statPath = "/proc/stat"

var errNoCPULine = errors.New("cpu: no aggregate cpu line in /proc/stat")

// times holds the cumulative jiffie fields of the aggregate "cpu" line.
type times struct {
	user, nice, system, idle, iowait, irq, softirq, steal uint64
}

func (t times) total() uint64 {
	return t.user + t.nice + t.system + t.idle + t.iowait + t.irq + t.softirq + t.steal
}

// Collector computes CPU percentages from consecutive /proc/stat reads.
type Collector struct {
	fs   platform.FS
	cfg  func() *config.Config
	prev times
	seed bool
}

// New builds a CPU collector reading through fs; cfg supplies the live enabled
// flag so a hot config change takes effect without a restart.
func New(fs platform.FS, cfg func() *config.Config) *Collector {
	return &Collector{fs: fs, cfg: cfg}
}

// Name returns "cpu".
func (*Collector) Name() string { return "cpu" }

// Available reports whether the cpu collector is enabled in the current config.
// /proc/stat exists on every supported Linux host, so config is the only gate.
func (c *Collector) Available() bool { return c.cfg().Collectors.CPU }

// Collect reads /proc/stat, and from the delta to the previous read derives the
// total/user/system/iowait/steal percentages. The first successful read seeds
// the baseline and yields no samples.
func (c *Collector) Collect(context.Context) ([]collector.Sample, error) {
	data, err := c.fs.ReadFile(statPath)
	if err != nil {
		return nil, err
	}
	cur, err := parseCPULine(data)
	if err != nil {
		return nil, err
	}

	if !c.seed {
		c.prev = cur
		c.seed = true
		return nil, nil
	}

	dTotal, reset := collector.CounterDelta(cur.total(), c.prev.total())
	if reset || dTotal == 0 {
		// Backwards jump or no elapsed time: re-baseline, emit nothing.
		c.prev = cur
		return nil, nil
	}

	busy := dTotal - fwd(cur.idle, c.prev.idle)
	samples := []collector.Sample{
		{Key: "sys.cpu.total_pct", Value: pct(busy, dTotal)},
		{Key: "sys.cpu.user_pct", Value: pct(fwd(cur.user, c.prev.user)+fwd(cur.nice, c.prev.nice), dTotal)},
		{Key: "sys.cpu.system_pct", Value: pct(fwd(cur.system, c.prev.system)+fwd(cur.irq, c.prev.irq)+fwd(cur.softirq, c.prev.softirq), dTotal)},
		{Key: "sys.cpu.iowait_pct", Value: pct(fwd(cur.iowait, c.prev.iowait), dTotal)},
		{Key: "sys.cpu.steal_pct", Value: pct(fwd(cur.steal, c.prev.steal), dTotal)},
	}
	c.prev = cur
	return samples, nil
}

// fwd is a per-field forward delta clamped at zero so an individual counter
// dropping (steal during a live migration) never produces a negative share.
func fwd(cur, prev uint64) uint64 {
	if cur < prev {
		return 0
	}
	return cur - prev
}

func pct(part, whole uint64) float64 {
	return collector.RatioPercent(float64(part), float64(whole))
}

// parseCPULine reads the aggregate "cpu" line (the first line of /proc/stat).
// Older kernels omit the tail fields (steal, guest); missing fields read as 0.
func parseCPULine(data []byte) (times, error) {
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "cpu ") {
			continue
		}
		f := strings.Fields(line)
		// f[0] == "cpu"; the counters start at f[1].
		get := func(i int) uint64 {
			if i >= len(f) {
				return 0
			}
			v, err := strconv.ParseUint(f[i], 10, 64)
			if err != nil {
				return 0
			}
			return v
		}
		return times{
			user:    get(1),
			nice:    get(2),
			system:  get(3),
			idle:    get(4),
			iowait:  get(5),
			irq:     get(6),
			softirq: get(7),
			steal:   get(8),
		}, nil
	}
	return times{}, errNoCPULine
}

var _ collector.Collector = (*Collector)(nil)
