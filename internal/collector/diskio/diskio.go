// Package diskio reports per-device throughput and IOPS from /proc/diskstats.
// Every key is a rate: the delta of a cumulative counter divided by the elapsed
// wall time. The first read seeds the baseline and emits nothing; a device that
// appears later is seeded on first sight; a counter that moves backwards
// re-baselines that device for one sample. Only the base diskstats fields
// (reads, sectors read, writes, sectors written) are read, so the 14/18/20-field
// layout variants across kernels all parse.
package diskio

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/LIVCK/agent/internal/collector"
	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/platform"
)

const (
	diskstatsPath = "/proc/diskstats"
	// sectorBytes is the fixed /proc/diskstats sector unit (always 512 bytes,
	// independent of the device's physical sector size).
	sectorBytes = 512
)

// counters holds the four cumulative diskstats fields diskio reads.
type counters struct {
	readsCompleted  uint64
	sectorsRead     uint64
	writesCompleted uint64
	sectorsWritten  uint64
}

// Collector computes per-device IO rates.
type Collector struct {
	fs    platform.FS
	clock platform.Clock
	cfg   func() *config.Config

	prev     map[string]counters
	prevTime time.Time
	seeded   bool
}

// New builds a diskio collector. clock provides the elapsed time between reads
// for the rate calculation.
func New(fs platform.FS, clock platform.Clock, cfg func() *config.Config) *Collector {
	return &Collector{fs: fs, clock: clock, cfg: cfg, prev: map[string]counters{}}
}

// Name returns "diskio".
func (*Collector) Name() string { return "diskio" }

// Available reports whether the diskio collector is enabled in the current
// config.
func (c *Collector) Available() bool { return c.cfg().Collectors.DiskIO.Enabled }

// Collect reads diskstats and emits rx/wx throughput and IOPS per device,
// capped to the configured device count by busiest total throughput.
func (c *Collector) Collect(context.Context) ([]collector.Sample, error) {
	data, err := c.fs.ReadFile(diskstatsPath)
	if err != nil {
		return nil, err
	}
	now := c.clock.Now()
	cur := parseDiskstats(data)

	if !c.seeded {
		c.prev = cur
		c.prevTime = now
		c.seeded = true
		return nil, nil
	}
	dt := now.Sub(c.prevTime)

	type devRate struct {
		dev                 string
		readBps, writeBps   float64
		readIops, writeIops float64
		total               float64
	}
	var rates []devRate
	for dev, cc := range cur {
		pc, ok := c.prev[dev]
		if !ok {
			continue // newly appeared: seeded below, no rate this sample
		}
		dReads, r1 := collector.CounterDelta(cc.readsCompleted, pc.readsCompleted)
		dSecR, r2 := collector.CounterDelta(cc.sectorsRead, pc.sectorsRead)
		dWrites, r3 := collector.CounterDelta(cc.writesCompleted, pc.writesCompleted)
		dSecW, r4 := collector.CounterDelta(cc.sectorsWritten, pc.sectorsWritten)
		if r1 || r2 || r3 || r4 {
			continue // reset: re-baselined via prev swap below
		}
		rb := collector.Rate(float64(dSecR*sectorBytes), dt)
		wb := collector.Rate(float64(dSecW*sectorBytes), dt)
		rates = append(rates, devRate{
			dev:       dev,
			readBps:   rb,
			writeBps:  wb,
			readIops:  collector.Rate(float64(dReads), dt),
			writeIops: collector.Rate(float64(dWrites), dt),
			total:     rb + wb,
		})
	}

	rates = collector.CapBySize(rates, c.cfg().Collectors.DiskIO.MaxDevices,
		func(d devRate) string { return d.dev },
		func(d devRate) float64 { return d.total })

	used := make(map[string]bool, len(rates))
	var samples []collector.Sample
	for _, r := range rates {
		seg := collector.DedupeSegment(collector.NormalizeSegment(r.dev), used)
		base := "sys.diskio." + seg + "."
		samples = append(samples,
			collector.Sample{Key: base + "read_bps", Value: r.readBps},
			collector.Sample{Key: base + "write_bps", Value: r.writeBps},
			collector.Sample{Key: base + "read_iops", Value: r.readIops},
			collector.Sample{Key: base + "write_iops", Value: r.writeIops},
		)
	}

	c.prev = cur
	c.prevTime = now
	return samples, nil
}

// parseDiskstats reads the base counters for each real block device, skipping
// loop/ram/fd/sr virtual devices and devices with no recorded IO, then drops
// partitions whose whole-disk parent is also present (sda1 when sda is listed)
// so the chart shows one series per physical disk, not per partition.
func parseDiskstats(data []byte) map[string]counters {
	out := make(map[string]counters, 16)
	for _, line := range strings.Split(string(data), "\n") {
		f := strings.Fields(line)
		if len(f) < 10 {
			continue
		}
		dev := f[2]
		if skipDevice(dev) {
			continue
		}
		c := counters{
			readsCompleted:  parseU(f[3]),
			sectorsRead:     parseU(f[5]),
			writesCompleted: parseU(f[7]),
			sectorsWritten:  parseU(f[9]),
		}
		if c == (counters{}) {
			continue // never did any IO: an idle virtual device
		}
		out[dev] = c
	}
	// A partition is redundant with its whole disk, which aggregates the same IO.
	for dev := range out {
		if parent := parentDevice(dev); parent != "" {
			if _, ok := out[parent]; ok {
				delete(out, dev)
			}
		}
	}
	return out
}

// skipDevice filters out virtual devices that are not real storage.
func skipDevice(dev string) bool {
	for _, p := range []string{"loop", "ram", "fd", "sr"} {
		if strings.HasPrefix(dev, p) {
			return true
		}
	}
	return false
}

// parentDevice returns a candidate whole-disk name for a partition, or "" when
// dev can form none. It is purely lexical (no sysfs) and deliberately only a
// candidate: name alone cannot tell sda1 (a partition of sda) from mmcblk0 (a
// whole disk), since both are letters followed by digits. The caller resolves
// the ambiguity by keeping dev unless the candidate is itself a listed device,
// so a bogus candidate like nvme0n (for the whole disk nvme0n1) is never in the
// set and nvme0n1 is kept. It strips the trailing digits, then the "p" separator
// for nvme/mmc (nvme0n1p1 -> nvme0n1) or requires a letter before the digits for
// sd/vd/hd (sda1 -> sda).
func parentDevice(dev string) string {
	i := len(dev)
	for i > 0 && dev[i-1] >= '0' && dev[i-1] <= '9' {
		i--
	}
	if i == len(dev) || i == 0 {
		return "" // no trailing digits (or all digits): not a partition name
	}
	base := dev[:i]
	if strings.HasSuffix(base, "p") && len(base) > 1 {
		return base[:len(base)-1] // nvme/mmc style: <parent>p<n>
	}
	if last := base[len(base)-1]; last >= 'a' && last <= 'z' {
		return base // sd/vd/hd style: <parent><n>
	}
	return ""
}

func parseU(s string) uint64 {
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	return v
}

var _ collector.Collector = (*Collector)(nil)
