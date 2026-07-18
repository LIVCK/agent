// Package gpu reports per-GPU utilization, memory, temperature and power for
// AMD and NVIDIA cards. It is an opt-in-by-presence feature: it runs only when
// features.gpu is enabled AND a supported GPU is actually present, and it never
// installs anything. AMD is read purely from sysfs (/sys/class/drm/card*/device,
// the same file-only path the sensors feature uses). NVIDIA has no file-only
// source for live util/VRAM on the proprietary CUDA stack, so it uses the
// structured query form of nvidia-smi (--query-gpu=... --format=csv) - the same
// structured, machine-readable tool output the agent prefers elsewhere (smartctl
// --json, systemctl show -p), never the human dashboard. The nvidia-smi fork happens
// only inside Collect (once per report interval), behind a cheap presence gate,
// bounded by a hard timeout, and can never block the collect loop.
package gpu

import (
	"context"
	"sort"

	"github.com/LIVCK/agent/internal/collector"
	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/platform"
)

// maxGPUs caps how many GPUs one report describes. 6 keys per GPU means 8 GPUs
// contribute at most 48 keys, which stays clear of the 120-key report ceiling
// alongside the ~40-key sys.* baseline. 8 covers DGX/HGX 8-way boxes; over-cap
// GPUs are dropped deterministically by ascending PCI address (identity
// stability over activity, since GPUs have no meaningful "size" to rank by).
const maxGPUs = 8

// metric is an optional float: has is false when the source did not report the
// value (an [N/A] field, a missing sysfs attribute) so the key is omitted rather
// than faked to zero.
type metric struct {
	val float64
	has bool
}

// reading is one GPU's raw values before key assembly. pci is the identity
// segment: the PCI bus address is stable across reboots and GPU add/remove,
// unlike an enumeration index, so the time series does not migrate between
// cards.
type reading struct {
	pci      string
	util     metric
	memUsed  metric
	memTotal metric
	temp     metric
	power    metric
}

// Collector reports GPU telemetry from AMD sysfs and/or NVIDIA nvidia-smi.
type Collector struct {
	fs   platform.FS
	exec platform.Exec
	cfg  func() *config.Config
}

// New builds a gpu collector. exec runs nvidia-smi (may be nil in a build that
// never enables the feature; Available then only reports AMD presence).
func New(fs platform.FS, exec platform.Exec, cfg func() *config.Config) *Collector {
	return &Collector{fs: fs, exec: exec, cfg: cfg}
}

// Name returns "gpu".
func (*Collector) Name() string { return "gpu" }

// IntervalSampled marks gpu as interval-sampled: the NVIDIA path forks
// nvidia-smi, so the runner collects it once per report interval instead of at
// the 5s sub-sample cadence (no fork storm). The AMD sysfs path is cheap but the
// collector is one unit, so it rides the same cadence; one sample per interval
// means an avg+max key's mean equals its peak, which is acceptable.
func (*Collector) IntervalSampled() bool { return true }

// Available reports whether the feature is enabled and a GPU is present. Both
// presence checks are cheap (a sysfs scan for AMD, LookPath plus a device-node
// stat for NVIDIA); neither forks nvidia-smi, so this is safe to call every
// cycle.
func (c *Collector) Available() bool {
	if !c.cfg().Features.GPU {
		return false
	}
	return c.amdPresent() || c.nvidiaPresent()
}

// Collect gathers every present GPU, caps the set to maxGPUs by ascending PCI
// address, and emits the per-GPU keys. It is best-effort: a tool failure or a
// missing attribute yields fewer keys, never an error that would spam the loop.
func (c *Collector) Collect(ctx context.Context) ([]collector.Sample, error) {
	var readings []reading
	readings = append(readings, c.collectAMD()...)
	if c.nvidiaPresent() {
		readings = append(readings, c.collectNVIDIA(ctx)...)
	}

	// Stable, deterministic identity order for both the cap and the key order.
	sort.Slice(readings, func(i, j int) bool { return readings[i].pci < readings[j].pci })
	if len(readings) > maxGPUs {
		readings = readings[:maxGPUs]
	}

	used := make(map[string]bool, len(readings))
	var samples []collector.Sample
	for _, rd := range readings {
		seg := collector.DedupeSegment(collector.NormalizeSegment(rd.pci), used)
		samples = append(samples, rd.samples(seg)...)
	}
	return samples, nil
}

// samples turns one reading into its present keys. mem_used_pct is derived from
// the two byte values and emitted only when both are known.
func (rd reading) samples(seg string) []collector.Sample {
	base := "sys.gpu." + seg + "."
	var out []collector.Sample
	if rd.util.has {
		out = append(out, collector.Sample{Key: base + "util_pct", Value: collector.ClampPercent(rd.util.val)})
	}
	if rd.memUsed.has {
		out = append(out, collector.Sample{Key: base + "mem_used_bytes", Value: rd.memUsed.val})
	}
	if rd.memTotal.has {
		out = append(out, collector.Sample{Key: base + "mem_total_bytes", Value: rd.memTotal.val})
	}
	if rd.memUsed.has && rd.memTotal.has {
		out = append(out, collector.Sample{Key: base + "mem_used_pct", Value: collector.RatioPercent(rd.memUsed.val, rd.memTotal.val)})
	}
	if rd.temp.has {
		out = append(out, collector.Sample{Key: base + "temp_c", Value: rd.temp.val})
	}
	if rd.power.has {
		out = append(out, collector.Sample{Key: base + "power_w", Value: rd.power.val})
	}
	return out
}

var _ collector.Collector = (*Collector)(nil)
