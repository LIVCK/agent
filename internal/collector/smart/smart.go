// Package smart reports SMART health for the host's storage devices. Like the
// gpu collector it is opt-in-by-presence: it runs only when features.smart is
// enabled AND smartctl is installed, and it never installs anything. It uses the
// structured JSON form of smartctl (--json=c), never the human-readable report,
// so parsing stays stable across smartctl versions. The device list comes
// from smartctl's own --scan output, never from user or config input, and each
// device is re-validated before it reaches an argv, so there is no injection
// surface. A key is emitted only for a value the drive actually reports; a
// missing attribute is omitted, never zero-faked.
package smart

import (
	"context"
	"encoding/json"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/LIVCK/agent/internal/collector"
	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/platform"
)

const (
	smartctl = "smartctl"
	// execTimeout hard-bounds each smartctl fork so a wedged device probe can
	// never stall the collect loop.
	execTimeout = 2 * time.Second
	// maxDevices caps how many drives one report describes (6 keys each). Drives
	// are ordered by device name and the surplus is dropped deterministically.
	maxDevices = 16
)

// scanArgs and the per-device flags are constant. Only the device path is
// variable, and it comes from --scan (validated by validDevice), never a user.
var scanArgs = []string{"--json=c", "--scan"}

// deviceRe bounds an acceptable device path. --scan yields paths like /dev/sda,
// /dev/nvme0 or /dev/bus/0; anything outside this shape is refused before it can
// become an argv element.
var deviceRe = regexp.MustCompile(`^/dev/[A-Za-z0-9/_.-]+$`)

// Collector reports SMART health via smartctl.
type Collector struct {
	exec platform.Exec
	cfg  func() *config.Config
}

// New builds a smart collector.
func New(exec platform.Exec, cfg func() *config.Config) *Collector {
	return &Collector{exec: exec, cfg: cfg}
}

// Name returns "smart".
func (*Collector) Name() string { return "smart" }

// IntervalSampled marks smart as interval-sampled: it forks smartctl per device,
// so the runner collects it once per report interval instead of at the 5s
// sub-sample cadence (no fork storm across a host's drives every 5s).
func (*Collector) IntervalSampled() bool { return true }

// Available reports whether the feature is enabled and smartctl is installed.
// LookPath does not fork; the scan fork is deferred to Collect.
func (c *Collector) Available() bool {
	if !c.cfg().Features.Smart || c.exec == nil {
		return false
	}
	_, ok := c.exec.LookPath(smartctl)
	return ok
}

// Collect scans for devices and reads each one's SMART attributes. It is
// best-effort: a failed scan or a failed device read yields fewer keys, never an
// error that would spam the loop.
func (c *Collector) Collect(ctx context.Context) ([]collector.Sample, error) {
	devs := c.scan(ctx)
	if len(devs) > maxDevices {
		devs = devs[:maxDevices]
	}
	used := make(map[string]bool, len(devs))
	var samples []collector.Sample
	for _, dev := range devs {
		info, ok := c.readDevice(ctx, dev)
		if !ok {
			continue
		}
		seg := collector.DedupeSegment(collector.NormalizeSegment(devSegment(dev)), used)
		samples = append(samples, info.samples(seg)...)
	}
	return samples, nil
}

// scan runs smartctl --scan and returns the validated, de-duplicated, sorted device paths.
func (c *Collector) scan(parent context.Context) []string {
	return scanDevices(parent, c.exec)
}

// scanDevices runs `smartctl --json --scan` and returns the validated, de-duplicated, sorted device
// paths. Shared by the collector and QueryNames; --scan needs no privileges (per-device reads do).
func scanDevices(parent context.Context, exec platform.Exec) []string {
	ctx, cancel := context.WithTimeout(parent, execTimeout)
	defer cancel()
	out, err := exec.Run(ctx, smartctl, scanArgs...)
	if err != nil && len(out) == 0 {
		return nil
	}
	var sr scanResult
	if json.Unmarshal(out, &sr) != nil {
		return nil
	}
	seen := make(map[string]bool, len(sr.Devices))
	var names []string
	for _, d := range sr.Devices {
		name := strings.TrimSpace(d.Name)
		if !validDevice(name) || seen[name] {
			continue
		}
		seen[name] = true
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// QueryNames returns a normalized device-segment → model-name map for the host's drives (e.g.
// "nvme0" → "Samsung SSD 9100 PRO 2TB"). Forked ONCE at startup to seed the report meta with
// human-readable disk names — metric keys are float64 and cannot carry a string. It needs the same
// privileges as the collector (root for NVMe SMART); on a non-privileged/absent host it returns nil
// or a partial map. Best-effort, never fatal.
func QueryNames(ctx context.Context, exec platform.Exec) map[string]string {
	if exec == nil {
		return nil
	}
	if _, ok := exec.LookPath(smartctl); !ok {
		return nil
	}
	devs := scanDevices(ctx, exec)
	if len(devs) > maxDevices {
		devs = devs[:maxDevices]
	}
	names := make(map[string]string)
	used := make(map[string]bool, len(devs))
	for _, dev := range devs {
		cctx, cancel := context.WithTimeout(ctx, execTimeout)
		out, _ := exec.Run(cctx, smartctl, "--json=c", "-i", dev)
		cancel()
		var info deviceInfo
		if len(out) == 0 || json.Unmarshal(out, &info) != nil {
			continue
		}
		model := strings.TrimSpace(info.ModelName)
		if model == "" {
			continue
		}
		seg := collector.DedupeSegment(collector.NormalizeSegment(devSegment(dev)), used)
		names[seg] = model
	}
	return names
}

// readDevice runs smartctl -a on one device and parses its JSON. smartctl exits
// non-zero on a failing or ageing drive while still printing valid JSON on
// stdout, so the exit error is deliberately ignored and only a parse failure (or
// empty output, e.g. after a timeout) drops the device.
func (c *Collector) readDevice(parent context.Context, dev string) (deviceInfo, bool) {
	ctx, cancel := context.WithTimeout(parent, execTimeout)
	defer cancel()
	out, _ := c.exec.Run(ctx, smartctl, "--json=c", "-a", dev)
	var info deviceInfo
	if len(out) == 0 || json.Unmarshal(out, &info) != nil {
		return deviceInfo{}, false
	}
	return info, true
}

// validDevice reports whether path is a plausible /dev node, guarding the argv.
func validDevice(path string) bool {
	return len(path) > len("/dev/") && len(path) <= 256 && deviceRe.MatchString(path)
}

// devSegment strips the leading /dev/ so the metric segment reads sda / nvme0
// rather than _dev_sda, matching the diskio device style.
func devSegment(dev string) string {
	return strings.TrimPrefix(dev, "/dev/")
}
