package gpu

import (
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/LIVCK/agent/internal/platform"
)

const (
	drmDir    = "/sys/class/drm"
	amdVendor = "0x1002"
)

// cardRe matches a real DRM card node (card0, card1, ...) and excludes the
// connector sub-nodes (card0-DP-1) that also live under /sys/class/drm.
var cardRe = regexp.MustCompile(`^card[0-9]+$`)

// amdPresent reports whether at least one AMD GPU card is exposed under
// /sys/class/drm. It is a pure file scan, cheap enough for Available.
func (c *Collector) amdPresent() bool {
	return len(c.amdCards()) > 0
}

// amdCards lists the DRM card names that are AMD GPUs: vendor 0x1002 with a
// gpu_busy_percent attribute (which confirms an amdgpu device, integrated or
// discrete). The result is sorted for a deterministic scan.
func (c *Collector) amdCards() []string {
	entries, err := c.fs.ReadDir(drmDir)
	if err != nil {
		return nil
	}
	var cards []string
	for _, e := range entries {
		name := e.Name()
		if !cardRe.MatchString(name) {
			continue
		}
		dev := drmDir + "/" + name + "/device"
		if strings.TrimSpace(c.readString(dev+"/vendor")) != amdVendor {
			continue
		}
		if _, err := c.fs.Stat(dev + "/gpu_busy_percent"); err != nil {
			continue
		}
		cards = append(cards, name)
	}
	sort.Strings(cards)
	return cards
}

// collectAMD reads every AMD card's sysfs attributes into a reading. A card
// without a resolvable PCI address is skipped (no stable identity); any
// individual missing attribute is left absent, never zero-faked.
func (c *Collector) collectAMD() []reading {
	var out []reading
	for _, name := range c.amdCards() {
		dev := drmDir + "/" + name + "/device"
		pci := c.readPCISlot(dev)
		if pci == "" {
			continue
		}
		rd := reading{pci: pci}
		if v, ok := readSysFloat(c.fs, dev+"/gpu_busy_percent"); ok {
			rd.util = metric{v, true}
		}
		if v, ok := readSysFloat(c.fs, dev+"/mem_info_vram_used"); ok {
			rd.memUsed = metric{v, true}
		}
		if v, ok := readSysFloat(c.fs, dev+"/mem_info_vram_total"); ok {
			rd.memTotal = metric{v, true}
		}
		// hwmon temp is milli-degrees C, power is micro-watts.
		if v, ok := c.readHwmon(dev, "temp1_input"); ok {
			rd.temp = metric{v / 1000.0, true}
		}
		if v, ok := c.readHwmon(dev, "power1_average"); ok {
			rd.power = metric{v / 1e6, true}
		}
		out = append(out, rd)
	}
	return out
}

// readPCISlot pulls PCI_SLOT_NAME (e.g. 0000:65:00.0) from the device uevent
// file, avoiding a readlink the FS abstraction does not expose.
func (c *Collector) readPCISlot(dev string) string {
	data, err := c.fs.ReadFile(dev + "/uevent")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		if s, ok := strings.CutPrefix(line, "PCI_SLOT_NAME="); ok {
			return strings.TrimSpace(s)
		}
	}
	return ""
}

// readHwmon finds the card's hwmon subdirectory and reads attr from it. The
// hwmon index is not stable, so the directory is scanned rather than assumed.
func (c *Collector) readHwmon(dev, attr string) (float64, bool) {
	hwmonDir := dev + "/hwmon"
	entries, err := c.fs.ReadDir(hwmonDir)
	if err != nil {
		return 0, false
	}
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), "hwmon") {
			continue
		}
		if v, ok := readSysFloat(c.fs, hwmonDir+"/"+e.Name()+"/"+attr); ok {
			return v, true
		}
	}
	return 0, false
}

// readString reads a small sysfs file, returning "" on any error.
func (c *Collector) readString(path string) string {
	data, err := c.fs.ReadFile(path)
	if err != nil {
		return ""
	}
	return string(data)
}

// readSysFloat parses a single-value sysfs file as a float, reporting ok=false
// on a read error or a non-numeric body.
func readSysFloat(fs platform.FS, path string) (float64, bool) {
	data, err := fs.ReadFile(path)
	if err != nil {
		return 0, false
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(string(data)), 64)
	if err != nil {
		return 0, false
	}
	return v, true
}
