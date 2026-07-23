package gpu

import (
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/LIVCK/agent/internal/collector"
	"github.com/LIVCK/agent/internal/platform"
)

const (
	drmDir    = "/sys/class/drm"
	amdVendor = "0x1002"
)

// pciIDsPaths lists the standard host locations of the PCI ID database, tried in
// order. AMD has no nvidia-smi equivalent that prints a model name, so the name
// is resolved from the card's PCI vendor:device against this file. It is a pure
// read — the agent never installs it — so a host without hwdata simply yields no
// AMD names (the console falls back to an ordinal label).
var pciIDsPaths = []string{
	"/usr/share/hwdata/pci.ids",
	"/usr/share/misc/pci.ids",
}

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

// QueryAMDNames returns a normalized-PCI → product-name map for the present AMD
// GPUs (e.g. "0000_28_00.0" → "Navi 31 [Radeon RX 7900 XTX/...]"). AMD has no
// nvidia-smi equivalent, so each card's model is resolved from its PCI
// vendor:device (sysfs device/vendor + device/device) against the host pci.ids
// database. It mirrors the NVIDIA QueryNames seeding path so an AMD card reads as
// a model name instead of a bare PCI address in the "Devices" panel. Best-effort:
// returns nil when no AMD card is present or pci.ids is absent, and a card whose
// id is not in the database is simply omitted (never fatal, like NVIDIA's
// nil-on-absence).
func QueryAMDNames(fs platform.FS) map[string]string {
	if fs == nil {
		return nil
	}
	c := &Collector{fs: fs}
	cards := c.amdCards()
	if len(cards) == 0 {
		return nil
	}
	db := readPCIIDs(fs)
	if db == nil {
		return nil
	}
	names := make(map[string]string)
	for _, name := range cards {
		dev := drmDir + "/" + name + "/device"
		pci := c.readPCISlot(dev)
		if pci == "" {
			continue
		}
		vendor := normalizeHexID(c.readString(dev + "/vendor"))
		device := normalizeHexID(c.readString(dev + "/device"))
		if vendor == "" || device == "" {
			continue
		}
		if model := resolvePCIName(db, vendor, device); model != "" {
			names[collector.NormalizeSegment(pci)] = model
		}
	}
	if len(names) == 0 {
		return nil
	}
	return names
}

// readPCIIDs loads the first available host pci.ids database, or nil when none
// exists (the name lookup then yields nothing, best-effort).
func readPCIIDs(fs platform.FS) []byte {
	for _, p := range pciIDsPaths {
		if data, err := fs.ReadFile(p); err == nil && len(data) > 0 {
			return data
		}
	}
	return nil
}

// normalizeHexID turns a sysfs PCI id file body ("0x1002\n") into the bare,
// lowercase 4-hex-digit form ("1002") pci.ids uses. Returns "" for an empty body.
func normalizeHexID(raw string) string {
	s := strings.ToLower(strings.TrimSpace(raw))
	return strings.TrimPrefix(s, "0x")
}

// resolvePCIName scans a pci.ids database body for the model name of a
// vendor:device pair (both bare, lowercase hex, e.g. "1002", "13c0"). The file is
// flat and tab-indented: a vendor line has no leading tab, its device lines carry
// one leading tab, and per-device subsystem lines carry two. It walks into the
// matching vendor stanza, returns the matching device's name, and stops at the
// next vendor line (or end) — returning "" when either is not found.
func resolvePCIName(db []byte, vendor, device string) string {
	inVendor := false
	for _, line := range strings.Split(string(db), "\n") {
		if line == "" || line[0] == '#' {
			continue
		}
		if line[0] != '\t' {
			if inVendor {
				return "" // left the vendor stanza without a device match
			}
			if id, _, ok := cutPCIEntry(line); ok && id == vendor {
				inVendor = true
			}
			continue
		}
		if !inVendor {
			continue
		}
		if strings.HasPrefix(line, "\t\t") {
			continue // subsystem line, not a device
		}
		if id, name, ok := cutPCIEntry(line[1:]); ok && id == device {
			return name
		}
	}
	return ""
}

// cutPCIEntry splits one pci.ids entry ("id  name": a hex id, two spaces, the
// name) into its id and name. It reports ok=false when the line lacks the
// two-space separator or either half is empty.
func cutPCIEntry(line string) (id, name string, ok bool) {
	id, name, found := strings.Cut(line, "  ")
	if !found {
		return "", "", false
	}
	id = strings.TrimSpace(id)
	name = strings.TrimSpace(name)
	if id == "" || name == "" {
		return "", "", false
	}
	return id, name, true
}
