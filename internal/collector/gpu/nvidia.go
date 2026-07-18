package gpu

import (
	"bytes"
	"context"
	"encoding/csv"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/LIVCK/agent/internal/collector"
	"github.com/LIVCK/agent/internal/platform"
)

// QueryNames returns a normalized-PCI → product-name map for the present NVIDIA GPUs (e.g.
// "00000000_01_00.0" → "NVIDIA GeForce RTX 4080"). It forks nvidia-smi ONCE — intended for startup,
// to seed the report meta with human-readable GPU names while the periodic metric query stays
// name-free. Returns nil when nvidia-smi is absent or on any error (best-effort, never fatal).
func QueryNames(ctx context.Context, exec platform.Exec) map[string]string {
	if exec == nil {
		return nil
	}
	if _, ok := exec.LookPath(nvidiaSMI); !ok {
		return nil
	}
	cctx, cancel := context.WithTimeout(ctx, execTimeout)
	defer cancel()
	out, err := exec.Run(cctx, nvidiaSMI, "--query-gpu=pci.bus_id,name", "--format=csv,noheader")
	if err != nil {
		return nil
	}

	names := make(map[string]string)
	r := csv.NewReader(bytes.NewReader(out))
	r.TrimLeadingSpace = true
	r.FieldsPerRecord = -1
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil || len(rec) < 2 {
			continue
		}
		pci := strings.TrimSpace(rec[0])
		name := strings.TrimSpace(rec[1])
		if pci == "" || name == "" || strings.HasPrefix(pci, "[") {
			continue
		}
		names[collector.NormalizeSegment(pci)] = name
	}
	return names
}

const (
	nvidiaSMI = "nvidia-smi"
	// execTimeout hard-bounds the nvidia-smi fork so a hung driver context can
	// never stall the collect loop. It mirrors the disk stuck-mount deadline.
	execTimeout = 2 * time.Second
	// mib converts nvidia-smi's MiB memory fields (with nounits) to bytes.
	mib = 1024 * 1024
)

// nvidiaArgs is the fixed, constant argv. The column order is pinned by the
// explicit --query-gpu list (so parsing is positional and deterministic) and
// nounits strips the unit suffixes so every field is a bare number. No element
// of this argv is user- or config-derived: there is no injection surface.
var nvidiaArgs = []string{
	"--query-gpu=index,uuid,pci.bus_id,utilization.gpu,memory.used,memory.total,temperature.gpu,power.draw",
	"--format=csv,noheader,nounits",
}

// nvidiaPresent reports whether nvidia-smi exists and an NVIDIA GPU is plausibly
// present, without forking nvidia-smi (the expensive fork is deferred to
// Collect). LookPath resolves the binary; a device node or the driver's proc
// directory confirms hardware.
func (c *Collector) nvidiaPresent() bool {
	if c.exec == nil {
		return false
	}
	if _, ok := c.exec.LookPath(nvidiaSMI); !ok {
		return false
	}
	if _, err := c.fs.Stat("/dev/nvidia0"); err == nil {
		return true
	}
	if entries, err := c.fs.ReadDir("/proc/driver/nvidia/gpus"); err == nil && len(entries) > 0 {
		return true
	}
	return false
}

// collectNVIDIA runs the pinned nvidia-smi query under a hard timeout and parses
// its CSV. Any error (missing tool, timeout, non-zero exit) degrades to no
// readings.
func (c *Collector) collectNVIDIA(parent context.Context) []reading {
	ctx, cancel := context.WithTimeout(parent, execTimeout)
	defer cancel()
	out, err := c.exec.Run(ctx, nvidiaSMI, nvidiaArgs...)
	if err != nil {
		return nil
	}
	return parseNVIDIACSV(out)
}

// parseNVIDIACSV turns nvidia-smi CSV rows into readings. It reads record by
// record so one malformed row cannot discard the rest, tolerates a driver that
// returns a different column count, and treats [N/A]/[Not Supported]/empty
// fields as absent. A row without a PCI address is skipped (no stable identity).
func parseNVIDIACSV(data []byte) []reading {
	r := csv.NewReader(bytes.NewReader(data))
	r.TrimLeadingSpace = true
	r.FieldsPerRecord = -1

	var out []reading
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		if len(rec) < 8 {
			continue
		}
		pci := strings.TrimSpace(rec[2])
		if pci == "" || strings.HasPrefix(pci, "[") {
			continue
		}
		rd := reading{pci: pci}
		if v, ok := parseNVVal(rec[3]); ok {
			rd.util = metric{v, true}
		}
		if v, ok := parseNVVal(rec[4]); ok {
			rd.memUsed = metric{v * mib, true}
		}
		if v, ok := parseNVVal(rec[5]); ok {
			rd.memTotal = metric{v * mib, true}
		}
		if v, ok := parseNVVal(rec[6]); ok {
			rd.temp = metric{v, true}
		}
		if v, ok := parseNVVal(rec[7]); ok {
			rd.power = metric{v, true}
		}
		out = append(out, rd)
	}
	return out
}

// parseNVVal parses one nounits field. nvidia-smi renders an unsupported field
// as "[N/A]" or "[Not Supported]"; those and any non-numeric value are absent.
func parseNVVal(s string) (float64, bool) {
	s = strings.TrimSpace(s)
	if s == "" || strings.HasPrefix(s, "[") {
		return 0, false
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}
