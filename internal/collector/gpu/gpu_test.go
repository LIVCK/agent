package gpu

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/LIVCK/agent/internal/collector"
	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/platform"
	"github.com/LIVCK/agent/internal/platform/platformtest"
)

func toMap(s []collector.Sample) map[string]float64 {
	m := map[string]float64{}
	for _, x := range s {
		m[x.Key] = x.Value
	}
	return m
}

func cfgGPU(on bool) func() *config.Config {
	c := config.Defaults()
	c.Features.GPU = on
	return func() *config.Config { return c }
}

// nvidiaFS returns a MemFS that presents an NVIDIA device node so nvidiaPresent
// passes without forking.
func nvidiaFS() *platformtest.MemFS {
	fs := platformtest.NewMemFS()
	fs.WriteFileAtomic("/dev/nvidia0", []byte{}, 0o600)
	return fs
}

func nvidiaExec(csv string) *platformtest.Exec {
	ex := platformtest.NewExec().AddPath(nvidiaSMI)
	ex.SetResponse([]byte(csv), nil, nvidiaSMI, nvidiaArgs...)
	return ex
}

func TestNVIDIACSVEmitsKeys(t *testing.T) {
	fs := nvidiaFS()
	ex := nvidiaExec("0, GPU-abc, 00000000:65:00.0, 15, 1024, 24576, 45, 70.50\n")
	c := New(fs, ex, cfgGPU(true))

	if !c.Available() {
		t.Fatal("expected Available with feature on + nvidia present")
	}
	m := toMap(mustCollect(t, c))
	seg := "00000000_65_00.0"
	if m["sys.gpu."+seg+".util_pct"] != 15 {
		t.Errorf("util_pct = %v", m["sys.gpu."+seg+".util_pct"])
	}
	if m["sys.gpu."+seg+".mem_used_bytes"] != 1024*1024*1024 {
		t.Errorf("mem_used_bytes = %v", m["sys.gpu."+seg+".mem_used_bytes"])
	}
	if m["sys.gpu."+seg+".mem_total_bytes"] != 24576*1024*1024 {
		t.Errorf("mem_total_bytes = %v", m["sys.gpu."+seg+".mem_total_bytes"])
	}
	if got := m["sys.gpu."+seg+".mem_used_pct"]; got < 4.16 || got > 4.17 {
		t.Errorf("mem_used_pct = %v, want ~4.166", got)
	}
	if m["sys.gpu."+seg+".temp_c"] != 45 {
		t.Errorf("temp_c = %v", m["sys.gpu."+seg+".temp_c"])
	}
	if m["sys.gpu."+seg+".power_w"] != 70.5 {
		t.Errorf("power_w = %v", m["sys.gpu."+seg+".power_w"])
	}
}

func TestNVIDIANotAvailableFieldsOmitted(t *testing.T) {
	fs := nvidiaFS()
	// power.draw and temperature are [N/A]/[Not Supported] on some consumer cards.
	ex := nvidiaExec("0, GPU-x, 00000000:01:00.0, 7, 512, 8192, [N/A], [Not Supported]\n")
	c := New(fs, ex, cfgGPU(true))

	m := toMap(mustCollect(t, c))
	seg := "00000000_01_00.0"
	if _, ok := m["sys.gpu."+seg+".temp_c"]; ok {
		t.Error("temp_c must be omitted when [N/A]")
	}
	if _, ok := m["sys.gpu."+seg+".power_w"]; ok {
		t.Error("power_w must be omitted when [Not Supported]")
	}
	if m["sys.gpu."+seg+".util_pct"] != 7 {
		t.Errorf("util_pct = %v", m["sys.gpu."+seg+".util_pct"])
	}
}

func TestNVIDIAExecErrorNoKeysNoCrash(t *testing.T) {
	fs := nvidiaFS()
	ex := platformtest.NewExec().AddPath(nvidiaSMI)
	ex.SetResponse(nil, errors.New("boom"), nvidiaSMI, nvidiaArgs...)
	c := New(fs, ex, cfgGPU(true))

	if s := mustCollect(t, c); len(s) != 0 {
		t.Fatalf("exec error must yield no samples, got %v", s)
	}
}

func TestNVIDIATimeoutNoKeys(t *testing.T) {
	fs := nvidiaFS()
	ex := platformtest.NewExec().AddPath(nvidiaSMI)
	ex.SetResponse(nil, context.DeadlineExceeded, nvidiaSMI, nvidiaArgs...)
	c := New(fs, ex, cfgGPU(true))

	if s := mustCollect(t, c); len(s) != 0 {
		t.Fatalf("timeout must yield no samples, got %v", s)
	}
}

func TestNVIDIAMalformedRowSkipped(t *testing.T) {
	fs := nvidiaFS()
	// A short row is skipped; the well-formed row still emits.
	ex := nvidiaExec("not,enough,fields\n1, GPU-y, 00000000:02:00.0, 9, 100, 200, 40, 30\n")
	c := New(fs, ex, cfgGPU(true))

	m := toMap(mustCollect(t, c))
	if m["sys.gpu.00000000_02_00.0.util_pct"] != 9 {
		t.Errorf("well-formed row must still emit: %v", m)
	}
}

func TestNVIDIACardinalityCap(t *testing.T) {
	fs := nvidiaFS()
	var b strings.Builder
	for i := 1; i <= 10; i++ {
		fmt.Fprintf(&b, "%d, GPU-%d, 00000000:%02x:00.0, 5, 100, 200, 40, 30\n", i, i, i)
	}
	c := New(fs, nvidiaExec(b.String()), cfgGPU(true))

	m := toMap(mustCollect(t, c))
	gpuKeys := 0
	for k := range m {
		if strings.HasPrefix(k, "sys.gpu.") {
			gpuKeys++
		}
	}
	if gpuKeys != maxGPUs*6 {
		t.Errorf("expected %d gpu keys (cap), got %d", maxGPUs*6, gpuKeys)
	}
	// PCI 01..08 kept, 09 and 0a dropped (ascending PCI, stability over activity).
	if _, ok := m["sys.gpu.00000000_08_00.0.util_pct"]; !ok {
		t.Error("lowest 8 PCI addresses must be kept")
	}
	if _, ok := m["sys.gpu.00000000_09_00.0.util_pct"]; ok {
		t.Error("9th PCI address must be dropped by the cap")
	}
}

func TestAMDSysfsEmitsKeys(t *testing.T) {
	fs := platformtest.NewMemFS()
	dev := drmDir + "/card0/device"
	fs.WriteFileAtomic(dev+"/vendor", []byte("0x1002\n"), 0o600)
	fs.WriteFileAtomic(dev+"/gpu_busy_percent", []byte("42\n"), 0o600)
	fs.WriteFileAtomic(dev+"/mem_info_vram_used", []byte("8589934592\n"), 0o600)
	fs.WriteFileAtomic(dev+"/mem_info_vram_total", []byte("17179869184\n"), 0o600)
	fs.WriteFileAtomic(dev+"/uevent", []byte("DRIVER=amdgpu\nPCI_SLOT_NAME=0000:03:00.0\n"), 0o600)
	fs.WriteFileAtomic(dev+"/hwmon/hwmon2/temp1_input", []byte("55000\n"), 0o600)
	fs.WriteFileAtomic(dev+"/hwmon/hwmon2/power1_average", []byte("120000000\n"), 0o600)
	// A connector sub-node must be ignored by the ^card[0-9]+$ filter.
	fs.WriteFileAtomic(drmDir+"/card0-DP-1/dpms", []byte("On\n"), 0o600)

	c := New(fs, platformtest.NewExec(), cfgGPU(true))
	if !c.Available() {
		t.Fatal("expected Available with feature on + AMD card present")
	}
	m := toMap(mustCollect(t, c))
	seg := "0000_03_00.0"
	if m["sys.gpu."+seg+".util_pct"] != 42 {
		t.Errorf("util_pct = %v", m["sys.gpu."+seg+".util_pct"])
	}
	if m["sys.gpu."+seg+".mem_used_bytes"] != 8589934592 {
		t.Errorf("mem_used_bytes = %v", m["sys.gpu."+seg+".mem_used_bytes"])
	}
	if m["sys.gpu."+seg+".mem_used_pct"] != 50 {
		t.Errorf("mem_used_pct = %v, want 50", m["sys.gpu."+seg+".mem_used_pct"])
	}
	if m["sys.gpu."+seg+".temp_c"] != 55 {
		t.Errorf("temp_c = %v, want 55 (milli-degrees /1000)", m["sys.gpu."+seg+".temp_c"])
	}
	if m["sys.gpu."+seg+".power_w"] != 120 {
		t.Errorf("power_w = %v, want 120 (micro-watts /1e6)", m["sys.gpu."+seg+".power_w"])
	}
}

func TestAMDNonAMDVendorIgnored(t *testing.T) {
	fs := platformtest.NewMemFS()
	dev := drmDir + "/card0/device"
	fs.WriteFileAtomic(dev+"/vendor", []byte("0x8086\n"), 0o600) // Intel
	fs.WriteFileAtomic(dev+"/gpu_busy_percent", []byte("10\n"), 0o600)
	fs.WriteFileAtomic(dev+"/uevent", []byte("PCI_SLOT_NAME=0000:00:02.0\n"), 0o600)

	c := New(fs, platformtest.NewExec(), cfgGPU(true))
	if c.Available() {
		t.Fatal("non-AMD vendor must not count as an available GPU")
	}
	if s := mustCollect(t, c); len(s) != 0 {
		t.Fatalf("non-AMD vendor must yield no samples, got %v", s)
	}
}

func TestAMDMissingAttributesOmitted(t *testing.T) {
	fs := platformtest.NewMemFS()
	dev := drmDir + "/card0/device"
	fs.WriteFileAtomic(dev+"/vendor", []byte("0x1002\n"), 0o600)
	fs.WriteFileAtomic(dev+"/gpu_busy_percent", []byte("33\n"), 0o600)
	fs.WriteFileAtomic(dev+"/uevent", []byte("PCI_SLOT_NAME=0000:03:00.0\n"), 0o600)
	// No vram, no hwmon: those keys must be absent, util still present.

	c := New(fs, platformtest.NewExec(), cfgGPU(true))
	m := toMap(mustCollect(t, c))
	seg := "0000_03_00.0"
	if m["sys.gpu."+seg+".util_pct"] != 33 {
		t.Errorf("util_pct = %v", m["sys.gpu."+seg+".util_pct"])
	}
	for _, k := range []string{"mem_used_bytes", "mem_total_bytes", "mem_used_pct", "temp_c", "power_w"} {
		if _, ok := m["sys.gpu."+seg+"."+k]; ok {
			t.Errorf("%s must be omitted when the sysfs attribute is missing", k)
		}
	}
}

func TestFeatureOffNotAvailable(t *testing.T) {
	fs := nvidiaFS()
	c := New(fs, nvidiaExec("0, GPU, 00000000:65:00.0, 10, 1, 2, 40, 30\n"), cfgGPU(false))
	if c.Available() {
		t.Fatal("feature off must not be Available even with hardware present")
	}
}

func TestNoHardwareNotAvailable(t *testing.T) {
	fs := platformtest.NewMemFS()
	ex := platformtest.NewExec() // no nvidia-smi in PATH, no drm cards
	c := New(fs, ex, cfgGPU(true))
	if c.Available() {
		t.Fatal("feature on but no hardware must not be Available")
	}
}

func TestNilExecAMDStillWorks(t *testing.T) {
	// A build that wires no Exec must still report AMD (pure sysfs) and never
	// nil-panic on the NVIDIA path.
	fs := platformtest.NewMemFS()
	dev := drmDir + "/card0/device"
	fs.WriteFileAtomic(dev+"/vendor", []byte("0x1002\n"), 0o600)
	fs.WriteFileAtomic(dev+"/gpu_busy_percent", []byte("5\n"), 0o600)
	fs.WriteFileAtomic(dev+"/uevent", []byte("PCI_SLOT_NAME=0000:03:00.0\n"), 0o600)

	var noExec platform.Exec
	c := New(fs, noExec, cfgGPU(true))
	if !c.Available() {
		t.Fatal("AMD must be available with a nil Exec")
	}
	if s := mustCollect(t, c); len(s) == 0 {
		t.Fatal("AMD samples must be produced with a nil Exec")
	}
}

func mustCollect(t *testing.T, c *Collector) []collector.Sample {
	t.Helper()
	s, err := c.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect error: %v", err)
	}
	return s
}

func TestQueryNamesMapsNormalizedPCIToName(t *testing.T) {
	nameArgs := []string{"--query-gpu=pci.bus_id,name", "--format=csv,noheader"}
	ex := platformtest.NewExec().AddPath(nvidiaSMI)
	ex.SetResponse(
		[]byte("00000000:01:00.0, NVIDIA GeForce RTX 4080\n00000000:02:00.0, NVIDIA RTX A2000\n"),
		nil, nvidiaSMI, nameArgs...,
	)

	names := QueryNames(context.Background(), ex)

	if got := names["00000000_01_00.0"]; got != "NVIDIA GeForce RTX 4080" {
		t.Fatalf("gpu 01:00.0 name = %q, want NVIDIA GeForce RTX 4080", got)
	}
	if got := names["00000000_02_00.0"]; got != "NVIDIA RTX A2000" {
		t.Fatalf("gpu 02:00.0 name = %q, want NVIDIA RTX A2000", got)
	}
}

func TestQueryNamesAbsentToolReturnsNil(t *testing.T) {
	if QueryNames(context.Background(), platformtest.NewExec()) != nil {
		t.Fatal("want nil when nvidia-smi is not on PATH")
	}
}
