package smart

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/LIVCK/agent/internal/collector"
	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/platform/platformtest"
)

func toMap(s []collector.Sample) map[string]float64 {
	m := map[string]float64{}
	for _, x := range s {
		m[x.Key] = x.Value
	}
	return m
}

func cfgSmart(on bool) func() *config.Config {
	c := config.Defaults()
	c.Features.Smart = on
	return func() *config.Config { return c }
}

// execWith wires a scan response plus one -a response per device.
func execWith(scanJSON string, devJSON map[string]string) *platformtest.Exec {
	ex := platformtest.NewExec().AddPath(smartctl)
	ex.SetResponse([]byte(scanJSON), nil, smartctl, scanArgs...)
	for dev, j := range devJSON {
		ex.SetResponse([]byte(j), nil, smartctl, "--json=c", "-a", dev)
	}
	return ex
}

func mustCollect(t *testing.T, c *Collector) []collector.Sample {
	t.Helper()
	s, err := c.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect error: %v", err)
	}
	return s
}

func TestATADeviceEmitsKeys(t *testing.T) {
	ex := execWith(
		`{"devices":[{"name":"/dev/sda","type":"sat"}]}`,
		map[string]string{
			"/dev/sda": `{"smart_status":{"passed":true},"temperature":{"current":32},` +
				`"power_on_time":{"hours":12345},"ata_smart_attributes":{"table":[` +
				`{"id":5,"raw":{"value":4}},{"id":197,"raw":{"value":2}}]}}`,
		})
	c := New(ex, cfgSmart(true))
	if !c.Available() {
		t.Fatal("expected Available with feature on + smartctl present")
	}
	m := toMap(mustCollect(t, c))
	want := map[string]float64{
		"sys.smart.sda.health_ok":      1,
		"sys.smart.sda.temp_c":         32,
		"sys.smart.sda.reallocated":    4,
		"sys.smart.sda.pending":        2,
		"sys.smart.sda.power_on_hours": 12345,
	}
	for k, v := range want {
		if m[k] != v {
			t.Errorf("%s = %v, want %v", k, m[k], v)
		}
	}
	if _, ok := m["sys.smart.sda.wear_pct"]; ok {
		t.Error("wear_pct must be omitted for an ATA drive with no percentage_used")
	}
}

func TestNVMeDeviceEmitsWear(t *testing.T) {
	ex := execWith(
		`{"devices":[{"name":"/dev/nvme0","type":"nvme"}]}`,
		map[string]string{
			"/dev/nvme0": `{"smart_status":{"passed":true},"temperature":{"current":40},` +
				`"power_on_time":{"hours":500},"nvme_smart_health_information_log":{"percentage_used":3}}`,
		})
	c := New(ex, cfgSmart(true))
	m := toMap(mustCollect(t, c))
	want := map[string]float64{
		"sys.smart.nvme0.health_ok":      1,
		"sys.smart.nvme0.temp_c":         40,
		"sys.smart.nvme0.power_on_hours": 500,
		"sys.smart.nvme0.wear_pct":       3,
	}
	for k, v := range want {
		if m[k] != v {
			t.Errorf("%s = %v, want %v", k, m[k], v)
		}
	}
	for _, k := range []string{"reallocated", "pending"} {
		if _, ok := m["sys.smart.nvme0."+k]; ok {
			t.Errorf("%s must be omitted for an NVMe drive", k)
		}
	}
}

func TestHealthFailReportsZero(t *testing.T) {
	ex := execWith(
		`{"devices":[{"name":"/dev/sda"}]}`,
		map[string]string{"/dev/sda": `{"smart_status":{"passed":false}}`})
	c := New(ex, cfgSmart(true))
	m := toMap(mustCollect(t, c))
	if m["sys.smart.sda.health_ok"] != 0 {
		t.Errorf("failing drive health_ok = %v, want 0", m["sys.smart.sda.health_ok"])
	}
}

func TestFailingDriveNonZeroExitStillParsed(t *testing.T) {
	// smartctl exits non-zero (a status bitmask) on an ageing/failing drive but
	// still prints valid JSON; the device must not be dropped.
	ex := platformtest.NewExec().AddPath(smartctl)
	ex.SetResponse([]byte(`{"devices":[{"name":"/dev/sda"}]}`), nil, smartctl, scanArgs...)
	ex.SetResponse(
		[]byte(`{"smart_status":{"passed":false},"temperature":{"current":60}}`),
		errors.New("exit status 8"),
		smartctl, "--json=c", "-a", "/dev/sda")
	c := New(ex, cfgSmart(true))
	m := toMap(mustCollect(t, c))
	if m["sys.smart.sda.temp_c"] != 60 {
		t.Errorf("non-zero exit with valid JSON must still parse: %v", m)
	}
	if m["sys.smart.sda.health_ok"] != 0 {
		t.Errorf("health_ok = %v, want 0", m["sys.smart.sda.health_ok"])
	}
}

func TestBrokenDeviceJSONSkipped(t *testing.T) {
	ex := execWith(
		`{"devices":[{"name":"/dev/sda"}]}`,
		map[string]string{"/dev/sda": `this is not json`})
	c := New(ex, cfgSmart(true))
	if s := mustCollect(t, c); len(s) != 0 {
		t.Fatalf("broken device JSON must yield no samples, got %v", s)
	}
}

func TestBrokenScanNoKeysNoCrash(t *testing.T) {
	ex := platformtest.NewExec().AddPath(smartctl)
	ex.SetResponse([]byte(`{not json`), nil, smartctl, scanArgs...)
	c := New(ex, cfgSmart(true))
	if s := mustCollect(t, c); len(s) != 0 {
		t.Fatalf("broken scan must yield no samples, got %v", s)
	}
}

func TestInvalidDeviceNameRefused(t *testing.T) {
	// A device path outside the /dev shape must never reach an argv.
	ex := execWith(
		`{"devices":[{"name":"garbage; rm -rf /"},{"name":"/dev/sda"}]}`,
		map[string]string{"/dev/sda": `{"smart_status":{"passed":true}}`})
	c := New(ex, cfgSmart(true))
	m := toMap(mustCollect(t, c))
	if m["sys.smart.sda.health_ok"] != 1 {
		t.Errorf("valid device must still be read: %v", m)
	}
	// The bogus name never produced a -a call.
	for _, call := range ex.Calls() {
		for _, arg := range call {
			if strings.Contains(arg, "rm -rf") {
				t.Fatalf("bogus device name reached an argv: %v", call)
			}
		}
	}
}

func TestCardinalityCap(t *testing.T) {
	var devices []string
	devJSON := map[string]string{}
	for i := 0; i < 20; i++ {
		name := fmt.Sprintf("/dev/sd%c", 'a'+i)
		devices = append(devices, fmt.Sprintf(`{"name":%q}`, name))
		devJSON[name] = `{"smart_status":{"passed":true}}`
	}
	scanJSON := `{"devices":[` + strings.Join(devices, ",") + `]}`
	c := New(execWith(scanJSON, devJSON), cfgSmart(true))

	m := toMap(mustCollect(t, c))
	if len(m) != maxDevices {
		t.Errorf("expected %d keys (one per capped device), got %d", maxDevices, len(m))
	}
	if _, ok := m["sys.smart.sdp.health_ok"]; !ok {
		t.Error("sdp (16th, sorted) must be kept")
	}
	if _, ok := m["sys.smart.sdq.health_ok"]; ok {
		t.Error("sdq (17th) must be dropped by the cap")
	}
}

func TestFeatureOffNotAvailable(t *testing.T) {
	ex := execWith(`{"devices":[{"name":"/dev/sda"}]}`, map[string]string{"/dev/sda": `{"smart_status":{"passed":true}}`})
	if New(ex, cfgSmart(false)).Available() {
		t.Fatal("feature off must not be Available")
	}
}

func TestNoSmartctlNotAvailable(t *testing.T) {
	ex := platformtest.NewExec() // smartctl not in PATH
	if New(ex, cfgSmart(true)).Available() {
		t.Fatal("feature on but no smartctl must not be Available")
	}
}

func TestNVMeRichHealthFields(t *testing.T) {
	// Trimmed from a real `smartctl -a -j /dev/nvme0` (Samsung SSD 9100 PRO 2TB).
	dev := `{"model_name":"Samsung SSD 9100 PRO 2TB","smart_status":{"passed":true},` +
		`"temperature":{"current":41},"power_on_time":{"hours":1512},"power_cycle_count":263,` +
		`"nvme_smart_health_information_log":{"critical_warning":0,"available_spare":100,` +
		`"percentage_used":1,"data_units_read":18366696,"data_units_written":37690082,` +
		`"media_errors":0,"unsafe_shutdowns":205}}`
	ex := execWith(`{"devices":[{"name":"/dev/nvme0","type":"nvme"}]}`, map[string]string{"/dev/nvme0": dev})
	m := toMap(mustCollect(t, New(ex, cfgSmart(true))))

	want := map[string]float64{
		"sys.smart.nvme0.health_ok":           1,
		"sys.smart.nvme0.temp_c":              41,
		"sys.smart.nvme0.power_on_hours":      1512,
		"sys.smart.nvme0.power_cycles":        263,
		"sys.smart.nvme0.wear_pct":            1,
		"sys.smart.nvme0.available_spare_pct": 100,
		"sys.smart.nvme0.unsafe_shutdowns":    205,
		"sys.smart.nvme0.data_written_bytes":  37690082 * 512000,
		"sys.smart.nvme0.data_read_bytes":     18366696 * 512000,
	}
	for k, v := range want {
		if m[k] != v {
			t.Errorf("%s = %v, want %v", k, m[k], v)
		}
	}
	// Zero-valued health signals must still be EMITTED (present), not silently dropped.
	for _, k := range []string{"media_errors", "critical_warning"} {
		if _, ok := m["sys.smart.nvme0."+k]; !ok {
			t.Errorf("%s must be emitted even when zero", k)
		}
	}
}

func TestQueryNamesReadsModelName(t *testing.T) {
	ex := platformtest.NewExec().AddPath(smartctl)
	ex.SetResponse([]byte(`{"devices":[{"name":"/dev/nvme0","type":"nvme"}]}`), nil, smartctl, scanArgs...)
	ex.SetResponse([]byte(`{"model_name":"Samsung SSD 9100 PRO 2TB"}`), nil, smartctl, "--json=c", "-i", "/dev/nvme0")

	names := QueryNames(context.Background(), ex)
	if names["nvme0"] != "Samsung SSD 9100 PRO 2TB" {
		t.Fatalf("nvme0 name = %q, want Samsung SSD 9100 PRO 2TB", names["nvme0"])
	}
}
