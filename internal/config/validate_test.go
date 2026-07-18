package config

import "testing"

const fullDoc = `{
  "config_version": 7,
  "interval_seconds": 60,
  "collectors": {
    "cpu": {}, "load": {}, "mem": {}, "swap": {},
    "disk": {"exclude_fstypes": ["tmpfs"], "exclude_mounts": [], "max_mounts": 20, "exclude_network_fs": false},
    "diskio": {"max_devices": 10},
    "net": {"exclude_ifaces": ["lo"], "max_ifaces": 10},
    "psi": {}, "system": {}
  },
  "features": {"live": {"enabled": false}, "systemd": {"enabled": false, "units": []}},
  "update": {"autoupdate": true, "target_version": "1.2.3", "window": {"start": "00:00", "end": "04:00"}},
  "limits": {"max_keys_per_report": 120, "max_reports_per_batch": 200}
}`

func TestValidateFullDoc(t *testing.T) {
	cfg, issues, err := Validate([]byte(fullDoc))
	if err != nil {
		t.Fatalf("unexpected fatal error: %v", err)
	}
	if HasErrors(issues) {
		t.Fatalf("unexpected error issues: %v", issues)
	}
	if cfg.ConfigVersion != 7 || cfg.IntervalSeconds != 60 {
		t.Fatalf("got version=%d interval=%d", cfg.ConfigVersion, cfg.IntervalSeconds)
	}
	if !cfg.Collectors.CPU || !cfg.Collectors.Disk.Enabled || !cfg.Collectors.Net.Enabled {
		t.Fatal("expected cpu, disk and net enabled")
	}
	if cfg.Update.TargetVersion != "1.2.3" {
		t.Fatalf("target_version = %q", cfg.Update.TargetVersion)
	}
}

func TestValidateIntervalClamp(t *testing.T) {
	cases := []struct {
		in   int
		want int
	}{
		{5, HardIntervalFloor},
		{30, 30},
		{60, 60},
		{999999, IntervalCeiling},
	}
	for _, c := range cases {
		doc := `{"config_version":1,"interval_seconds":` + itoa(c.in) + `}`
		cfg, _, err := Validate([]byte(doc))
		if err != nil {
			t.Fatalf("in=%d: %v", c.in, err)
		}
		if cfg.IntervalSeconds != c.want {
			t.Fatalf("in=%d: got %d want %d", c.in, cfg.IntervalSeconds, c.want)
		}
	}
}

func TestValidateSampleIntervalClamp(t *testing.T) {
	cases := []struct {
		in   int
		want int
	}{
		{1, MinSampleIntervalSeconds},
		{3, 3},
		{5, 5},
		{10, 10},
		{99, MaxSampleIntervalSeconds},
	}
	for _, c := range cases {
		doc := `{"config_version":1,"sample_interval_seconds":` + itoa(c.in) + `}`
		cfg, _, err := Validate([]byte(doc))
		if err != nil {
			t.Fatalf("in=%d: %v", c.in, err)
		}
		if cfg.SampleIntervalSeconds != c.want {
			t.Fatalf("in=%d: got %d want %d", c.in, cfg.SampleIntervalSeconds, c.want)
		}
	}
}

func TestValidateSampleIntervalDefaultsWhenAbsent(t *testing.T) {
	// Absence is the normal v1 case: the field silently takes the fixed default
	// and raises no issue.
	cfg, issues, err := Validate([]byte(`{"config_version":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SampleIntervalSeconds != DefaultSampleIntervalSeconds {
		t.Fatalf("absent sample_interval = %d, want %d", cfg.SampleIntervalSeconds, DefaultSampleIntervalSeconds)
	}
	for _, i := range issues {
		if i.Field == "sample_interval_seconds" {
			t.Fatalf("absent sample_interval must not raise an issue, got %+v", i)
		}
	}
}

func TestValidateUnknownCollectorIgnored(t *testing.T) {
	doc := `{"config_version":1,"collectors":{"cpu":{},"bogus":{}}}`
	cfg, issues, err := Validate([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Collectors.CPU {
		t.Fatal("cpu should be on")
	}
	if cfg.Collectors.Mem {
		t.Fatal("mem should be off (presence-driven)")
	}
	if !hasField(issues, "collectors.bogus") {
		t.Fatalf("expected an issue for the unknown collector, got %v", issues)
	}
}

func TestValidateExcludeNetworkFSNonBool(t *testing.T) {
	doc := `{"config_version":1,"collectors":{"disk":{"exclude_network_fs":"yes"}}}`
	cfg, issues, err := Validate([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Collectors.Disk.ExcludeNetworkFS {
		t.Fatal("non-bool exclude_network_fs should coerce to false")
	}
	if !HasErrors(issues) {
		t.Fatal("expected an error issue for non-bool exclude_network_fs")
	}
}

func TestValidateSystemdUnits(t *testing.T) {
	doc := `{"config_version":1,"features":{"systemd":{"enabled":true,"units":["nginx.service","bad name","sshd.service"]}}}`
	cfg, issues, err := Validate([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Features.Systemd.Units) != 2 {
		t.Fatalf("want 2 valid units, got %v", cfg.Features.Systemd.Units)
	}
	if !HasErrors(issues) {
		t.Fatal("expected an error issue for the invalid unit name")
	}
}

func TestValidateTargetVersionAndWindow(t *testing.T) {
	doc := `{"config_version":1,"update":{"target_version":"not-a-version","window":{"start":"9:99","end":"04:00"}}}`
	cfg, issues, err := Validate([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Update.TargetVersion != "" {
		t.Fatalf("invalid semver should clear target_version, got %q", cfg.Update.TargetVersion)
	}
	if cfg.Update.WindowStart != "00:00" || cfg.Update.WindowEnd != "04:00" {
		t.Fatalf("invalid window should fall back to default, got %s-%s", cfg.Update.WindowStart, cfg.Update.WindowEnd)
	}
	if !HasErrors(issues) {
		t.Fatal("expected error issues for target_version and window")
	}
}

func TestValidateLimitsClamp(t *testing.T) {
	doc := `{"config_version":1,"limits":{"max_keys_per_report":500,"max_reports_per_batch":999}}`
	cfg, _, err := Validate([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Limits.MaxKeysPerReport != maxKeysCeil || cfg.Limits.MaxReportsPerBatch != maxReportsCeil {
		t.Fatalf("limits not clamped: %+v", cfg.Limits)
	}
}

func TestValidateFatalCases(t *testing.T) {
	if _, _, err := Validate([]byte(`{not json`)); err == nil {
		t.Fatal("unparseable doc should be a fatal error")
	}
	if _, _, err := Validate([]byte(`{"config_version":0}`)); err == nil {
		t.Fatal("config_version 0 should be a fatal error")
	}
}

func hasField(issues []Issue, field string) bool {
	for _, i := range issues {
		if i.Field == field {
			return true
		}
	}
	return false
}

func itoa(v int) string {
	neg := v < 0
	if neg {
		v = -v
	}
	if v == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

func TestValidateProbeTargets(t *testing.T) {
	doc := `{"config_version":1,"collectors":{"probe":{
		"probe_interval_seconds": 9999,
		"targets":[
			{"id":"t1","type":"tcp","target":"10.0.0.5","port":5432},
			{"id":"t2","type":"https","target":"registry.internal","port":443,"path":"/v2/"},
			{"id":"t3","type":"dns","target":"svc.internal","resolver":"10.0.0.53"},
			{"id":"bad1","type":"icmp","target":"1.1.1.1"},
			{"id":"bad2","type":"tcp","target":"169.254.169.254"},
			{"id":"","type":"tcp","target":"x"}
		]}}}`
	cfg, issues, err := Validate([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	p := cfg.Collectors.Probe

	// Cadence clamped to the ceiling.
	if p.IntervalSeconds != ProbeIntervalCeiling {
		t.Fatalf("interval = %d, want clamp to %d", p.IntervalSeconds, ProbeIntervalCeiling)
	}
	// Only the three valid targets survive; icmp (not in v1 allowlist), the metadata IP, and the
	// empty id are all dropped.
	if len(p.Targets) != 3 {
		t.Fatalf("valid targets = %d, want 3 (got %+v)", len(p.Targets), p.Targets)
	}
	ids := map[string]bool{}
	for _, tg := range p.Targets {
		ids[tg.ID] = true
	}
	if !ids["t1"] || !ids["t2"] || !ids["t3"] || ids["bad1"] || ids["bad2"] {
		t.Fatalf("wrong surviving targets: %+v", p.Targets)
	}
	// Each drop raises a config_error.
	if !HasErrors(issues) {
		t.Fatal("dropped invalid probe targets must raise config_error issues")
	}
}
