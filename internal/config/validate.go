package config

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Severity classifies a validation issue. Warn issues are logged only; Error
// issues also raise a config_error event (the config still applies, clamped).
type Severity int

const (
	// SeverityWarn is a silent clamp of a numeric knob.
	SeverityWarn Severity = iota
	// SeverityError is a field the agent had to drop or coerce because the
	// server sent something out of contract.
	SeverityError
)

// Issue is one validation finding.
type Issue struct {
	Field    string
	Severity Severity
	Message  string
}

// HasErrors reports whether any issue is SeverityError.
func HasErrors(issues []Issue) bool {
	for _, i := range issues {
		if i.Severity == SeverityError {
			return true
		}
	}
	return false
}

// Summary renders the error issues into a single capped string for a
// config_error event's meta.error.
func Summary(issues []Issue) string {
	var parts []string
	for _, i := range issues {
		if i.Severity == SeverityError {
			parts = append(parts, i.Field+": "+i.Message)
		}
	}
	s := ""
	for n, p := range parts {
		if n > 0 {
			s += "; "
		}
		s += p
	}
	if len(s) > 200 {
		s = s[:200]
	}
	return s
}

var (
	semverRe  = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$`)
	hhmmRe    = regexp.MustCompile(`^([01][0-9]|2[0-3]):[0-5][0-9]$`)
	unitReStr = regexp.MustCompile(`^[A-Za-z0-9:_.@\\-]+\.[A-Za-z]+$`)
)

var knownCollectorKeys = map[string]bool{
	"cpu": true, "load": true, "mem": true, "swap": true,
	"psi": true, "system": true, "disk": true, "diskio": true, "net": true, "probe": true,
}

// Validate parses and defensively validates a server config document. A non-nil
// error means the whole document is unusable (unparseable, or config_version <
// 1): the caller keeps the last-good config and raises one config_error. A nil
// error returns the clamped config plus any per-field issues.
func Validate(raw []byte) (*Config, []Issue, error) {
	var rc rawConfig
	if err := json.Unmarshal(raw, &rc); err != nil {
		return nil, nil, fmt.Errorf("parse config document: %w", err)
	}
	if rc.ConfigVersion < 1 {
		return nil, nil, fmt.Errorf("config_version %d is below 1", rc.ConfigVersion)
	}

	cfg := Defaults()
	var issues []Issue
	cfg.ConfigVersion = rc.ConfigVersion

	validateInterval(&rc, cfg, &issues)
	validateSampleInterval(&rc, cfg, &issues)
	validateCollectors(&rc, cfg, &issues)
	validateFeatures(&rc, cfg, &issues)
	validateUpdate(&rc, cfg, &issues)
	validateLimits(&rc, cfg, &issues)

	return cfg, issues, nil
}

func validateInterval(rc *rawConfig, cfg *Config, issues *[]Issue) {
	if rc.IntervalSeconds == nil {
		cfg.IntervalSeconds = DefaultIntervalSeconds
		*issues = append(*issues, Issue{"interval_seconds", SeverityWarn, "missing, using default"})
		return
	}
	v := *rc.IntervalSeconds
	clamped := v
	if clamped < HardIntervalFloor {
		clamped = HardIntervalFloor
	}
	if clamped > IntervalCeiling {
		clamped = IntervalCeiling
	}
	cfg.IntervalSeconds = clamped
	if clamped != v {
		*issues = append(*issues, Issue{"interval_seconds", SeverityWarn,
			fmt.Sprintf("out of range %d, clamped to %d", v, clamped)})
	}
}

// validateSampleInterval clamps the agent-local sub-sample cadence into
// [Min,Max]SampleIntervalSeconds. Absence is the normal case (the field is
// optional and kept fixed at 5s): a missing value silently takes the default
// without an issue, only an out-of-range server value warns.
func validateSampleInterval(rc *rawConfig, cfg *Config, issues *[]Issue) {
	if rc.SampleIntervalSeconds == nil {
		cfg.SampleIntervalSeconds = DefaultSampleIntervalSeconds
		return
	}
	v := *rc.SampleIntervalSeconds
	clamped := clampInt(v, MinSampleIntervalSeconds, MaxSampleIntervalSeconds)
	cfg.SampleIntervalSeconds = clamped
	if clamped != v {
		*issues = append(*issues, Issue{"sample_interval_seconds", SeverityWarn,
			fmt.Sprintf("out of range %d, clamped to %d", v, clamped)})
	}
}

func validateCollectors(rc *rawConfig, cfg *Config, issues *[]Issue) {
	if rc.Collectors == nil {
		return // keep defaults (all on)
	}
	// Presence drives on/off: reset all to off, then enable present keys.
	cfg.Collectors.CPU = false
	cfg.Collectors.Load = false
	cfg.Collectors.Mem = false
	cfg.Collectors.Swap = false
	cfg.Collectors.PSI = false
	cfg.Collectors.System = false
	cfg.Collectors.Disk.Enabled = false
	cfg.Collectors.DiskIO.Enabled = false
	cfg.Collectors.Net.Enabled = false

	for key, rawVal := range rc.Collectors {
		if !knownCollectorKeys[key] {
			*issues = append(*issues, Issue{"collectors." + key, SeverityWarn, "unknown collector, ignored"})
			continue
		}
		switch key {
		case "cpu":
			cfg.Collectors.CPU = true
		case "load":
			cfg.Collectors.Load = true
		case "mem":
			cfg.Collectors.Mem = true
		case "swap":
			cfg.Collectors.Swap = true
		case "psi":
			cfg.Collectors.PSI = true
		case "system":
			cfg.Collectors.System = true
		case "disk":
			cfg.Collectors.Disk.Enabled = true
			validateDisk(rawVal, cfg, issues)
		case "diskio":
			cfg.Collectors.DiskIO.Enabled = true
			validateDiskIO(rawVal, cfg, issues)
		case "net":
			cfg.Collectors.Net.Enabled = true
			validateNet(rawVal, cfg, issues)
		case "probe":
			validateProbe(rawVal, cfg, issues)
		}
	}
}

// validProbeTypes is the allowlist of reachability probe kinds. icmp is intentionally absent from v1
// (raw ICMP is structurally impossible under the zero-capability systemd unit; a future opt-in
// unprivileged-datagram path can add it).
var validProbeTypes = map[string]bool{"tcp": true, "http": true, "https": true, "dns": true}

// validateProbe parses the reachability probe config: it clamps the cadence, then validates each
// target (allowlisted type, non-empty id+target within length bounds, sane port, a blocked
// cloud-metadata address) and drops any invalid entry with a config_error, capping the survivors at
// probeMaxTargets. A dropped-but-recoverable document is never fatal.
func validateProbe(rawVal json.RawMessage, cfg *Config, issues *[]Issue) {
	var p rawProbe
	if len(rawVal) == 0 || json.Unmarshal(rawVal, &p) != nil {
		return
	}

	interval := DefaultProbeIntervalSeconds
	if p.IntervalSeconds != nil {
		interval = clampInt(*p.IntervalSeconds, ProbeIntervalFloor, ProbeIntervalCeiling)
	}

	targets := make([]ProbeTarget, 0, len(p.Targets))
	for _, rt := range p.Targets {
		if len(targets) >= probeMaxTargets {
			*issues = append(*issues, Issue{"collectors.probe.targets", SeverityError, "too many targets, extra dropped"})
			break
		}
		t, ok := validateProbeTarget(rt, issues)
		if ok {
			targets = append(targets, t)
		}
	}

	cfg.Collectors.Probe = ProbeConfig{IntervalSeconds: interval, Targets: targets}
}

func validateProbeTarget(rt rawProbeTarget, issues *[]Issue) (ProbeTarget, bool) {
	id := strings.TrimSpace(rt.ID)
	typ := strings.ToLower(strings.TrimSpace(rt.Type))
	target := strings.TrimSpace(rt.Target)

	if id == "" || len(id) > maxProbeIDLen {
		*issues = append(*issues, Issue{"collectors.probe.targets", SeverityError, "target missing/oversized id, dropped"})
		return ProbeTarget{}, false
	}
	if !validProbeTypes[typ] {
		*issues = append(*issues, Issue{"collectors.probe.targets." + id, SeverityError, "invalid probe type, dropped"})
		return ProbeTarget{}, false
	}
	if target == "" || len(target) > maxProbeTargetLen {
		*issues = append(*issues, Issue{"collectors.probe.targets." + id, SeverityError, "missing/oversized target, dropped"})
		return ProbeTarget{}, false
	}
	// SSRF/scanner guardrail: never probe the cloud-metadata endpoint.
	if target == "169.254.169.254" {
		*issues = append(*issues, Issue{"collectors.probe.targets." + id, SeverityError, "blocked target, dropped"})
		return ProbeTarget{}, false
	}

	port := 0
	if rt.Port != nil {
		port = clampInt(*rt.Port, 1, 65535)
	}
	path := rt.Path
	if len(path) > maxProbePathLen {
		path = path[:maxProbePathLen]
	}

	return ProbeTarget{
		ID:       id,
		Type:     typ,
		Target:   target,
		Port:     port,
		Path:     path,
		Resolver: strings.TrimSpace(rt.Resolver),
	}, true
}

func validateDisk(rawVal json.RawMessage, cfg *Config, issues *[]Issue) {
	var d rawDisk
	if len(rawVal) == 0 || json.Unmarshal(rawVal, &d) != nil {
		return // keep defaults for the disk knobs
	}
	if d.ExcludeFstypes != nil {
		cfg.Collectors.Disk.ExcludeFstypes = clampStringList(
			d.ExcludeFstypes, maxExcludeFstypes, maxFstypeLen, "collectors.disk.exclude_fstypes", issues)
	}
	if d.ExcludeMounts != nil {
		cfg.Collectors.Disk.ExcludeMounts = clampStringList(
			d.ExcludeMounts, maxExcludeMounts, maxMountPathLen, "collectors.disk.exclude_mounts", issues)
	}
	if d.MaxMounts != nil {
		cfg.Collectors.Disk.MaxMounts = clampInt(*d.MaxMounts, 1, diskMaxMountsCeil)
	}
	if len(d.ExcludeNetworkFS) > 0 {
		var b bool
		if json.Unmarshal(d.ExcludeNetworkFS, &b) == nil {
			cfg.Collectors.Disk.ExcludeNetworkFS = b
		} else {
			cfg.Collectors.Disk.ExcludeNetworkFS = false
			*issues = append(*issues, Issue{"collectors.disk.exclude_network_fs", SeverityError, "not a bool, using false"})
		}
	}
}

func validateDiskIO(rawVal json.RawMessage, cfg *Config, issues *[]Issue) {
	var d rawDiskIO
	if len(rawVal) == 0 || json.Unmarshal(rawVal, &d) != nil {
		return
	}
	if d.MaxDevices != nil {
		cfg.Collectors.DiskIO.MaxDevices = clampInt(*d.MaxDevices, 1, diskioMaxDevicesCeil)
	}
}

func validateNet(rawVal json.RawMessage, cfg *Config, issues *[]Issue) {
	var n rawNet
	if len(rawVal) == 0 || json.Unmarshal(rawVal, &n) != nil {
		return
	}
	if n.ExcludeIfaces != nil {
		cfg.Collectors.Net.ExcludeIfaces = clampStringList(
			n.ExcludeIfaces, maxExcludeIfaces, maxIfaceLen, "collectors.net.exclude_ifaces", issues)
	}
	if n.MaxIfaces != nil {
		cfg.Collectors.Net.MaxIfaces = clampInt(*n.MaxIfaces, 1, netMaxIfacesCeil)
	}
}

func validateFeatures(rc *rawConfig, cfg *Config, issues *[]Issue) {
	if rc.Features == nil {
		return
	}
	if rc.Features.Live != nil {
		cfg.Features.Live = rc.Features.Live.Enabled
	}
	if rc.Features.Smart != nil {
		cfg.Features.Smart = rc.Features.Smart.Enabled
	}
	if rc.Features.Sensors != nil {
		cfg.Features.Sensors = rc.Features.Sensors.Enabled
	}
	if rc.Features.GPU != nil {
		cfg.Features.GPU = rc.Features.GPU.Enabled
	}
	if rc.Features.Systemd != nil {
		cfg.Features.Systemd.Enabled = rc.Features.Systemd.Enabled
		cfg.Features.Systemd.Units = validateUnits(rc.Features.Systemd.Units, issues)
	}
}

func validateUnits(units []string, issues *[]Issue) []string {
	out := make([]string, 0, len(units))
	for _, u := range units {
		if len(u) == 0 || len(u) > maxSystemdUnitLen || !unitReStr.MatchString(u) {
			*issues = append(*issues, Issue{"features.systemd.units", SeverityError, "invalid unit name dropped: " + clip(u, 64)})
			continue
		}
		out = append(out, u)
	}
	if len(out) > maxSystemdUnits {
		*issues = append(*issues, Issue{"features.systemd.units", SeverityWarn,
			fmt.Sprintf("more than %d units, truncated", maxSystemdUnits)})
		out = out[:maxSystemdUnits]
	}
	return out
}

func validateUpdate(rc *rawConfig, cfg *Config, issues *[]Issue) {
	if rc.Update == nil {
		return
	}
	if rc.Update.Autoupdate != nil {
		cfg.Update.Autoupdate = *rc.Update.Autoupdate
	}
	if rc.Update.TargetVersion != nil {
		tv := *rc.Update.TargetVersion
		switch {
		case tv == "":
			cfg.Update.TargetVersion = ""
		case semverRe.MatchString(tv):
			cfg.Update.TargetVersion = tv
		default:
			cfg.Update.TargetVersion = ""
			*issues = append(*issues, Issue{"update.target_version", SeverityError, "invalid semver, ignored"})
		}
	}
	if rc.Update.Window != nil {
		start, end := rc.Update.Window.Start, rc.Update.Window.End
		if hhmmRe.MatchString(start) && hhmmRe.MatchString(end) {
			cfg.Update.WindowStart = start
			cfg.Update.WindowEnd = end
		} else {
			// Keep the default window (already set from Defaults()).
			*issues = append(*issues, Issue{"update.window", SeverityError, "invalid HH:MM, using default window"})
		}
	}
}

func validateLimits(rc *rawConfig, cfg *Config, issues *[]Issue) {
	if rc.Limits == nil {
		return
	}
	if rc.Limits.MaxKeysPerReport != nil {
		cfg.Limits.MaxKeysPerReport = clampInt(*rc.Limits.MaxKeysPerReport, 1, maxKeysCeil)
	}
	if rc.Limits.MaxReportsPerBatch != nil {
		cfg.Limits.MaxReportsPerBatch = clampInt(*rc.Limits.MaxReportsPerBatch, 1, maxReportsCeil)
	}
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// clampStringList trims each entry to maxLen and the list to maxEntries. Any
// change raises a SeverityError issue (the server sent an over-contract list).
func clampStringList(in []string, maxEntries, maxLen int, field string, issues *[]Issue) []string {
	changed := false
	out := make([]string, 0, len(in))
	for _, s := range in {
		if len(s) > maxLen {
			s = s[:maxLen]
			changed = true
		}
		out = append(out, s)
	}
	if len(out) > maxEntries {
		out = out[:maxEntries]
		changed = true
	}
	if changed {
		*issues = append(*issues, Issue{field, SeverityError, "over limit, truncated"})
	}
	// Deterministic order aids equality checks and tests.
	sort.Strings(out)
	return out
}

func clip(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}
