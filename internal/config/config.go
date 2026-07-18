// Package config owns the agent's runtime configuration: pulling it from the
// control plane, validating it defensively, and swapping it atomically so a
// hot-reload is never half-applied. The agent has no local config authority
// (bootstrap is only the ingest URL and the token path); the server renders the
// config from the database and clamps every value. The agent re-validates on top
// of that as a safety net against a server bug: a field outside its range is
// clamped or dropped with a config_error event, and a whole document that will
// not parse is rejected with keep-last-good plus exactly one config_error. It is
// never silent.
package config

// Value ranges and defaults. The server clamps authoritatively; these are the
// agent's defensive backstops.
const (
	// HardIntervalFloor is the smallest plausible interval across all plans (the
	// 30s Team/Business floor). Below it means a server bug; the agent clamps up.
	// The agent cannot know its own plan floor, so this is deliberately the
	// global minimum, not a per-plan value.
	HardIntervalFloor = 30
	// IntervalCeiling is the liveness sanity ceiling for agent services (not the
	// generic 604800 database ceiling): past it a dead server would stay green
	// for over a week.
	IntervalCeiling = 3600
	// DefaultIntervalSeconds is used when the server omits interval_seconds. It
	// is the Free-plan floor, the safest default for an unknown plan.
	DefaultIntervalSeconds = 120

	// DefaultSampleIntervalSeconds is the agent-local sub-sample cadence: the
	// cheap collectors read every this-many seconds and the per-key values are
	// collapsed into one report value per interval_seconds by the catalog agg
	// mode. This is kept internal and fixed at 5s; exposing it as an optional
	// server-settable field that defaults to 5 when the server omits it preserves
	// that behaviour while allowing a future opt-in.
	DefaultSampleIntervalSeconds = 5
	// MinSampleIntervalSeconds / MaxSampleIntervalSeconds bound the sub-sample
	// cadence to the 3-10s range. A value outside this is a server bug; the agent
	// clamps defensively.
	MinSampleIntervalSeconds = 3
	MaxSampleIntervalSeconds = 10

	maxExcludeFstypes    = 32
	maxFstypeLen         = 32
	maxExcludeMounts     = 64
	maxMountPathLen      = 512
	diskMaxMountsCeil    = 20
	diskioMaxDevicesCeil = 10
	maxExcludeIfaces     = 32
	maxIfaceLen          = 15
	netMaxIfacesCeil     = 10
	maxSystemdUnits      = 10
	maxSystemdUnitLen    = 256
	maxKeysCeil          = 120
	maxReportsCeil       = 200

	// Reachability probe bounds. The target count is capped server-side too; this is the agent's
	// defensive ceiling. Interval is separate from the report interval so probes stay light.
	probeMaxTargets   = 15
	maxProbeTargetLen = 253 // max DNS name length
	maxProbePathLen   = 512
	maxProbeIDLen     = 32
	// ProbeIntervalFloor/Ceiling bound the probe cadence (seconds). DefaultProbeIntervalSeconds is
	// used when the server enables probes without a cadence.
	ProbeIntervalFloor          = 30
	ProbeIntervalCeiling        = 300
	DefaultProbeIntervalSeconds = 60
)

// Config is the validated, applied agent configuration. It is immutable once
// built and swapped behind an atomic pointer; collectors and the run loop read a
// consistent snapshot.
type Config struct {
	ConfigVersion   int
	IntervalSeconds int
	// SampleIntervalSeconds is the agent-local sub-sample cadence. The runner
	// samples the cheap collectors every SampleIntervalSeconds and collapses the
	// per-key sub-samples into one report value per IntervalSeconds. Defaults to
	// DefaultSampleIntervalSeconds; clamped to [Min,Max]SampleIntervalSeconds.
	SampleIntervalSeconds int
	Collectors            CollectorsConfig
	Features              FeaturesConfig
	Update                UpdateConfig
	Limits                LimitsConfig
}

// CollectorsConfig captures which collectors run and their per-collector knobs.
// Presence in the server document means on; an absent collector is off.
type CollectorsConfig struct {
	CPU    bool
	Load   bool
	Mem    bool
	Swap   bool
	PSI    bool
	System bool
	Disk   DiskConfig
	DiskIO DiskIOConfig
	Net    NetConfig
	Probe  ProbeConfig
}

// ProbeConfig configures the reachability/latency collector: a server-declared
// list of targets the agent probes from INSIDE the customer's network. It is
// interval-sampled (never on the 2s live path) and unprivileged (tcp/http/dns need no capabilities).
type ProbeConfig struct {
	// IntervalSeconds is the probe cadence, clamped to [ProbeIntervalFloor, ProbeIntervalCeiling].
	// Zero means "fall back to the report interval".
	IntervalSeconds int
	Targets         []ProbeTarget
}

// ProbeTarget is one reachability target. Type is one of tcp/http/https/dns. ID is a server-assigned
// stable opaque slug (NanoID-derived) — the metric segment, so no user hostname/PII reaches ClickHouse.
type ProbeTarget struct {
	ID       string
	Type     string
	Target   string
	Port     int
	Path     string
	Resolver string
}

// DiskConfig configures the disk collector. Filters are applied against
// /proc/self/mountinfo, never statfs (see the stuck-mount mechanism in the disk collector).
type DiskConfig struct {
	Enabled          bool
	ExcludeFstypes   []string
	ExcludeMounts    []string
	MaxMounts        int
	ExcludeNetworkFS bool
}

// DiskIOConfig configures the diskio collector.
type DiskIOConfig struct {
	Enabled    bool
	MaxDevices int
}

// NetConfig configures the net collector. lo is excluded by default.
type NetConfig struct {
	Enabled       bool
	ExcludeIfaces []string
	MaxIfaces     int
}

// FeaturesConfig holds opt-in features. All default off. smart and gpu each drive
// an opt-in-by-presence hardware-telemetry collector: the flag arms the collector,
// which then runs only when the underlying tool or sysfs interface is actually present.
type FeaturesConfig struct {
	Live    bool
	Systemd SystemdFeature
	Smart   bool
	Sensors bool
	GPU     bool
}

// SystemdFeature configures the systemd unit-watch feature (F2b).
type SystemdFeature struct {
	Enabled bool
	Units   []string
}

// UpdateConfig holds the update channel. TargetVersion is empty unless the
// server has released this agent's rollout group. v1 is advertise-only.
type UpdateConfig struct {
	Autoupdate    bool
	TargetVersion string
	WindowStart   string
	WindowEnd     string
}

// LimitsConfig holds the per-report and per-batch caps.
type LimitsConfig struct {
	MaxKeysPerReport   int
	MaxReportsPerBatch int
}

// DefaultExcludeFstypes is the default, user-overridable disk fstype denylist.
// It holds only the RAM/image filesystems a user might legitimately want to see
// (a large tmpfs, an overlay): they default off but can be re-enabled by editing
// exclude_fstypes. The kernel's pure pseudo-filesystems (proc, sysfs, cgroup,
// ...) are not here; they are excluded unconditionally by the disk collector's
// always-on filter, since they are never real storage and code-default changes
// to this list would not reach an already-enrolled agent (no config_version
// bump). Classification is always by mountinfo fstype, never statfs.
func DefaultExcludeFstypes() []string {
	return []string{"tmpfs", "overlay", "squashfs", "aufs", "ramfs"}
}

// Defaults returns the built-in configuration used before the first successful
// pull and when the last-good cache is missing or unreadable.
func Defaults() *Config {
	return &Config{
		ConfigVersion:         1,
		IntervalSeconds:       DefaultIntervalSeconds,
		SampleIntervalSeconds: DefaultSampleIntervalSeconds,
		Collectors: CollectorsConfig{
			CPU: true, Load: true, Mem: true, Swap: true, PSI: true, System: true,
			Disk: DiskConfig{
				Enabled:          true,
				ExcludeFstypes:   DefaultExcludeFstypes(),
				ExcludeMounts:    []string{},
				MaxMounts:        diskMaxMountsCeil,
				ExcludeNetworkFS: false,
			},
			DiskIO: DiskIOConfig{Enabled: true, MaxDevices: diskioMaxDevicesCeil},
			Net:    NetConfig{Enabled: true, ExcludeIfaces: []string{"lo"}, MaxIfaces: netMaxIfacesCeil},
		},
		Features: FeaturesConfig{
			Live:    false,
			Systemd: SystemdFeature{Enabled: false, Units: []string{}},
			Smart:   false,
			Sensors: false,
			GPU:     false,
		},
		Update: UpdateConfig{Autoupdate: true, TargetVersion: "", WindowStart: "00:00", WindowEnd: "04:00"},
		Limits: LimitsConfig{MaxKeysPerReport: maxKeysCeil, MaxReportsPerBatch: maxReportsCeil},
	}
}
