package config

import "encoding/json"

// rawConfig mirrors the server config document. Pointers and
// json.RawMessage mark presence and defer type-tolerant parsing: a missing field
// keeps its default, and a field of the wrong type is coerced with an issue
// rather than failing the whole document, except where a wrong type is a genuine
// schema break that should trip keep-last-good.
type rawConfig struct {
	ConfigVersion         int                        `json:"config_version"`
	IntervalSeconds       *int                       `json:"interval_seconds"`
	SampleIntervalSeconds *int                       `json:"sample_interval_seconds"`
	Collectors            map[string]json.RawMessage `json:"collectors"`
	Features              *rawFeatures               `json:"features"`
	Update                *rawUpdate                 `json:"update"`
	Limits                *rawLimits                 `json:"limits"`
}

type rawDisk struct {
	ExcludeFstypes   []string        `json:"exclude_fstypes"`
	ExcludeMounts    []string        `json:"exclude_mounts"`
	MaxMounts        *int            `json:"max_mounts"`
	ExcludeNetworkFS json.RawMessage `json:"exclude_network_fs"`
}

type rawDiskIO struct {
	MaxDevices *int `json:"max_devices"`
}

type rawNet struct {
	ExcludeIfaces []string `json:"exclude_ifaces"`
	MaxIfaces     *int     `json:"max_ifaces"`
}

type rawProbe struct {
	IntervalSeconds *int             `json:"probe_interval_seconds"`
	Targets         []rawProbeTarget `json:"targets"`
}

type rawProbeTarget struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Target   string `json:"target"`
	Port     *int   `json:"port"`
	Path     string `json:"path"`
	Resolver string `json:"resolver"`
}

type rawFeatures struct {
	Live    *rawEnabled `json:"live"`
	Systemd *rawSystemd `json:"systemd"`
	Smart   *rawEnabled `json:"smart"`
	Sensors *rawEnabled `json:"sensors"`
	GPU     *rawEnabled `json:"gpu"`
}

type rawEnabled struct {
	Enabled bool `json:"enabled"`
}

type rawSystemd struct {
	Enabled bool     `json:"enabled"`
	Units   []string `json:"units"`
}

type rawUpdate struct {
	Autoupdate    *bool      `json:"autoupdate"`
	TargetVersion *string    `json:"target_version"`
	Window        *rawWindow `json:"window"`
}

type rawWindow struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

type rawLimits struct {
	MaxKeysPerReport   *int `json:"max_keys_per_report"`
	MaxReportsPerBatch *int `json:"max_reports_per_batch"`
}
