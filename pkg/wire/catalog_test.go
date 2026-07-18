package wire

import "testing"

// expectedKeys is the full sys.* catalog. If a key is added or removed, this
// list and catalog.json move together.
var expectedKeys = []string{
	"sys.cpu.total_pct",
	"sys.cpu.user_pct",
	"sys.cpu.system_pct",
	"sys.cpu.iowait_pct",
	"sys.cpu.steal_pct",
	"sys.load.1",
	"sys.load.5",
	"sys.load.15",
	"sys.mem.total_bytes",
	"sys.mem.used_bytes",
	"sys.mem.available_bytes",
	"sys.mem.used_pct",
	"sys.mem.cached_bytes",
	"sys.mem.buffers_bytes",
	"sys.mem.oom_kills",
	"sys.swap.total_bytes",
	"sys.swap.used_bytes",
	"sys.swap.used_pct",
	"sys.disk.{mount}.total_bytes",
	"sys.disk.{mount}.used_bytes",
	"sys.disk.{mount}.used_pct",
	"sys.disk.{mount}.inodes_used_pct",
	"sys.diskio.{dev}.read_bps",
	"sys.diskio.{dev}.write_bps",
	"sys.diskio.{dev}.read_iops",
	"sys.diskio.{dev}.write_iops",
	"sys.net.{iface}.rx_bps",
	"sys.net.{iface}.tx_bps",
	"sys.net.{iface}.rx_errors_ps",
	"sys.net.{iface}.tx_errors_ps",
	"sys.psi.cpu_some_pct",
	"sys.psi.mem_some_pct",
	"sys.psi.io_some_pct",
	"sys.uptime_seconds",
	"sys.procs.total",
	"sys.procs.zombies",
	"sys.agent.rss_bytes",
	"sys.agent.cpu_pct",
	"sys.agent.buffer_fill_pct",
	"sys.agent.dropped_reports",
	"sys.agent.stuck_mounts",
	"sys.gpu.{gpu}.util_pct",
	"sys.gpu.{gpu}.mem_used_pct",
	"sys.gpu.{gpu}.mem_used_bytes",
	"sys.gpu.{gpu}.mem_total_bytes",
	"sys.gpu.{gpu}.temp_c",
	"sys.gpu.{gpu}.power_w",
	"sys.smart.{dev}.health_ok",
	"sys.smart.{dev}.temp_c",
	"sys.smart.{dev}.reallocated",
	"sys.smart.{dev}.pending",
	"sys.smart.{dev}.power_on_hours",
	"sys.smart.{dev}.wear_pct",
	"sys.smart.{dev}.power_cycles",
	"sys.smart.{dev}.available_spare_pct",
	"sys.smart.{dev}.media_errors",
	"sys.smart.{dev}.unsafe_shutdowns",
	"sys.smart.{dev}.critical_warning",
	"sys.smart.{dev}.data_written_bytes",
	"sys.smart.{dev}.data_read_bytes",
	"sys.probe.{target}.up",
	"sys.probe.{target}.rtt_avg_ms",
	"sys.probe.{target}.rtt_min_ms",
	"sys.probe.{target}.rtt_max_ms",
	"sys.probe.{target}.loss_pct",
	"sys.probe.{target}.http_status",
	"sys.probe.{target}.cert_expiry_hours",
}

func TestCatalogVersion(t *testing.T) {
	if got := CatalogVersion(); got != 3 {
		t.Fatalf("CatalogVersion = %d, want 3", got)
	}
}

func TestCatalogValidate(t *testing.T) {
	if err := Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestEveryExpectedKeyPresent(t *testing.T) {
	for _, k := range expectedKeys {
		if _, ok := Lookup(k); !ok {
			t.Errorf("catalog is missing key %q", k)
		}
	}
}

func TestNoUnexpectedKeys(t *testing.T) {
	want := make(map[string]bool, len(expectedKeys))
	for _, k := range expectedKeys {
		want[k] = true
	}
	for _, e := range Catalog() {
		if !want[e.Key] {
			t.Errorf("catalog has unexpected key %q", e.Key)
		}
	}
	if got, exp := len(Catalog()), len(expectedKeys); got != exp {
		t.Errorf("catalog has %d keys, want %d", got, exp)
	}
}

func TestAggValuesValid(t *testing.T) {
	for _, e := range Catalog() {
		switch e.Agg {
		case AggAvg, AggAvgMax, AggMax, AggLast, AggDelta:
		default:
			t.Errorf("key %q has invalid agg %q", e.Key, e.Agg)
		}
	}
}

func TestKnownAggAssignments(t *testing.T) {
	cases := map[string]string{
		"sys.cpu.total_pct":             AggAvgMax,
		"sys.agent.stuck_mounts":        AggMax,
		"sys.mem.oom_kills":             AggLast,
		"sys.agent.dropped_reports":     AggLast,
		"sys.uptime_seconds":            AggLast,
		"sys.load.1":                    AggAvg,
		"sys.gpu.{gpu}.util_pct":        AggAvgMax,
		"sys.gpu.{gpu}.mem_used_pct":    AggAvgMax,
		"sys.gpu.{gpu}.mem_total_bytes": AggLast,
		"sys.smart.{dev}.health_ok":     AggLast,
		"sys.smart.{dev}.temp_c":        AggAvgMax,
		"sys.smart.{dev}.wear_pct":      AggLast,
	}
	for key, wantAgg := range cases {
		e, ok := Lookup(key)
		if !ok {
			t.Errorf("missing key %q", key)
			continue
		}
		if e.Agg != wantAgg {
			t.Errorf("key %q agg = %q, want %q", key, e.Agg, wantAgg)
		}
	}
}

func TestWildcardKeysMarked(t *testing.T) {
	for _, e := range Catalog() {
		hasPlaceholder := containsAny(e.Key, "{}")
		if hasPlaceholder && e.Wildcard == "" {
			t.Errorf("key %q has a placeholder but no wildcard", e.Key)
		}
		if !hasPlaceholder && e.Wildcard != "" {
			t.Errorf("key %q has wildcard %q but no placeholder", e.Key, e.Wildcard)
		}
		if e.Wildcard != "" {
			switch e.Wildcard {
			case WildcardMount, WildcardDev, WildcardIface, WildcardGPU, WildcardTarget:
			default:
				t.Errorf("key %q has invalid wildcard %q", e.Key, e.Wildcard)
			}
		}
	}
}

func containsAny(s, chars string) bool {
	for _, c := range chars {
		for _, sc := range s {
			if sc == c {
				return true
			}
		}
	}
	return false
}
