// Package loadgen holds the reusable, testable pieces of the livck-loadgen
// fake-fleet driver: the catalog-driven synthetic report generator and the
// sender-side latency/status recorder. The
// live orchestration (enroll storm, ramp, pulse sampling) lives in
// cmd/livck-loadgen so this package stays pure and unit-testable. ASCII only.
package loadgen

import (
	"math/rand/v2"
	"strings"

	"github.com/LIVCK/agent/pkg/wire"
)

// deviceExpansions turns each wildcard family into a small, realistic set of
// concrete devices so a generated report resembles a modest real host. GPU is
// empty by default: GPU telemetry is opt-in and absent on most hosts.
var deviceExpansions = map[string][]string{
	wire.WildcardMount: {"_root", "_var"},
	wire.WildcardDev:   {"sda", "nvme0n1"},
	wire.WildcardIface: {"eth0"},
	wire.WildcardGPU:   {},
}

// keySpec is one concrete (wildcard-expanded) catalog key plus the aggregation
// mode and unit that drive value generation.
type keySpec struct {
	key  string
	agg  string
	unit string
}

// buildKeySpecs expands the frozen catalog into the concrete keys a report
// carries: exact keys as-is, wildcard keys expanded per deviceExpansions. It is
// the single source of which keys a synthetic host emits, so the generator can
// never invent a key outside the catalog.
func buildKeySpecs() []keySpec {
	var specs []keySpec
	for _, e := range wire.Catalog() {
		if e.Wildcard == "" {
			specs = append(specs, keySpec{e.Key, e.Agg, e.Unit})
			continue
		}
		for _, dev := range deviceExpansions[e.Wildcard] {
			specs = append(specs, keySpec{
				key:  strings.Replace(e.Key, "{"+e.Wildcard+"}", dev, 1),
				agg:  e.Agg,
				unit: e.Unit,
			})
		}
	}
	return specs
}

// Generator produces synthetic reports for one simulated agent. It is NOT safe
// for concurrent use: each simulated agent owns one generator on its own
// goroutine (matching a real agent's single collect loop).
type Generator struct {
	specs []keySpec
	rng   *rand.Rand
}

// NewGenerator builds a generator seeded from seed so each simulated agent walks
// a distinct value path (distinct-looking hosts) while a run stays reproducible.
func NewGenerator(seed uint64) *Generator {
	return &Generator{
		specs: buildKeySpecs(),
		rng:   rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
	}
}

// KeyCount is the number of base catalog keys a clean report carries (before
// avg+max companions). Exposed for the loadgen's throughput accounting.
func (g *Generator) KeyCount() int { return len(g.specs) }

// Report returns one report's metrics: a plausible value for every catalog key,
// plus a "<key>.max" companion for every avg+max key with peak >= mean - exactly
// the shape the real agent's aggregator emits, so pulse sees a faithful batch.
func (g *Generator) Report() map[string]float64 {
	out := make(map[string]float64, len(g.specs)+8)
	for _, s := range g.specs {
		v := g.value(s.unit)
		out[s.key] = v
		if s.agg == wire.AggAvgMax {
			out[s.key+".max"] = v + g.rng.Float64()*g.spread(s.unit)
		}
	}
	return out
}

// PoisonReport returns a deliberately malformed report to exercise pulse's
// validation ladder under load: unknown keys (dropped, counted) and out-of-range
// values (clamped). The envelope stays valid protobuf+zstd, so this probes the
// per-key validation cost, not the batch-reject path.
func (g *Generator) PoisonReport() map[string]float64 {
	out := g.Report()
	// Keys outside the catalog: pulse must drop-and-count, never reject the batch.
	out["sys.garbage.unknown_metric"] = 1
	out["totally.not.a.sys.key"] = 2
	out["sys.cpu.made_up_field"] = 3
	// Out-of-range values on real keys: pulse must clamp.
	out["sys.cpu.total_pct"] = 999999
	out["sys.mem.used_pct"] = -50
	return out
}

// value returns a plausible value for a unit token.
func (g *Generator) value(unit string) float64 {
	switch unit {
	case "percent":
		return g.rng.Float64() * 80 // 0-80%
	case "bytes":
		return 1e9 + g.rng.Float64()*7e9 // ~1-8 GiB
	case "bytes_per_second":
		return g.rng.Float64() * 5e8 // 0-500 MB/s
	case "per_second":
		return g.rng.Float64() * 1e4
	case "seconds":
		return g.rng.Float64() * 2
	case "count":
		return float64(g.rng.IntN(500))
	case "float":
		return g.rng.Float64() * 10 // e.g. load average
	default:
		return g.rng.Float64() * 100
	}
}

// spread is the upper bound of the avg->max gap for a unit, so a companion peak
// is a realistic amount above the mean.
func (g *Generator) spread(unit string) float64 {
	switch unit {
	case "percent":
		return 20
	case "bytes_per_second":
		return 1e8
	case "per_second":
		return 2e3
	case "float":
		return 2
	default:
		return 5
	}
}
