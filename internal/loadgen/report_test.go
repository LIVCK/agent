package loadgen

import (
	"strings"
	"testing"

	"github.com/LIVCK/agent/pkg/wire"
)

// resolveAgg is the test oracle: it resolves a concrete key to its catalog agg
// mode, exact or wildcard-template, mirroring the agent's own resolution.
func resolveAgg(key string) (string, bool) {
	if e, ok := wire.Lookup(key); ok {
		return e.Agg, true
	}
	for _, e := range wire.Catalog() {
		if e.Wildcard == "" {
			continue
		}
		ph := "{" + e.Wildcard + "}"
		i := strings.Index(e.Key, ph)
		if i < 0 {
			continue
		}
		prefix, suffix := e.Key[:i], e.Key[i+len(ph):]
		if len(key) > len(prefix)+len(suffix) &&
			strings.HasPrefix(key, prefix) && strings.HasSuffix(key, suffix) {
			return e.Agg, true
		}
	}
	return "", false
}

// TestReportEmitsOnlyCatalogKeysAndCompanions is the contract the generator must
// hold to be a faithful fake: every emitted key is a catalog key (exact or
// wildcard) or a ".max" companion of an avg+max key with peak >= mean, and every
// avg+max key carries its companion while no other key does.
func TestReportEmitsOnlyCatalogKeysAndCompanions(t *testing.T) {
	rep := NewGenerator(1).Report()
	if len(rep) == 0 {
		t.Fatal("empty report")
	}
	sawCompanion := false
	for key, val := range rep {
		if strings.HasSuffix(key, ".max") {
			base := strings.TrimSuffix(key, ".max")
			agg, ok := resolveAgg(base)
			if !ok || agg != wire.AggAvgMax {
				t.Errorf("companion %q has no avg+max base in the catalog", key)
			}
			if rep[base] > val {
				t.Errorf("companion %q=%v below its mean %q=%v (peak must be >= mean)", key, val, base, rep[base])
			}
			sawCompanion = true
			continue
		}
		agg, ok := resolveAgg(key)
		if !ok {
			t.Errorf("emitted key %q is not in the catalog", key)
			continue
		}
		_, hasCompanion := rep[key+".max"]
		if agg == wire.AggAvgMax && !hasCompanion {
			t.Errorf("avg+max key %q is missing its .max companion", key)
		}
		if agg != wire.AggAvgMax && hasCompanion {
			t.Errorf("non-avg+max key %q must not carry a .max companion", key)
		}
	}
	if !sawCompanion {
		t.Fatal("expected at least one avg+max companion in the report")
	}
}

// TestPoisonHasUnknownKeysAndBadValues asserts the poison path actually carries
// garbage: keys outside the catalog and an out-of-range value on a real key.
func TestPoisonHasUnknownKeysAndBadValues(t *testing.T) {
	p := NewGenerator(2).PoisonReport()
	unknown := 0
	for key := range p {
		if strings.HasSuffix(key, ".max") {
			continue
		}
		if _, ok := resolveAgg(key); !ok {
			unknown++
		}
	}
	if unknown == 0 {
		t.Fatal("poison report carries no unknown keys")
	}
	if p["sys.cpu.total_pct"] <= 100 {
		t.Errorf("expected an out-of-range poison value on sys.cpu.total_pct, got %v", p["sys.cpu.total_pct"])
	}
}

// TestReportDeterministicPerSeed keeps a run reproducible: the same seed yields
// the same first report.
func TestReportDeterministicPerSeed(t *testing.T) {
	a := NewGenerator(42).Report()
	b := NewGenerator(42).Report()
	if len(a) != len(b) {
		t.Fatalf("length differs: %d vs %d", len(a), len(b))
	}
	for k, v := range a {
		if b[k] != v {
			t.Fatalf("seed not deterministic at %q: %v vs %v", k, v, b[k])
		}
	}
}
