package wire

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"
)

// catalogRaw is the frozen sys.* metric catalog. It is the machine-readable,
// authoritative form of the catalog and a cross-repo contract: pulse imports
// this package and drops any sys.* key not listed here (counted, never a batch
// reject). The catalog version is tied to this module's version.
//
//go:embed catalog.json
var catalogRaw []byte

// Valid values for CatalogEntry.Agg. avg collapses a window to its mean,
// avg+max emits both the mean and a ".max" companion key, max keeps the window
// peak (so a transient spike is not averaged away), last keeps the most recent
// sample (cumulative counters and static gauges), delta is reserved for future
// use.
const (
	AggAvg    = "avg"
	AggAvgMax = "avg+max"
	AggMax    = "max"
	AggLast   = "last"
	AggDelta  = "delta"
)

// Valid values for CatalogEntry.Wildcard.
const (
	WildcardMount  = "mount"
	WildcardDev    = "dev"
	WildcardIface  = "iface"
	WildcardGPU    = "gpu"
	WildcardTarget = "target"
)

// CatalogEntry describes one sys.* metric key.
type CatalogEntry struct {
	// Key is the metric key. For wildcard entries it contains the placeholder
	// segment literally, e.g. "sys.disk.{mount}.used_pct".
	Key string `json:"key"`
	// Unit is a normalized unit token, e.g. "percent", "bytes", "seconds",
	// "count", "float", "bytes_per_second", "per_second".
	Unit string `json:"unit"`
	// Source is where the agent reads the value, e.g. "/proc/stat" or "self".
	Source string `json:"source"`
	// Agg is how per-sample values collapse into one report value. One of the
	// Agg* constants.
	Agg string `json:"agg"`
	// Wildcard names the placeholder segment for per-device keys, or is empty.
	// One of the Wildcard* constants.
	Wildcard string `json:"wildcard,omitempty"`
}

type catalogFile struct {
	CatalogVersion int            `json:"catalog_version"`
	Keys           []CatalogEntry `json:"keys"`
}

var (
	catalog catalogFile
	byKey   map[string]CatalogEntry
)

func init() {
	if err := json.Unmarshal(catalogRaw, &catalog); err != nil {
		panic("wire: embedded catalog.json is not valid JSON: " + err.Error())
	}
	byKey = make(map[string]CatalogEntry, len(catalog.Keys))
	for _, e := range catalog.Keys {
		byKey[e.Key] = e
	}
	if err := Validate(); err != nil {
		panic("wire: embedded catalog.json is invalid: " + err.Error())
	}
}

// CatalogVersion returns the version of the embedded catalog.
func CatalogVersion() int { return catalog.CatalogVersion }

// Catalog returns a copy of all catalog entries in file order.
func Catalog() []CatalogEntry {
	out := make([]CatalogEntry, len(catalog.Keys))
	copy(out, catalog.Keys)
	return out
}

// Lookup returns the catalog entry for an exact key. Wildcard entries are keyed
// by their literal template (e.g. "sys.disk.{mount}.used_pct"); Lookup does not
// resolve a concrete key like "sys.disk./.used_pct" against a template.
func Lookup(key string) (CatalogEntry, bool) {
	e, ok := byKey[key]
	return e, ok
}

var validAgg = map[string]bool{
	AggAvg:    true,
	AggAvgMax: true,
	AggMax:    true,
	AggLast:   true,
	AggDelta:  true,
}

var validWildcard = map[string]bool{
	WildcardMount:  true,
	WildcardDev:    true,
	WildcardIface:  true,
	WildcardGPU:    true,
	WildcardTarget: true,
}

// Validate checks the embedded catalog for internal consistency. It runs at
// package init and is exported so contract tests can assert it directly.
func Validate() error {
	if catalog.CatalogVersion < 1 {
		return fmt.Errorf("catalog_version must be >= 1, got %d", catalog.CatalogVersion)
	}
	seen := make(map[string]bool, len(catalog.Keys))
	for i, e := range catalog.Keys {
		if e.Key == "" {
			return fmt.Errorf("entry %d has an empty key", i)
		}
		if !strings.HasPrefix(e.Key, "sys.") {
			return fmt.Errorf("key %q must start with sys.", e.Key)
		}
		if seen[e.Key] {
			return fmt.Errorf("duplicate key %q", e.Key)
		}
		seen[e.Key] = true
		if e.Unit == "" {
			return fmt.Errorf("key %q has an empty unit", e.Key)
		}
		if e.Source == "" {
			return fmt.Errorf("key %q has an empty source", e.Key)
		}
		if !validAgg[e.Agg] {
			return fmt.Errorf("key %q has invalid agg %q", e.Key, e.Agg)
		}
		placeholder := strings.Contains(e.Key, "{") || strings.Contains(e.Key, "}")
		if e.Wildcard == "" {
			if placeholder {
				return fmt.Errorf("key %q has a placeholder segment but no wildcard field", e.Key)
			}
			continue
		}
		if !validWildcard[e.Wildcard] {
			return fmt.Errorf("key %q has invalid wildcard %q", e.Key, e.Wildcard)
		}
		if !strings.Contains(e.Key, "{"+e.Wildcard+"}") {
			return fmt.Errorf("key %q is missing the {%s} placeholder segment", e.Key, e.Wildcard)
		}
	}
	return nil
}
