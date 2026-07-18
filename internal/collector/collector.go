// Package collector defines the metric-source contract and the report assembly
// the run loop drives. The concrete sources (cpu, load, mem, disk, diskio, net,
// psi, system) implement the Collector interface defined here, which also fixes
// the Sample type, the shared delta/clamp/rate conventions those sources use, a
// registry, and a self collector so the loop is runnable end to end.
package collector

import "context"

// Sample is one aggregated metric value for a single report window. The
// collector has already applied its own delta/clamp/normalize math over the raw
// samples in the window (see math.go); the run loop only assembles Samples into
// a wire.Report. A collector emits no Sample for a key it cannot produce this
// window (a rate key with no predecessor, a stuck mount, missing PSI): there is
// no zero-fake.
type Sample struct {
	Key   string
	Value float64
}

// Collector is one metric source. A collector never crashes the agent: a
// Collect error yields a WARN and the loop keeps the other collectors' samples
// (see Registry.Collect). Available reports whether the source exists on this
// host (for example PSI below kernel 4.20), letting the registry skip it
// without treating absence as an error.
type Collector interface {
	// Name is a short stable identifier, e.g. "cpu" or "self".
	Name() string
	// Available reports whether this source can produce samples on this host.
	Available() bool
	// Collect returns this window's samples. It must respect ctx cancellation
	// and must not block indefinitely (disk collectors run behind a deadline,
	// see the stuck-mount mechanism in the disk collector).
	Collect(ctx context.Context) ([]Sample, error)
}

// IntervalSampled is an optional marker a Collector may implement to opt out of
// sub-sampling. A collector that implements it and returns true is collected
// once per report interval instead of at each sample_interval tick; a collector
// that does not implement it (or returns false) is sub-sampled at the sample
// cadence. Exec-based sources (gpu via nvidia-smi, smart via smartctl) implement
// it so the 5s sub-sample cadence never forks an external tool repeatedly. This
// is a separate optional interface: it does not change the frozen Collector
// contract, and the run loop discovers it with a type assertion.
type IntervalSampled interface {
	// IntervalSampled reports whether this collector must be sampled once per
	// report interval rather than at every sub-sample tick.
	IntervalSampled() bool
}

// IsIntervalSampled reports whether c opts into interval sampling via the
// IntervalSampled marker. A collector that does not implement the marker is
// sub-sampled.
func IsIntervalSampled(c Collector) bool {
	m, ok := c.(IntervalSampled)
	return ok && m.IntervalSampled()
}

// Registry holds the enabled collectors in registration order.
type Registry struct {
	collectors []Collector
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{} }

// Register appends c. Registration order is the report key order.
func (r *Registry) Register(c Collector) { r.collectors = append(r.collectors, c) }

// Collectors returns the registered collectors in order.
func (r *Registry) Collectors() []Collector { return r.collectors }

// CollectError pairs a failing collector's name with its error so the caller can
// log a WARN per source without aborting the window.
type CollectError struct {
	Collector string
	Err       error
}

func (e CollectError) Error() string { return e.Collector + ": " + e.Err.Error() }

// Collect runs every Available collector and returns all samples plus any
// per-collector errors. It never returns a fatal error: a collector fault
// degrades to a CollectError and the remaining samples still form the report.
func (r *Registry) Collect(ctx context.Context) ([]Sample, []CollectError) {
	var samples []Sample
	var errs []CollectError
	for _, c := range r.collectors {
		if !c.Available() {
			continue
		}
		s, err := c.Collect(ctx)
		if err != nil {
			errs = append(errs, CollectError{Collector: c.Name(), Err: err})
			continue
		}
		samples = append(samples, s...)
	}
	return samples, errs
}
