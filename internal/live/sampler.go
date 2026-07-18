package live

import (
	"context"
	"log/slog"
	"time"

	"github.com/LIVCK/agent/internal/collector"
)

// defaultSampleTimeout bounds one live snapshot. It is short: a live burst is
// best-effort and a slow collector must never stall the 2s cadence.
const defaultSampleTimeout = 2 * time.Second

// registrySampler snapshots the cheap collectors of a dedicated registry. The
// registry is the streamer's own (main.go builds a second one) so its rate
// collectors keep delta state independent of the report loop.
type registrySampler struct {
	reg     *collector.Registry
	timeout time.Duration
	log     *slog.Logger
}

// NewRegistrySampler returns a Sampler backed by reg. Only the cheap
// (non-interval-sampled) collectors run: the exec-based gpu/smart sources are
// far too heavy for a 2s cadence and are skipped.
func NewRegistrySampler(reg *collector.Registry, log *slog.Logger) Sampler {
	if log == nil {
		log = slog.Default()
	}
	return &registrySampler{reg: reg, timeout: defaultSampleTimeout, log: log}
}

// Sample reads every available cheap collector once and flattens their samples
// into a single key→value map. A per-collector error is swallowed — a live
// frame missing one source is fine; the next frame will carry it. Rate keys are
// absent on the very first snapshot after (re)connect until each collector has
// seeded its baseline, then appear from the second burst on.
func (rs *registrySampler) Sample(parent context.Context) map[string]float64 {
	ctx, cancel := context.WithTimeout(parent, rs.timeout)
	defer cancel()

	out := make(map[string]float64)
	for _, c := range rs.reg.Collectors() {
		if collector.IsIntervalSampled(c) || !c.Available() {
			continue
		}
		samples, err := c.Collect(ctx)
		if err != nil {
			rs.log.Debug("live sample: collector error", "collector", c.Name(), "err", err.Error())
			continue
		}
		for _, sm := range samples {
			out[sm.Key] = sm.Value
		}
	}
	return out
}
