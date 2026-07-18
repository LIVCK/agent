// Package runner wires the foundation together into the run loop: it seeds the
// buffer and event queue from the exit spool, sends the bootstrap report so a
// fresh chart has real data within a minute, then produces one report per
// interval and hands delivery to the sender. On shutdown it spools the buffer
// and pending events so a restart costs no data point. The system collectors and
// the lifecycle event sources plug in here; the self collector always runs so
// the loop is exercised end to end.
//
// A report is not a single snapshot: over each report interval the runner
// sub-samples the cheap collectors every sample_interval seconds and folds every
// value into an aggregator, then collapses each key into one report value by its
// catalog agg mode (see aggregate.go). Exec-based collectors (gpu, smart) opt
// out of sub-sampling via the collector.IntervalSampled marker and are read once
// per interval, so the 5s cadence never forks nvidia-smi or smartctl repeatedly.
package runner

import (
	"context"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/LIVCK/agent/internal/buffer"
	"github.com/LIVCK/agent/internal/collector"
	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/event"
	"github.com/LIVCK/agent/internal/platform"
	"github.com/LIVCK/agent/internal/sender"
	"github.com/LIVCK/agent/pkg/wire"
)

// Lifecycle detects and emits the host lifecycle events. The runner drives it so
// clean_shutdown is emitted before the exit spool snapshots the queue. It is
// optional: A1 ran without it.
type Lifecycle interface {
	// Startup runs once before the bootstrap report (boot / unexpected_reboot).
	Startup(ctx context.Context)
	// Tick runs each collect interval (oom_kill / fs_readonly).
	Tick(ctx context.Context)
	// Shutdown runs on cancellation before the exit spool (clean_shutdown).
	Shutdown(ctx context.Context)
}

// Options bundles the runner's dependencies.
type Options struct {
	Platform      platform.Platform
	Config        *config.Manager
	Registry      *collector.Registry
	Ring          *buffer.Ring
	Events        *event.Queue
	Sender        *sender.Sender
	Spool         *buffer.Spool
	Lifecycle     Lifecycle
	ConfigTrigger chan struct{}
	// Meta is the report-level host profile sent on the bootstrap report and the
	// first regular report. A2 fills the full profile; A1 sends hostname and os.
	Meta map[string]string
	Log  *slog.Logger
	// CollectTimeout bounds ONE cheap (sub-sampled) collector's collect call.
	// Zero uses a default. Cheap collectors read /proc and finish in well under a
	// millisecond, so this stays short.
	CollectTimeout time.Duration
	// IntervalCollectTimeout bounds ONE interval-sampled (exec-based) collector's
	// collect call — probe, smart, gpu. It is far larger than CollectTimeout
	// because probe fans out to probeMaxTargets (each TCP target can take
	// tcpAttempts*tcpTimeout under a black-holed link) and smart reads up to
	// maxDevices sequentially. Each such collector gets its OWN budget so a slow
	// probe cycle can never starve smart/gpu or truncate a healthy target's dial
	// into a false "down". Zero uses a default. The default (30s) fully covers the
	// probe worst case yet stays well inside the liveness grace (interval +
	// max(60, 1.5*interval)), so a delayed report never trips a false down.
	IntervalCollectTimeout time.Duration
}

// Runner owns the collect loop and shutdown spool.
type Runner struct {
	opt      Options
	log      *slog.Logger
	metaSent bool
}

// New builds a Runner.
func New(opt Options) *Runner {
	log := opt.Log
	if log == nil {
		log = slog.Default()
	}
	if opt.CollectTimeout <= 0 {
		opt.CollectTimeout = 4 * time.Second
	}
	if opt.IntervalCollectTimeout <= 0 {
		opt.IntervalCollectTimeout = 30 * time.Second
	}
	return &Runner{opt: opt, log: log}
}

// Run executes the agent until ctx is cancelled, then spools state and returns.
func (r *Runner) Run(ctx context.Context) error {
	r.loadSpool()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); r.opt.Sender.Run(ctx) }()
	go func() {
		defer wg.Done()
		r.opt.Config.Run(ctx, r.opt.ConfigTrigger, config.DefaultPollInterval)
	}()

	// Lifecycle startup detection (boot / unexpected_reboot) before the first
	// report so those events ride along with the earliest batches.
	if r.opt.Lifecycle != nil {
		r.opt.Lifecycle.Startup(ctx)
	}

	// Bootstrap report: one sample, full meta, sent immediately. It
	// is deliberately a single-sample report (exempt from sub-sampling) so a fresh
	// chart has data within a minute; rate keys are absent until the first regular
	// report seeds their predecessors.
	r.enqueue(r.bootstrapReport(ctx))

	r.collectLoop(ctx)

	// ctx is done. Emit clean_shutdown and persist its marker before the exit
	// spool snapshots the queue, so a graceful stop is delivered via the spool +
	// boot backfill and the next boot is recognised as clean.
	if r.opt.Lifecycle != nil {
		r.opt.Lifecycle.Shutdown(ctx)
	}

	// Wait for the sender and config loops to unwind so no report is removed
	// after we snapshot, then spool.
	wg.Wait()
	r.saveSpool()
	return nil
}

// collectLoop produces one report per (hot-reloadable) interval until ctx ends.
// Each iteration sub-samples the cheap collectors across the interval and
// collapses the accumulated values into a single report.
func (r *Runner) collectLoop(ctx context.Context) {
	for {
		rep := r.collectInterval(ctx)
		if rep == nil {
			return // ctx cancelled mid-interval: no partial report, shutdown spools.
		}
		r.enqueue(rep)
		if r.opt.Lifecycle != nil {
			r.opt.Lifecycle.Tick(ctx)
		}
	}
}

// collectInterval runs one report interval: it reads the interval-sampled
// (exec-based) collectors once, then sub-samples the cheap collectors every
// sample_interval, folding every value into an aggregator, and returns the
// collapsed report once the interval has elapsed. The interval and cadence are
// read once at the start so a mid-interval hot-reload takes effect on the next
// interval, not halfway through. It returns nil if ctx is cancelled before the
// interval completes.
func (r *Runner) collectInterval(ctx context.Context) *wire.Report {
	cur := r.opt.Config.Current()
	sampleInterval := time.Duration(cur.SampleIntervalSeconds) * time.Second
	subSamples := subSampleCount(cur.IntervalSeconds, cur.SampleIntervalSeconds)

	agg := newAggregator()
	// Interval-sampled (exec) collectors once, at the interval start.
	r.collectInto(ctx, agg, true)

	for i := 0; i < subSamples; i++ {
		if err := r.opt.Platform.Clock.Sleep(ctx, sampleInterval); err != nil {
			return nil
		}
		r.collectInto(ctx, agg, false)
		r.addBufferMetrics(agg)
	}
	return r.assembleReport(agg, cur.Limits.MaxKeysPerReport)
}

// bootstrapReport is the single-sample startup report: it reads every collector
// once (cheap and interval-sampled) so the first chart carries the available
// gauges immediately. Rate keys have no predecessor yet and are absent until the
// first regular report.
func (r *Runner) bootstrapReport(ctx context.Context) *wire.Report {
	agg := newAggregator()
	r.collectInto(ctx, agg, true)
	r.collectInto(ctx, agg, false)
	r.addBufferMetrics(agg)
	return r.assembleReport(agg, r.opt.Config.Current().Limits.MaxKeysPerReport)
}

// subSampleCount is the number of cheap sub-samples per report interval, the
// interval rounded to the nearest whole multiple of the sample cadence and at
// least one so a report is always produced. With the 30s interval floor and the
// 3-10s cadence range this is always >= 3.
func subSampleCount(intervalSeconds, sampleSeconds int) int {
	if sampleSeconds <= 0 {
		sampleSeconds = config.DefaultSampleIntervalSeconds
	}
	n := (intervalSeconds + sampleSeconds/2) / sampleSeconds
	if n < 1 {
		n = 1
	}
	return n
}

// collectInto runs the collectors matching wantInterval (interval-sampled when
// true, cheap sub-sampled when false) and folds their samples into agg. Each
// collector runs under its OWN timeout, not one shared budget: cheap collectors
// get CollectTimeout, exec-based interval-sampled collectors (probe/smart/gpu)
// get the larger IntervalCollectTimeout. Per-collector budgets are what keep a
// slow probe cycle from starving smart/gpu or from truncating a healthy target's
// dial into a false "down". A per-collector error degrades to a WARN and the
// other collectors' samples still land, mirroring Registry.Collect.
func (r *Runner) collectInto(parent context.Context, agg *aggregator, wantInterval bool) {
	budget := r.opt.CollectTimeout
	if wantInterval {
		budget = r.opt.IntervalCollectTimeout
	}
	for _, c := range r.opt.Registry.Collectors() {
		if collector.IsIntervalSampled(c) != wantInterval {
			continue
		}
		if !c.Available() {
			continue
		}
		ctx, cancel := context.WithTimeout(parent, budget)
		samples, err := c.Collect(ctx)
		cancel()
		if err != nil {
			r.log.Warn("collector error", "collector", c.Name(), "err", err.Error())
			continue
		}
		for _, s := range samples {
			agg.add(s)
		}
	}
}

// addBufferMetrics folds the buffer-derived self keys into the aggregator on
// each cheap sub-sample: dropped_reports (agg=last, latest counter) and
// buffer_fill_pct (agg=avg, mean fill). The runner owns the buffer, so these are
// not produced by any collector.
func (r *Runner) addBufferMetrics(agg *aggregator) {
	if r.opt.Ring == nil {
		return
	}
	agg.add(collector.Sample{Key: "sys.agent.dropped_reports", Value: float64(r.opt.Ring.DroppedTotal())})
	agg.add(collector.Sample{Key: "sys.agent.buffer_fill_pct", Value: r.opt.Ring.FillPct()})
}

func (r *Runner) enqueue(rep *wire.Report) {
	r.opt.Ring.Add(rep)
	r.opt.Sender.Wake()
}

// assembleReport collapses the aggregator into one wire report, caps the key
// count, and stamps sampled_at at the END of the aggregation window (the current
// wall-clock, giving monotone ordering and correct rollup-bucket assignment).
// Meta rides the bootstrap report and the first regular report only.
func (r *Runner) assembleReport(agg *aggregator, maxKeys int) *wire.Report {
	metrics := capKeys(agg.collapse(), maxKeys)
	rep := &wire.Report{
		SampledAtUnixMs: r.opt.Platform.Clock.Now().UnixMilli(),
		Metrics:         metrics,
	}
	if !r.metaSent && len(r.opt.Meta) > 0 {
		rep.Meta = r.opt.Meta
		r.metaSent = true
	}
	return rep
}

// wildcardPrefixes are the per-device / per-target metric families. When a report
// exceeds the key cap these are shed first, preserving the fixed base and
// sys.agent.* keys. sys.probe.* belongs here too: it fans out to up to 7 keys per
// target (probeMaxTargets targets), so without it a probe-heavy host would shed
// its fixed base metrics (procs/psi/swap/uptime) in favour of surplus probe keys.
var wildcardPrefixes = []string{"sys.disk.", "sys.diskio.", "sys.net.", "sys.gpu.", "sys.smart.", "sys.probe."}

// capKeys enforces the per-report key ceiling. On realistic hosts the
// map is far below the cap; when it is not, the fixed base keys are kept whole
// and the per-device keys are shed in sorted order so the result is
// deterministic. pulse also caps, but the agent self-limits so over-cap keys
// never leave the host.
func capKeys(metrics map[string]float64, max int) map[string]float64 {
	if max <= 0 || len(metrics) <= max {
		return metrics
	}
	var core, wild []string
	for k := range metrics {
		if hasAnyPrefix(k, wildcardPrefixes) {
			wild = append(wild, k)
		} else {
			core = append(core, k)
		}
	}
	sort.Strings(core)
	sort.Strings(wild)

	out := make(map[string]float64, max)
	for _, k := range core {
		if len(out) >= max {
			return out
		}
		out[k] = metrics[k]
	}
	for _, k := range wild {
		if len(out) >= max {
			break
		}
		out[k] = metrics[k]
	}
	return out
}

func hasAnyPrefix(s string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}

func (r *Runner) loadSpool() {
	if r.opt.Spool == nil {
		return
	}
	reports, events, err := r.opt.Spool.Load()
	if err != nil {
		r.log.Warn("could not load exit spool", "err", err.Error())
		return
	}
	if len(reports) > 0 {
		r.opt.Ring.Restore(reports)
	}
	if len(events) > 0 {
		restored := make([]event.Event, len(events))
		for i, e := range events {
			restored[i] = event.FromWire(e)
		}
		r.opt.Events.Restore(restored)
	}
	if len(reports) > 0 || len(events) > 0 {
		r.log.Info("restored exit spool", "reports", len(reports), "events", len(events))
	}
}

func (r *Runner) saveSpool() {
	if r.opt.Spool == nil {
		return
	}
	pending := r.opt.Events.Snapshot()
	wireEvents := make([]*wire.Event, len(pending))
	for i, e := range pending {
		wireEvents[i] = e.ToWire()
	}
	if err := r.opt.Spool.Save(r.opt.Ring.Snapshot(), wireEvents); err != nil {
		r.log.Warn("could not write exit spool", "err", err.Error())
		return
	}
	r.log.Info("wrote exit spool", "reports", r.opt.Ring.Len(), "events", len(pending))
}
