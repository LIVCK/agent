// e2e_test drives the full run loop (runner + config + buffer + sender) against
// an in-process httptest ingest and asserts the release gates end to end:
// bootstrap delivery, newest-first backfill after downtime,
// Retry-After backoff, mid-flight config flip, remove-on-202, and a
// kill/restart spool roundtrip. The per-key aggregation seams (avg / avg+max /
// last / max, the .max companion, cheap-vs-exec sub-sampling) are unit-tested in
// aggregate_test.go; here we prove they survive intact all the way into the wire
// batch the ingest receives. ASCII only.
package runner

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"

	"github.com/LIVCK/agent/internal/buffer"
	"github.com/LIVCK/agent/internal/collector"
	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/event"
	"github.com/LIVCK/agent/internal/platform"
	"github.com/LIVCK/agent/internal/platform/platformtest"
	"github.com/LIVCK/agent/internal/sender"
	"github.com/LIVCK/agent/pkg/wire"
)

// clockAnchor is the shared fake-time epoch for the runner clock across a test
// (and across the two phases of the restart test) so report timestamps are
// deterministic and comparable.
var clockAnchor = time.Unix(1_700_000_000, 0)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// gateClock is a budget-limited instant clock. Sleep advances time and returns
// immediately while the budget lasts, then blocks until ctx is cancelled. A
// negative budget is unlimited (used by the soak). This lets a test run an exact
// number of collect sub-samples (hence a bounded number of report intervals)
// with zero real wall-clock time, then cleanly stops production.
type gateClock struct {
	mu     sync.Mutex
	now    time.Time
	budget int
}

func newGateClock(start time.Time, budget int) *gateClock {
	return &gateClock{now: start, budget: budget}
}

func (c *gateClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *gateClock) Sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	c.mu.Lock()
	if c.budget != 0 {
		if c.budget > 0 {
			c.budget--
		}
		if d > 0 {
			c.now = c.now.Add(d)
		}
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()
	<-ctx.Done()
	return ctx.Err()
}

// e2eIngest is a controllable httptest ingest: the status, advertised config
// version and Retry-After are settable mid-test, and decoded batches are
// retained for assertions (disable retention for the soak). It decodes the same
// zstd+protobuf envelope pulse expects, so a decode failure here would be a real
// wire regression.
type e2eIngest struct {
	dec        *zstd.Decoder
	status     atomic.Int32
	cfgVersion atomic.Int32
	retryAfter atomic.Int32 // seconds; only attached on 429/503
	attempts   atomic.Int64
	retain     atomic.Bool

	mu      sync.Mutex
	batches []*wire.MetricBatch
}

func newE2EIngest(t *testing.T, status int) *e2eIngest {
	t.Helper()
	dec, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(dec.Close)
	i := &e2eIngest{dec: dec}
	i.status.Store(int32(status))
	i.cfgVersion.Store(1)
	i.retain.Store(true)
	return i
}

func (i *e2eIngest) setStatus(s int) { i.status.Store(int32(s)) }

func (i *e2eIngest) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		i.attempts.Add(1)
		raw, _ := io.ReadAll(req.Body)
		if i.retain.Load() {
			if plain, err := i.dec.DecodeAll(raw, nil); err == nil {
				var b wire.MetricBatch
				if proto.Unmarshal(plain, &b) == nil {
					i.mu.Lock()
					i.batches = append(i.batches, &b)
					i.mu.Unlock()
				}
			}
		}
		st := int(i.status.Load())
		if ra := i.retryAfter.Load(); ra > 0 && (st == http.StatusTooManyRequests || st == http.StatusServiceUnavailable) {
			w.Header().Set("Retry-After", strconv.Itoa(int(ra)))
		}
		w.WriteHeader(st)
		if st == http.StatusAccepted {
			_, _ = io.WriteString(w, fmt.Sprintf(`{"status":"ok","config_version":%d,"server_time_unix_ms":0}`, i.cfgVersion.Load()))
			return
		}
		_, _ = io.WriteString(w, `{"error":{"code":"RETRY_LATER","retryable":true}}`)
	}
}

// reportsInOrder flattens every retained batch's reports in arrival order (the
// sender is single-goroutine, so arrival order is delivery order).
func (i *e2eIngest) reportsInOrder() []*wire.Report {
	i.mu.Lock()
	defer i.mu.Unlock()
	var out []*wire.Report
	for _, b := range i.batches {
		out = append(out, b.Reports...)
	}
	return out
}

// --- synthetic collectors --------------------------------------------------

// e2eCheap is a cheap (sub-sampled) collector. It emits an avg+max key whose
// value climbs with each call (so the interval mean and peak differ and the
// .max companion is meaningful) and a plain avg key (which must never gain a
// companion).
type e2eCheap struct{ calls atomic.Int64 }

func (c *e2eCheap) Name() string    { return "e2e-cheap" }
func (c *e2eCheap) Available() bool { return true }
func (c *e2eCheap) Collect(context.Context) ([]collector.Sample, error) {
	n := float64(c.calls.Add(1))
	return []collector.Sample{
		{Key: "sys.cpu.total_pct", Value: n}, // avg+max
		{Key: "sys.mem.used_pct", Value: 50}, // avg, no companion
	}, nil
}

// e2eExec is an exec-based (interval-sampled) collector: it opts out of
// sub-sampling and must be read once per interval, not per sub-sample.
type e2eExec struct{ calls atomic.Int64 }

func (c *e2eExec) Name() string          { return "e2e-exec" }
func (c *e2eExec) Available() bool       { return true }
func (c *e2eExec) IntervalSampled() bool { return true }
func (c *e2eExec) Collect(context.Context) ([]collector.Sample, error) {
	c.calls.Add(1)
	return []collector.Sample{{Key: "sys.smart.sda.temp_c", Value: 42}}, nil // avg+max wildcard
}

// --- fixture ---------------------------------------------------------------

type e2eOpts struct {
	budget           int            // runner sleep budget (< 0 = unlimited)
	status           int            // initial ingest status (default 202)
	serverCfgVersion int            // config_version the ingest advertises on 202 (default 1)
	retryAfter       int            // Retry-After seconds on 429/503 (0 = none)
	fetcher          config.Fetcher // config pull source (nil = none)
	applyCfg         string         // raw config JSON applied before the run
	fs               *platformtest.MemFS
	retain           *bool // retain decoded batches (default true)
}

type e2eFixture struct {
	runner      *Runner
	ing         *e2eIngest
	ring        *buffer.Ring
	spool       *buffer.Spool
	fs          *platformtest.MemFS
	cfg         *config.Manager
	senderClock *platformtest.Clock
}

func newE2E(t *testing.T, o e2eOpts) *e2eFixture {
	t.Helper()
	if o.status == 0 {
		o.status = http.StatusAccepted
	}
	ing := newE2EIngest(t, o.status)
	if o.serverCfgVersion != 0 {
		ing.cfgVersion.Store(int32(o.serverCfgVersion))
	}
	if o.retryAfter != 0 {
		ing.retryAfter.Store(int32(o.retryAfter))
	}
	if o.retain != nil {
		ing.retain.Store(*o.retain)
	}
	srv := httptest.NewServer(ing.handler())
	t.Cleanup(srv.Close)

	fs := o.fs
	if fs == nil {
		fs = platformtest.NewMemFS()
	}

	runnerClock := newGateClock(clockAnchor, o.budget)
	senderClock := platformtest.NewClock(clockAnchor)
	plat := platform.Platform{Clock: runnerClock, FS: fs, Host: stubHost{name: "e2e-host"}}

	var idc atomic.Int64
	newID := func() string { return "id-" + strconv.FormatInt(idc.Add(1), 10) }

	queue := event.NewQueue(senderClock, newID, event.DefaultCapacity)
	ring := buffer.New(runnerClock, queue, buffer.DefaultMaxBytes, buffer.DefaultHorizon)
	spool := buffer.NewSpool(fs, "/state/spool.pb", buffer.DefaultMaxBytes)
	cfg := config.NewManager(o.fetcher, queue, fs, "/state/config.json", discardLogger())
	if o.applyCfg != "" {
		cfg.Apply([]byte(o.applyCfg))
	}

	reg := collector.NewRegistry()
	reg.Register(&e2eCheap{})
	reg.Register(&e2eExec{})

	trigger := make(chan struct{}, 1)
	sndr, err := sender.New(sender.Options{
		Client:       srv.Client(),
		URL:          srv.URL,
		TokenFn:      func() string { return "lvk_test" },
		InstanceID:   "11111111-1111-4111-8111-111111111111",
		AgentVersion: "0.0.0-e2e",
		Clock:        senderClock,
		Buffer:       ring,
		Events:       queue,
		Config:       cfg.Current,
		Emitter:      queue,
		NewID:        newID,
		Jitter:       func(int64) int64 { return 0 },
		Log:          discardLogger(),
		OnConfigVersion: func(uint32) {
			select {
			case trigger <- struct{}{}:
			default:
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sndr.Close)

	r := New(Options{
		Platform:      plat,
		Config:        cfg,
		Registry:      reg,
		Ring:          ring,
		Events:        queue,
		Sender:        sndr,
		Spool:         spool,
		ConfigTrigger: trigger,
		Meta:          map[string]string{"os": "linux", "hostname": "e2e-host"},
		Log:           discardLogger(),
	})
	return &e2eFixture{runner: r, ing: ing, ring: ring, spool: spool, fs: fs, cfg: cfg, senderClock: senderClock}
}

// start runs the agent in the background and registers cleanup that cancels it
// and waits for a clean shutdown.
func (f *e2eFixture) start(t *testing.T) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = f.runner.Run(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("runner did not shut down within 3s")
		}
	})
	return cancel
}

// smallInterval is a valid config with the smallest legal cadence (interval 30s,
// sample 10s -> 3 sub-samples per interval) so a report interval costs exactly 3
// sleep-budget units.
const smallInterval = `{"config_version":1,"interval_seconds":30,"sample_interval_seconds":10}`

const subSamplesSmall = 3 // subSampleCount(30, 10)

// --- scenarios -------------------------------------------------------------

// TestE2EBootstrapAndSeamsSurviveToWire proves a bootstrap report plus one full
// interval reach the ingest, and that the aggregation seams survive end to end:
// the avg+max key carries its .max companion into the wire batch with peak >=
// mean, the plain avg key never gains a companion, and the exec (interval-
// sampled) collector's key is present.
func TestE2EBootstrapAndSeamsSurviveToWire(t *testing.T) {
	f := newE2E(t, e2eOpts{budget: subSamplesSmall, applyCfg: smallInterval})
	f.start(t)

	// Wait until a delivered report carries both the avg+max companion and the
	// exec collector's key, proving the seams reached the wire (not just that a
	// report arrived).
	waitFor(t, func() bool {
		for _, rep := range f.ing.reportsInOrder() {
			_, hasMax := rep.Metrics["sys.cpu.total_pct.max"]
			_, hasExec := rep.Metrics["sys.smart.sda.temp_c"]
			if hasMax && hasExec {
				return true
			}
		}
		return false
	}, "avg+max companion and exec key reached the wire")

	reps := f.ing.reportsInOrder()
	if len(reps) == 0 {
		t.Fatal("no reports delivered")
	}

	var sawCompanion, sawExec, sawMeta bool
	for _, rep := range reps {
		if rep.Meta["hostname"] == "e2e-host" {
			sawMeta = true
		}
		base, hasBase := rep.Metrics["sys.cpu.total_pct"]
		peak, hasMax := rep.Metrics["sys.cpu.total_pct.max"]
		if hasBase && hasMax {
			sawCompanion = true
			if peak < base {
				t.Errorf("avg+max companion %v < mean %v in the wire report", peak, base)
			}
		}
		if _, ok := rep.Metrics["sys.mem.used_pct.max"]; ok {
			t.Error("plain avg key sys.mem.used_pct gained a .max companion end to end")
		}
		if _, ok := rep.Metrics["sys.smart.sda.temp_c"]; ok {
			sawExec = true
		}
	}
	if !sawCompanion {
		t.Error("avg+max .max companion did not reach the wire batch")
	}
	if !sawExec {
		t.Error("interval-sampled (exec) collector key did not reach the wire batch")
	}
	if !sawMeta {
		t.Error("host meta did not reach the wire batch")
	}
}

// TestE2EDowntimeThenCompleteBackfill is the full-loop downtime gate: the ingest
// is down (503) while the agent buffers a bootstrap plus three interval reports,
// then recovers. Every buffered report must be delivered after recovery -
// nothing is lost, the backlog heals completely. (Strict replay ORDER is covered
// deterministically by TestBackfillReplayNewestFirst, free of the sender's
// in-flight held-batch timing.)
func TestE2EDowntimeThenCompleteBackfill(t *testing.T) {
	f := newE2E(t, e2eOpts{
		budget:   3 * subSamplesSmall, // bootstrap + exactly three interval reports
		status:   http.StatusServiceUnavailable,
		applyCfg: smallInterval,
	})
	f.start(t)

	waitFor(t, func() bool { return f.ring.Len() >= 4 }, "four reports buffered during downtime")

	var buffered []int64
	for _, rep := range f.ring.Snapshot() {
		buffered = append(buffered, rep.SampledAtUnixMs)
	}

	f.ing.setStatus(http.StatusAccepted)
	waitFor(t, func() bool {
		got := distinctSampledAt(f.ing.reportsInOrder())
		for _, s := range buffered {
			if _, ok := got[s]; !ok {
				return false
			}
		}
		return true
	}, "every buffered report delivered after recovery (no loss)")
}

// TestBackfillReplayNewestFirst drives the buffer+sender replay directly with
// the whole backlog present before the first send, so the newest-first ordering
// is deterministic: one report per batch must be delivered strictly newest-first
// (live leads, backlog heals backwards).
func TestBackfillReplayNewestFirst(t *testing.T) {
	ing := newE2EIngest(t, http.StatusAccepted)
	srv := httptest.NewServer(ing.handler())
	t.Cleanup(srv.Close)

	clock := platformtest.NewClock(clockAnchor)
	var idc atomic.Int64
	newID := func() string { return "id-" + strconv.FormatInt(idc.Add(1), 10) }
	queue := event.NewQueue(clock, newID, event.DefaultCapacity)
	ring := buffer.New(clock, queue, buffer.DefaultMaxBytes, buffer.DefaultHorizon)
	cfg := config.NewManager(nil, queue, nil, "", discardLogger())
	cfg.Apply([]byte(`{"config_version":1,"limits":{"max_reports_per_batch":1}}`))

	// Four reports in chronological (insertion) order; all present before sending.
	base := clockAnchor.UnixMilli()
	offsets := []int64{0, 100_000, 200_000, 300_000}
	for _, off := range offsets {
		ring.Add(&wire.Report{SampledAtUnixMs: base + off, Metrics: map[string]float64{"sys.mem.used_pct": 1}})
	}

	sndr, err := sender.New(sender.Options{
		Client: srv.Client(), URL: srv.URL,
		TokenFn:    func() string { return "lvk_test" },
		InstanceID: "11111111-1111-4111-8111-111111111111",
		Clock:      clock, Buffer: ring, Events: queue, Config: cfg.Current,
		Emitter: queue, NewID: newID, Jitter: func(int64) int64 { return 0 }, Log: discardLogger(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sndr.Close)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { sndr.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })

	waitFor(t, func() bool { return len(ing.reportsInOrder()) >= len(offsets) }, "whole backlog replayed")

	reps := ing.reportsInOrder()
	for i := 1; i < len(reps); i++ {
		if reps[i].SampledAtUnixMs >= reps[i-1].SampledAtUnixMs {
			t.Fatalf("replay not strictly newest-first at %d: %d then %d",
				i, reps[i-1].SampledAtUnixMs, reps[i].SampledAtUnixMs)
		}
	}
	if reps[0].SampledAtUnixMs != base+300_000 {
		t.Fatalf("newest report was not replayed first: got %d, want %d", reps[0].SampledAtUnixMs, base+300_000)
	}
}

// TestE2ERetryAfterRespected returns 429 with Retry-After: 3 and asserts the
// sender's backoff sleep honours it as a floor (>= 3s) before the flip to 202
// clears the buffer.
func TestE2ERetryAfterRespected(t *testing.T) {
	retain := false
	f := newE2E(t, e2eOpts{
		budget:     0, // bootstrap only
		status:     http.StatusTooManyRequests,
		retryAfter: 3,
		retain:     &retain,
	})
	f.start(t)

	waitFor(t, func() bool {
		for _, d := range f.senderClock.Sleeps() {
			if d >= 3*time.Second {
				return true
			}
		}
		return false
	}, "sender honoured Retry-After as a backoff floor")

	f.ing.setStatus(http.StatusAccepted)
	waitFor(t, func() bool { return f.ring.Len() == 0 }, "buffer cleared after recovery")
}

// TestE2EConfigFlipMidFlight proves the config_version piggyback on a 202
// triggers a config pull that applies the new document: the ingest advertises
// version 2, the stub fetcher serves a v2 document with a new interval, and the
// applied config flips from 1 to 2.
func TestE2EConfigFlipMidFlight(t *testing.T) {
	v2 := []byte(`{"config_version":2,"interval_seconds":60}`)
	f := newE2E(t, e2eOpts{
		budget:           0, // bootstrap is enough to earn a 202
		serverCfgVersion: 2,
		fetcher:          stubFetcher{raw: v2},
	})
	if f.cfg.Current().ConfigVersion != 1 {
		t.Fatalf("precondition: config version = %d, want 1", f.cfg.Current().ConfigVersion)
	}
	f.start(t)

	waitFor(t, func() bool { return f.cfg.Current().ConfigVersion == 2 }, "config flipped to v2 after the 202 piggyback")
	if got := f.cfg.Current().IntervalSeconds; got != 60 {
		t.Fatalf("v2 interval_seconds = %d, want 60", got)
	}
}

// TestE2ERemoveOn202NoResend runs several intervals against an always-202 ingest
// and asserts every delivered report is unique: remove-on-202 means an
// acknowledged report is never resent (at-least-once, deduped by removal).
func TestE2ERemoveOn202NoResend(t *testing.T) {
	f := newE2E(t, e2eOpts{budget: 5 * subSamplesSmall, applyCfg: smallInterval})
	f.start(t)

	// bootstrap + 5 intervals = 6 distinct reports; wait until all are delivered.
	waitFor(t, func() bool { return len(distinctSampledAt(f.ing.reportsInOrder())) >= 6 }, "all six reports delivered")

	reps := f.ing.reportsInOrder()
	if got := len(distinctSampledAt(reps)); got != len(reps) {
		t.Fatalf("a report was resent after its 202: %d delivered, %d distinct", len(reps), got)
	}
}

// TestE2EKillRestartSpoolRoundtrip is the restart-continuity gate: phase 1 runs
// against a down ingest so nothing is acked, then is killed (ctx cancel) and
// spools its unacked reports; phase 2 starts a fresh agent over the SAME state
// dir against a healthy ingest and must reload and deliver exactly those
// spooled reports.
func TestE2EKillRestartSpoolRoundtrip(t *testing.T) {
	fs := platformtest.NewMemFS()

	// Phase 1: ingest down, produce reports, capture their timestamps, then kill.
	p1 := newE2E(t, e2eOpts{
		budget:   3 * subSamplesSmall,
		status:   http.StatusServiceUnavailable,
		applyCfg: smallInterval,
		fs:       fs,
	})
	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan struct{})
	go func() { _ = p1.runner.Run(ctx1); close(done1) }()

	waitFor(t, func() bool { return p1.ring.Len() >= 4 }, "phase 1 buffered its reports")
	// Interval reports only (sampled_at strictly after the anchor) can come solely
	// from the spool in phase 2, which produces no intervals of its own.
	var intervalStamps []int64
	for _, rep := range p1.ring.Snapshot() {
		if rep.SampledAtUnixMs > clockAnchor.UnixMilli() {
			intervalStamps = append(intervalStamps, rep.SampledAtUnixMs)
		}
	}
	if len(intervalStamps) == 0 {
		t.Fatal("phase 1 produced no interval reports to spool")
	}
	cancel1() // simulate the kill/SIGTERM
	select {
	case <-done1:
	case <-time.After(3 * time.Second):
		t.Fatal("phase 1 did not shut down")
	}

	// Phase 2: healthy ingest, same state dir. loadSpool must restore and deliver.
	p2 := newE2E(t, e2eOpts{budget: 0, status: http.StatusAccepted, fs: fs})
	p2.start(t)

	want := make(map[int64]bool, len(intervalStamps))
	for _, s := range intervalStamps {
		want[s] = true
	}
	waitFor(t, func() bool {
		got := 0
		for _, rep := range p2.ing.reportsInOrder() {
			if want[rep.SampledAtUnixMs] {
				got++
			}
		}
		return got >= len(intervalStamps)
	}, "phase 2 reloaded and delivered every spooled interval report")
}

// distinctSampledAt returns the set of report timestamps.
func distinctSampledAt(reps []*wire.Report) map[int64]struct{} {
	set := make(map[int64]struct{}, len(reps))
	for _, r := range reps {
		set[r.SampledAtUnixMs] = struct{}{}
	}
	return set
}

// stubFetcher serves one fixed config document on every pull.
type stubFetcher struct{ raw []byte }

func (f stubFetcher) Fetch(context.Context, string) (raw []byte, newETag string, notModified bool, err error) {
	return f.raw, "etag-fixed", false, nil
}
