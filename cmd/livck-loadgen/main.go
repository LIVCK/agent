// Command livck-loadgen drives a fake fleet of N simulated agents against a live
// pulse ingest to find its throughput knee on the dev host. It reuses the REAL
// agent code paths - enroll.Do, the
// sender with its full retry matrix, the frozen wire contract - so a 429/503 or
// backoff behaves exactly as a real agent would; only the collectors are
// replaced by the catalog-driven synthetic generator.
//
// It is a load/dev tool, not part of the shipped agent: it lives in cmd/ (wiring
// only) and the testable generation + measurement logic lives in
// internal/loadgen. It enrolls into a throwaway org via a fleet lve_ token; org
// creation and cleanup are done out of band (tinker), never by this binary.
//
// ASCII only.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/LIVCK/agent/internal/buffer"
	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/enroll"
	"github.com/LIVCK/agent/internal/event"
	"github.com/LIVCK/agent/internal/loadgen"
	"github.com/LIVCK/agent/internal/platform"
	"github.com/LIVCK/agent/internal/platform/platformtest"
	"github.com/LIVCK/agent/internal/sender"
	"github.com/LIVCK/agent/pkg/wire"
)

// clkTck is the kernel USER_HZ used to turn /proc CPU ticks into seconds. 100 is
// the near-universal Linux value; a wrong value only scales the reported pulse
// CPU%, never the throughput numbers.
const clkTck = 100.0

type flags struct {
	enrollURL  string
	ingestURL  string
	token      string
	ramp       string
	rates      string
	hold       time.Duration
	interval   time.Duration
	poisonPct  int
	enrollConc int
	pulsePID   int
	reportPath string
	agentVer   string
	tokensFile string
}

func main() {
	f := parseFlags()
	log.SetFlags(log.Ltime)

	if f.token == "" && f.tokensFile == "" {
		log.Fatal("livck-loadgen: --token (lve_ fleet token) or --tokens-file is required")
	}
	steps, err := parseRamp(f.ramp)
	if err != nil {
		log.Fatalf("livck-loadgen: bad --ramp: %v", err)
	}
	maxAgents := steps[len(steps)-1]

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rec := loadgen.NewRecorder()
	ingestClient := loadgen.InstrumentClient(sharedClient(maxAgents), rec)
	enrollClient := sharedClient(f.enrollConc)

	runID := time.Now().UTC().Format("20060102T150405Z")
	intervalNs := new(atomic.Int64)
	intervalNs.Store(int64(f.interval))

	var agents []*simAgent
	var enrollErr error
	if f.tokensFile != "" {
		log.Printf("run %s: ingest-only mode from %s -> %s", runID, f.tokensFile, f.ingestURL)
		agents, enrollErr = buildFleetFromTokens(f, ingestClient, intervalNs)
	} else {
		log.Printf("run %s: enrolling up to %d agents against %s ...", runID, maxAgents, f.enrollURL)
		agents, enrollErr = enrollFleet(ctx, f, enrollClient, ingestClient, runID, maxAgents, intervalNs)
	}
	if len(agents) == 0 {
		log.Fatalf("livck-loadgen: no agents available (%v)", enrollErr)
	}
	if enrollErr != nil {
		log.Printf("fleet build stopped early after %d agents: %v", len(agents), enrollErr)
	}
	log.Printf("fleet ready: %d agents; %d marked poison (%d%%)", len(agents), countPoison(agents), f.poisonPct)

	report := runRamp(ctx, f, steps, agents, rec, intervalNs)
	report.RunID = runID
	report.Enrolled = len(agents)
	report.EnrollError = errString(enrollErr)

	printSummary(report)
	if f.reportPath != "" {
		writeReport(f.reportPath, report)
		log.Printf("wrote JSON report to %s", f.reportPath)
	}
}

func parseFlags() flags {
	var f flags
	flag.StringVar(&f.enrollURL, "enroll-url", envOr("LIVCK_ENROLL_URL", "http://localhost:15800"), "Laravel enroll base URL")
	flag.StringVar(&f.ingestURL, "ingest-url", envOr("LIVCK_INGEST_URL", "http://localhost:15810"), "pulse ingest base URL")
	flag.StringVar(&f.token, "token", os.Getenv("LIVCK_FLEET_TOKEN"), "fleet enrollment token (lve_...)")
	flag.StringVar(&f.ramp, "ramp", "200,1000,2000", "comma-separated cumulative agent counts to ramp through")
	flag.StringVar(&f.rates, "rates", "", "optional comma-separated report intervals (e.g. 2s,1s,500ms,200ms) swept over the FULL fleet after the count ramp, to find pulse's throughput knee")
	flag.DurationVar(&f.hold, "hold", 60*time.Second, "hold duration per ramp/rate step")
	flag.DurationVar(&f.interval, "interval", 5*time.Second, "report interval per simulated agent (count-ramp phase)")
	flag.IntVar(&f.poisonPct, "poison-pct", 5, "percent of agents that emit poison (unknown keys + out-of-range values)")
	flag.IntVar(&f.enrollConc, "enroll-concurrency", 32, "parallel enroll workers")
	flag.IntVar(&f.pulsePID, "pulse-pid", 0, "pulse process PID for RSS/CPU sampling (0 = skip)")
	flag.StringVar(&f.reportPath, "report", "", "path to write a JSON report (optional)")
	flag.StringVar(&f.agentVer, "agent-version", "loadgen/0.0.0", "reported agent_version")
	flag.StringVar(&f.tokensFile, "tokens-file", "", "ingest-only mode: file of pre-seeded 'lvk_token,instance_id' lines (skips enroll, avoids the enroll throttle)")
	flag.Parse()
	return f
}

// sharedClient builds one keep-alive HTTP client for many senders. Connections
// are pooled (so this under-represents real-world connection COUNT, but faithfully
// drives request THROUGHPUT, which is what pulse must survive).
func sharedClient(conns int) *http.Client {
	if conns < 64 {
		conns = 64
	}
	tr := &http.Transport{
		MaxIdleConns:        conns * 2,
		MaxIdleConnsPerHost: conns,
		MaxConnsPerHost:     0,
		IdleConnTimeout:     90 * time.Second,
	}
	return &http.Client{Timeout: 30 * time.Second, Transport: tr}
}

// --- simulated agent -------------------------------------------------------

type simAgent struct {
	idx        int
	instID     string
	ring       *buffer.Ring
	sender     *sender.Sender
	gen        *loadgen.Generator
	poison     bool
	intervalNs *atomic.Int64 // shared: the current report interval; a rate sweep updates it live
	started    atomic.Bool
	cancel     context.CancelFunc
	closeOnce  sync.Once
}

var staticCfg = config.Defaults()

func enrollFleet(ctx context.Context, f flags, enrollClient, ingestClient *http.Client, runID string, n int, intervalNs *atomic.Int64) ([]*simAgent, error) {
	type outcome struct {
		agent *simAgent
		err   error
	}
	idxCh := make(chan int)
	outCh := make(chan outcome, f.enrollConc)

	var wg sync.WaitGroup
	for w := 0; w < f.enrollConc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range idxCh {
				a, err := enrollOne(ctx, f, enrollClient, ingestClient, runID, idx, n, intervalNs)
				outCh <- outcome{agent: a, err: err}
			}
		}()
	}
	go func() {
		defer close(idxCh)
		for i := 0; i < n; i++ {
			select {
			case <-ctx.Done():
				return
			case idxCh <- i:
			}
		}
	}()
	go func() { wg.Wait(); close(outCh) }()

	agents := make([]*simAgent, 0, n)
	var firstErr error
	for o := range outCh {
		if o.err != nil {
			if firstErr == nil {
				firstErr = o.err
			}
			continue
		}
		agents = append(agents, o.agent)
	}
	return agents, firstErr
}

func enrollOne(ctx context.Context, f flags, enrollClient, ingestClient *http.Client, runID string, idx, total int, intervalNs *atomic.Int64) (*simAgent, error) {
	fs := platformtest.NewMemFS()
	store := enroll.NewStore(fs, "/state", uuid.NewString)

	hostname := fmt.Sprintf("loadgen-%s-%05d", runID, idx)
	meta := map[string]string{
		"os": "linux", "hostname": hostname,
		"distro": "ubuntu", "distro_version": "24.04", "arch": "amd64",
		"kernel": "6.8.0-loadgen", "virtualization": "kvm",
	}
	fp := map[string]string{
		"hostname":        hostname,
		"machine_id_hash": sha256Hex(hostname),
		"boot_id":         uuid.NewString(),
	}

	res, err := enroll.Do(ctx, enroll.Options{
		Store: store, HTTP: enrollClient, BaseURL: f.enrollURL,
		Token: f.token, Name: hostname, AgentVersion: f.agentVer,
		Meta: meta, Fingerprint: fp,
	})
	if err != nil {
		return nil, err
	}

	instID, err := store.InstanceID()
	if err != nil {
		return nil, err
	}
	_ = res

	return newSimAgent(f, ingestClient, idx, total, func() string { t, _ := store.Token(); return t }, instID, intervalNs)
}

// newSimAgent wires the real sender + buffer for one simulated agent around a
// token source and its bound instance_id.
func newSimAgent(f flags, ingestClient *http.Client, idx, total int, tokenFn func() string, instID string, intervalNs *atomic.Int64) (*simAgent, error) {
	clock := platform.SystemClock{}
	queue := event.NewQueue(clock, uuid.NewString, event.DefaultCapacity)
	ring := buffer.New(clock, queue, buffer.DefaultMaxBytes, buffer.DefaultHorizon)

	sndr, err := sender.New(sender.Options{
		Client:       ingestClient,
		URL:          strings.TrimRight(f.ingestURL, "/") + "/v1/ingest",
		TokenFn:      tokenFn,
		InstanceID:   instID,
		AgentVersion: f.agentVer,
		Clock:        clock,
		Buffer:       ring,
		Events:       queue,
		Config:       func() *config.Config { return staticCfg },
		Emitter:      queue,
		NewID:        uuid.NewString,
	})
	if err != nil {
		return nil, err
	}
	return &simAgent{
		idx:        idx,
		instID:     instID,
		ring:       ring,
		sender:     sndr,
		gen:        loadgen.NewGenerator(uint64(idx) + 1),
		poison:     total > 0 && idx*100/total < f.poisonPct,
		intervalNs: intervalNs,
	}, nil
}

// buildFleetFromTokens builds the sim fleet from a pre-seeded 'token,instance_id'
// file (ingest-only mode), skipping enroll entirely so the enroll throttle never
// binds and a large fleet is available instantly.
func buildFleetFromTokens(f flags, ingestClient *http.Client, intervalNs *atomic.Int64) ([]*simAgent, error) {
	raw, err := os.ReadFile(f.tokensFile)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	total := len(lines)
	agents := make([]*simAgent, 0, total)
	for idx, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, ",", 2)
		if len(parts) != 2 {
			return agents, fmt.Errorf("tokens file line %d: want 'token,instance_id'", idx+1)
		}
		token, instID := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
		a, err := newSimAgent(f, ingestClient, idx, total, func() string { return token }, instID, intervalNs)
		if err != nil {
			return agents, err
		}
		agents = append(agents, a)
	}
	return agents, nil
}

// start launches the agent's sender loop and its producer. Idempotent. The
// producer re-reads the shared interval each cycle so a rate sweep takes effect
// live without restarting agents.
func (a *simAgent) start(parent context.Context) {
	if !a.started.CompareAndSwap(false, true) {
		return
	}
	ctx, cancel := context.WithCancel(parent)
	a.cancel = cancel
	go a.sender.Run(ctx)
	go func() {
		a.produce() // immediate first report so the flow warms up fast
		a.sender.Wake()
		for {
			d := time.Duration(a.intervalNs.Load())
			if d <= 0 {
				d = time.Second
			}
			timer := time.NewTimer(d)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
				a.produce()
				a.sender.Wake()
			}
		}
	}()
}

func (a *simAgent) produce() {
	metrics := a.gen.Report()
	if a.poison {
		metrics = a.gen.PoisonReport()
	}
	a.ring.Add(&wire.Report{SampledAtUnixMs: time.Now().UnixMilli(), Metrics: metrics})
}

func (a *simAgent) stop() {
	a.closeOnce.Do(func() {
		if a.cancel != nil {
			a.cancel()
		}
		a.sender.Close()
	})
}

// --- ramp orchestration ----------------------------------------------------

type stepResult struct {
	Phase      string  `json:"phase"` // "count" or "rate"
	Agents     int     `json:"agents"`
	IntervalMs int64   `json:"interval_ms"`
	ReqPerSec  float64 `json:"req_per_sec"`
	OKRate     float64 `json:"ok_rate"`
	P50Ms      float64 `json:"p50_ms"`
	P90Ms      float64 `json:"p90_ms"`
	P99Ms      float64 `json:"p99_ms"`
	Retry429   int64   `json:"retry_429"`
	Retry5xx   int64   `json:"retry_5xx"`
	Drop4xx    int64   `json:"drop_4xx"`
	Auth       int64   `json:"auth"`
	Transport  int64   `json:"transport_err"`
	PulseRSSMB float64 `json:"pulse_rss_mb"`
	PulseCPU   float64 `json:"pulse_cpu_pct"`
}

type runReport struct {
	RunID       string       `json:"run_id"`
	Enrolled    int          `json:"enrolled"`
	IntervalSec float64      `json:"interval_sec"`
	PoisonPct   int          `json:"poison_pct"`
	EnrollError string       `json:"enroll_error,omitempty"`
	Steps       []stepResult `json:"steps"`
}

func runRamp(ctx context.Context, f flags, steps []int, agents []*simAgent, rec *loadgen.Recorder, intervalNs *atomic.Int64) runReport {
	report := runReport{IntervalSec: f.interval.Seconds(), PoisonPct: f.poisonPct}

	// Phase 1: count ramp at the fixed --interval. Each jump activates a new
	// tranche at once, so a big jump (e.g. 200 -> 2000) is also a reconnect herd.
	intervalNs.Store(int64(f.interval))
	for _, want := range steps {
		if ctx.Err() != nil {
			break
		}
		if want > len(agents) {
			want = len(agents)
		}
		for i := 0; i < want; i++ {
			agents[i].start(ctx)
		}
		log.Printf("count-ramp: %d agents active @ %s; holding %s", want, f.interval, f.hold)
		res := holdAndMeasure(ctx, f, want, rec)
		res.Phase = "count"
		res.IntervalMs = f.interval.Milliseconds()
		report.Steps = append(report.Steps, res)
		logStep(res)
	}

	// Phase 2 (optional): with the full fleet active, sweep the report interval
	// down to push req/s until pulse's knee shows (rising p99 / 429 / plateau).
	if rates, err := parseDurations(f.rates); err == nil && len(rates) > 0 && ctx.Err() == nil {
		active := len(agents)
		for _, d := range rates {
			if ctx.Err() != nil {
				break
			}
			intervalNs.Store(int64(d))
			log.Printf("rate-ramp: %d agents @ %s (~%.0f req/s target); holding %s", active, d, float64(active)/d.Seconds(), f.hold)
			res := holdAndMeasure(ctx, f, active, rec)
			res.Phase = "rate"
			res.IntervalMs = d.Milliseconds()
			report.Steps = append(report.Steps, res)
			logStep(res)
		}
	}

	for _, a := range agents {
		a.stop()
	}
	return report
}

func logStep(res stepResult) {
	log.Printf("  -> %.0f req/s | 202 %.1f%% | p50 %.0fms p99 %.0fms | 429 %d 5xx %d 4xx %d transport %d | pulse rss %.0fMB cpu %.0f%%",
		res.ReqPerSec, res.OKRate*100, res.P50Ms, res.P99Ms, res.Retry429, res.Retry5xx, res.Drop4xx, res.Transport, res.PulseRSSMB, res.PulseCPU)
}

// holdAndMeasure holds the current active set for f.hold and returns the readout
// for that step ALONE (counts and percentiles diffed from the step's start, so
// earlier steps do not bleed in). pulse RSS is the step peak; pulse CPU% is the
// average over the step.
func holdAndMeasure(ctx context.Context, f flags, active int, rec *loadgen.Recorder) stepResult {
	startRaw := rec.Raw()
	startTicks := pulseCPUTicks(f.pulsePID)
	startTime := time.Now()
	deadline := startTime.Add(f.hold)
	rssPeak := pulseRSSMB(f.pulsePID)

	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			deadline = time.Now()
		case <-tick.C:
			if rss := pulseRSSMB(f.pulsePID); rss > rssPeak {
				rssPeak = rss
			}
		}
	}

	dt := time.Since(startTime).Seconds()
	snap := loadgen.DeltaSnapshot(startRaw, rec.Raw())
	res := stepResult{
		Agents:     active,
		OKRate:     snap.OKRate(),
		P50Ms:      snap.P50Ms,
		P90Ms:      snap.P90Ms,
		P99Ms:      snap.P99Ms,
		Retry429:   snap.Retry429,
		Retry5xx:   snap.Retry5xx,
		Drop4xx:    snap.Drop4xx,
		Auth:       snap.Auth,
		Transport:  snap.TransportErr,
		PulseRSSMB: rssPeak,
	}
	if dt > 0 {
		res.ReqPerSec = float64(snap.Total) / dt
		res.PulseCPU = (pulseCPUTicks(f.pulsePID) - startTicks) / clkTck / dt * 100
	}
	return res
}

// --- pulse /proc sampling --------------------------------------------------

func pulseRSSMB(pid int) float64 {
	if pid <= 0 {
		return 0
	}
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/statm", pid))
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(raw))
	if len(fields) < 2 {
		return 0
	}
	pages, _ := strconv.ParseUint(fields[1], 10, 64)
	return float64(pages*uint64(os.Getpagesize())) / (1024 * 1024)
}

// pulseCPUTicks returns cumulative utime+stime in clock ticks from /proc/pid/stat.
func pulseCPUTicks(pid int) float64 {
	if pid <= 0 {
		return 0
	}
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0
	}
	s := string(raw)
	// Field 14 (utime) and 15 (stime) follow the comm field, which is
	// parenthesized and may contain spaces: split after the last ')'.
	rp := strings.LastIndexByte(s, ')')
	if rp < 0 {
		return 0
	}
	fields := strings.Fields(s[rp+1:])
	// After ')', index 0 = state (field 3). utime = field 14 -> index 11; stime index 12.
	if len(fields) < 13 {
		return 0
	}
	utime, _ := strconv.ParseFloat(fields[11], 64)
	stime, _ := strconv.ParseFloat(fields[12], 64)
	return utime + stime
}

// --- reporting -------------------------------------------------------------

func printSummary(r runReport) {
	fmt.Println()
	fmt.Printf("livck-loadgen report  run=%s  enrolled=%d  interval=%.0fs  poison=%d%%\n",
		r.RunID, r.Enrolled, r.IntervalSec, r.PoisonPct)
	fmt.Println(strings.Repeat("-", 108))
	fmt.Printf("%-6s %-8s %7s %10s %8s %8s %8s %7s %7s %7s %9s %8s\n",
		"phase", "agents", "int_ms", "req/s", "202%", "p50ms", "p99ms", "429", "5xx", "4xx", "pulseMB", "pulse%")
	for _, s := range r.Steps {
		fmt.Printf("%-6s %-8d %7d %10.0f %8.1f %8.0f %8.0f %7d %7d %7d %9.0f %8.0f\n",
			s.Phase, s.Agents, s.IntervalMs, s.ReqPerSec, s.OKRate*100, s.P50Ms, s.P99Ms,
			s.Retry429, s.Retry5xx, s.Drop4xx, s.PulseRSSMB, s.PulseCPU)
	}
	fmt.Println(strings.Repeat("-", 108))
	if r.EnrollError != "" {
		fmt.Printf("enroll note: %s\n", r.EnrollError)
	}
}

func writeReport(path string, r runReport) {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		log.Printf("marshal report: %v", err)
		return
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		log.Printf("write report: %v", err)
	}
}

// --- helpers ---------------------------------------------------------------

func parseRamp(s string) ([]int, error) {
	parts := strings.Split(s, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		n, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("invalid step %q", p)
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no steps")
	}
	return out, nil
}

func parseDurations(s string) ([]time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	var out []time.Duration
	for _, p := range strings.Split(s, ",") {
		d, err := time.ParseDuration(strings.TrimSpace(p))
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("invalid interval %q", p)
		}
		out = append(out, d)
	}
	return out, nil
}

func countPoison(agents []*simAgent) int {
	n := 0
	for _, a := range agents {
		if a.poison {
			n++
		}
	}
	return n
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
