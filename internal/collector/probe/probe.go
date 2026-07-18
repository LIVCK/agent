// Package probe implements the reachability/latency collector: from INSIDE the customer's network the
// agent probes a server-declared list of targets (tcp/http/https/dns) and reports per-target latency,
// loss and reachability. It is the "vantage point external probes can never reach" — private DBs,
// internal gateways, registries, resolvers.
//
// Business-safety by construction: the collector is IntervalSampled (never on the 2s live path,
// never at the 5s sub-sample cadence — internal/live/sampler.go skips it), self-throttled to its own
// probe cadence, concurrency-capped, and every probe is tightly timeout-bounded. tcp/http/dns need
// NO privileges and no systemd-unit change. A target is identified by an opaque server slug (not the
// user's hostname), so no internal name/PII reaches the metric namespace.
package probe

import (
	"context"
	"net"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"sync"
	"time"

	"github.com/LIVCK/agent/internal/collector"
	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/platform"
)

const (
	// maxConcurrent bounds in-flight probes so 15 targets never open 15 sockets at once.
	maxConcurrent = 4
	// tcpAttempts / dnsAttempts give a small loss+min/avg/max signal without being noisy.
	tcpAttempts = 3
	dnsAttempts = 2

	tcpTimeout  = 2 * time.Second
	dnsTimeout  = 2 * time.Second
	httpTimeout = 5 * time.Second

	userAgent = "livck-agent (+https://livck.cloud)"
)

// Prober runs one reachability check. The production implementation (netProber) uses the net stdlib;
// tests substitute a fake so the collector logic is exercised without real network I/O.
type Prober interface {
	Probe(ctx context.Context, t config.ProbeTarget) Result
}

// Result is one target's probe outcome. RTTs holds the successful-attempt latencies; Attempts is the
// total tried (Attempts-len(RTTs) = losses). Status/CertExpiry are HTTP-only.
type Result struct {
	Up         bool
	Attempts   int
	RTTs       []time.Duration
	Status     int
	CertExpiry *time.Duration
}

// Collector probes the configured targets once per probe interval.
type Collector struct {
	cfg    func() *config.Config
	prober Prober
	clock  platform.Clock

	mu       sync.Mutex
	lastRun  time.Time
	cached   []collector.Sample
	hasCache bool
}

// New builds the production probe collector.
func New(cfg func() *config.Config, clock platform.Clock) *Collector {
	return &Collector{cfg: cfg, prober: netProber{}, clock: clock}
}

// Name returns "probe".
func (*Collector) Name() string { return "probe" }

// IntervalSampled keeps probes off the 2s live path and the 5s sub-sample cadence: they run once per
// report interval (further self-throttled to the probe cadence).
func (*Collector) IntervalSampled() bool { return true }

// Available reports whether any target is configured.
func (c *Collector) Available() bool { return len(c.cfg().Collectors.Probe.Targets) > 0 }

// Collect probes every configured target (concurrency-capped) and returns their samples. It
// self-throttles: if the probe cadence has not elapsed since the last run it re-emits the cached
// samples instead of re-probing, so a report interval shorter than the probe interval never turns
// into a probe storm.
func (c *Collector) Collect(ctx context.Context) ([]collector.Sample, error) {
	pcfg := c.cfg().Collectors.Probe
	if len(pcfg.Targets) == 0 {
		return nil, nil
	}

	interval := time.Duration(pcfg.IntervalSeconds) * time.Second
	if interval <= 0 {
		interval = config.DefaultProbeIntervalSeconds * time.Second
	}

	c.mu.Lock()
	if c.hasCache && !c.lastRun.IsZero() && c.clock.Now().Sub(c.lastRun) < interval {
		cached := c.cached
		c.mu.Unlock()
		return cached, nil
	}
	c.mu.Unlock()

	results := make([]Result, len(pcfg.Targets))
	sem := make(chan struct{}, maxConcurrent)
	var wg sync.WaitGroup
	for i, t := range pcfg.Targets {
		wg.Add(1)
		go func(i int, t config.ProbeTarget) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			results[i] = c.prober.Probe(ctx, t)
		}(i, t)
	}
	wg.Wait()

	var samples []collector.Sample
	for i, t := range pcfg.Targets {
		samples = append(samples, resultSamples(t, results[i])...)
	}

	c.mu.Lock()
	c.cached = samples
	c.hasCache = true
	c.lastRun = c.clock.Now()
	c.mu.Unlock()

	return samples, nil
}

// resultSamples turns one target's Result into its present keys under sys.probe.<id>.*. The id is a
// server-assigned opaque slug; NormalizeSegment is a defensive safety net. A metric is omitted when
// the probe produced no value for it (no zero-fake) — but `up` and `loss_pct` are always meaningful.
func resultSamples(t config.ProbeTarget, r Result) []collector.Sample {
	base := "sys.probe." + collector.NormalizeSegment(t.ID) + "."
	out := make([]collector.Sample, 0, 6)

	up := 0.0
	if r.Up {
		up = 1.0
	}
	out = append(out, collector.Sample{Key: base + "up", Value: up})

	if len(r.RTTs) > 0 {
		lo, hi, sum := r.RTTs[0], r.RTTs[0], time.Duration(0)
		for _, d := range r.RTTs {
			if d < lo {
				lo = d
			}
			if d > hi {
				hi = d
			}
			sum += d
		}
		out = append(out,
			collector.Sample{Key: base + "rtt_avg_ms", Value: ms(sum / time.Duration(len(r.RTTs)))},
			collector.Sample{Key: base + "rtt_min_ms", Value: ms(lo)},
			collector.Sample{Key: base + "rtt_max_ms", Value: ms(hi)},
		)
	}
	if r.Attempts > 0 {
		lost := r.Attempts - len(r.RTTs)
		out = append(out, collector.Sample{Key: base + "loss_pct", Value: float64(lost) / float64(r.Attempts) * 100})
	}
	if r.Status > 0 {
		out = append(out, collector.Sample{Key: base + "http_status", Value: float64(r.Status)})
	}
	if r.CertExpiry != nil {
		out = append(out, collector.Sample{Key: base + "cert_expiry_hours", Value: r.CertExpiry.Hours()})
	}
	return out
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// netProber is the production Prober: stdlib net dials, no privileges, no raw sockets.
type netProber struct{}

func (netProber) Probe(ctx context.Context, t config.ProbeTarget) Result {
	switch t.Type {
	case "tcp":
		return probeTCP(ctx, t)
	case "dns":
		return probeDNS(ctx, t)
	case "http", "https":
		return probeHTTP(ctx, t)
	}
	return Result{}
}

// probeTCP measures connect latency to host:port over a full connect + clean close (never a half-open
// SYN scan). Reachable = at least one connect succeeded.
func probeTCP(ctx context.Context, t config.ProbeTarget) Result {
	addr := net.JoinHostPort(t.Target, strconv.Itoa(portOr(t.Port, 80)))
	r := Result{Attempts: tcpAttempts}
	var d net.Dialer
	for i := 0; i < tcpAttempts; i++ {
		cctx, cancel := context.WithTimeout(ctx, tcpTimeout)
		start := time.Now()
		conn, err := d.DialContext(cctx, "tcp", addr)
		cancel()
		if err == nil {
			r.RTTs = append(r.RTTs, time.Since(start))
			r.Up = true
			_ = conn.Close()
		}
	}
	return r
}

// probeDNS measures resolution latency for the target name, optionally against a specific resolver.
func probeDNS(ctx context.Context, t config.ProbeTarget) Result {
	resolver := net.DefaultResolver
	if t.Resolver != "" {
		res := net.JoinHostPort(t.Resolver, "53")
		resolver = &net.Resolver{
			PreferGo: true,
			Dial: func(dctx context.Context, network, _ string) (net.Conn, error) {
				var dd net.Dialer
				return dd.DialContext(dctx, "udp", res)
			},
		}
	}
	r := Result{Attempts: dnsAttempts}
	for i := 0; i < dnsAttempts; i++ {
		cctx, cancel := context.WithTimeout(ctx, dnsTimeout)
		start := time.Now()
		_, err := resolver.LookupHost(cctx, t.Target)
		cancel()
		if err == nil {
			r.RTTs = append(r.RTTs, time.Since(start))
			r.Up = true
		}
	}
	return r
}

// probeHTTP issues one GET and records total latency, status, and (https) peer-cert expiry. Any HTTP
// response — even 401/404 — counts as reachable; only a transport error is "down".
func probeHTTP(ctx context.Context, t config.ProbeTarget) Result {
	scheme := "http"
	if t.Type == "https" {
		scheme = "https"
	}
	host := t.Target
	if t.Port > 0 {
		host = net.JoinHostPort(t.Target, strconv.Itoa(t.Port))
	}
	url := scheme + "://" + host + t.Path

	r := Result{Attempts: 1}
	cctx, cancel := context.WithTimeout(ctx, httpTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(cctx, http.MethodGet, url, nil)
	if err != nil {
		return r
	}
	req.Header.Set("User-Agent", userAgent)
	req = req.WithContext(httptrace.WithClientTrace(cctx, &httptrace.ClientTrace{}))

	client := &http.Client{
		Timeout: httpTimeout,
		// Never follow redirects — a redirect is still "reachable".
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return r
	}
	defer func() { _ = resp.Body.Close() }()

	r.Up = true
	r.RTTs = append(r.RTTs, time.Since(start))
	r.Status = resp.StatusCode
	if resp.TLS != nil && len(resp.TLS.PeerCertificates) > 0 {
		expiry := until(resp.TLS.PeerCertificates[0].NotAfter)
		r.CertExpiry = &expiry
	}
	return r
}

// until returns the duration from now to t, floored at zero (an expired cert reads as 0h, not negative).
func until(t time.Time) time.Duration {
	d := time.Until(t)
	if d < 0 {
		return 0
	}
	return d
}

func portOr(p, fallback int) int {
	if p > 0 {
		return p
	}
	return fallback
}

var _ Prober = netProber{}
