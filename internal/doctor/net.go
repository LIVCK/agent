package doctor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// maxBodyBytes caps how much of a probe response we read; these bodies are a few
// KB of JSON at most.
const maxBodyBytes = 1 << 20

// clockSkewHardLimit mirrors the server's batch-level rejection threshold: a
// skew above it means every batch is dropped with CLOCK_SKEW.
const clockSkewHardLimit = 5 * time.Minute

// checkNetwork probes the two hosts the agent depends on. The control-plane
// probe hits /v1/me, which in one round-trip proves DNS + reachability, exercises
// the managed token, and yields the server Date header for the clock-skew check.
// The ingest probe is an unauthenticated reachability check (a 401/405 still
// proves the host answers).
func checkNetwork(ctx context.Context, r *Report, opts Options) {
	token := managedToken(opts)

	controlHost := hostOf(opts.ControlBase)
	me := probe(ctx, opts, strings.TrimRight(opts.ControlBase, "/")+"/api/v1/me", token)
	if me.err != nil {
		classifyTransport(r, "control plane ("+controlHost+")", controlHost, me.err)
	} else {
		r.add("control plane ("+controlHost+")", StatusOK, "reachable (HTTP "+strconv.Itoa(me.status)+")")
		checkSkew(r, opts, me.date)
		checkAuth(r, me, token)
		if token != "" {
			checkConfig(ctx, r, opts, token)
		}
	}

	ingestHost := hostOf(opts.IngestBase)
	ing := probe(ctx, opts, strings.TrimRight(opts.IngestBase, "/")+"/v1/ingest", "")
	if ing.err != nil {
		classifyTransport(r, "ingest ("+ingestHost+")", ingestHost, ing.err)
	} else {
		r.add("ingest ("+ingestHost+")", StatusOK, "reachable (HTTP "+strconv.Itoa(ing.status)+")")
	}
}

// probeResult is the outcome of a single HTTP probe.
type probeResult struct {
	status int
	date   string
	body   []byte
	err    error
}

// probe issues a GET with a per-probe timeout, attaching the managed token when
// present. A transport error is returned verbatim so the caller can tell DNS
// from connectivity; any HTTP status (even 4xx) counts as "reachable".
func probe(ctx context.Context, opts Options, rawURL, token string) probeResult {
	pctx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(pctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return probeResult{err: err}
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "livck-agent/"+valueOr(opts.AgentVersion, "0.0.0")+" (doctor)")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := opts.HTTP.Do(req)
	if err != nil {
		return probeResult{err: err}
	}
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	return probeResult{status: resp.StatusCode, date: resp.Header.Get("Date"), body: body}
}

// classifyTransport turns a transport error into a hard-failure check, telling
// a name-resolution failure (nothing to connect to) apart from a connect/timeout
// failure (resolves, but egress is blocked or the host is down).
func classifyTransport(r *Report, name, host string, err error) {
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		r.addHint(name, StatusFail, "DNS resolution failed for "+host,
			"check the host's DNS resolver and that "+host+" is spelled correctly")
		return
	}
	r.addHint(name, StatusFail, "could not connect to "+host+": "+trimErr(err),
		"check outbound HTTPS (443) egress and any firewall/proxy between this server and "+host)
}

// checkSkew compares the server Date header to the local clock. Skew is a
// warning (exit 1): moderate drift is informational, but a drift past the
// hard limit means batches are actively rejected until NTP is fixed.
func checkSkew(r *Report, opts Options, date string) {
	if strings.TrimSpace(date) == "" {
		r.add("clock skew", StatusSkip, "server sent no Date header")
		return
	}
	serverTime, err := http.ParseTime(date)
	if err != nil {
		r.add("clock skew", StatusSkip, "could not parse server time")
		return
	}
	skew := opts.Platform.Clock.Now().Sub(serverTime)
	abs := skew
	if abs < 0 {
		abs = -abs
	}
	switch {
	case abs <= 10*time.Second:
		r.add("clock skew", StatusOK, "clock in sync with the server (±"+abs.Round(time.Second).String()+")")
	case abs <= clockSkewHardLimit:
		r.addHint("clock skew", StatusWarn, "clock drifts from the server by "+abs.Round(time.Second).String(),
			"keep NTP running (timedatectl set-ntp true) — drift over 5 min gets batches rejected")
	default:
		r.addHint("clock skew", StatusWarn, "clock is off by "+abs.Round(time.Second).String()+" — batches will be rejected (CLOCK_SKEW)",
			"fix the clock now: timedatectl set-ntp true, or sync NTP manually")
	}
}

// checkAuth interprets the /v1/me status: 200 authenticates and prints the
// token's org/service/limit; 401/403 is a hard failure (invalid/revoked token);
// no token means the box is not enrolled and the check is skipped.
func checkAuth(r *Report, res probeResult, token string) {
	if token == "" {
		r.add("authentication (/v1/me)", StatusSkip, "not enrolled — control plane answered HTTP "+strconv.Itoa(res.status))
		return
	}
	switch res.status {
	case http.StatusOK:
		r.add("authentication (/v1/me)", StatusOK, describeMe(res.body))
	case http.StatusUnauthorized, http.StatusForbidden:
		r.addHint("authentication (/v1/me)", StatusFail, "the managed token was rejected (HTTP "+strconv.Itoa(res.status)+")",
			"the token is invalid or revoked; re-enroll this server (livck-agent enroll --force)")
	default:
		r.add("authentication (/v1/me)", StatusWarn, "unexpected response from /v1/me (HTTP "+strconv.Itoa(res.status)+")")
	}
}

// checkConfig confirms the control plane serves this agent's config — the pull
// the run loop depends on. It runs only when enrolled.
func checkConfig(ctx context.Context, r *Report, opts Options, token string) {
	res := probe(ctx, opts, strings.TrimRight(opts.ControlBase, "/")+"/api/v1/agents/config", token)
	switch {
	case res.err != nil:
		r.add("agent config pull", StatusWarn, "config request failed: "+trimErr(res.err))
	case res.status == http.StatusOK:
		r.add("agent config pull", StatusOK, "control plane serves this agent's config")
	case res.status == http.StatusUnauthorized || res.status == http.StatusForbidden:
		r.addHint("agent config pull", StatusFail, "config rejected the token (HTTP "+strconv.Itoa(res.status)+")",
			"the token is invalid or revoked; re-enroll this server")
	default:
		r.add("agent config pull", StatusWarn, "unexpected config response (HTTP "+strconv.Itoa(res.status)+")")
	}
}

// meResponse is the subset of the /v1/me body doctor renders.
type meResponse struct {
	Type         string   `json:"type"`
	Permissions  []string `json:"permissions"`
	Organization struct {
		PublicID string `json:"public_id"`
		Name     string `json:"name"`
	} `json:"organization"`
	Service *struct {
		PublicID string `json:"public_id"`
		Name     string `json:"name"`
	} `json:"service"`
	RateLimit struct {
		RequestsPerMinute int `json:"requests_per_minute"`
	} `json:"rate_limit"`
}

// describeMe renders a compact one-line summary of the authenticated token.
func describeMe(body []byte) string {
	var me meResponse
	if err := json.Unmarshal(body, &me); err != nil || me.Organization.PublicID == "" {
		return "authenticated"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s token · org %q (%s)", valueOr(me.Type, "token"), me.Organization.Name, me.Organization.PublicID)
	if me.Service != nil {
		fmt.Fprintf(&b, " · service %q", me.Service.Name)
	}
	if me.RateLimit.RequestsPerMinute > 0 {
		fmt.Fprintf(&b, " · %d req/min", me.RateLimit.RequestsPerMinute)
	}
	return b.String()
}

// managedToken returns the stored managed token, or "" when the host is not
// enrolled.
func managedToken(opts Options) string {
	if opts.Store == nil || !opts.Store.HasToken() {
		return ""
	}
	t, err := opts.Store.Token()
	if err != nil {
		return ""
	}
	return t
}

// hostOf returns the hostname of a base URL for messages, falling back to the
// raw string when it does not parse.
func hostOf(base string) string {
	u, err := url.Parse(strings.TrimSpace(base))
	if err != nil || u.Host == "" {
		return strings.TrimSpace(base)
	}
	return u.Hostname()
}

func trimErr(err error) string {
	if err == nil {
		return ""
	}
	return strings.TrimSpace(err.Error())
}

func valueOr(v, fallback string) string {
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	return v
}
