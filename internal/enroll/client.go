// This file holds the client side of the enroll handshake: the single,
// client-initiated POST that registers a fresh server with the LIVCK brain and
// yields the managed token, ingest URL and initial config the agent then reports
// with. The agent is outbound-only, so this is the whole verb: no command
// channel, no server push. Do is exported so other tools (loadgen, e2e tests)
// reuse the exact same client instead of reimplementing the contract.
//
// Bootstrap inputs are deliberately just two: an enrollment token and the enroll
// URL. Everything else the agent needs (managed token, ingest URL, config) is
// LEARNED from the response and persisted, so a template image carries no
// service-specific secret.
package enroll

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"

	"github.com/LIVCK/agent/internal/config"
)

// enrollPath is appended to the enroll base URL. The enroll endpoint lives on
// the Laravel app (app.livck.cloud), NOT on the ingest host - the ingest URL is
// returned in the response.
const enrollPath = "/api/v1/agents/enroll"

// maxResponseBytes caps the response body we read. An enroll response is a few
// KB of config; this only guards against a misbehaving endpoint.
const maxResponseBytes = 1 << 20

// ErrAlreadyEnrolled is returned by Do when a managed token already exists and
// Force is false. Re-running enroll must never mint a second service (a clone),
// so the safe default is to refuse and tell the operator to pass --force.
var ErrAlreadyEnrolled = errors.New("enroll: this server already has a managed token (use --force to re-enroll)")

// APIError is a typed enroll failure carrying the frozen error code from the
// canonical agent envelope. The installer branches on Status:
// a 4xx is FINAL (abort legibly), a 5xx/429 is RETRYABLE.
type APIError struct {
	Status     int
	Code       string
	Message    string
	Limit      *int   // AGENT_LIMIT_REACHED context
	Used       *int   // AGENT_LIMIT_REACHED context
	UpgradeURL string // AGENT_LIMIT_REACHED context
}

func (e *APIError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("enroll: %s (%s, HTTP %d)", e.Message, e.Code, e.Status)
	}
	return fmt.Sprintf("enroll: unexpected HTTP %d", e.Status)
}

// Retryable reports whether the installer may retry: only 5xx and 429 are
// transient, every 4xx is final.
func (e *APIError) Retryable() bool { return e.Status >= 500 || e.Status == 429 }

// Doer is the subset of *http.Client the enroll call needs, so tests inject a
// fake transport.
type Doer interface {
	Do(*http.Request) (*http.Response, error)
}

// Options configures a single enroll attempt.
type Options struct {
	Store        *Store            // identity + secret persistence (required)
	HTTP         Doer              // HTTP client (required)
	BaseURL      string            // enroll endpoint base, e.g. https://app.livck.cloud (required)
	Token        string            // bootstrap enrollment token lve_... (or a managed lvk_... for re-enroll) (required)
	Force        bool              // re-enroll even if a managed token already exists
	Name         string            // optional operator-supplied service name; empty -> server uses hostname
	Tags         []string          // optional request tags, e.g. env:prod
	AgentVersion string            // reported agent version
	Meta         map[string]string // host profile (hostinfo.Meta); hostname is read from here
	Fingerprint  map[string]string // clone-detection fingerprint (hostinfo.Fingerprint)
	PrivateIPs   []string          // enroll-only private IPs -> meta.ips_private
	PublicIPs    []string          // enroll-only public IPs -> meta.ips_public (best-effort, may be empty)
}

// Result is the outcome of a successful enroll, for the CLI to report.
type Result struct {
	ServicePublicID string
	ServiceName     string
	IngestURL       string
	ConfigVersion   int
	Bootstrap       bool
	AlreadyEnrolled bool
	TokenIssued     bool // a fresh managed token was persisted (fresh enroll only)
}

// Do runs the enroll handshake: load/mint identity, POST the request, then
// atomically persist the token, config and ingest URL from the response. Nothing
// is persisted unless the whole response parses (no half-write). It is safe to
// retry: the reused enrollment_id dedupes server-side to one service.
func Do(ctx context.Context, opts Options) (Result, error) {
	if opts.Store == nil {
		return Result{}, errors.New("enroll: store is required")
	}
	if opts.HTTP == nil {
		return Result{}, errors.New("enroll: http client is required")
	}
	token := strings.TrimSpace(opts.Token)
	if token == "" {
		return Result{}, errors.New("enroll: a token is required")
	}
	base := strings.TrimRight(strings.TrimSpace(opts.BaseURL), "/")
	if base == "" {
		return Result{}, errors.New("enroll: an enroll URL is required")
	}

	// Idempotency guard: an existing managed token means this host is enrolled.
	// Without --force, refuse rather than risk a duplicate service (clone).
	if opts.Store.HasToken() && !opts.Force {
		return Result{}, ErrAlreadyEnrolled
	}

	// Identity is never regenerated silently: a corrupt instance_id aborts here
	// (only the reset verb wipes it).
	instanceID, err := opts.Store.LoadOrCreateInstanceID()
	if err != nil {
		return Result{}, err
	}
	hostname := firstNonEmpty(opts.Meta["hostname"], opts.Fingerprint["hostname"])
	if hostname == "" {
		return Result{}, errors.New("enroll: could not determine hostname")
	}
	// The enrollment_id is the server-side idempotency anchor: minted once and
	// reused on every retry so a retried enroll dedupes to one service.
	enrollmentID, err := opts.Store.LoadOrCreateEnrollmentID()
	if err != nil {
		return Result{}, err
	}

	body, err := json.Marshal(buildRequest(opts, instanceID, enrollmentID, hostname))
	if err != nil {
		return Result{}, fmt.Errorf("enroll: encode request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+enrollPath, bytes.NewReader(body))
	if err != nil {
		return Result{}, fmt.Errorf("enroll: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "livck-agent/"+valueOr(opts.AgentVersion, "0.0.0"))

	resp, err := opts.HTTP.Do(req)
	if err != nil {
		return Result{}, fmt.Errorf("enroll: transport: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return Result{}, fmt.Errorf("enroll: read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return Result{}, parseError(resp.StatusCode, raw)
	}

	return applySuccess(opts.Store, raw)
}

// enrollRequest is the enroll request body. Field names and shapes are the
// frozen contract matched against EnrollAgentRequest.
type enrollRequest struct {
	EnrollmentID string            `json:"enrollment_id"`
	InstanceID   string            `json:"instance_id"`
	Hostname     string            `json:"hostname"`
	Name         *string           `json:"name"`
	Tags         []string          `json:"tags"`
	Fingerprint  map[string]string `json:"fingerprint"`
	Meta         map[string]any    `json:"meta"`
	AgentVersion string            `json:"agent_version"`
}

func buildRequest(opts Options, instanceID, enrollmentID, hostname string) enrollRequest {
	var name *string
	if n := strings.TrimSpace(opts.Name); n != "" {
		name = &n
	}
	tags := opts.Tags
	if tags == nil {
		tags = []string{}
	}
	fp := opts.Fingerprint
	if fp == nil {
		fp = map[string]string{}
	}
	// meta is a mixed map because ips_* are arrays (enroll-only); the rest are
	// the flat string host profile.
	meta := make(map[string]any, len(opts.Meta)+2)
	for k, v := range opts.Meta {
		meta[k] = v
	}
	if len(opts.PrivateIPs) > 0 {
		meta["ips_private"] = opts.PrivateIPs
	}
	if len(opts.PublicIPs) > 0 {
		meta["ips_public"] = opts.PublicIPs
	}
	return enrollRequest{
		EnrollmentID: enrollmentID,
		InstanceID:   instanceID,
		Hostname:     hostname,
		Name:         name,
		Tags:         tags,
		Fingerprint:  fp,
		Meta:         meta,
		AgentVersion: opts.AgentVersion,
	}
}

// enrollResponse is the enroll success body. token is a pointer: it is
// present ONLY on a fresh enroll (201) and absent on an idempotent re-enroll
// (200), where it is never re-displayed.
type enrollResponse struct {
	Service struct {
		PublicID string `json:"public_id"`
		Name     string `json:"name"`
	} `json:"service"`
	Token           *string         `json:"token"`
	Config          json.RawMessage `json:"config"`
	ConfigVersion   int             `json:"config_version"`
	IngestURL       string          `json:"ingest_url"`
	Bootstrap       bool            `json:"bootstrap"`
	AlreadyEnrolled bool            `json:"already_enrolled"`
}

// applySuccess parses a 200/201 body, validates every field it will persist, and
// only then writes to disk. It writes the token first (the one irreplaceable
// secret, shown once) so a later write failure never leaves the token unsaved.
func applySuccess(store *Store, raw []byte) (Result, error) {
	var resp enrollResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return Result{}, fmt.Errorf("enroll: malformed success response: %w", err)
	}
	if resp.Service.PublicID == "" {
		return Result{}, errors.New("enroll: success response missing service.public_id")
	}
	ingest := strings.TrimRight(strings.TrimSpace(resp.IngestURL), "/")
	if ingest == "" {
		return Result{}, errors.New("enroll: success response missing ingest_url")
	}
	if len(resp.Config) == 0 {
		return Result{}, errors.New("enroll: success response missing config")
	}
	// A fatal config error means the server sent garbage; abort before any write.
	// Field-level clamps are tolerated (the run loop's manager re-validates).
	if _, _, err := config.Validate(resp.Config); err != nil {
		return Result{}, fmt.Errorf("enroll: server config invalid: %w", err)
	}

	fresh := !resp.AlreadyEnrolled
	tokenPresent := resp.Token != nil && strings.TrimSpace(*resp.Token) != ""
	if fresh && !tokenPresent {
		return Result{}, errors.New("enroll: fresh enroll response missing token")
	}

	if tokenPresent {
		if err := store.SetToken(*resp.Token); err != nil {
			return Result{}, fmt.Errorf("enroll: persist token: %w", err)
		}
	}
	if err := store.SetConfig(resp.Config); err != nil {
		return Result{}, fmt.Errorf("enroll: persist config: %w", err)
	}
	if err := store.SetIngestURL(ingest); err != nil {
		return Result{}, fmt.Errorf("enroll: persist ingest url: %w", err)
	}

	return Result{
		ServicePublicID: resp.Service.PublicID,
		ServiceName:     resp.Service.Name,
		IngestURL:       ingest,
		ConfigVersion:   resp.ConfigVersion,
		Bootstrap:       resp.Bootstrap,
		AlreadyEnrolled: resp.AlreadyEnrolled,
		TokenIssued:     tokenPresent,
	}, nil
}

// errorEnvelope mirrors the canonical agent error shape {"error":{code,message,...}}.
type errorEnvelope struct {
	Error struct {
		Code       string `json:"code"`
		Message    string `json:"message"`
		Limit      *int   `json:"limit"`
		Used       *int   `json:"used"`
		UpgradeURL string `json:"upgrade_url"`
	} `json:"error"`
}

func parseError(status int, raw []byte) *APIError {
	e := &APIError{Status: status}
	var env errorEnvelope
	if err := json.Unmarshal(raw, &env); err == nil && env.Error.Code != "" {
		e.Code = env.Error.Code
		e.Message = env.Error.Message
		e.Limit = env.Error.Limit
		e.Used = env.Error.Used
		e.UpgradeURL = env.Error.UpgradeURL
	}
	return e
}

// LocalPrivateIPs enumerates the host's private unicast IPs (RFC-1918 / ULA) for
// the enroll-only meta.ips_private field. It is best-effort: an enumeration
// error yields nil and never fails an enroll. Public IP discovery is deferred
// (like cloud_instance_id in the fingerprint) and not implemented yet.
func LocalPrivateIPs() []string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil
	}
	var out []string
	for _, a := range addrs {
		var ip net.IP
		switch v := a.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}
		if ip == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
			continue
		}
		if ip.IsPrivate() {
			out = append(out, ip.String())
		}
	}
	return out
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if s := strings.TrimSpace(v); s != "" {
			return s
		}
	}
	return ""
}

func valueOr(v, fallback string) string {
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	return v
}
