package config

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

// maxConfigBody caps the config response the agent will read. The document is a
// few kilobytes; this is a defensive ceiling against a misbehaving endpoint.
const maxConfigBody = 256 * 1024

// forcedFamilyTimeout bounds a single dial over one address family. A host with
// no route for that family should fail fast and fall back, not stall the poll.
const forcedFamilyTimeout = 10 * time.Second

// HTTPFetcher pulls the config from GET /api/v1/agents/config with ETag /
// If-None-Match, so an unchanged config costs a 304 with no body. Auth is the
// managed token, read fresh on each call so rotation is picked up.
//
// Successive polls alternate between IPv4 and IPv6. The control plane records the
// address it sees us call from, so alternating is what lets it learn BOTH of our
// egress addresses. It can only ever observe the family a given request arrived
// over. Nothing about the config itself depends on the family.
//
// Reliability comes first: if the forced family cannot connect (a single-stack
// host, which is the common case), the call is retried on the default dialler.
// Fetching config is essential, learning an address is a bonus, and one must
// never cost the other.
type HTTPFetcher struct {
	client   *http.Client
	v4Client *http.Client
	v6Client *http.Client
	url      string
	tokenFn  func() string
	// polls counts calls so families alternate; atomic because the manager loop and
	// a trigger-driven refresh can both land here.
	polls atomic.Uint64
}

// NewHTTPFetcher builds a fetcher for the given config URL and token accessor.
func NewHTTPFetcher(client *http.Client, url string, tokenFn func() string) *HTTPFetcher {
	if client == nil {
		client = http.DefaultClient
	}
	return &HTTPFetcher{
		client:   client,
		v4Client: forceFamily(client, "tcp4"),
		v6Client: forceFamily(client, "tcp6"),
		url:      url,
		tokenFn:  tokenFn,
	}
}

// forceFamily clones a client so every dial uses exactly one address family.
// Everything else about the transport is inherited; only the dial is pinned.
func forceFamily(base *http.Client, network string) *http.Client {
	transport, ok := base.Transport.(*http.Transport)
	if !ok || transport == nil {
		transport, ok = http.DefaultTransport.(*http.Transport)
		if !ok {
			return base
		}
	}

	forced := transport.Clone()
	dialer := &net.Dialer{Timeout: forcedFamilyTimeout}
	forced.DialContext = func(ctx context.Context, _, addr string) (net.Conn, error) {
		return dialer.DialContext(ctx, network, addr)
	}

	clone := *base
	clone.Transport = forced

	return &clone
}

// Fetch performs the conditional GET. A 304 returns notModified true. A 200
// returns the body and the new ETag. Any other status is an error.
func (f *HTTPFetcher) Fetch(ctx context.Context, etag string) (raw []byte, newETag string, notModified bool, err error) {
	client := f.nextClient()

	resp, err := f.do(ctx, client, etag)
	if err != nil && client != f.client {
		// The pinned family has no route from this host. Not worth logging: a
		// single-stack machine hits this on every other poll by design.
		resp, err = f.do(ctx, f.client, etag)
	}
	if err != nil {
		return nil, "", false, err
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusNotModified:
		return nil, etag, true, nil
	case http.StatusOK:
		body, err := io.ReadAll(io.LimitReader(resp.Body, maxConfigBody))
		if err != nil {
			return nil, "", false, fmt.Errorf("read config body: %w", err)
		}
		return body, resp.Header.Get("ETag"), false, nil
	default:
		return nil, "", false, fmt.Errorf("config endpoint returned %d", resp.StatusCode)
	}
}

// nextClient alternates v4 / v6 across calls.
func (f *HTTPFetcher) nextClient() *http.Client {
	if f.v4Client == nil || f.v6Client == nil {
		return f.client
	}
	if f.polls.Add(1)%2 == 1 {
		return f.v4Client
	}
	return f.v6Client
}

func (f *HTTPFetcher) do(ctx context.Context, client *http.Client, etag string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+f.tokenFn())
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}

	return client.Do(req)
}
