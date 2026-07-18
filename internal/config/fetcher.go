package config

import (
	"context"
	"fmt"
	"io"
	"net/http"
)

// maxConfigBody caps the config response the agent will read. The document is a
// few kilobytes; this is a defensive ceiling against a misbehaving endpoint.
const maxConfigBody = 256 * 1024

// HTTPFetcher pulls the config from GET /api/v1/agents/config with ETag /
// If-None-Match, so an unchanged config costs a 304 with no body. Auth is the
// managed token, read fresh on each call so rotation is picked up.
type HTTPFetcher struct {
	client  *http.Client
	url     string
	tokenFn func() string
}

// NewHTTPFetcher builds a fetcher for the given config URL and token accessor.
func NewHTTPFetcher(client *http.Client, url string, tokenFn func() string) *HTTPFetcher {
	if client == nil {
		client = http.DefaultClient
	}
	return &HTTPFetcher{client: client, url: url, tokenFn: tokenFn}
}

// Fetch performs the conditional GET. A 304 returns notModified true. A 200
// returns the body and the new ETag. Any other status is an error.
func (f *HTTPFetcher) Fetch(ctx context.Context, etag string) (raw []byte, newETag string, notModified bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.url, nil)
	if err != nil {
		return nil, "", false, err
	}
	req.Header.Set("Authorization", "Bearer "+f.tokenFn())
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}

	resp, err := f.client.Do(req)
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
