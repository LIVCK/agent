package config

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestHTTPFetcher200AndETag(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer lvk_x" {
			t.Errorf("missing bearer auth, got %q", r.Header.Get("Authorization"))
		}
		w.Header().Set("ETag", `"v3"`)
		_, _ = w.Write([]byte(`{"config_version":3}`))
	}))
	defer srv.Close()

	f := NewHTTPFetcher(srv.Client(), srv.URL, func() string { return "lvk_x" })
	raw, etag, notModified, err := f.Fetch(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if notModified || etag != `"v3"` || len(raw) == 0 {
		t.Fatalf("unexpected fetch result: etag=%q notModified=%v", etag, notModified)
	}
}

func TestHTTPFetcher304(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") != `"v3"` {
			t.Errorf("expected If-None-Match, got %q", r.Header.Get("If-None-Match"))
		}
		w.WriteHeader(http.StatusNotModified)
	}))
	defer srv.Close()

	f := NewHTTPFetcher(srv.Client(), srv.URL, func() string { return "t" })
	_, etag, notModified, err := f.Fetch(context.Background(), `"v3"`)
	if err != nil {
		t.Fatal(err)
	}
	if !notModified || etag != `"v3"` {
		t.Fatalf("expected 304 no-op keeping etag, got notModified=%v etag=%q", notModified, etag)
	}
}

func TestHTTPFetcherErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	f := NewHTTPFetcher(srv.Client(), srv.URL, func() string { return "t" })
	if _, _, _, err := f.Fetch(context.Background(), ""); err == nil {
		t.Fatal("expected an error on 500")
	}
}

// The control plane can only observe the address family a request actually arrived
// over, so successive polls have to alternate for it to learn both. Proven against
// REAL sockets. A weaker assertion ("the family is v4 or v6") would pass even if
// every poll went out over the same one.
func TestHTTPFetcherAlternatesAddressFamily(t *testing.T) {
	var mu sync.Mutex
	var families []string

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			t.Errorf("remote addr %q: %v", r.RemoteAddr, err)
		}
		family := "v6"
		if ip := net.ParseIP(host); ip != nil && ip.To4() != nil {
			family = "v4"
		}
		mu.Lock()
		families = append(families, family)
		mu.Unlock()
		w.Header().Set("ETag", `"v1"`)
		_, _ = w.Write([]byte(`{"config_version":1}`))
	})

	// One hostname resolving to BOTH families on the same port, the way
	// app.livck.cloud does. Without a dual-stack listener the test cannot tell
	// alternation from fallback, so it skips rather than pretending.
	ln6, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skip("no IPv6 loopback on this machine")
	}
	defer func() { _ = ln6.Close() }()

	port := ln6.Addr().(*net.TCPAddr).Port
	ln4, err := net.Listen("tcp4", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Skipf("port %d not free on IPv4: %v", port, err)
	}
	defer func() { _ = ln4.Close() }()

	srv := &http.Server{Handler: handler}
	go func() { _ = srv.Serve(ln6) }()
	go func() { _ = srv.Serve(ln4) }()
	defer func() { _ = srv.Close() }()

	f := NewHTTPFetcher(&http.Client{}, fmt.Sprintf("http://localhost:%d/", port), func() string { return "t" })

	for i := 0; i < 4; i++ {
		if _, _, _, err := f.Fetch(context.Background(), ""); err != nil {
			t.Fatalf("poll %d: %v", i, err)
		}
	}

	mu.Lock()
	defer mu.Unlock()

	want := []string{"v4", "v6", "v4", "v6"}
	if len(families) != len(want) {
		t.Fatalf("expected %d polls, got %v", len(want), families)
	}
	for i, family := range families {
		if family != want[i] {
			t.Fatalf("poll %d arrived over %s, want %s (all: %v)", i+1, family, want[i], families)
		}
	}
}

// A single-stack host is the common case: every other poll targets a family it
// has no route for. Config must still arrive, without an error surfacing.
func TestHTTPFetcherFallsBackWhenFamilyUnavailable(t *testing.T) {
	var hits int

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.Header().Set("ETag", `"v7"`)
		_, _ = w.Write([]byte(`{"config_version":7}`))
	}))
	defer srv.Close()

	f := NewHTTPFetcher(srv.Client(), srv.URL, func() string { return "t" })

	// Ten polls across both families against an IPv4-only listener.
	for i := 0; i < 10; i++ {
		raw, etag, notModified, err := f.Fetch(context.Background(), "")
		if err != nil {
			t.Fatalf("poll %d failed: %v", i, err)
		}
		if notModified || etag != `"v7"` || len(raw) == 0 {
			t.Fatalf("poll %d returned nothing useful", i)
		}
	}

	if hits != 10 {
		t.Fatalf("expected every poll to reach the server, got %d", hits)
	}
}

// A real HTTP error must not be masked by the fallback retry.
func TestHTTPFetcherStillReportsServerErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	f := NewHTTPFetcher(srv.Client(), srv.URL, func() string { return "t" })

	if _, _, _, err := f.Fetch(context.Background(), ""); err == nil {
		t.Fatal("expected an error for a 500 response")
	}
}

// The token is read per call so a rotation mid-life is picked up, so the forced
// clients must not have captured a stale one.
func TestHTTPFetcherReadsTokenPerCallAcrossFamilies(t *testing.T) {
	var seen []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Header.Get("Authorization"))
		w.Header().Set("ETag", `"v1"`)
		_, _ = w.Write([]byte(`{"config_version":1}`))
	}))
	defer srv.Close()

	token := "lvk_first"
	f := NewHTTPFetcher(srv.Client(), srv.URL, func() string { return token })

	if _, _, _, err := f.Fetch(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	token = "lvk_rotated"
	if _, _, _, err := f.Fetch(context.Background(), ""); err != nil {
		t.Fatal(err)
	}

	if len(seen) != 2 || seen[0] != "Bearer lvk_first" || seen[1] != "Bearer lvk_rotated" {
		t.Fatalf("token not read per call: %v", seen)
	}
}
