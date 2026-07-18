package config

import (
	"context"
	"net/http"
	"net/http/httptest"
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
