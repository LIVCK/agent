package enroll

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// uuidB is the second id the shared newStore() idgen hands out: instance_id
// takes uuidA, enrollment_id takes uuidB.
const uuidB = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"

// validConfig is the smallest config document that passes config.Validate
// without a fatal error (config_version >= 1).
const validConfig = `{"config_version":3,"interval_seconds":60}`

func baseOpts(store *Store, srv *httptest.Server) Options {
	return Options{
		Store:        store,
		HTTP:         srv.Client(),
		BaseURL:      srv.URL,
		Token:        "lve_bootstraptoken",
		AgentVersion: "1.2.3",
		Meta:         map[string]string{"hostname": "api-prod-1", "os": "linux", "arch": "amd64"},
		Fingerprint:  map[string]string{"machine_id_hash": "deadbeef", "boot_id": "boot-1", "hostname": "api-prod-1"},
		PrivateIPs:   []string{"10.0.0.5"},
	}
}

func TestEnrollHappyPath(t *testing.T) {
	store, fs := newStore()

	var gotBody map[string]any
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != enrollPath {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"service":{"public_id":"svc_abc","name":"api-prod-1"},` +
			`"token":"lvk_managedsecret","config":` + validConfig + `,` +
			`"config_version":3,"ingest_url":"http://ingest.local/","bootstrap":true}`))
	}))
	defer srv.Close()

	res, err := Do(context.Background(), baseOpts(store, srv))
	if err != nil {
		t.Fatalf("Do: %v", err)
	}

	if res.ServicePublicID != "svc_abc" || res.ServiceName != "api-prod-1" {
		t.Fatalf("service = %+v", res)
	}
	if res.IngestURL != "http://ingest.local" { // trailing slash trimmed
		t.Fatalf("ingest url = %q", res.IngestURL)
	}
	if !res.Bootstrap || res.AlreadyEnrolled || !res.TokenIssued {
		t.Fatalf("flags = %+v", res)
	}

	// Request matched the expected enroll shape.
	if gotAuth != "Bearer lve_bootstraptoken" {
		t.Fatalf("auth header = %q", gotAuth)
	}
	if gotBody["instance_id"] != uuidA {
		t.Fatalf("instance_id = %v", gotBody["instance_id"])
	}
	if gotBody["enrollment_id"] != uuidB {
		t.Fatalf("enrollment_id = %v", gotBody["enrollment_id"])
	}
	if gotBody["hostname"] != "api-prod-1" {
		t.Fatalf("hostname = %v", gotBody["hostname"])
	}
	meta, _ := gotBody["meta"].(map[string]any)
	if meta["os"] != "linux" {
		t.Fatalf("meta.os = %v", meta["os"])
	}
	ips, _ := meta["ips_private"].([]any)
	if len(ips) != 1 || ips[0] != "10.0.0.5" {
		t.Fatalf("meta.ips_private = %v", meta["ips_private"])
	}
	fp, _ := gotBody["fingerprint"].(map[string]any)
	if fp["machine_id_hash"] != "deadbeef" {
		t.Fatalf("fingerprint = %v", gotBody["fingerprint"])
	}

	// Token, config and ingest url persisted 0600.
	if tok, err := store.Token(); err != nil || tok != "lvk_managedsecret" {
		t.Fatalf("token = %q err=%v", tok, err)
	}
	if u, err := store.IngestURL(); err != nil || u != "http://ingest.local" {
		t.Fatalf("ingest url = %q err=%v", u, err)
	}
	if raw, err := fs.ReadFile("/state/" + ConfigFile); err != nil || !strings.Contains(string(raw), `"config_version":3`) {
		t.Fatalf("config = %q err=%v", string(raw), err)
	}
	for _, name := range []string{tokenFile, ConfigFile, ingestURLFile} {
		if mode, ok := fs.Perm("/state/" + name); !ok || mode != os.FileMode(0o600) {
			t.Fatalf("%s perm = %v (ok=%v)", name, mode, ok)
		}
	}
}

func TestEnrollConsumedTokenIsFinal(t *testing.T) {
	store, fs := newStore()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":{"code":"ENROLLMENT_TOKEN_MAX_USES","message":"used up"}}`))
	}))
	defer srv.Close()

	_, err := Do(context.Background(), baseOpts(store, srv))
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("want APIError, got %v", err)
	}
	if apiErr.Status != 403 || apiErr.Code != "ENROLLMENT_TOKEN_MAX_USES" || apiErr.Retryable() {
		t.Fatalf("apiErr = %+v", apiErr)
	}
	// A failed enroll persists no secret and no config/ingest url.
	if store.HasToken() {
		t.Fatal("token must not be persisted on a failed enroll")
	}
	if _, ok := fs.Perm("/state/" + ConfigFile); ok {
		t.Fatal("config must not be persisted on a failed enroll")
	}
	if _, ok := fs.Perm("/state/" + ingestURLFile); ok {
		t.Fatal("ingest url must not be persisted on a failed enroll")
	}
}

func TestEnrollAgentLimitCarriesContext(t *testing.T) {
	store, _ := newStore()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(`{"error":{"code":"AGENT_LIMIT_REACHED","message":"limit","limit":2,"used":2,"upgrade_url":"https://x/upgrade"}}`))
	}))
	defer srv.Close()

	_, err := Do(context.Background(), baseOpts(store, srv))
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("want APIError, got %v", err)
	}
	if apiErr.Status != 422 || apiErr.Retryable() {
		t.Fatalf("apiErr = %+v", apiErr)
	}
	if apiErr.Limit == nil || *apiErr.Limit != 2 || apiErr.Used == nil || *apiErr.Used != 2 {
		t.Fatalf("limit context = %+v", apiErr)
	}
	if apiErr.UpgradeURL != "https://x/upgrade" {
		t.Fatalf("upgrade url = %q", apiErr.UpgradeURL)
	}
}

func TestEnrollAlreadyEnrolledGuard(t *testing.T) {
	store, _ := newStore()
	if err := store.SetToken("lvk_existing"); err != nil {
		t.Fatal(err)
	}

	hit := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit = true
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	_, err := Do(context.Background(), baseOpts(store, srv))
	if !errors.Is(err, ErrAlreadyEnrolled) {
		t.Fatalf("want ErrAlreadyEnrolled, got %v", err)
	}
	if hit {
		t.Fatal("guard must abort before hitting the server")
	}
	if tok, _ := store.Token(); tok != "lvk_existing" {
		t.Fatalf("token must be untouched, got %q", tok)
	}
}

func TestEnrollForceReEnrollKeepsToken(t *testing.T) {
	store, fs := newStore()
	if err := store.SetToken("lvk_existing"); err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Idempotent re-enroll: 200, no token, config refresh only.
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"service":{"public_id":"svc_abc","name":"api-prod-1"},` +
			`"config":` + validConfig + `,"config_version":4,` +
			`"ingest_url":"http://ingest.local","bootstrap":false,"already_enrolled":true}`))
	}))
	defer srv.Close()

	opts := baseOpts(store, srv)
	opts.Force = true
	res, err := Do(context.Background(), opts)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if !res.AlreadyEnrolled || res.TokenIssued || res.Bootstrap {
		t.Fatalf("flags = %+v", res)
	}
	// Existing token is preserved (200 never re-issues one); config was refreshed.
	if tok, _ := store.Token(); tok != "lvk_existing" {
		t.Fatalf("token changed on re-enroll: %q", tok)
	}
	if raw, err := fs.ReadFile("/state/" + ConfigFile); err != nil || !strings.Contains(string(raw), `"config_version":3`) {
		t.Fatalf("config not refreshed: %q err=%v", string(raw), err)
	}
}

func TestEnrollCorruptResponseNoHalfWrite(t *testing.T) {
	store, fs := newStore()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"service":{"public_id":"svc_abc"`)) // truncated JSON
	}))
	defer srv.Close()

	_, err := Do(context.Background(), baseOpts(store, srv))
	if err == nil {
		t.Fatal("want error on corrupt response")
	}
	if store.HasToken() {
		t.Fatal("no token may be persisted from a corrupt response")
	}
	if _, ok := fs.Perm("/state/" + ConfigFile); ok {
		t.Fatal("no config may be persisted from a corrupt response")
	}
	if _, ok := fs.Perm("/state/" + ingestURLFile); ok {
		t.Fatal("no ingest url may be persisted from a corrupt response")
	}
}

func TestEnrollFatalConfigNotPersisted(t *testing.T) {
	store, fs := newStore()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Valid envelope, but config_version 0 is a fatal config error.
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"service":{"public_id":"svc_abc","name":"h"},` +
			`"token":"lvk_managedsecret","config":{"config_version":0},` +
			`"config_version":0,"ingest_url":"http://ingest.local","bootstrap":true}`))
	}))
	defer srv.Close()

	_, err := Do(context.Background(), baseOpts(store, srv))
	if err == nil {
		t.Fatal("want error on fatally invalid config")
	}
	// The token write must NOT happen when the config is rejected (validate-first).
	if store.HasToken() {
		t.Fatal("token must not be persisted when config is rejected")
	}
	if _, ok := fs.Perm("/state/" + ConfigFile); ok {
		t.Fatal("config must not be persisted when rejected")
	}
}

func TestEnrollFreshResponseMissingTokenRejected(t *testing.T) {
	store, _ := newStore()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 201 fresh enroll but no token: a broken response, not persisted.
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"service":{"public_id":"svc_abc","name":"h"},` +
			`"config":` + validConfig + `,"config_version":3,` +
			`"ingest_url":"http://ingest.local","bootstrap":true}`))
	}))
	defer srv.Close()

	_, err := Do(context.Background(), baseOpts(store, srv))
	if err == nil {
		t.Fatal("want error when a fresh 201 omits the token")
	}
	if store.HasToken() {
		t.Fatal("token must not be persisted")
	}
}

func TestEnrollServerErrorIsRetryable(t *testing.T) {
	store, _ := newStore()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":{"code":"RETRY_LATER","message":"try again"}}`))
	}))
	defer srv.Close()

	_, err := Do(context.Background(), baseOpts(store, srv))
	var apiErr *APIError
	if !errors.As(err, &apiErr) || !apiErr.Retryable() {
		t.Fatalf("want retryable APIError, got %v", err)
	}
}
