package update

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseSemverAcceptsAndRejects(t *testing.T) {
	valid := []string{"v0.1.0", "0.1.0", "v1.2.3-rc1", "10.20.30"}
	for _, v := range valid {
		if _, _, _, _, err := ParseSemver(v); err != nil {
			t.Errorf("valid %q rejected: %v", v, err)
		}
	}
	invalid := []string{"", "v1.2", "1.2.3.4", "latest", "v1.2.x", "1;rm -rf /", "v1.2.3-$(id)", "v1.2.3-" + strings.Repeat("a", 65)}
	for _, v := range invalid {
		if _, _, _, _, err := ParseSemver(v); err == nil {
			t.Errorf("invalid %q accepted", v)
		}
	}
}

func TestCompareSemver(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"v0.1.0", "v0.2.0", -1},
		{"v0.2.0", "v0.2.0", 0},
		{"v1.0.0", "v0.9.9", 1},
		{"v1.2.3-rc1", "v1.2.3", -1},
		{"v1.2.3", "v1.2.3-rc1", 1},
	}
	for _, c := range cases {
		got, err := CompareSemver(c.a, c.b)
		if err != nil || got != c.want {
			t.Errorf("Compare(%s,%s) = %d,%v want %d", c.a, c.b, got, err, c.want)
		}
	}
}

func TestInWindowWrapsMidnight(t *testing.T) {
	at := func(h, m int) time.Time { return time.Date(2026, 7, 23, h, m, 0, 0, time.Local) }

	// Plain window 00:00–04:00.
	if !InWindow(at(1, 30), "00:00", "04:00") {
		t.Error("01:30 must be inside 00:00–04:00")
	}
	if InWindow(at(5, 0), "00:00", "04:00") {
		t.Error("05:00 must be outside 00:00–04:00")
	}
	// Midnight wraparound 22:00–02:00.
	if !InWindow(at(23, 0), "22:00", "02:00") {
		t.Error("23:00 must be inside 22:00–02:00")
	}
	if !InWindow(at(1, 0), "22:00", "02:00") {
		t.Error("01:00 must be inside 22:00–02:00")
	}
	if InWindow(at(12, 0), "22:00", "02:00") {
		t.Error("12:00 must be outside 22:00–02:00")
	}
	// Malformed/equal windows never block.
	if !InWindow(at(12, 0), "bogus", "04:00") || !InWindow(at(12, 0), "02:00", "02:00") {
		t.Error("malformed/zero windows must not block")
	}
}

func TestSanitizeRedactsSecretsAndURLs(t *testing.T) {
	in := "download https://get.livck.cloud/x failed with token lvk_abc123 body"
	out := sanitize(in)
	if strings.Contains(out, "lvk_abc123") || strings.Contains(out, "https://") {
		t.Fatalf("sanitize leaked: %s", out)
	}
}

func TestLockIsExclusive(t *testing.T) {
	dir := t.TempDir()
	release, err := Lock(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Lock(dir); err != ErrLocked {
		t.Fatalf("second lock: want ErrLocked, got %v", err)
	}
	release()
	release2, err := Lock(dir)
	if err != nil {
		t.Fatalf("relock after release: %v", err)
	}
	release2()
}

func TestRecoverPreviousRestoresBackup(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "livck-agent")
	e := New(Options{BinaryPath: bin, StateDir: dir})

	// Crash case: live binary gone, .old present, stale .new lying around.
	if err := os.WriteFile(bin+oldSuffix, []byte("old-binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bin+newSuffix, []byte("half-download"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := e.recoverPrevious(); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(bin); err != nil || string(b) != "old-binary" {
		t.Fatalf("backup not restored: %v %q", err, b)
	}
	if _, err := os.Stat(bin + newSuffix); !os.IsNotExist(err) {
		t.Fatal("stale .new not cleaned")
	}
}

func TestDownloadEnforcesPinnedDigest(t *testing.T) {
	payload := []byte("agent-binary-bytes")
	sum := sha256.Sum256(payload)
	hexSum := hex.EncodeToString(sum[:])

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(payload)
	}))
	defer srv.Close()

	dir := t.TempDir()
	e := New(Options{BinaryPath: filepath.Join(dir, "bin"), StateDir: dir, HTTP: srv.Client()})

	dst := filepath.Join(dir, "dl")
	if _, err := e.download(context.Background(), srv.URL, dst, 0o644, hexSum); err != nil {
		t.Fatalf("matching digest rejected: %v", err)
	}
	if _, err := e.download(context.Background(), srv.URL, dst, 0o644, strings.Repeat("0", 64)); err == nil {
		t.Fatal("wrong digest accepted")
	}
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Fatal("failed download left the destination file behind")
	}
}

func TestDownloadFailsClosedWithoutChecksum(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".sha256") {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte("bytes"))
	}))
	defer srv.Close()

	dir := t.TempDir()
	e := New(Options{BinaryPath: filepath.Join(dir, "bin"), StateDir: dir, HTTP: srv.Client()})

	if _, err := e.download(context.Background(), srv.URL+"/artifact", filepath.Join(dir, "dl"), 0o644, ""); err == nil {
		t.Fatal("download without any integrity source must fail closed")
	}
}

func TestDownloadRefusesPlainHTTP(t *testing.T) {
	dir := t.TempDir()
	e := New(Options{BinaryPath: filepath.Join(dir, "bin"), StateDir: dir})
	if _, err := e.get(context.Background(), "http://evil.example/x"); err == nil {
		t.Fatal("plain http accepted")
	}
}

func TestParseShow(t *testing.T) {
	sub, n := parseShow("SubState=running\nNRestarts=3\n")
	if sub != "running" || n != 3 {
		t.Fatalf("parseShow = %q,%d", sub, n)
	}
}

func TestWriteResultIsReadable(t *testing.T) {
	dir := t.TempDir()
	e := New(Options{BinaryPath: filepath.Join(dir, "bin"), StateDir: dir, Now: func() time.Time { return time.Unix(1_700_000_000, 0) }})
	e.WriteResult(Result{From: "v0.1.0", To: "v0.2.0", Outcome: "ok"})

	b, err := os.ReadFile(filepath.Join(dir, ResultFile))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"outcome":"ok"`) {
		t.Fatalf("result content: %s", b)
	}
	info, _ := os.Stat(filepath.Join(dir, ResultFile))
	if info.Mode().Perm() != 0o644 {
		t.Fatalf("result perms = %v, want 0644", info.Mode().Perm())
	}
}

// --- End-to-end orchestration (real files, faked init system) ----------------

// fakeCmd scripts responses for systemctl/runuser/dpkg/rpm/restorecon by the
// first two argv tokens, and records the calls.
type fakeCmd struct {
	responses map[string]cmdReply // key: "name arg0 arg1" prefix match
	lookable  map[string]bool
	calls     []string
	// showSeq lets `systemctl show SubState,NRestarts` return a sequence across
	// successive health-poll iterations (index advances per call).
	showSeq []string
	showIdx int
	// valueSeq lets `systemctl show -p NRestarts --value` return a sequence (the
	// pre-upgrade baseline vs. the post-crash rollback re-baseline).
	valueSeq []string
	valueIdx int
}

type cmdReply struct {
	out string
	err error
}

func (f *fakeCmd) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	full := strings.TrimSpace(name + " " + strings.Join(args, " "))
	f.calls = append(f.calls, full)

	// Health poll (systemctl show -p SubState,NRestarts <svc>) returns a sequence.
	if strings.HasPrefix(full, "systemctl show") && strings.Contains(full, "SubState") && len(f.showSeq) > 0 {
		out := f.showSeq[min(f.showIdx, len(f.showSeq)-1)]
		f.showIdx++
		return []byte(out), nil
	}
	// NRestarts baseline (systemctl show -p NRestarts --value <svc>) — sequence.
	if strings.HasPrefix(full, "systemctl show -p NRestarts --value") && len(f.valueSeq) > 0 {
		out := f.valueSeq[min(f.valueIdx, len(f.valueSeq)-1)]
		f.valueIdx++
		return []byte(out), nil
	}
	for prefix, r := range f.responses {
		if strings.HasPrefix(full, prefix) {
			return []byte(r.out), r.err
		}
	}
	return nil, nil
}

func (f *fakeCmd) LookPath(name string) bool { return f.lookable[name] }

// stageBinary writes a fake "installed" binary and returns the engine + paths.
func e2eEngine(t *testing.T, srv *httptest.Server, cmd *fakeCmd, current string) (*Engine, string) {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "livck-agent")
	if err := os.WriteFile(bin, []byte("OLD-BINARY-"+current), 0o755); err != nil {
		t.Fatal(err)
	}
	e := New(Options{
		BinaryPath:     bin,
		ServiceName:    "livck-agent.service",
		StateDir:       dir,
		DownloadBase:   srv.URL,
		Arch:           "amd64",
		CurrentVersion: current,
		HTTP:           srv.Client(),
		Cmd:            cmd,
		HealthWindow:   30 * time.Millisecond,
		HealthPoll:     5 * time.Millisecond,
		Now:            func() time.Time { return time.Unix(1_700_000_000, 0) },
	})
	return e, bin
}

// binaryServer serves /binary/<ver>/livck-agent_linux_amd64 (+ .sha256) with the
// given payload, TLS.
func binaryServer(t *testing.T, payload []byte) *httptest.Server {
	t.Helper()
	sum := sha256.Sum256(payload)
	hexSum := hex.EncodeToString(sum[:])
	return httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".sha256") {
			_, _ = w.Write([]byte(hexSum + "  livck-agent_linux_amd64\n"))
			return
		}
		_, _ = w.Write(payload)
	}))
}

func TestUpgradeEndToEndSelfManagedSwap(t *testing.T) {
	newBytes := []byte("NEW-BINARY-v0.2.0")
	srv := binaryServer(t, newBytes)
	defer srv.Close()

	cmd := &fakeCmd{
		lookable: map[string]bool{"runuser": true}, // no dpkg/rpm → self-managed
		responses: map[string]cmdReply{
			"runuser":                             {out: "livck-agent v0.2.0"}, // smoke passes
			"systemctl restart":                   {out: ""},
			"systemctl show -p NRestarts --value": {out: "0"},
		},
		showSeq: []string{"SubState=running\nNRestarts=0"}, // healthy for the whole window
	}
	e, bin := e2eEngine(t, srv, cmd, "v0.1.0")

	res, err := e.Run(context.Background(), Request{Target: "v0.2.0"})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if res.Outcome != "ok" {
		t.Fatalf("outcome = %q, want ok (err=%q)", res.Outcome, res.Error)
	}

	// The live binary is now the NEW bytes, on the real path, executable.
	got, _ := os.ReadFile(bin)
	if string(got) != string(newBytes) {
		t.Fatalf("live binary = %q, want the new bytes", got)
	}
	// The previous binary was preserved as .old (rollback slot).
	old, err := os.ReadFile(bin + oldSuffix)
	if err != nil || !strings.HasPrefix(string(old), "OLD-BINARY") {
		t.Fatalf(".old backup missing/wrong: %v %q", err, old)
	}
	// A restart actually happened.
	assertCalled(t, cmd, "systemctl restart")
	// The result heartbeat was written.
	if _, err := os.Stat(filepath.Join(e.opt.StateDir, ResultFile)); err != nil {
		t.Fatal("update-result.json not written")
	}
}

func TestUpgradeEndToEndRollsBackOnCrashLoop(t *testing.T) {
	newBytes := []byte("NEW-BROKEN-v0.2.0")
	srv := binaryServer(t, newBytes)
	defer srv.Close()

	cmd := &fakeCmd{
		lookable: map[string]bool{"runuser": true},
		responses: map[string]cmdReply{
			"runuser":           {out: "livck-agent v0.2.0"}, // smoke passes (static ok)
			"systemctl restart": {out: ""},
		},
		// Baseline reads: 0 before the upgrade, then 2 when the rollback re-baselines
		// (the crash loop climbed the monotonic counter — the fix must re-capture it).
		valueSeq: []string{"0", "2"},
		// Upgrade health poll: crash (NRestarts 2 > baseline 0). Rollback health poll:
		// the old binary is STABLE at 2 — healthy ONLY against the re-captured baseline
		// of 2; against the stale pre-upgrade 0 it would falsely read as still crashing.
		showSeq: []string{
			"SubState=auto-restart\nNRestarts=2",
			"SubState=running\nNRestarts=2",
		},
	}
	e, bin := e2eEngine(t, srv, cmd, "v0.1.0")

	res, err := e.Run(context.Background(), Request{Target: "v0.2.0"})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if res.Outcome != "rolled_back" {
		t.Fatalf("outcome = %q, want rolled_back", res.Outcome)
	}

	// The live binary is the ORIGINAL again (rolled back from .old).
	got, _ := os.ReadFile(bin)
	if !strings.HasPrefix(string(got), "OLD-BINARY") {
		t.Fatalf("live binary not rolled back: %q", got)
	}
	// Two restarts: the upgrade attempt + the rollback.
	restarts := 0
	for _, c := range cmd.calls {
		if strings.HasPrefix(c, "systemctl restart") {
			restarts++
		}
	}
	if restarts < 2 {
		t.Fatalf("want restart on upgrade AND rollback, got %d", restarts)
	}
}

func TestUpgradeDigestMismatchNeverSwaps(t *testing.T) {
	srv := binaryServer(t, []byte("legit"))
	defer srv.Close()

	cmd := &fakeCmd{lookable: map[string]bool{"runuser": true}}
	e, bin := e2eEngine(t, srv, cmd, "v0.1.0")

	// A wrong pinned digest must abort before any swap.
	res, err := e.Run(context.Background(), Request{Target: "v0.2.0", Digest: strings.Repeat("0", 64)})
	if err == nil {
		t.Fatal("expected a hard error on digest mismatch")
	}
	if res.Outcome != "failed" {
		t.Fatalf("outcome = %q, want failed", res.Outcome)
	}
	got, _ := os.ReadFile(bin)
	if !strings.HasPrefix(string(got), "OLD-BINARY") {
		t.Fatal("binary was swapped despite a digest mismatch")
	}
	for _, c := range cmd.calls {
		if c == "systemctl restart" {
			t.Fatal("restarted despite a digest mismatch")
		}
	}
}

func TestUpgradeSkipsWhenAlreadyCurrent(t *testing.T) {
	srv := binaryServer(t, []byte("x"))
	defer srv.Close()
	cmd := &fakeCmd{
		lookable: map[string]bool{},
		responses: map[string]cmdReply{
			// Daemon runs the current binary → nothing to do.
			"systemctl show MainPID": {out: "0"},
		},
	}
	e, _ := e2eEngine(t, srv, cmd, "v0.2.0")

	res, err := e.Run(context.Background(), Request{Target: "v0.2.0"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome != "skipped" {
		t.Fatalf("outcome = %q, want skipped", res.Outcome)
	}
}

func assertCalled(t *testing.T, cmd *fakeCmd, wantPrefix string) {
	t.Helper()
	for _, c := range cmd.calls {
		if strings.HasPrefix(c, wantPrefix) {
			return
		}
	}
	t.Fatalf("expected a %q call; calls=%v", wantPrefix, cmd.calls)
}

// TestUpgradePackagePathUsesVersionlessAssetName locks the package asset naming
// to what the release workflow uploads + install.sh fetches (livck-agent_<arch>.deb,
// versionless). A regression here 404s every package-managed upgrade in production.
func TestUpgradePackagePathUsesVersionlessAssetName(t *testing.T) {
	newDeb := []byte("FAKE-DEB-v0.2.5")
	sum := sha256.Sum256(newDeb)
	hexSum := hex.EncodeToString(sum[:])

	var requested []string
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested = append(requested, r.URL.Path)
		_, _ = w.Write(newDeb)
	}))
	defer srv.Close()

	cmd := &fakeCmd{
		lookable: map[string]bool{"dpkg": true, "runuser": true},
		responses: map[string]cmdReply{
			"dpkg -S":           {out: "livck-agent: /usr/bin/livck-agent"}, // package-managed
			"dpkg -i":           {out: ""},                                  // install ok
			"systemctl restart": {out: ""},
		},
		showSeq:  []string{"SubState=running\nNRestarts=0"},
		valueSeq: []string{"0"},
	}
	e, _ := e2eEngine(t, srv, cmd, "v0.2.4")

	res, err := e.Run(context.Background(), Request{Target: "v0.2.5", Digest: hexSum})
	if err != nil || res.Outcome != "ok" {
		t.Fatalf("package upgrade: outcome=%q err=%v", res.Outcome, err)
	}
	if res.ManagedBy != "package" {
		t.Fatalf("managed_by = %q, want package", res.ManagedBy)
	}
	// The download MUST target the versionless release asset name.
	want := "/deb/v0.2.5/livck-agent_amd64.deb"
	found := false
	for _, p := range requested {
		if p == want {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a request to %q; got %v", want, requested)
	}
}

func TestRpmArchMapping(t *testing.T) {
	if rpmArch("amd64") != "x86_64" || rpmArch("arm64") != "aarch64" {
		t.Fatal("rpm arch mapping wrong")
	}
}
