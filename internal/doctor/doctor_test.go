package doctor

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/LIVCK/agent/internal/enroll"
	"github.com/LIVCK/agent/internal/platform"
	"github.com/LIVCK/agent/internal/platform/platformtest"
)

// ---- test doubles ----

type fakeHTTP struct {
	fn func(*http.Request) (*http.Response, error)
}

func (f fakeHTTP) Do(req *http.Request) (*http.Response, error) { return f.fn(req) }

func httpResp(status int, date, body string) *http.Response {
	h := http.Header{}
	if date != "" {
		h.Set("Date", date)
	}
	return &http.Response{StatusCode: status, Header: h, Body: io.NopCloser(strings.NewReader(body))}
}

type stubHost struct{ name string }

func (h stubHost) Hostname() (string, error) { return h.name, nil }

var fixedNow = time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)

// realPlatform assembles a Platform from the fakes with a clock pinned to
// fixedNow.
func fakePlatform(fs *platformtest.MemFS, exec *platformtest.Exec) platform.Platform {
	p := platform.Platform{
		Clock: platformtest.NewClock(fixedNow),
		FS:    fs,
		Host:  stubHost{name: "test-host"},
	}
	// Leave Exec a true nil interface when no fake is given (a typed nil pointer
	// in the interface would defeat the nil guard in containerKind).
	if exec != nil {
		p.Exec = exec
	}
	return p
}

func findCheck(r Report, name string) (Check, bool) {
	for _, c := range r.Checks {
		if c.Name == name {
			return c, true
		}
	}
	return Check{}, false
}

func mustCheck(t *testing.T, r Report, name string) Check {
	t.Helper()
	c, ok := findCheck(r, name)
	if !ok {
		t.Fatalf("no check named %q in report (%d checks)", name, len(r.Checks))
	}
	return c
}

// ---- pure classifiers ----

func TestDistroSupport(t *testing.T) {
	cases := []struct {
		id, ver, like string
		want          distroVerdict
	}{
		{"ubuntu", "22.04", "debian", distroOK},
		{"ubuntu", "24.04", "debian", distroOK},
		{"ubuntu", "20.04", "debian", distroUnsupported},
		{"debian", "12", "", distroOK},
		{"debian", "11", "", distroUnsupported},
		{"rhel", "9.3", "fedora", distroOK},
		{"rocky", "9.4", "rhel centos fedora", distroRHELRebuild},
		{"almalinux", "9", "rhel centos fedora", distroRHELRebuild},
		{"alpine", "3.20", "", distroUnsupported},
		{"", "", "", distroUnsupported},
	}
	for _, c := range cases {
		if got := distroSupport(c.id, c.ver, c.like); got != c.want {
			t.Errorf("distroSupport(%q,%q,%q) = %v, want %v", c.id, c.ver, c.like, got, c.want)
		}
	}
}

func TestIsSystemd(t *testing.T) {
	systemd := platformtest.NewMemFS()
	systemd.WriteFileAtomic("/proc/1/comm", []byte("systemd\n"), 0o444)
	if !isSystemd(fakePlatform(systemd, nil)) {
		t.Error("expected systemd host with /proc/1/comm=systemd")
	}

	other := platformtest.NewMemFS()
	other.WriteFileAtomic("/proc/1/comm", []byte("init\n"), 0o444)
	if isSystemd(fakePlatform(other, nil)) {
		t.Error("expected non-systemd host with /proc/1/comm=init")
	}

	fallback := platformtest.NewMemFS()
	fallback.WriteFileAtomic("/run/systemd/system", []byte(""), 0o755)
	if !isSystemd(fakePlatform(fallback, nil)) {
		t.Error("expected systemd via /run/systemd/system fallback when comm is unreadable")
	}

	if isSystemd(fakePlatform(platformtest.NewMemFS(), nil)) {
		t.Error("expected non-systemd on an empty fs")
	}
}

func TestContainerKind(t *testing.T) {
	dockerenv := platformtest.NewMemFS()
	dockerenv.WriteFileAtomic("/.dockerenv", []byte(""), 0o644)
	if kind, in := containerKind(fakePlatform(dockerenv, nil)); !in || kind != "docker" {
		t.Errorf("dockerenv: got (%q,%v), want (docker,true)", kind, in)
	}

	cgroup := platformtest.NewMemFS()
	cgroup.WriteFileAtomic("/proc/1/cgroup", []byte("0::/kubepods/pod123/abc\n"), 0o444)
	if kind, in := containerKind(fakePlatform(cgroup, nil)); !in || kind != "kubepods" {
		t.Errorf("cgroup: got (%q,%v), want (kubepods,true)", kind, in)
	}

	virt := platformtest.NewMemFS()
	exec := platformtest.NewExec().AddPath("systemd-detect-virt")
	exec.SetResponse([]byte("lxc\n"), nil, "systemd-detect-virt", "--container")
	if kind, in := containerKind(fakePlatform(virt, exec)); !in || kind != "lxc" {
		t.Errorf("detect-virt: got (%q,%v), want (lxc,true)", kind, in)
	}

	clean := platformtest.NewMemFS()
	clean.WriteFileAtomic("/proc/1/cgroup", []byte("0::/init.scope\n"), 0o444)
	cleanExec := platformtest.NewExec().AddPath("systemd-detect-virt")
	cleanExec.SetResponse([]byte("none\n"), nil, "systemd-detect-virt", "--container")
	if kind, in := containerKind(fakePlatform(clean, cleanExec)); in {
		t.Errorf("clean host: got (%q,%v), want (_,false)", kind, in)
	}
}

func TestExitCode(t *testing.T) {
	cases := []struct {
		statuses []Status
		want     int
	}{
		{[]Status{StatusOK, StatusOK}, 0},
		{[]Status{StatusOK, StatusWarn}, 1},
		{[]Status{StatusWarn, StatusFail}, 2},
		{[]Status{StatusFail, StatusUnsupported}, 3},
		{[]Status{StatusInfo, StatusSkip}, 0},
	}
	for _, c := range cases {
		var r Report
		for _, s := range c.statuses {
			r.add("x", s, "")
		}
		if got := r.ExitCode(); got != c.want {
			t.Errorf("ExitCode(%v) = %d, want %d", c.statuses, got, c.want)
		}
	}
}

// ---- platform gate ----

func TestCheckPlatform_Container(t *testing.T) {
	fs := platformtest.NewMemFS()
	fs.WriteFileAtomic("/proc/1/comm", []byte("systemd\n"), 0o444)
	fs.WriteFileAtomic("/.dockerenv", []byte(""), 0o644)
	fs.WriteFileAtomic("/etc/os-release", []byte("ID=ubuntu\nVERSION_ID=\"24.04\"\n"), 0o444)

	var r Report
	unsupported := checkPlatform(&r, fakePlatform(fs, nil))
	if !unsupported {
		t.Fatal("a container host must be unsupported")
	}
	if c := mustCheck(t, r, "container"); c.Status != StatusUnsupported {
		t.Errorf("container check = %v, want Unsupported", c.Status)
	}
}

func TestCheckPlatform_SupportedUbuntu(t *testing.T) {
	fs := platformtest.NewMemFS()
	fs.WriteFileAtomic("/proc/1/comm", []byte("systemd\n"), 0o444)
	fs.WriteFileAtomic("/proc/1/cgroup", []byte("0::/init.scope\n"), 0o444)
	fs.WriteFileAtomic("/etc/os-release", []byte("ID=ubuntu\nVERSION_ID=\"24.04\"\n"), 0o444)

	var r Report
	if checkPlatform(&r, fakePlatform(fs, nil)) {
		t.Fatal("Ubuntu 24.04 on systemd must be supported")
	}
	if c := mustCheck(t, r, "distribution"); c.Status != StatusOK {
		t.Errorf("distribution = %v, want OK", c.Status)
	}
	if c := mustCheck(t, r, "systemd"); c.Status != StatusOK {
		t.Errorf("systemd = %v, want OK", c.Status)
	}
}

// ---- local prerequisites ----

func TestCheckLocal_NotEnrolled(t *testing.T) {
	fs := platformtest.NewMemFS()
	fs.WriteFileAtomic("/proc/stat", []byte("cpu 1 2 3\n"), 0o444)
	opts := Options{Platform: fakePlatform(fs, nil), StateDir: "/var/lib/livck-agent",
		Store: enroll.NewStore(fs, "/var/lib/livck-agent", func() string { return "id" })}

	var r Report
	checkLocal(&r, opts)

	if c := mustCheck(t, r, "/proc access"); c.Status != StatusOK {
		t.Errorf("/proc access = %v, want OK", c.Status)
	}
	if c := mustCheck(t, r, "state directory"); c.Status != StatusOK {
		t.Errorf("state directory = %v, want OK", c.Status)
	}
	if c := mustCheck(t, r, "identity"); c.Status != StatusInfo {
		t.Errorf("identity = %v, want Info (not enrolled)", c.Status)
	}
}

func TestCheckLocal_ProcUnreadable(t *testing.T) {
	fs := platformtest.NewMemFS() // no /proc/stat
	opts := Options{Platform: fakePlatform(fs, nil), StateDir: "/var/lib/livck-agent"}

	var r Report
	checkLocal(&r, opts)
	if c := mustCheck(t, r, "/proc access"); c.Status != StatusFail {
		t.Errorf("/proc access = %v, want Fail", c.Status)
	}
}

func TestCheckLocal_LooseTokenPerms(t *testing.T) {
	dir := "/var/lib/livck-agent"
	fs := platformtest.NewMemFS()
	fs.WriteFileAtomic("/proc/stat", []byte("cpu 1 2 3\n"), 0o444)
	fs.WriteFileAtomic(dir+"/instance_id", []byte("3f2504e0-4f89-41d3-9a0c-0305e82c3301\n"), 0o600)
	fs.WriteFileAtomic(dir+"/token", []byte("lvk_secret\n"), 0o644) // too open

	opts := Options{Platform: fakePlatform(fs, nil), StateDir: dir,
		Store: enroll.NewStore(fs, dir, func() string { return "id" })}

	var r Report
	checkLocal(&r, opts)
	if c := mustCheck(t, r, "identity"); c.Status != StatusOK {
		t.Errorf("identity = %v, want OK (enrolled)", c.Status)
	}
	if c := mustCheck(t, r, "token permissions"); c.Status != StatusWarn {
		t.Errorf("token permissions = %v, want Warn", c.Status)
	}
}

// ---- network ----

const meBody = `{"type":"managed","permissions":["ingest"],"organization":{"public_id":"org_abc","name":"Acme"},"service":{"public_id":"svc_1","name":"web-1"},"rate_limit":{"requests_per_minute":2}}`

func enrolledOpts(fs *platformtest.MemFS, client HTTPClient) Options {
	dir := "/var/lib/livck-agent"
	fs.WriteFileAtomic(dir+"/instance_id", []byte("3f2504e0-4f89-41d3-9a0c-0305e82c3301\n"), 0o600)
	fs.WriteFileAtomic(dir+"/token", []byte("lvk_secret\n"), 0o600)
	return Options{
		Platform:    fakePlatform(fs, nil),
		Store:       enroll.NewStore(fs, dir, func() string { return "id" }),
		HTTP:        client,
		ControlBase: "https://app.example.test",
		IngestBase:  "https://ingest.example.test",
		StateDir:    dir,
		Timeout:     5 * time.Second,
	}
}

func TestCheckNetwork_HealthyEnrolled(t *testing.T) {
	date := fixedNow.Format(http.TimeFormat)
	client := fakeHTTP{fn: func(req *http.Request) (*http.Response, error) {
		switch {
		case strings.HasSuffix(req.URL.Path, "/api/v1/me"):
			return httpResp(200, date, meBody), nil
		case strings.HasSuffix(req.URL.Path, "/api/v1/agents/config"):
			return httpResp(200, date, `{"version":1}`), nil
		case strings.HasSuffix(req.URL.Path, "/v1/ingest"):
			return httpResp(401, date, ""), nil
		}
		return httpResp(404, date, ""), nil
	}}

	var r Report
	checkNetwork(context.Background(), &r, enrolledOpts(platformtest.NewMemFS(), client))

	if c := mustCheck(t, r, "control plane (app.example.test)"); c.Status != StatusOK {
		t.Errorf("control plane = %v (%s), want OK", c.Status, c.Detail)
	}
	if c := mustCheck(t, r, "authentication (/v1/me)"); c.Status != StatusOK || !strings.Contains(c.Detail, "Acme") {
		t.Errorf("auth = %v (%s), want OK containing org name", c.Status, c.Detail)
	}
	if c := mustCheck(t, r, "clock skew"); c.Status != StatusOK {
		t.Errorf("clock skew = %v (%s), want OK", c.Status, c.Detail)
	}
	if c := mustCheck(t, r, "agent config pull"); c.Status != StatusOK {
		t.Errorf("config = %v, want OK", c.Status)
	}
	if c := mustCheck(t, r, "ingest (ingest.example.test)"); c.Status != StatusOK {
		t.Errorf("ingest = %v, want OK (401 still proves reachable)", c.Status)
	}
}

func TestCheckNetwork_DNSFailure(t *testing.T) {
	client := fakeHTTP{fn: func(req *http.Request) (*http.Response, error) {
		return nil, &net.DNSError{Err: "no such host", Name: hostOf("https://app.example.test"), IsNotFound: true}
	}}

	var r Report
	checkNetwork(context.Background(), &r, enrolledOpts(platformtest.NewMemFS(), client))
	c := mustCheck(t, r, "control plane (app.example.test)")
	if c.Status != StatusFail || !strings.Contains(c.Detail, "DNS") {
		t.Errorf("control plane = %v (%s), want Fail mentioning DNS", c.Status, c.Detail)
	}
}

func TestCheckNetwork_AuthRejected(t *testing.T) {
	date := fixedNow.Format(http.TimeFormat)
	client := fakeHTTP{fn: func(req *http.Request) (*http.Response, error) {
		return httpResp(401, date, `{"error":{"code":"UNAUTHENTICATED"}}`), nil
	}}

	var r Report
	checkNetwork(context.Background(), &r, enrolledOpts(platformtest.NewMemFS(), client))
	if c := mustCheck(t, r, "authentication (/v1/me)"); c.Status != StatusFail {
		t.Errorf("auth = %v, want Fail on 401", c.Status)
	}
}

func TestCheckSkew_HardDrift(t *testing.T) {
	// Server clock 10 minutes behind local -> past the 5-minute reject threshold.
	serverDate := fixedNow.Add(-10 * time.Minute).Format(http.TimeFormat)
	opts := Options{Platform: fakePlatform(platformtest.NewMemFS(), nil)}

	var r Report
	checkSkew(&r, opts, serverDate)
	c := mustCheck(t, r, "clock skew")
	if c.Status != StatusWarn || !strings.Contains(c.Detail, "rejected") {
		t.Errorf("skew = %v (%s), want Warn mentioning rejected", c.Status, c.Detail)
	}
}

// ---- rendering + PSI ----

func TestCheckPSI_Missing(t *testing.T) {
	fs := platformtest.NewMemFS()
	fs.WriteFileAtomic("/etc/os-release", []byte("ID=rhel\nVERSION_ID=\"9.3\"\n"), 0o444)
	var r Report
	checkPSI(&r, fakePlatform(fs, nil))
	c := mustCheck(t, r, "PSI (pressure stall info)")
	if c.Status != StatusWarn || !strings.Contains(c.Hint, "psi=1") {
		t.Errorf("psi = %v (hint %q), want Warn with RHEL psi=1 hint", c.Status, c.Hint)
	}
}

func TestRenderSmoke(t *testing.T) {
	var r Report
	r.add("systemd", StatusOK, "ok")
	r.addHint("clock skew", StatusWarn, "drift", "run ntp")
	var buf bytes.Buffer
	Render(&buf, r, false)
	out := buf.String()
	if !strings.Contains(out, "systemd") || !strings.Contains(out, "warning(s)") {
		t.Errorf("render output missing expected content:\n%s", out)
	}
}
