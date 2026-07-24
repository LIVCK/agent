// Package update implements the root-side upgrade path of the agent
// (SERVER-AGENT.md §10.8): the `livck-agent upgrade` verb and the hourly
// livck-agent-update.timer both land here.
//
// Design constraints (review-agreed):
//   - The DAEMON never self-updates: it runs as the non-root `livck-agent` user
//     under ProtectSystem=strict and cannot replace /usr/bin/livck-agent nor call
//     systemctl. Upgrades are a ROOT CLI concern, like enroll/reset/uninstall.
//   - Pull-only: the target version comes from the agent config document
//     (server-evaluated staged rollout). --auto NEVER uses "latest".
//   - Fail closed: checksums are mandatory on every unattended path. The config
//     may pin the artifact digest (update.artifact_sha256[arch]); the same-origin
//     .sha256 file is the fallback for manual --version runs.
//   - No window where /usr/bin/livck-agent does not exist: the old binary is
//     hardlinked to .old, then the new one atomically renamed over the live path.
//   - Health is judged by an NRestarts delta + SubState, NOT `is-active`
//     (Restart=always makes is-active a false green during a crash loop).
package update

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	// LockFile serializes upgrade runs (timer vs manual) via non-blocking flock.
	LockFile = "update.lock"
	// ResultFile is the machine-readable outcome of the last upgrade run. The
	// daemon picks it up and reports it as batch meta (updater heartbeat →
	// service_agents.updater_last_*), which feeds the §16 rollout halt criteria.
	ResultFile = "update-result.json"

	oldSuffix = ".old"
	newSuffix = ".new"

	// maxArtifactBytes caps a download — a compromised origin must not be able to
	// fill the root filesystem via an endless stream.
	maxArtifactBytes = 256 << 20

	// healthWindow is how long the restarted service is watched for a crash loop.
	healthWindow = 20 * time.Second
	// smokeTimeout bounds the unprivileged `version` smoke run of a fresh binary.
	smokeTimeout = 10 * time.Second
	// serviceUser is the unprivileged account the smoke test runs as (the same
	// user the daemon runs under; a fresh root-executed binary is never run).
	serviceUser = "livck-agent"
)

// Result is the persisted outcome of one upgrade run (ResultFile schema).
// Never contains tokens or full URLs.
type Result struct {
	FinishedAt string `json:"finished_at"`
	From       string `json:"from"`
	To         string `json:"to"`
	// Outcome: ok | rolled_back | failed | skipped (image-managed /usr etc.)
	Outcome   string `json:"outcome"`
	ManagedBy string `json:"managed_by,omitempty"` // package | self
	Error     string `json:"error,omitempty"`
}

// Commander runs external programs (systemctl, runuser, dpkg, …). Abstracted so
// the swap/restart/health/rollback orchestration is testable end to end with real
// files but a faked init system — the one part that otherwise only runs on a live
// systemd host.
type Commander interface {
	Run(ctx context.Context, name string, args ...string) ([]byte, error)
	LookPath(name string) bool
}

type execCommander struct{}

func (execCommander) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}

func (execCommander) LookPath(name string) bool {
	_, err := exec.LookPath(name)
	return err == nil
}

// Options wires the upgrade engine. Everything is injectable for tests; the CLI
// fills the real values.
type Options struct {
	// BinaryPath is the live agent binary (install.sh contract: /usr/bin/livck-agent).
	BinaryPath string
	// ServiceName is the systemd unit to restart/health-check.
	ServiceName string
	// StateDir holds the lock + result files (systemd StateDirectory).
	StateDir string
	// DownloadBase is the distribution host (get.livck.cloud), no trailing slash.
	DownloadBase string
	// Arch is the Go arch label used in artifact names (amd64|arm64).
	Arch string
	// CurrentVersion is this CLI's own version (== the installed binary).
	CurrentVersion string
	HTTP           *http.Client
	Log            *slog.Logger
	Now            func() time.Time
	// Cmd runs external programs; defaults to real exec. Tests inject a fake.
	Cmd Commander
	// HealthWindow / HealthPoll bound the post-restart crash-loop watch; zero uses
	// the production defaults (20s / 2s). Tests shrink them.
	HealthWindow time.Duration
	HealthPoll   time.Duration
}

// Request is one upgrade invocation.
type Request struct {
	// Target is the version to install (SemVer, without enforcement of a "v"
	// prefix). Resolved by the CLI: --version > config target_version > latest
	// tag (manual only).
	Target string
	// Digest pins the artifact sha256 (hex) when the config supplied one for
	// this arch. Empty = fall back to the same-origin .sha256 file (still hard).
	Digest string
	// Force allows a downgrade / same-version reinstall.
	Force bool
	// Auto marks a timer run (stricter no-op semantics; callers gate window and
	// autoupdate before calling Run).
	Auto bool
}

// ErrLocked is returned when another upgrade run holds the lock.
var ErrLocked = errors.New("another upgrade is already running")

// Engine performs upgrades.
type Engine struct {
	opt Options
}

// New builds an Engine, defaulting the injectables.
func New(opt Options) *Engine {
	if opt.HTTP == nil {
		opt.HTTP = &http.Client{Timeout: 5 * time.Minute}
	}
	if opt.Log == nil {
		opt.Log = slog.Default()
	}
	if opt.Now == nil {
		opt.Now = time.Now
	}
	if opt.Cmd == nil {
		opt.Cmd = execCommander{}
	}
	if opt.HealthWindow <= 0 {
		opt.HealthWindow = healthWindow
	}
	if opt.HealthPoll <= 0 {
		opt.HealthPoll = 2 * time.Second
	}
	return &Engine{opt: opt}
}

// ---------------------------------------------------------------------------
// SemVer (internal, dependency-free): enough for our own release tags.
// ---------------------------------------------------------------------------

// ParseSemver validates "vX.Y.Z[-pre]" / "X.Y.Z[-pre]" and returns the numeric
// triple. Strict on shape — this runs on SERVER-SUPPLIED input before any URL
// interpolation, so anything unexpected is rejected.
func ParseSemver(s string) (maj, min, pat int, pre string, err error) {
	v := strings.TrimPrefix(strings.TrimSpace(s), "v")
	core := v
	if i := strings.IndexByte(v, '-'); i >= 0 {
		core, pre = v[:i], v[i+1:]
		if pre == "" || len(pre) > 64 || !isSafeIdent(pre) {
			return 0, 0, 0, "", fmt.Errorf("invalid prerelease in %q", s)
		}
	}
	parts := strings.Split(core, ".")
	if len(parts) != 3 {
		return 0, 0, 0, "", fmt.Errorf("invalid semver %q", s)
	}
	nums := make([]int, 3)
	for i, p := range parts {
		if p == "" || len(p) > 9 {
			return 0, 0, 0, "", fmt.Errorf("invalid semver %q", s)
		}
		n, convErr := strconv.Atoi(p)
		if convErr != nil || n < 0 {
			return 0, 0, 0, "", fmt.Errorf("invalid semver %q", s)
		}
		nums[i] = n
	}
	return nums[0], nums[1], nums[2], pre, nil
}

func isSafeIdent(s string) bool {
	for _, r := range s {
		ok := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '.' || r == '-'
		if !ok {
			return false
		}
	}
	return true
}

// CompareSemver returns -1/0/+1 for a<b, a==b, a>b. A prerelease sorts below
// its release (1.2.3-rc1 < 1.2.3). Invalid input never reaches this (parse first).
func CompareSemver(a, b string) (int, error) {
	am, an, ap, apre, err := ParseSemver(a)
	if err != nil {
		return 0, err
	}
	bm, bn, bp, bpre, err := ParseSemver(b)
	if err != nil {
		return 0, err
	}
	at := [3]int{am, an, ap}
	bt := [3]int{bm, bn, bp}
	for i := range at {
		if at[i] != bt[i] {
			if at[i] < bt[i] {
				return -1, nil
			}
			return 1, nil
		}
	}
	switch {
	case apre == bpre:
		return 0, nil
	case apre == "":
		return 1, nil
	case bpre == "":
		return -1, nil
	case apre < bpre:
		return -1, nil
	default:
		return 1, nil
	}
}

// ---------------------------------------------------------------------------
// Update window (host-local time, midnight wraparound).
// ---------------------------------------------------------------------------

// InWindow reports whether now (HOST-LOCAL wall clock) falls inside [start,end)
// given as "HH:MM". A start after the end wraps over midnight (22:00–02:00).
// Equal start/end means "always" (a zero-length window would be useless and the
// panel default is 00:00–04:00 anyway).
func InWindow(now time.Time, start, end string) bool {
	s, okS := parseHHMM(start)
	e, okE := parseHHMM(end)
	if !okS || !okE || s == e {
		return true
	}
	cur := now.Hour()*60 + now.Minute()
	if s < e {
		return cur >= s && cur < e
	}
	return cur >= s || cur < e // wraps midnight
}

func parseHHMM(v string) (int, bool) {
	parts := strings.Split(strings.TrimSpace(v), ":")
	if len(parts) != 2 {
		return 0, false
	}
	h, err1 := strconv.Atoi(parts[0])
	m, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil || h < 0 || h > 23 || m < 0 || m > 59 {
		return 0, false
	}
	return h*60 + m, true
}

// ---------------------------------------------------------------------------
// Lock
// ---------------------------------------------------------------------------

// Lock takes the non-blocking exclusive upgrade lock. The returned release func
// is nil-safe. ErrLocked means another run (timer or manual) is in flight.
func Lock(stateDir string) (func(), error) {
	path := filepath.Join(stateDir, LockFile)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, ErrLocked
	}
	return func() { _ = f.Close() }, nil
}

// ---------------------------------------------------------------------------
// Install-shape detection
// ---------------------------------------------------------------------------

// ManagedBy reports how the binary is installed: "package" when the live path is
// owned by dpkg/rpm (install.sh's bare .deb/.rpm install — NOT a configured
// repo; a real apt/yum repo does not exist today, and if one ever ships, the
// self-update must stand down per §10.8 "never two mechanisms"), else "self".
func (e *Engine) ManagedBy(ctx context.Context) string {
	if e.opt.Cmd.LookPath("dpkg") {
		if out, err := e.opt.Cmd.Run(ctx, "dpkg", "-S", e.opt.BinaryPath); err == nil && strings.Contains(string(out), "livck-agent") {
			return "package"
		}
	}
	if e.opt.Cmd.LookPath("rpm") {
		if out, err := e.opt.Cmd.Run(ctx, "rpm", "-qf", e.opt.BinaryPath); err == nil && strings.Contains(string(out), "livck-agent") {
			return "package"
		}
	}
	return "self"
}

// ---------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------

// Run executes one upgrade attempt end to end and persists the Result. The
// returned error is non-nil only when the host is left NOT healthy on the old
// version (i.e. even the rollback failed) OR the run could not start; a clean
// rollback returns nil with Outcome=rolled_back (callers decide exit codes).
func (e *Engine) Run(ctx context.Context, req Request) (Result, error) {
	res := Result{From: e.opt.CurrentVersion, To: req.Target}

	if _, _, _, _, err := ParseSemver(req.Target); err != nil {
		return e.finish(res, "failed", fmt.Errorf("target version: %w", err))
	}

	// Downgrade guard: server-supplied targets can never move backwards; only
	// the LOCAL --force flag may (a compromised/buggy rollout must not be able
	// to roll the fleet onto an older, vulnerable build).
	if cmp, err := CompareSemver(req.Target, e.opt.CurrentVersion); err == nil && cmp <= 0 && !req.Force {
		if cmp == 0 {
			// Same version: the only legitimate work left is a missed restart
			// (crash between swap and restart left the daemon on the old build).
			if e.daemonRunsStaleBinary(ctx) {
				e.opt.Log.Info("binary already current but the daemon still runs the previous build — restarting")
				prev := e.nRestarts(ctx)
				if err := e.restart(ctx); err != nil {
					return e.finish(res, "failed", err)
				}
				if err := e.healthy(ctx, prev); err != nil {
					return e.finish(res, "failed", err)
				}
				return e.finish(res, "ok", nil)
			}
			return e.finish(res, "skipped", nil)
		}
		return e.finish(res, "failed", fmt.Errorf("target %s is older than installed %s (use --force to downgrade)", req.Target, e.opt.CurrentVersion))
	}

	if err := e.recoverPrevious(); err != nil {
		return e.finish(res, "failed", err)
	}

	// Read-only /usr (rpm-ostree, MicroOS…): image-managed hosts cannot be
	// self-updated — record it once instead of failing every hour.
	if err := e.writableCheck(); err != nil {
		res.ManagedBy = "image"
		return e.finish(res, "skipped", err)
	}

	managed := e.ManagedBy(ctx)
	res.ManagedBy = managed

	prevRestarts := e.nRestarts(ctx)

	var applyErr error
	if managed == "package" {
		applyErr = e.applyPackage(ctx, req)
	} else {
		applyErr = e.applyBinary(ctx, req)
	}
	if applyErr != nil {
		return e.finish(res, "failed", applyErr)
	}

	if err := e.restart(ctx); err != nil {
		_ = e.rollback(ctx)
		return e.finish(res, "rolled_back", err)
	}
	if err := e.healthy(ctx, prevRestarts); err != nil {
		if rbErr := e.rollback(ctx); rbErr != nil {
			return e.finish(res, "failed", fmt.Errorf("unhealthy after upgrade (%v) AND rollback failed: %w", err, rbErr))
		}
		return e.finish(res, "rolled_back", err)
	}

	e.opt.Log.Info("upgrade complete", "from", res.From, "to", res.To)
	return e.finish(res, "ok", nil)
}

// finish stamps + persists the result. The write is best-effort (an unwritable
// state dir must not mask the real outcome).
func (e *Engine) finish(res Result, outcome string, cause error) (Result, error) {
	res.Outcome = outcome
	res.FinishedAt = e.opt.Now().UTC().Format(time.RFC3339)
	if cause != nil {
		res.Error = sanitize(cause.Error())
	}
	e.WriteResult(res)
	if outcome == "failed" && cause != nil {
		return res, cause
	}
	if outcome == "rolled_back" && cause != nil {
		e.opt.Log.Warn("upgrade rolled back", "reason", cause.Error())
	}
	if outcome == "skipped" && cause != nil {
		e.opt.Log.Info("upgrade skipped", "reason", cause.Error())
	}
	return res, nil
}

// WriteResult persists the ResultFile atomically, world-readable and owned by
// the service user so the non-root daemon can pick it up for the heartbeat.
func (e *Engine) WriteResult(res Result) {
	b, err := json.Marshal(res)
	if err != nil {
		return
	}
	path := filepath.Join(e.opt.StateDir, ResultFile)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		e.opt.Log.Warn("cannot write update result", "err", err.Error())
		return
	}
	if u, err := user.Lookup(serviceUser); err == nil {
		uid, _ := strconv.Atoi(u.Uid)
		gid, _ := strconv.Atoi(u.Gid)
		_ = os.Chown(tmp, uid, gid)
	}
	if err := os.Rename(tmp, path); err != nil {
		e.opt.Log.Warn("cannot publish update result", "err", err.Error())
	}
}

// sanitize strips anything URL-/token-shaped from error text before it lands in
// the world-readable result file.
func sanitize(s string) string {
	fields := strings.Fields(s)
	for i, f := range fields {
		if strings.Contains(f, "://") || strings.HasPrefix(f, "lvk_") || strings.HasPrefix(f, "lve_") {
			fields[i] = "[redacted]"
		}
	}
	out := strings.Join(fields, " ")
	if len(out) > 300 {
		out = out[:300]
	}
	return out
}

// ---------------------------------------------------------------------------
// Binary path (self-managed install)
// ---------------------------------------------------------------------------

func (e *Engine) applyBinary(ctx context.Context, req Request) error {
	newPath := e.opt.BinaryPath + newSuffix
	url := fmt.Sprintf("%s/binary/%s/livck-agent_linux_%s", e.opt.DownloadBase, req.Target, e.opt.Arch)

	sum, err := e.download(ctx, url, newPath, 0o755, req.Digest)
	if err != nil {
		_ = os.Remove(newPath)
		return err
	}
	// Re-verify what actually landed on disk (paranoia against racing writers).
	if diskSum, err := fileSHA256(newPath); err != nil || diskSum != sum {
		_ = os.Remove(newPath)
		return errors.New("on-disk hash mismatch after download")
	}

	if err := e.smoke(ctx, newPath); err != nil {
		_ = os.Remove(newPath)
		return fmt.Errorf("smoke test: %w", err)
	}

	// Swap with NO window where the live path is missing: hardlink the current
	// binary to .old (crash-safe backup), then atomically rename .new over it.
	oldPath := e.opt.BinaryPath + oldSuffix
	_ = os.Remove(oldPath)
	if err := os.Link(e.opt.BinaryPath, oldPath); err != nil {
		_ = os.Remove(newPath)
		return fmt.Errorf("backup link: %w", err)
	}
	if err := os.Rename(newPath, e.opt.BinaryPath); err != nil {
		_ = os.Remove(newPath)
		return fmt.Errorf("swap: %w", err)
	}
	fsyncDir(filepath.Dir(e.opt.BinaryPath))
	e.restorecon(ctx)
	return nil
}

// rollback restores the .old binary over the live path and restarts. Only
// meaningful for the binary path; on the package path the .old hardlink was
// taken too, so the same restore applies (the package DB then reports the new
// version while the binary is the old one — visible drift the operator resolves,
// but the host keeps monitoring).
//
// The health baseline is RE-CAPTURED here, not inherited from before the upgrade:
// NRestarts is a monotonic counter that systemctl restart does NOT reset, so the
// failed upgrade's crash loop already pushed it above the pre-upgrade value. Reusing
// that stale baseline would flag the (healthy) rolled-back service as still crashing.
func (e *Engine) rollback(ctx context.Context) error {
	oldPath := e.opt.BinaryPath + oldSuffix
	if _, err := os.Stat(oldPath); err != nil {
		return fmt.Errorf("no rollback binary: %w", err)
	}
	if err := os.Rename(oldPath, e.opt.BinaryPath); err != nil {
		return fmt.Errorf("rollback swap: %w", err)
	}
	fsyncDir(filepath.Dir(e.opt.BinaryPath))
	e.restorecon(ctx)
	baseline := e.nRestarts(ctx)
	if err := e.restart(ctx); err != nil {
		return err
	}
	return e.healthy(ctx, baseline)
}

// recoverPrevious heals a crashed prior run: live binary missing but .old
// present → restore it; stale .new downloads are removed.
func (e *Engine) recoverPrevious() error {
	_ = os.Remove(e.opt.BinaryPath + newSuffix)
	if _, err := os.Stat(e.opt.BinaryPath); err == nil {
		return nil
	}
	oldPath := e.opt.BinaryPath + oldSuffix
	if _, err := os.Stat(oldPath); err != nil {
		return fmt.Errorf("live binary missing and no %s backup — reinstall via install.sh", oldSuffix)
	}
	if err := os.Rename(oldPath, e.opt.BinaryPath); err != nil {
		return fmt.Errorf("restore backup: %w", err)
	}
	fsyncDir(filepath.Dir(e.opt.BinaryPath))
	return nil
}

func (e *Engine) writableCheck() error {
	dir := filepath.Dir(e.opt.BinaryPath)
	probe := filepath.Join(dir, ".livck-agent.wtest")
	f, err := os.OpenFile(probe, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("%s is not writable (image-managed host?): %w", dir, err)
	}
	_ = f.Close()
	_ = os.Remove(probe)
	return nil
}

// ---------------------------------------------------------------------------
// Package path (bare dpkg/rpm install)
// ---------------------------------------------------------------------------

// rpmArch maps the Go arch to the RPM arch label used in the release asset name.
func rpmArch(goArch string) string {
	switch goArch {
	case "amd64":
		return "x86_64"
	case "arm64":
		return "aarch64"
	default:
		return goArch
	}
}

func (e *Engine) applyPackage(ctx context.Context, req Request) error {
	var format, installName string
	if e.opt.Cmd.LookPath("dpkg") {
		format = "deb"
	} else if e.opt.Cmd.LookPath("rpm") {
		format = "rpm"
	} else {
		return errors.New("package-managed install but neither dpkg nor rpm found")
	}
	// Asset name MUST match what the release workflow uploads and install.sh
	// fetches: VERSIONLESS, deb keyed by the Go arch (amd64/arm64), rpm by the
	// RPM arch (x86_64/aarch64). A versioned "..._linux_..." name (the earlier
	// mistake) 404s against the real GitHub release assets.
	if format == "deb" {
		installName = fmt.Sprintf("livck-agent_%s.deb", e.opt.Arch)
	} else {
		installName = fmt.Sprintf("livck-agent_%s.rpm", rpmArch(e.opt.Arch))
	}

	dir := filepath.Join(e.opt.StateDir, "upgrade")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	pkgPath := filepath.Join(dir, installName)
	defer os.Remove(pkgPath)

	url := fmt.Sprintf("%s/%s/%s/%s", e.opt.DownloadBase, format, req.Target, installName)
	if _, err := e.download(ctx, url, pkgPath, 0o600, req.Digest); err != nil {
		return err
	}

	// Keep the same crash-safe .old backup as the binary path so rollback works
	// even though the package DB will drift on a rolled-back host (documented).
	oldPath := e.opt.BinaryPath + oldSuffix
	_ = os.Remove(oldPath)
	if err := os.Link(e.opt.BinaryPath, oldPath); err != nil {
		return fmt.Errorf("backup link: %w", err)
	}

	if format == "deb" {
		// unattended-upgrades may hold the dpkg frontend lock — probe instead of
		// queueing behind it; a held lock is a clean retry-next-timer no-op.
		if !dpkgLockFree() {
			return errors.New("dpkg lock held (unattended-upgrades?) — retrying on the next run")
		}
		if out, err := e.opt.Cmd.Run(ctx, "dpkg", "-i", pkgPath); err != nil {
			return fmt.Errorf("dpkg -i: %v: %s", err, firstLine(out))
		}
	} else {
		if out, err := e.opt.Cmd.Run(ctx, "rpm", "-U", "--force", pkgPath); err != nil {
			return fmt.Errorf("rpm -U: %v: %s", err, firstLine(out))
		}
	}
	e.restorecon(ctx)
	return nil
}

func dpkgLockFree() bool {
	f, err := os.OpenFile("/var/lib/dpkg/lock-frontend", os.O_RDWR, 0)
	if err != nil {
		return true // no lock file → not Debian-managed enough to block us
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return false
	}
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return true
}

// ---------------------------------------------------------------------------
// Download + verification
// ---------------------------------------------------------------------------

// download fetches url into dst (perm), enforcing https-only redirects, the
// size cap, and a MANDATORY sha256: pinnedDigest when the config supplied one,
// else the same-origin .sha256 file — absence is a hard failure (fail closed;
// unattended installs must never skip integrity). Returns the artifact hash.
func (e *Engine) download(ctx context.Context, url, dst string, perm os.FileMode, pinnedDigest string) (string, error) {
	expected := strings.ToLower(strings.TrimSpace(pinnedDigest))
	if expected == "" {
		sum, err := e.fetchChecksum(ctx, url+".sha256")
		if err != nil {
			return "", fmt.Errorf("no integrity source for %s: %w", filepath.Base(dst), err)
		}
		expected = sum
	}
	if !isHex64(expected) {
		return "", errors.New("integrity digest is not a sha256 hex string")
	}

	body, err := e.get(ctx, url)
	if err != nil {
		return "", err
	}
	defer body.Close()

	f, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(f, h), io.LimitReader(body, maxArtifactBytes+1))
	if copyErr == nil && n > maxArtifactBytes {
		copyErr = errors.New("artifact exceeds the size cap")
	}
	if copyErr == nil {
		copyErr = f.Sync()
	}
	if closeErr := f.Close(); copyErr == nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		_ = os.Remove(dst)
		return "", copyErr
	}

	got := hex.EncodeToString(h.Sum(nil))
	if got != expected {
		_ = os.Remove(dst)
		return "", fmt.Errorf("sha256 mismatch for %s", filepath.Base(dst))
	}
	return got, nil
}

func (e *Engine) fetchChecksum(ctx context.Context, url string) (string, error) {
	body, err := e.get(ctx, url)
	if err != nil {
		return "", err
	}
	defer body.Close()
	raw, err := io.ReadAll(io.LimitReader(body, 4096))
	if err != nil {
		return "", err
	}
	// "abc123...  filename" or bare hash.
	first := strings.Fields(strings.TrimSpace(string(raw)))
	if len(first) == 0 {
		return "", errors.New("empty checksum file")
	}
	return strings.ToLower(first[0]), nil
}

// get performs a GET with https-only redirect enforcement.
func (e *Engine) get(ctx context.Context, url string) (io.ReadCloser, error) {
	if !strings.HasPrefix(url, "https://") && !strings.HasPrefix(url, "http://127.0.0.1") && !strings.HasPrefix(url, "http://localhost") {
		return nil, fmt.Errorf("refusing non-https download")
	}
	client := *e.opt.HTTP
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 6 {
			return errors.New("too many redirects")
		}
		if req.URL.Scheme != "https" {
			return fmt.Errorf("refusing redirect to %s", req.URL.Scheme)
		}
		return nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("download failed: HTTP %d", resp.StatusCode)
	}
	return resp.Body, nil
}

// ResolveLatest resolves the distribution host's "latest" pointer to a concrete
// release tag WITHOUT downloading the artifact: the /binary/latest/... route
// answers with a 302 whose Location embeds the tag
// (.../releases/download/vX.Y.Z/...). Manual runs only — --auto never uses latest.
func (e *Engine) ResolveLatest(ctx context.Context) (string, error) {
	url := fmt.Sprintf("%s/binary/latest/livck-agent_linux_%s", e.opt.DownloadBase, e.opt.Arch)
	client := *e.opt.HTTP
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))

	loc := resp.Header.Get("Location")
	if resp.StatusCode < 300 || resp.StatusCode >= 400 || loc == "" {
		return "", fmt.Errorf("latest did not redirect (HTTP %d)", resp.StatusCode)
	}
	const marker = "/releases/download/"
	i := strings.Index(loc, marker)
	if i < 0 {
		return "", errors.New("unexpected latest redirect target")
	}
	rest := loc[i+len(marker):]
	tag, _, ok := strings.Cut(rest, "/")
	if !ok || tag == "" {
		return "", errors.New("unexpected latest redirect target")
	}
	if _, _, _, _, err := ParseSemver(tag); err != nil {
		return "", fmt.Errorf("latest tag %q: %w", tag, err)
	}
	return tag, nil
}

func isHex64(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, r := range s {
		ok := (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')
		if !ok {
			return false
		}
	}
	return true
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func fsyncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
}

func firstLine(b []byte) string {
	s := strings.TrimSpace(string(b))
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	return s
}

// ---------------------------------------------------------------------------
// Smoke, restart, health
// ---------------------------------------------------------------------------

// smoke executes the fresh binary's `version` as the UNPRIVILEGED service user
// — a just-downloaded binary is never first run as root.
func (e *Engine) smoke(ctx context.Context, bin string) error {
	ctx, cancel := context.WithTimeout(ctx, smokeTimeout)
	defer cancel()
	if e.opt.Cmd.LookPath("runuser") {
		out, err := e.opt.Cmd.Run(ctx, "runuser", "-u", serviceUser, "--", bin, "version")
		if err != nil {
			return fmt.Errorf("%v: %s", err, firstLine(out))
		}
		return nil
	}
	// No runuser (unusual): still never run it as root — setuid via su.
	out, err := e.opt.Cmd.Run(ctx, "su", "-s", bin, "-c", "version", serviceUser)
	if err != nil {
		return fmt.Errorf("%v: %s", err, firstLine(out))
	}
	return nil
}

func (e *Engine) restart(ctx context.Context) error {
	out, err := e.opt.Cmd.Run(ctx, "systemctl", "restart", e.opt.ServiceName)
	if err != nil {
		return fmt.Errorf("systemctl restart: %v: %s", err, firstLine(out))
	}
	return nil
}

// nRestarts reads the unit's automatic-restart counter (-1 when unknown).
func (e *Engine) nRestarts(ctx context.Context) int {
	out, err := e.opt.Cmd.Run(ctx, "systemctl", "show", "-p", "NRestarts", "--value", e.opt.ServiceName)
	if err != nil {
		return -1
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil {
		return -1
	}
	return n
}

// healthy watches the service for healthWindow: under Restart=always +
// StartLimitIntervalSec=0 `is-active` is a false green within milliseconds even
// in a crash loop — a rising NRestarts (systemd's automatic-restart counter,
// NOT bumped by `systemctl restart` itself) or a non-running SubState is the
// real signal.
func (e *Engine) healthy(ctx context.Context, prevRestarts int) error {
	// Real wall clock for the watch loop (opt.Now is for result timestamps, and may
	// be a frozen test clock); the window/poll are configurable so tests run fast.
	deadline := time.Now().Add(e.opt.HealthWindow)
	for {
		if remaining := time.Until(deadline); remaining <= 0 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(e.opt.HealthPoll):
		}
		out, err := e.opt.Cmd.Run(ctx, "systemctl", "show", "-p", "SubState,NRestarts", e.opt.ServiceName)
		if err != nil {
			return fmt.Errorf("systemctl show: %v", err)
		}
		sub, n := parseShow(string(out))
		if sub != "" && sub != "running" && sub != "start" && sub != "auto-restart" && sub != "start-pre" {
			return fmt.Errorf("service is %s after restart", sub)
		}
		if prevRestarts >= 0 && n > prevRestarts {
			return fmt.Errorf("service crash-looped after upgrade (NRestarts %d → %d)", prevRestarts, n)
		}
		if sub == "auto-restart" {
			return errors.New("service is crash-looping after upgrade")
		}
	}
	return nil
}

func parseShow(out string) (subState string, nRestarts int) {
	nRestarts = -1
	for _, line := range strings.Split(out, "\n") {
		k, v, ok := strings.Cut(strings.TrimSpace(line), "=")
		if !ok {
			continue
		}
		switch k {
		case "SubState":
			subState = v
		case "NRestarts":
			if n, err := strconv.Atoi(v); err == nil {
				nRestarts = n
			}
		}
	}
	return subState, nRestarts
}

// daemonRunsStaleBinary detects the crash-between-swap-and-restart case: the
// installed binary matches the target but the RUNNING daemon still executes the
// replaced (deleted) image.
func (e *Engine) daemonRunsStaleBinary(ctx context.Context) bool {
	out, err := e.opt.Cmd.Run(ctx, "systemctl", "show", "-p", "MainPID", "--value", e.opt.ServiceName)
	if err != nil {
		return false
	}
	pid := strings.TrimSpace(string(out))
	if pid == "" || pid == "0" {
		return false
	}
	exe, err := os.Readlink("/proc/" + pid + "/exe")
	if err != nil {
		return false
	}
	return strings.HasSuffix(exe, " (deleted)") || (exe != e.opt.BinaryPath && !strings.HasPrefix(exe, e.opt.BinaryPath))
}

// restorecon restores the SELinux context after a rename (RHEL9 & friends);
// silently a no-op where SELinux/restorecon are absent.
func (e *Engine) restorecon(ctx context.Context) {
	if !e.opt.Cmd.LookPath("restorecon") {
		return
	}
	_, _ = e.opt.Cmd.Run(ctx, "restorecon", "-F", e.opt.BinaryPath)
}


