package doctor

import (
	"context"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/LIVCK/agent/internal/platform"
)

// checkPlatform runs the four "is this the right kind of host" gates: systemd is
// PID 1, we are not inside a container, the distro is one we support, and the
// architecture is amd64/arm64. It appends a check per gate and
// returns true if any gate is unsupported, so Run can stop before the network
// probes. A RHEL-9 ABI rebuild (Rocky/Alma/CentOS Stream 9) is a warning, not an
// unsupported: it is expected-compatible but untested, and the installer does
// not abort on it.
func checkPlatform(r *Report, plat platform.Platform) bool {
	unsupported := false

	// systemd: /run/systemd/system exists AND PID 1 is systemd. The directory
	// alone can linger, so PID 1 is the authoritative signal when readable.
	if isSystemd(plat) {
		r.add("systemd", StatusOK, "systemd is the init system")
	} else {
		unsupported = true
		r.addHint("systemd", StatusUnsupported, "no systemd init detected",
			"the agent ships as a systemd unit; non-systemd inits (OpenRC/Alpine) are not supported in v1")
	}

	// container: a namespaced /proc reports the host's neighbours, not this
	// container's, so host KPIs would be meaningless. Refuse rather than lie.
	if kind, inContainer := containerKind(plat); inContainer {
		unsupported = true
		r.addHint("container", StatusUnsupported, "running inside a container ("+kind+")",
			"run the agent on the host, not in a container; a Docker variant is planned for a later version")
	} else {
		r.add("container", StatusOK, "running on a bare host (not containerized)")
	}

	// distro: a known ID+VERSION_ID, or a RHEL-9 ABI rebuild (warn).
	id, ver, like := osRelease(plat)
	switch distroSupport(id, ver, like) {
	case distroOK:
		r.add("distribution", StatusOK, distroLabel(id, ver)+" is supported")
	case distroRHELRebuild:
		r.addHint("distribution", StatusWarn, distroLabel(id, ver)+" is a RHEL-9 ABI rebuild",
			"expected-compatible but untested; report issues if you hit them")
	default:
		unsupported = true
		r.addHint("distribution", StatusUnsupported, distroLabel(id, ver)+" is not supported",
			"supported: Ubuntu 22.04/24.04, Debian 12, RHEL 9 (and ABI rebuilds)")
	}

	// architecture: amd64 or arm64.
	switch runtime.GOARCH {
	case "amd64", "arm64":
		r.add("architecture", StatusOK, runtime.GOARCH+" is supported")
	default:
		unsupported = true
		r.add("architecture", StatusUnsupported, runtime.GOARCH+" is not supported (need amd64 or arm64)")
	}

	return unsupported
}

// isSystemd reports whether systemd is the init. It prefers PID 1's comm (the
// authoritative signal) and falls back to the /run/systemd/system marker when
// /proc/1/comm is unreadable (e.g. hidepid).
func isSystemd(plat platform.Platform) bool {
	if comm, err := plat.FS.ReadFile("/proc/1/comm"); err == nil {
		return strings.TrimSpace(string(comm)) == "systemd"
	}
	_, err := plat.FS.Stat("/run/systemd/system")
	return err == nil
}

// containerKind detects whether the agent runs inside a container and, if so,
// names the flavour. It uses only file signals (no fork) plus a best-effort
// systemd-detect-virt when present, and only ever flips the result to "in a
// container" — it never overrides a positive file signal.
func containerKind(plat platform.Platform) (string, bool) {
	if _, err := plat.FS.Stat("/.dockerenv"); err == nil {
		return "docker", true
	}
	if data, err := plat.FS.ReadFile("/proc/1/cgroup"); err == nil {
		cg := string(data)
		for _, marker := range []string{"docker", "kubepods", "containerd", "/lxc", "libpod"} {
			if strings.Contains(cg, marker) {
				return strings.TrimPrefix(marker, "/"), true
			}
		}
	}
	if plat.Exec != nil {
		if _, ok := plat.Exec.LookPath("systemd-detect-virt"); ok {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			// --container prints the type and exits 0 in a container, prints
			// "none" and exits non-zero otherwise; Run returns stdout either way.
			out, _ := plat.Exec.Run(ctx, "systemd-detect-virt", "--container")
			if v := strings.TrimSpace(string(out)); v != "" && v != "none" {
				return v, true
			}
		}
	}
	return "", false
}

type distroVerdict int

const (
	distroUnsupported distroVerdict = iota
	distroOK
	distroRHELRebuild
)

// distroSupport classifies an /etc/os-release ID/VERSION_ID/ID_LIKE against the
// v1 support matrix.
func distroSupport(id, ver, like string) distroVerdict {
	major := ver
	if i := strings.IndexByte(ver, '.'); i >= 0 && id != "ubuntu" {
		major = ver[:i]
	}
	switch id {
	case "ubuntu":
		if ver == "22.04" || ver == "24.04" {
			return distroOK
		}
	case "debian":
		if major == "12" {
			return distroOK
		}
	case "rhel":
		if major == "9" {
			return distroOK
		}
	}
	if strings.Contains(like, "rhel") && major == "9" {
		return distroRHELRebuild
	}
	return distroUnsupported
}

// osRelease parses ID, VERSION_ID and ID_LIKE from /etc/os-release.
func osRelease(plat platform.Platform) (id, version, like string) {
	data, err := plat.FS.ReadFile("/etc/os-release")
	if err != nil {
		return "", "", ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		v = strings.Trim(strings.TrimSpace(v), `"`)
		switch strings.TrimSpace(k) {
		case "ID":
			id = strings.ToLower(v)
		case "VERSION_ID":
			version = v
		case "ID_LIKE":
			like = strings.ToLower(v)
		}
	}
	return id, version, like
}

func distroLabel(id, ver string) string {
	switch {
	case id == "" && ver == "":
		return "unknown distribution"
	case ver == "":
		return id
	default:
		return id + " " + ver
	}
}

// checkLocal covers the local, no-network prerequisites: the agent can read
// /proc (where every KPI comes from), it can write its state directory, and its
// identity is intact. It also reports whether the host is enrolled yet, since a
// missing token is expected on a fresh box and changes how the network checks
// read.
func checkLocal(r *Report, opts Options) {
	plat := opts.Platform

	if _, err := plat.FS.ReadFile("/proc/stat"); err != nil {
		r.addHint("/proc access", StatusFail, "cannot read /proc/stat: "+err.Error(),
			"the agent reads all metrics from /proc; check it is mounted and readable")
	} else {
		r.add("/proc access", StatusOK, "/proc is readable")
	}

	checkStateDir(r, opts)
	checkIdentity(r, opts)
}

// checkStateDir verifies the agent can write under its state directory by
// creating and removing a probe file — the same 0600 atomic write the identity
// and spool paths use.
func checkStateDir(r *Report, opts Options) {
	if opts.StateDir == "" {
		r.add("state directory", StatusSkip, "no state directory configured")
		return
	}
	probe := opts.StateDir + "/.doctor-probe"
	if err := opts.Platform.FS.WriteFileAtomic(probe, []byte("ok\n"), 0o600); err != nil {
		r.addHint("state directory", StatusFail, "cannot write to "+opts.StateDir+": "+err.Error(),
			"the service user needs write access to its StateDirectory; reinstall or fix ownership")
		return
	}
	_ = opts.Platform.FS.Remove(probe)
	r.add("state directory", StatusOK, opts.StateDir+" is writable")
}

// checkIdentity reports the enrollment state: an intact instance_id and whether
// a managed token is present, warning if that token is readable beyond 0600.
func checkIdentity(r *Report, opts Options) {
	if opts.Store == nil {
		return
	}
	if _, err := opts.Store.InstanceID(); err != nil {
		if opts.Store.HasToken() {
			r.addHint("identity", StatusFail, "instance_id is missing or corrupt: "+err.Error(),
				"run 'livck-agent reset' before re-enrolling")
		} else {
			r.add("identity", StatusInfo, "not enrolled yet — run 'livck-agent enroll'")
		}
		return
	}
	if !opts.Store.HasToken() {
		r.addHint("identity", StatusInfo, "identity present but no managed token — not enrolled",
			"run 'livck-agent enroll --token-file <path>' to register this server")
		return
	}
	r.add("identity", StatusOK, "enrolled (instance_id and managed token present)")

	if mode, ok := tokenMode(opts); ok && mode&0o077 != 0 {
		r.addHint("token permissions", StatusWarn, "token file is mode "+modeString(mode)+", expected 0600",
			"tighten it: chmod 600 the token under the state directory")
	}
}

func tokenMode(opts Options) (os.FileMode, bool) {
	info, err := opts.Platform.FS.Stat(opts.StateDir + "/token")
	if err != nil {
		return 0, false
	}
	return info.Mode().Perm(), true
}

func modeString(m os.FileMode) string {
	return "0" + strings.ToUpper(padOctal(uint32(m.Perm())))
}

func padOctal(v uint32) string {
	s := ""
	for i := 0; i < 3; i++ {
		s = string(rune('0'+(v&7))) + s
		v >>= 3
	}
	return s
}

// checkPSI reports whether the kernel exposes Pressure Stall Information. PSI is
// optional (a warning when off), and RHEL 9 ships it compiled-in but disabled
// unless booted with psi=1, so the hint is distro-aware.
func checkPSI(r *Report, plat platform.Platform) {
	if _, err := plat.FS.ReadFile("/proc/pressure/cpu"); err == nil {
		r.add("PSI (pressure stall info)", StatusOK, "/proc/pressure is available")
		return
	}
	id, _, like := osRelease(plat)
	hint := "PSI needs Linux ≥ 4.20; the sys.psi.* metrics are simply omitted without it"
	if id == "rhel" || strings.Contains(like, "rhel") {
		hint = "RHEL 9 ships PSI disabled; enable it with: grubby --update-kernel=ALL --args=psi=1 && reboot"
	}
	r.addHint("PSI (pressure stall info)", StatusWarn, "/proc/pressure is not available", hint)
}
