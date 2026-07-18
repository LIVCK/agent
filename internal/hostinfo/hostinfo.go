// Package hostinfo builds the two host-fact maps the agent attaches to batches:
// the report/enroll meta profile (os, distro, kernel, arch, cpu, ram, boot time)
// and the fingerprint used for clone detection (a hashed machine-id, the boot_id
// and the hostname). It reads only unprivileged /proc and /etc files through the
// platform abstraction so both maps are built from fakes in tests.
//
// Two hard rules from the contract: the machine-id is never sent raw, only its
// sha256 (privacy); and ips_* never appear in report meta (they are enroll-only),
// so this package deliberately produces no IP fields.
package hostinfo

import (
	"crypto/sha256"
	"encoding/hex"
	"runtime"
	"strconv"
	"strings"

	"github.com/LIVCK/agent/internal/collector"
	"github.com/LIVCK/agent/internal/platform"
)

const (
	osReleasePath = "/etc/os-release"
	kernelPath    = "/proc/sys/kernel/osrelease"
	cpuinfoPath   = "/proc/cpuinfo"
	meminfoPath   = "/proc/meminfo"
	statPath      = "/proc/stat"
	machineIDPath = "/etc/machine-id"
	dbusIDPath    = "/var/lib/dbus/machine-id"
	bootIDPath    = "/proc/sys/kernel/random/boot_id"
	rebootRunPath = "/run/reboot-required"
	rebootVarPath = "/var/run/reboot-required"
)

// Meta builds the report/enroll host profile. Missing facts are omitted rather
// than faked; os and arch are always known. It never includes ips_*.
func Meta(plat platform.Platform) map[string]string {
	m := map[string]string{
		"os":   "linux",
		"arch": runtime.GOARCH,
	}
	put(m, "hostname", hostname(plat))
	put(m, "kernel", strings.TrimSpace(readString(plat, kernelPath)))

	id, ver := osRelease(plat)
	put(m, "distro", id)
	put(m, "distro_version", ver)

	put(m, "cpu_model", cpuModel(plat))
	m["cpu_cores"] = strconv.Itoa(runtime.NumCPU())

	if ram := memTotalBytes(plat); ram > 0 {
		m["ram_total_bytes"] = strconv.FormatUint(ram, 10)
	}
	if bt := btime(plat); bt > 0 {
		m["boot_time"] = strconv.FormatInt(bt, 10)
	}
	m["reboot_required"] = strconv.FormatBool(rebootRequired(plat))
	return m
}

// Fingerprint builds the clone-detection fingerprint: a hashed machine-id, the
// boot_id and the hostname. The machine-id is hashed, never sent raw.
// cloud_instance_id is not collected yet (best-effort cloud metadata is a future addition).
func Fingerprint(plat platform.Platform) map[string]string {
	fp := map[string]string{}
	if h := machineIDHash(plat); h != "" {
		fp["machine_id_hash"] = h
	}
	put(fp, "boot_id", strings.TrimSpace(readString(plat, bootIDPath)))
	put(fp, "hostname", hostname(plat))
	return fp
}

func hostname(plat platform.Platform) string {
	if h, err := plat.Host.Hostname(); err == nil {
		return strings.TrimSpace(h)
	}
	return ""
}

// osRelease returns the distro id and version from /etc/os-release.
func osRelease(plat platform.Platform) (id, version string) {
	data := readString(plat, osReleasePath)
	for _, line := range strings.Split(data, "\n") {
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
		}
	}
	return id, version
}

// cpuModel returns the CPU model string from /proc/cpuinfo. x86 exposes
// "model name"; arm exposes no equivalent, so it is omitted there.
func cpuModel(plat platform.Platform) string {
	data := readString(plat, cpuinfoPath)
	for _, line := range strings.Split(data, "\n") {
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		if strings.TrimSpace(k) == "model name" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func memTotalBytes(plat platform.Platform) uint64 {
	data, err := plat.FS.ReadFile(meminfoPath)
	if err != nil {
		return 0
	}
	return collector.ParseMeminfo(data)["MemTotal"]
}

func btime(plat platform.Platform) int64 {
	data := readString(plat, statPath)
	for _, line := range strings.Split(data, "\n") {
		f := strings.Fields(line)
		if len(f) < 2 || f[0] != "btime" {
			continue
		}
		if v, err := strconv.ParseInt(f[1], 10, 64); err == nil {
			return v
		}
	}
	return 0
}

// rebootRequired reports whether a pending-reboot flag file exists (Debian and
// Ubuntu drop /run/reboot-required). Other distros lack a file marker, so this
// is best-effort and defaults to false.
func rebootRequired(plat platform.Platform) bool {
	for _, p := range []string{rebootRunPath, rebootVarPath} {
		if _, err := plat.FS.Stat(p); err == nil {
			return true
		}
	}
	return false
}

// machineIDHash returns the hex sha256 of the machine-id, or "" if unreadable.
func machineIDHash(plat platform.Platform) string {
	id := strings.TrimSpace(readString(plat, machineIDPath))
	if id == "" {
		id = strings.TrimSpace(readString(plat, dbusIDPath))
	}
	if id == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:])
}

func readString(plat platform.Platform, path string) string {
	data, err := plat.FS.ReadFile(path)
	if err != nil {
		return ""
	}
	return string(data)
}

func put(m map[string]string, key, val string) {
	if val != "" {
		m[key] = val
	}
}
