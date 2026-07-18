package hostinfo

import (
	"crypto/sha256"
	"encoding/hex"
	"runtime"
	"testing"
	"time"

	"github.com/LIVCK/agent/internal/platform"
	"github.com/LIVCK/agent/internal/platform/platformtest"
)

type stubHost struct{ name string }

func (h stubHost) Hostname() (string, error) { return h.name, nil }

func newPlatform(files map[string]string, host string) platform.Platform {
	fs := platformtest.NewMemFS()
	for p, body := range files {
		_ = fs.WriteFileAtomic(p, []byte(body), 0o644)
	}
	return platform.Platform{
		Clock: platformtest.NewClock(time.Unix(1000, 0)),
		FS:    fs,
		Host:  stubHost{name: host},
	}
}

func TestMetaProfile(t *testing.T) {
	plat := newPlatform(map[string]string{
		osReleasePath: "ID=ubuntu\nVERSION_ID=\"24.04\"\nPRETTY_NAME=\"Ubuntu 24.04 LTS\"\n",
		kernelPath:    "6.8.0-31-generic\n",
		cpuinfoPath:   "processor\t: 0\nmodel name\t: Intel Xeon 8272CL\n",
		meminfoPath:   "MemTotal:       1000 kB\n",
		statPath:      "cpu 1 2 3\nbtime 1700000000\n",
	}, "api-prod-1")

	m := Meta(plat)
	want := map[string]string{
		"os":              "linux",
		"arch":            runtime.GOARCH,
		"hostname":        "api-prod-1",
		"kernel":          "6.8.0-31-generic",
		"distro":          "ubuntu",
		"distro_version":  "24.04",
		"cpu_model":       "Intel Xeon 8272CL",
		"ram_total_bytes": "1024000",
		"boot_time":       "1700000000",
		"reboot_required": "false",
	}
	for k, v := range want {
		if m[k] != v {
			t.Errorf("meta[%q] = %q, want %q", k, m[k], v)
		}
	}
	if _, ok := m["ips_private"]; ok {
		t.Error("report meta must never carry ips_* (enroll-only)")
	}
}

func TestFingerprintHashesMachineID(t *testing.T) {
	plat := newPlatform(map[string]string{
		machineIDPath: "abc123def456\n",
		bootIDPath:    "boot-xyz\n",
	}, "api-prod-1")

	fp := Fingerprint(plat)
	sum := sha256.Sum256([]byte("abc123def456"))
	wantHash := hex.EncodeToString(sum[:])
	if fp["machine_id_hash"] != wantHash {
		t.Fatalf("machine_id_hash = %q, want %q", fp["machine_id_hash"], wantHash)
	}
	if fp["machine_id_hash"] == "abc123def456" {
		t.Fatal("machine-id must never be sent raw")
	}
	if fp["boot_id"] != "boot-xyz" || fp["hostname"] != "api-prod-1" {
		t.Fatalf("fingerprint fields wrong: %v", fp)
	}
}
