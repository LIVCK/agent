package hostinfo

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/LIVCK/agent/internal/platform"
	"github.com/LIVCK/agent/internal/platform/platformtest"
	"github.com/LIVCK/agent/internal/sender"
)

// newVirtPlatform builds a platform with the given files and an optional Exec.
// A nil exec models a host without systemd-detect-virt.
func newVirtPlatform(files map[string]string, exec platform.Exec) platform.Platform {
	fs := platformtest.NewMemFS()
	for p, body := range files {
		_ = fs.WriteFileAtomic(p, []byte(body), 0o644)
	}
	plat := platform.Platform{
		Clock: platformtest.NewClock(time.Unix(1000, 0)),
		FS:    fs,
		Host:  stubHost{name: "api-prod-1"},
	}
	if exec != nil {
		plat.Exec = exec
	}
	return plat
}

// detectVirtExec returns an Exec whose systemd-detect-virt prints out and exits
// with err (the tool exits non-zero exactly when it prints "none").
func detectVirtExec(out string, err error) *platformtest.Exec {
	e := platformtest.NewExec().AddPath(detectVirtBin)
	e.SetResponse([]byte(out), err, detectVirtBin)
	return e
}

func TestVirtualizationFromTool(t *testing.T) {
	plat := newVirtPlatform(nil, detectVirtExec("kvm\n", nil))
	if got := Virtualization(context.Background(), plat); got != "kvm" {
		t.Fatalf("Virtualization = %q, want kvm", got)
	}
}

// Bare metal is an ANSWER, not a gap: the tool prints "none" and exits non-zero,
// and that verdict has to survive as "none" so a fleet can be filtered on
// virt:none. Only a failed detection may yield "".
func TestVirtualizationBareMetalIsNoneNotEmpty(t *testing.T) {
	plat := newVirtPlatform(nil, detectVirtExec("none\n", errors.New("exit status 1")))
	if got := Virtualization(context.Background(), plat); got != virtNone {
		t.Fatalf("Virtualization = %q, want %q", got, virtNone)
	}
}

// A container on a VM reports the container: innermost wins, because the /proc
// the agent samples is the container's view.
func TestVirtualizationContainerBeatsVM(t *testing.T) {
	// Tool path: systemd-detect-virt itself answers with the innermost layer.
	plat := newVirtPlatform(nil, detectVirtExec("docker\n", nil))
	if got := Virtualization(context.Background(), plat); got != "docker" {
		t.Fatalf("tool: Virtualization = %q, want docker", got)
	}

	// Fallback path: a container marker outranks a hypervisor DMI vendor.
	plat = newVirtPlatform(map[string]string{
		systemdContainerPath: "lxc\n",
		dmiVendorPath:        "QEMU\n",
	}, nil)
	if got := Virtualization(context.Background(), plat); got != "lxc" {
		t.Fatalf("fallback: Virtualization = %q, want lxc", got)
	}

	plat = newVirtPlatform(map[string]string{
		dockerEnvPath: "",
		dmiVendorPath: "KVM\n",
	}, nil)
	if got := Virtualization(context.Background(), plat); got != "docker" {
		t.Fatalf("dockerenv: Virtualization = %q, want docker", got)
	}

	// A runtime systemd has no token for becomes container-other, never an
	// invented value that would fragment the fleet's tags.
	plat = newVirtPlatform(map[string]string{
		pid1CgroupPath: "0::/kubepods/pod123/abc\n",
	}, nil)
	if got := Virtualization(context.Background(), plat); got != virtContainerOther {
		t.Fatalf("kubepods: Virtualization = %q, want %q", got, virtContainerOther)
	}
}

func TestVirtualizationFallbackFromDMI(t *testing.T) {
	cases := []struct {
		name  string
		files map[string]string
		want  string
	}{
		{"vmware", map[string]string{dmiVendorPath: "VMware, Inc.\n"}, "vmware"},
		{"kvm", map[string]string{dmiVendorPath: "KVM\n"}, "kvm"},
		{"openstack", map[string]string{dmiVendorPath: "OpenStack Foundation\n"}, "kvm"},
		{"amazon", map[string]string{dmiVendorPath: "Amazon EC2\n"}, "amazon"},
		{"oracle", map[string]string{dmiVendorPath: "innotek GmbH\n"}, "oracle"},
		{"google", map[string]string{
			dmiVendorPath:  "Google\n",
			dmiProductPath: "Google Compute Engine\n",
		}, "google"},
		{"hyperv", map[string]string{
			dmiVendorPath:  "Microsoft Corporation\n",
			dmiProductPath: "Virtual Machine\n",
		}, "microsoft"},
		{"xen", map[string]string{hypervisorTypePath: "xen\n"}, "xen"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			plat := newVirtPlatform(c.files, nil)
			if got := Virtualization(context.Background(), plat); got != c.want {
				t.Fatalf("Virtualization = %q, want %q", got, c.want)
			}
		})
	}
}

// Microsoft's DMI vendor also sits on physical Microsoft hardware, so the vendor
// alone must not produce a microsoft VM verdict.
func TestVirtualizationMicrosoftVendorAloneIsNotAVM(t *testing.T) {
	plat := newVirtPlatform(map[string]string{
		dmiVendorPath:  "Microsoft Corporation\n",
		dmiProductPath: "Surface Laptop\n",
		cpuinfoPath:    "flags\t: fpu vme de pse\n",
	}, nil)
	if got := Virtualization(context.Background(), plat); got != virtNone {
		t.Fatalf("Virtualization = %q, want %q", got, virtNone)
	}
}

// An unnamed hypervisor is still a hypervisor: the x86 CPUID flag alone yields
// systemd's vm-other rather than a wrong "none".
func TestVirtualizationHypervisorFlagYieldsVMOther(t *testing.T) {
	plat := newVirtPlatform(map[string]string{
		dmiVendorPath: "Some Unknown Vendor\n",
		cpuinfoPath:   "processor\t: 0\nflags\t\t: fpu vme de pse hypervisor lm\n",
	}, nil)
	if got := Virtualization(context.Background(), plat); got != virtVMOther {
		t.Fatalf("Virtualization = %q, want %q", got, virtVMOther)
	}
}

func TestVirtualizationBareMetalFromDMI(t *testing.T) {
	plat := newVirtPlatform(map[string]string{
		dmiVendorPath:  "Dell Inc.\n",
		dmiProductPath: "PowerEdge R650\n",
		cpuinfoPath:    "processor\t: 0\nflags\t\t: fpu vme de pse lm\n",
	}, nil)
	if got := Virtualization(context.Background(), plat); got != virtNone {
		t.Fatalf("Virtualization = %q, want %q", got, virtNone)
	}
}

// No tool, no DMI, no container marker: there is no evidence either way, so the
// key must be omitted rather than guessed as bare metal.
func TestVirtualizationWithoutEvidenceIsEmpty(t *testing.T) {
	if got := Virtualization(context.Background(), newVirtPlatform(nil, nil)); got != "" {
		t.Fatalf("Virtualization = %q, want empty", got)
	}
}

// Garbage on stdout (an error message, a mangled read) is never shipped as a tag
// value; detection falls through to the file signals instead.
func TestVirtualizationRejectsUnparsableOutput(t *testing.T) {
	exec := detectVirtExec("Failed to detect: Permission denied\n", errors.New("exit status 1"))
	plat := newVirtPlatform(map[string]string{dmiVendorPath: "QEMU\n"}, exec)
	if got := Virtualization(context.Background(), plat); got != "qemu" {
		t.Fatalf("Virtualization = %q, want qemu (fallback)", got)
	}
}

// Failure modes must cost the key and nothing else: the rest of the profile has
// to arrive intact. A missing binary, a denied exec and a timeout are all "".
func TestVirtualizationFailureKeepsProfileIntact(t *testing.T) {
	files := map[string]string{
		osReleasePath: "ID=ubuntu\nVERSION_ID=\"24.04\"\n",
		kernelPath:    "6.8.0-31-generic\n",
		cpuinfoPath:   "processor\t: 0\nmodel name\t: Intel Xeon 8272CL\nflags\t\t: fpu lm\n",
		meminfoPath:   "MemTotal:       1000 kB\n",
		statPath:      "cpu 1 2 3\nbtime 1700000000\n",
	}

	execs := map[string]platform.Exec{
		"no binary":  nil,
		"exec fails": detectVirtExec("", errors.New("permission denied")),
		"timeout":    detectVirtExec("", context.DeadlineExceeded),
	}
	for name, ex := range execs {
		t.Run(name, func(t *testing.T) {
			plat := newVirtPlatform(files, ex)
			if got := Virtualization(context.Background(), plat); got != "" {
				t.Fatalf("Virtualization = %q, want empty", got)
			}

			m := NewProfiler(context.Background(), plat).Profile()
			if _, ok := m[VirtualizationKey]; ok {
				t.Errorf("profile must omit %q when detection fails, got %q", VirtualizationKey, m[VirtualizationKey])
			}
			for k, want := range map[string]string{
				"os":              "linux",
				"arch":            runtime.GOARCH,
				"hostname":        "api-prod-1",
				"kernel":          "6.8.0-31-generic",
				"distro":          "ubuntu",
				"distro_version":  "24.04",
				"cpu_model":       "Intel Xeon 8272CL",
				"ram_total_bytes": "1024000",
				"boot_time":       "1700000000",
			} {
				if m[k] != want {
					t.Errorf("profile[%q] = %q, want %q", k, m[k], want)
				}
			}
		})
	}
}

// An already-cancelled context must not hang or panic; it degrades to "".
func TestVirtualizationHonoursCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	plat := newVirtPlatform(nil, detectVirtExec("kvm\n", nil))
	if got := Virtualization(ctx, plat); got != "" {
		t.Fatalf("Virtualization = %q, want empty on a cancelled context", got)
	}
}

// THE expensive mistake: the runner re-reads the profile every five minutes on
// every host in the fleet. Re-forking systemd-detect-virt there would be pure
// waste, since virtualization cannot change while the kernel runs. The profiler
// must resolve it exactly ONCE and serve it from the static overlay forever.
func TestProfilerNeverReforksOnRefresh(t *testing.T) {
	exec := detectVirtExec("kvm\n", nil)
	plat := newVirtPlatform(map[string]string{
		osReleasePath: "ID=ubuntu\nVERSION_ID=\"24.04\"\n",
		statPath:      "cpu 1 2 3\nbtime 1700000000\n",
	}, exec)

	p := NewProfiler(context.Background(), plat)
	for i := 0; i < 5; i++ {
		m := p.Profile()
		if m[VirtualizationKey] != "kvm" {
			t.Fatalf("refresh %d: %s = %q, want kvm", i, VirtualizationKey, m[VirtualizationKey])
		}
	}

	calls := exec.Calls()
	if len(calls) != 1 {
		t.Fatalf("forked %s %d times across 5 profile refreshes, want exactly 1: %v", detectVirtBin, len(calls), calls)
	}
}

// A failed detection is cached too: a host without the binary must not fork (or
// retry) on every refresh either.
func TestProfilerDoesNotRetryFailedDetection(t *testing.T) {
	exec := detectVirtExec("", errors.New("permission denied"))
	plat := newVirtPlatform(nil, exec)

	p := NewProfiler(context.Background(), plat)
	for i := 0; i < 5; i++ {
		if _, ok := p.Profile()[VirtualizationKey]; ok {
			t.Fatalf("refresh %d: profile must omit %q", i, VirtualizationKey)
		}
	}
	if calls := exec.Calls(); len(calls) != 1 {
		t.Fatalf("retried detection %d times, want exactly 1: %v", len(calls), calls)
	}
}

// The static overlay carries the caller-supplied device names alongside
// virtualization, and survives every refresh without re-forking nvidia-smi.
func TestProfilerOverlaysStaticFacts(t *testing.T) {
	plat := newVirtPlatform(nil, detectVirtExec("kvm\n", nil))
	p := NewProfiler(context.Background(), plat)
	p.Set("gpu.00000000_01_00.0.name", "NVIDIA GeForce RTX 4080")
	p.Set("smart.nvme0.name", "")

	m := p.Profile()
	if m["gpu.00000000_01_00.0.name"] != "NVIDIA GeForce RTX 4080" {
		t.Errorf("device name lost: %v", m)
	}
	if _, ok := m["smart.nvme0.name"]; ok {
		t.Error("an empty value must not become a meta key")
	}
	if m[VirtualizationKey] != "kvm" {
		t.Errorf("%s = %q, want kvm", VirtualizationKey, m[VirtualizationKey])
	}

	// The returned map is the caller's own: mutating it must not leak back.
	m["gpu.00000000_01_00.0.name"] = "tampered"
	if p.Profile()["gpu.00000000_01_00.0.name"] != "NVIDIA GeForce RTX 4080" {
		t.Error("Profile() must hand out a fresh map, not shared state")
	}
}

// The new key changes the meta digest exactly ONCE per agent (the field is new);
// after that it must be rock stable, or every refresh would re-send the whole
// profile and defeat send-on-change.
func TestVirtualizationDigestIsStableAcrossRefreshes(t *testing.T) {
	plat := newVirtPlatform(map[string]string{
		osReleasePath: "ID=ubuntu\nVERSION_ID=\"24.04\"\n",
		kernelPath:    "6.8.0-31-generic\n",
		meminfoPath:   "MemTotal:       1000 kB\n",
		statPath:      "cpu 1 2 3\nbtime 1700000000\n",
	}, detectVirtExec("kvm\n", nil))

	p := NewProfiler(context.Background(), plat)
	first := p.Profile()
	if first[VirtualizationKey] != "kvm" {
		t.Fatalf("%s = %q, want kvm", VirtualizationKey, first[VirtualizationKey])
	}
	want := sender.MetaDigest(first)
	for i := 0; i < 5; i++ {
		if got := sender.MetaDigest(p.Profile()); got != want {
			t.Fatalf("refresh %d: digest %q != %q - the profile flaps", i, got, want)
		}
	}

	// And it really is part of the digest, so the one-time change lands.
	without := p.Profile()
	delete(without, VirtualizationKey)
	if sender.MetaDigest(without) == want {
		t.Fatal("virtualization must be part of the meta digest")
	}
}
