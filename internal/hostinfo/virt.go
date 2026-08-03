package hostinfo

import (
	"context"
	"strings"
	"time"

	"github.com/LIVCK/agent/internal/platform"
)

// VirtualizationKey is the profile key the detected technology rides under. The
// consumer turns it into the reserved virt:<value> system tag, so the value has
// to mean the same thing on every host in a fleet.
const VirtualizationKey = "virtualization"

const (
	detectVirtBin = "systemd-detect-virt"
	// detectVirtTimeout hard-bounds the single fork. It mirrors the gpu/smart
	// exec deadline; the tool answers from CPUID and a couple of sysfs reads and
	// is done in microseconds, so anything slower is a wedged host.
	detectVirtTimeout = 2 * time.Second
	// maxVirtLen mirrors the meta cap the ingest applies to virt (SERVER-AGENT
	// 3.5: distro/kernel/arch/virt <= 64).
	maxVirtLen = 64

	// virtNone is the bare-metal answer. It is a real answer, not a missing one:
	// "" means detection failed and the key is left out entirely, so the fleet
	// can tell "this host is bare metal" from "nobody could tell". The consumer
	// skips blank values, so "" degrades to no tag rather than to a wrong tag.
	virtNone = "none"
	// virtVMOther / virtContainerOther are systemd's own catch-alls for "it is
	// virtualized but the technology has no name here". Inventing a token
	// instead would fragment the fleet's virt: tags.
	virtVMOther        = "vm-other"
	virtContainerOther = "container-other"
)

const (
	systemdContainerPath = "/run/systemd/container"
	dockerEnvPath        = "/.dockerenv"
	pid1CgroupPath       = "/proc/1/cgroup"
	dmiVendorPath        = "/sys/class/dmi/id/sys_vendor"
	dmiProductPath       = "/sys/class/dmi/id/product_name"
	hypervisorTypePath   = "/sys/hypervisor/type"
)

// Virtualization reports the virtualization technology of this host in
// systemd-detect-virt's vocabulary: kvm, qemu, vmware, microsoft, xen, amazon,
// google, oracle, lxc, docker, podman and friends, "none" on bare metal, and the
// vm-other / container-other catch-alls. Borrowing that vocabulary verbatim is
// the point - two hosts on the same hypervisor must produce the same virt: tag,
// and systemd's list is the one every admin already reads.
//
// Detection order:
//
//  1. fork systemd-detect-virt (present on every distro in the v1 support
//     matrix, and the only source that can tell amazon from plain kvm),
//  2. unprivileged file signals when that binary is missing - a stripped image,
//     a non-systemd distro, a mangled PATH.
//
// A container on a VM reports the container. That is what systemd-detect-virt
// answers (innermost wins), and it is the honest one: the /proc this agent
// samples is the container's namespaced view, not the KVM host's, so calling the
// host kvm would label metrics that never describe that machine.
//
// Every failure mode - no binary, no permission, timeout, unparsable output -
// returns "". The profile is worth more than this one key, so nothing in here
// returns an error or takes the rest of the profile down with it.
//
// The caller must resolve this ONCE per process (see Profiler): the technology
// cannot change while the kernel is running, and the profile is re-read every
// five minutes on every host.
func Virtualization(ctx context.Context, plat platform.Platform) string {
	if v := detectVirtTool(ctx, plat.Exec); v != "" {
		return v
	}
	return detectVirtFiles(plat)
}

// detectVirtTool runs systemd-detect-virt once and returns its verdict. The
// exit status is deliberately ignored: the tool exits non-zero precisely when it
// found nothing ("none"), and Exec.Run hands back stdout either way, so the
// answer is read off stdout and validated instead.
func detectVirtTool(ctx context.Context, ex platform.Exec) string {
	if ex == nil {
		return ""
	}
	if _, ok := ex.LookPath(detectVirtBin); !ok {
		return ""
	}
	cctx, cancel := context.WithTimeout(ctx, detectVirtTimeout)
	defer cancel()
	out, _ := ex.Run(cctx, detectVirtBin)
	return virtToken(string(out))
}

// detectVirtFiles is the no-fork fallback. Containers are checked first so the
// innermost layer wins, exactly like the tool. The bare-metal verdict comes last
// and only with evidence.
func detectVirtFiles(plat platform.Platform) string {
	if v := containerFromFiles(plat); v != "" {
		return v
	}
	if v := vmFromDMI(plat); v != "" {
		return v
	}
	if hypervisorCPUFlag(plat) {
		return virtVMOther
	}
	// Claim bare metal only when the DMI table was actually readable: a host that
	// exposes its board vendor and neither a hypervisor vendor nor the hypervisor
	// CPU flag is bare metal. Without DMI there is no evidence either way, and an
	// omitted key beats a wrong "none".
	if strings.TrimSpace(readString(plat, dmiVendorPath)) != "" {
		return virtNone
	}
	return ""
}

// containerMarkers maps a fragment of PID 1's cgroup path to the technology.
// Only names systemd itself would print are used; a runtime systemd has no token
// for (containerd under kubernetes) becomes container-other.
var containerMarkers = []struct{ marker, virt string }{
	{"/docker", "docker"},
	{"libpod", "podman"},
	{"/lxc", "lxc"},
	{"kubepods", virtContainerOther},
	{"containerd", virtContainerOther},
}

// containerFromFiles names the container flavour from files alone. It is not
// shared with doctor.containerKind on purpose: doctor answers "may the agent run
// here at all" and may report a marker name, while this answers "which token
// goes on the wire" and must stay inside systemd's vocabulary.
func containerFromFiles(plat platform.Platform) string {
	// systemd records the flavour here for every process to read, in exactly the
	// string systemd-detect-virt would print - the best signal short of the fork.
	if v := virtToken(readString(plat, systemdContainerPath)); v != "" && v != virtNone {
		return v
	}
	if _, err := plat.FS.Stat(dockerEnvPath); err == nil {
		return "docker"
	}
	cgroup := readString(plat, pid1CgroupPath)
	for _, m := range containerMarkers {
		if strings.Contains(cgroup, m.marker) {
			return m.virt
		}
	}
	return ""
}

// dmiVirt maps a lowercased DMI vendor/product prefix to the technology systemd
// names it. The table mirrors systemd's dmi_vendor_table so the fallback lands
// on the same token the tool would have produced on that host.
var dmiVirt = []struct{ prefix, virt string }{
	{"kvm", "kvm"},
	{"openstack", "kvm"},
	{"kubevirt", "kvm"},
	{"amazon ec2", "amazon"},
	{"qemu", "qemu"},
	{"vmware", "vmware"},
	{"vmw", "vmware"},
	{"innotek gmbh", "oracle"},
	{"virtualbox", "oracle"},
	{"oracle corporation", "oracle"},
	{"apple virtualization", "apple"},
	{"parallels", "parallels"},
	{"bochs", "bochs"},
	{"bhyve", "bhyve"},
	{"hyper-v", "microsoft"},
	{"google", "google"},
	{"xen", "xen"},
}

// vmFromDMI names the hypervisor from the SMBIOS strings the kernel exports
// world-readable under /sys/class/dmi/id. Every hypervisor brands them; that is
// how the tool identifies a guest once CPUID is inconclusive.
func vmFromDMI(plat platform.Platform) string {
	// Xen PV has no useful DMI table but always exports its type here.
	if strings.EqualFold(strings.TrimSpace(readString(plat, hypervisorTypePath)), "xen") {
		return "xen"
	}
	vendor := strings.ToLower(strings.TrimSpace(readString(plat, dmiVendorPath)))
	product := strings.ToLower(strings.TrimSpace(readString(plat, dmiProductPath)))

	// Hyper-V brands the vendor plainly on some builds and only the product name
	// on others. The vendor alone is not enough: "Microsoft Corporation" also
	// sits on Microsoft's physical hardware, so the pair has to match.
	if strings.HasPrefix(vendor, "microsoft corporation") && strings.Contains(product, "virtual machine") {
		return "microsoft"
	}
	for _, e := range dmiVirt {
		if strings.HasPrefix(vendor, e.prefix) || strings.HasPrefix(product, e.prefix) {
			return e.virt
		}
	}
	return ""
}

// hypervisorCPUFlag reports the x86 "hypervisor" CPUID bit the kernel surfaces
// in /proc/cpuinfo. It proves a hypervisor is there but never names it, so it
// only ever yields vm-other. arm64 has no such flag, which is why a missing flag
// is not treated as evidence of bare metal - that verdict rests on DMI.
func hypervisorCPUFlag(plat platform.Platform) bool {
	for _, line := range strings.Split(readString(plat, cpuinfoPath), "\n") {
		k, v, ok := strings.Cut(line, ":")
		if !ok || strings.TrimSpace(k) != "flags" {
			continue
		}
		for _, f := range strings.Fields(v) {
			if f == "hypervisor" {
				return true
			}
		}
	}
	return false
}

// virtToken validates one candidate technology name. systemd prints a single
// lowercase token of letters, digits and dashes ("lxc-libvirt", "vm-other");
// anything else on stdout is an error message or a mangled read and is rejected
// rather than shipped as a tag value.
func virtToken(raw string) string {
	s := strings.ToLower(strings.TrimSpace(raw))
	if s == "" || len(s) > maxVirtLen {
		return ""
	}
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-':
		default:
			return ""
		}
	}
	return s
}
