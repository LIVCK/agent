package hostinfo

import (
	"context"
	"sync"

	"github.com/LIVCK/agent/internal/platform"
)

// Profiler produces the host profile that rides the enroll payload and
// decorates reports: the cheap procfs facts from Meta, re-read on every call,
// plus a static overlay of facts that cost a fork and cannot change while the
// kernel is running.
//
// The split is what makes the periodic re-read affordable. The runner re-reads
// the profile every DefaultProfileInterval (five minutes) on every host in the
// fleet, because a distro upgrade or a RAM resize has to reach the console
// without an agent restart - so Meta must run again. Forking
// systemd-detect-virt, nvidia-smi or smartctl again would buy nothing at all:
// virtualization and the device model names are decided before the agent even
// starts. They are therefore resolved ONCE - virtualization here in the
// constructor, the GPU/SMART display names by the caller via Set - and only
// overlaid afterwards. Profile() itself execs nothing; that is the invariant to
// keep, and hostinfo tests pin it.
//
// A failed detection is cached like any other result: no retry loop, no fork
// every five minutes on a host that has no such binary.
type Profiler struct {
	plat platform.Platform

	mu     sync.Mutex
	static map[string]string
}

// NewProfiler resolves the once-only facts and returns the profiler. This is the
// only place virtualization is detected; ctx bounds that single detection and
// nothing else.
func NewProfiler(ctx context.Context, plat platform.Platform) *Profiler {
	p := &Profiler{plat: plat, static: map[string]string{}}
	p.Set(VirtualizationKey, Virtualization(ctx, plat))
	return p
}

// Set records a fact resolved once at startup (the GPU/SMART display names).
// An empty value is dropped: an absent fact must not become an empty meta key,
// which the consumer would skip anyway after costing a meta digest change.
func (p *Profiler) Set(key, val string) {
	if key == "" || val == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.static[key] = val
}

// Profile returns a freshly built profile: the procfs facts read now, the static
// facts overlaid on top. The caller owns the returned map, so handing it to the
// sender never shares state with the next call.
func (p *Profiler) Profile() map[string]string {
	m := Meta(p.plat)
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, v := range p.static {
		m[k] = v
	}
	return m
}
