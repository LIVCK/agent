// Package registry wires the concrete system collectors into a collector
// Registry. It lives in its own package because it imports every collector
// implementation, which each import the collector package: putting the builder
// in package collector would be an import cycle. main.go calls Build once, so
// adding a new collector touches a single wiring line.
//
// Every collector reads its enabled flag from the live config through the cfg
// accessor, so a hot config change enables or disables a source on the next
// cycle without rebuilding the registry: a disabled collector reports
// Available()==false and the registry skips it.
package registry

import (
	"github.com/LIVCK/agent/internal/collector"
	"github.com/LIVCK/agent/internal/collector/cpu"
	"github.com/LIVCK/agent/internal/collector/disk"
	"github.com/LIVCK/agent/internal/collector/diskio"
	"github.com/LIVCK/agent/internal/collector/gpu"
	"github.com/LIVCK/agent/internal/collector/load"
	"github.com/LIVCK/agent/internal/collector/mem"
	netcollector "github.com/LIVCK/agent/internal/collector/net"
	"github.com/LIVCK/agent/internal/collector/probe"
	"github.com/LIVCK/agent/internal/collector/psi"
	"github.com/LIVCK/agent/internal/collector/self"
	"github.com/LIVCK/agent/internal/collector/smart"
	"github.com/LIVCK/agent/internal/collector/swap"
	"github.com/LIVCK/agent/internal/collector/system"
	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/platform"
)

// Build returns a Registry with every system collector registered. cfg is the
// live config accessor (config.Manager.Current); each collector reads its knobs
// and enabled flag from it on every cycle. emitter receives the disk_full
// lifecycle event the disk collector raises (it may be nil). self is registered
// last and is always available.
func Build(plat platform.Platform, cfg func() *config.Config, emitter disk.Emitter) *collector.Registry {
	r := collector.NewRegistry()
	r.Register(cpu.New(plat.FS, cfg))
	r.Register(load.New(plat.FS, cfg))
	r.Register(mem.New(plat.FS, cfg))
	r.Register(swap.New(plat.FS, cfg))
	r.Register(psi.New(plat.FS, cfg))
	r.Register(system.New(plat.FS, cfg))
	r.Register(disk.New(plat.FS, cfg, emitter))
	r.Register(diskio.New(plat.FS, plat.Clock, cfg))
	r.Register(netcollector.New(plat.FS, plat.Clock, cfg))
	// Reachability/latency probes (tcp/http/dns). Interval-sampled + config-gated (no targets ⇒ off),
	// so it costs nothing until the server declares targets.
	r.Register(probe.New(cfg, plat.Clock))
	// Opt-in-by-presence hardware telemetry: registered but Available()-gated on
	// their feature flag AND the underlying tool/sysfs being present, so they
	// cost nothing on a host that has neither enabled nor the hardware.
	r.Register(gpu.New(plat.FS, plat.Exec, cfg))
	r.Register(smart.New(plat.Exec, cfg))
	r.Register(self.New(plat.FS, plat.Clock))
	return r
}
