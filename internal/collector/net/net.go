// Package net reports per-interface throughput and error rates from
// /proc/net/dev. Every key is a rate over the elapsed wall time; the first read
// seeds the baseline and emits nothing, a new interface is seeded on first
// sight, and a backwards counter re-baselines that interface for one sample.
// Loopback is excluded by default via config.
package net

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/LIVCK/agent/internal/collector"
	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/platform"
)

const netdevPath = "/proc/net/dev"

// counters holds the four cumulative /proc/net/dev fields net reads.
type counters struct {
	rxBytes  uint64
	rxErrors uint64
	txBytes  uint64
	txErrors uint64
}

// Collector computes per-interface network rates.
type Collector struct {
	fs    platform.FS
	clock platform.Clock
	cfg   func() *config.Config

	prev     map[string]counters
	prevTime time.Time
	seeded   bool
}

// New builds a net collector.
func New(fs platform.FS, clock platform.Clock, cfg func() *config.Config) *Collector {
	return &Collector{fs: fs, clock: clock, cfg: cfg, prev: map[string]counters{}}
}

// Name returns "net".
func (*Collector) Name() string { return "net" }

// Available reports whether the net collector is enabled in the current config.
func (c *Collector) Available() bool { return c.cfg().Collectors.Net.Enabled }

// Collect reads /proc/net/dev and emits rx/tx throughput and error rates per
// interface, after excludes, capped to the configured interface count by
// busiest total throughput.
func (c *Collector) Collect(context.Context) ([]collector.Sample, error) {
	data, err := c.fs.ReadFile(netdevPath)
	if err != nil {
		return nil, err
	}
	ncfg := c.cfg().Collectors.Net
	excluded := toSet(ncfg.ExcludeIfaces)
	now := c.clock.Now()
	cur := parseNetdev(data, excluded)

	if !c.seeded {
		c.prev = cur
		c.prevTime = now
		c.seeded = true
		return nil, nil
	}
	dt := now.Sub(c.prevTime)

	type ifRate struct {
		iface            string
		rxBps, txBps     float64
		rxErrPs, txErrPs float64
		total            float64
	}
	var rates []ifRate
	for iface, cc := range cur {
		pc, ok := c.prev[iface]
		if !ok {
			continue
		}
		dRx, r1 := collector.CounterDelta(cc.rxBytes, pc.rxBytes)
		dTx, r2 := collector.CounterDelta(cc.txBytes, pc.txBytes)
		dRxE, r3 := collector.CounterDelta(cc.rxErrors, pc.rxErrors)
		dTxE, r4 := collector.CounterDelta(cc.txErrors, pc.txErrors)
		if r1 || r2 || r3 || r4 {
			continue
		}
		rxBps := collector.Rate(float64(dRx), dt)
		txBps := collector.Rate(float64(dTx), dt)
		rates = append(rates, ifRate{
			iface:   iface,
			rxBps:   rxBps,
			txBps:   txBps,
			rxErrPs: collector.Rate(float64(dRxE), dt),
			txErrPs: collector.Rate(float64(dTxE), dt),
			total:   rxBps + txBps,
		})
	}

	rates = collector.CapBySize(rates, ncfg.MaxIfaces,
		func(r ifRate) string { return r.iface },
		func(r ifRate) float64 { return r.total })

	used := make(map[string]bool, len(rates))
	var samples []collector.Sample
	for _, r := range rates {
		seg := collector.DedupeSegment(collector.NormalizeSegment(r.iface), used)
		base := "sys.net." + seg + "."
		samples = append(samples,
			collector.Sample{Key: base + "rx_bps", Value: r.rxBps},
			collector.Sample{Key: base + "tx_bps", Value: r.txBps},
			collector.Sample{Key: base + "rx_errors_ps", Value: r.rxErrPs},
			collector.Sample{Key: base + "tx_errors_ps", Value: r.txErrPs},
		)
	}

	c.prev = cur
	c.prevTime = now
	return samples, nil
}

// parseNetdev reads the four base counters per interface, skipping the two
// header lines and any excluded interface.
func parseNetdev(data []byte, excluded map[string]bool) map[string]counters {
	out := make(map[string]counters, 8)
	for _, line := range strings.Split(string(data), "\n") {
		i := strings.IndexByte(line, ':')
		if i < 0 {
			continue // header or blank line
		}
		iface := strings.TrimSpace(line[:i])
		if iface == "" || excluded[iface] || skipVirtualIface(iface) {
			continue
		}
		f := strings.Fields(line[i+1:])
		if len(f) < 11 {
			continue
		}
		out[iface] = counters{
			rxBytes:  parseU(f[0]),
			rxErrors: parseU(f[2]),
			txBytes:  parseU(f[8]),
			txErrors: parseU(f[10]),
		}
	}
	return out
}

// skipVirtualIface drops container/hypervisor virtual interfaces that only add
// chart noise: veth pairs, docker bridges (docker0 and user-defined br-<id>) and
// libvirt bridges (virbr*). It is prefix-based and always on, complementing the
// user-configurable exact ExcludeIfaces list (default lo). tun/tap/bond/wg are
// deliberately NOT matched: those are frequently real, monitored interfaces. A
// real bridge named br0/br1 is kept ("br-" requires the trailing dash docker
// uses).
func skipVirtualIface(iface string) bool {
	if iface == "docker0" {
		return true
	}
	for _, p := range []string{"veth", "br-", "virbr"} {
		if strings.HasPrefix(iface, p) {
			return true
		}
	}
	return false
}

func toSet(list []string) map[string]bool {
	m := make(map[string]bool, len(list))
	for _, s := range list {
		m[s] = true
	}
	return m
}

func parseU(s string) uint64 {
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	return v
}

var _ collector.Collector = (*Collector)(nil)
