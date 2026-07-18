package net

import (
	"context"
	"testing"
	"time"

	"github.com/LIVCK/agent/internal/collector"
	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/platform/platformtest"
)

func toMap(s []collector.Sample) map[string]float64 {
	m := map[string]float64{}
	for _, x := range s {
		m[x.Key] = x.Value
	}
	return m
}

const header = "Inter-|   Receive                    |  Transmit\n" +
	" face |bytes packets errs drop fifo frame compressed multicast|bytes packets errs drop fifo colls carrier compressed\n"

func TestSkipVirtualIface(t *testing.T) {
	skip := []string{"veth1234", "br-b4e2a7e7", "docker0", "virbr0", "virbr0-nic"}
	keep := []string{"eth0", "ens3", "br0", "bond0", "tun0", "tap0", "wg0", "enp1s0"}
	for _, i := range skip {
		if !skipVirtualIface(i) {
			t.Errorf("%q should be skipped as virtual", i)
		}
	}
	for _, i := range keep {
		if skipVirtualIface(i) {
			t.Errorf("%q is a real interface and must be kept", i)
		}
	}
}

func TestParseNetdevExcludesVirtual(t *testing.T) {
	body := header +
		"    lo:  1 1 0 0 0 0 0 0 1 1 0 0 0 0 0 0\n" +
		"  eth0:  1 1 0 0 0 0 0 0 1 1 0 0 0 0 0 0\n" +
		"   br0:  1 1 0 0 0 0 0 0 1 1 0 0 0 0 0 0\n" +
		"veth99:  1 1 0 0 0 0 0 0 1 1 0 0 0 0 0 0\n" +
		"br-abc:  1 1 0 0 0 0 0 0 1 1 0 0 0 0 0 0\n" +
		"docker0: 1 1 0 0 0 0 0 0 1 1 0 0 0 0 0 0\n"
	got := parseNetdev([]byte(body), map[string]bool{"lo": true})
	if _, ok := got["eth0"]; !ok {
		t.Error("eth0 must be kept")
	}
	if _, ok := got["br0"]; !ok {
		t.Error("real bridge br0 must be kept")
	}
	for _, v := range []string{"lo", "veth99", "br-abc", "docker0"} {
		if _, ok := got[v]; ok {
			t.Errorf("%q must be excluded", v)
		}
	}
}

func TestNetSeedsThenRatesAndExcludesLo(t *testing.T) {
	fs := platformtest.NewMemFS()
	clock := platformtest.NewClock(time.Unix(1000, 0))
	fs.WriteFileAtomic(netdevPath, []byte(header+
		"    lo:  100 1 0 0 0 0 0 0    50 1 0 0 0 0 0 0\n"+
		"  eth0: 1000 10 2 0 0 0 0 0 2000 20 3 0 0 0 0 0\n"), 0o644)
	c := New(fs, clock, config.Defaults)

	if first, err := c.Collect(context.Background()); err != nil || len(first) != 0 {
		t.Fatalf("first read must seed: %v %v", first, err)
	}

	clock.Advance(10 * time.Second)
	fs.WriteFileAtomic(netdevPath, []byte(header+
		"    lo:  100 1 0 0 0 0 0 0    50 1 0 0 0 0 0 0\n"+
		"  eth0: 3000 10 12 0 0 0 0 0 6000 20 8 0 0 0 0 0\n"), 0o644)
	got, err := c.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	m := toMap(got)
	if m["sys.net.eth0.rx_bps"] != 200 || m["sys.net.eth0.tx_bps"] != 400 {
		t.Fatalf("throughput wrong: %v", m)
	}
	if m["sys.net.eth0.rx_errors_ps"] != 1 || m["sys.net.eth0.tx_errors_ps"] != 0.5 {
		t.Fatalf("error rates wrong: %v", m)
	}
	if _, ok := m["sys.net.lo.rx_bps"]; ok {
		t.Fatal("lo must be excluded by default")
	}
}
