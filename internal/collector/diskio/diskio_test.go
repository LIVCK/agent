package diskio

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

func TestDiskioSeedsThenRates(t *testing.T) {
	fs := platformtest.NewMemFS()
	clock := platformtest.NewClock(time.Unix(1000, 0))
	// A loop device is present but must be excluded as virtual.
	write := func(reads, secR, writes, secW int) {
		fs.WriteFileAtomic(diskstatsPath, []byte(
			"   7       0 loop0 1 0 1 0 1 0 1 0 0 0 0\n"+
				"   8       0 sda "+itoa(reads)+" 0 "+itoa(secR)+" 0 "+itoa(writes)+" 0 "+itoa(secW)+" 0 0 0 0\n"), 0o644)
	}
	write(100, 200, 50, 400)
	c := New(fs, clock, config.Defaults)

	first, err := c.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 0 {
		t.Fatalf("first read must seed, got %v", first)
	}

	clock.Advance(10 * time.Second)
	write(200, 400, 100, 800) // deltas: reads 100, secR 200, writes 50, secW 400
	got, err := c.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	m := toMap(got)
	if m["sys.diskio.sda.read_bps"] != 200*512/10 {
		t.Fatalf("read_bps = %v, want %v", m["sys.diskio.sda.read_bps"], 200*512/10)
	}
	if m["sys.diskio.sda.write_bps"] != 400*512/10 {
		t.Fatalf("write_bps = %v", m["sys.diskio.sda.write_bps"])
	}
	if m["sys.diskio.sda.read_iops"] != 10 || m["sys.diskio.sda.write_iops"] != 5 {
		t.Fatalf("iops wrong: %v", m)
	}
	if _, ok := m["sys.diskio.loop0.read_bps"]; ok {
		t.Fatal("loop device must be excluded")
	}
}

func TestDiskioCounterResetSkipsDevice(t *testing.T) {
	fs := platformtest.NewMemFS()
	clock := platformtest.NewClock(time.Unix(1000, 0))
	fs.WriteFileAtomic(diskstatsPath, []byte("   8       0 sda 1000 0 2000 0 500 0 4000 0 0 0 0\n"), 0o644)
	c := New(fs, clock, config.Defaults)
	_, _ = c.Collect(context.Background())

	clock.Advance(10 * time.Second)
	// Counters dropped (reset): the device is skipped for this sample.
	fs.WriteFileAtomic(diskstatsPath, []byte("   8       0 sda 5 0 5 0 5 0 5 0 0 0 0\n"), 0o644)
	got, _ := c.Collect(context.Background())
	if len(got) != 0 {
		t.Fatalf("reset device must emit nothing, got %v", got)
	}
}

func TestParseDiskstatsDropsPartitionsAndVirtual(t *testing.T) {
	fixture := "" +
		"   8   0 sda 10 0 20 0 5 0 40 0 0\n" +
		"   8   1 sda1 5 0 10 0 2 0 20 0 0\n" + // partition of sda -> drop
		"   8   2 sda2 5 0 10 0 2 0 20 0 0\n" + // partition of sda -> drop
		" 259   0 nvme0n1 100 0 200 0 50 0 400 0 0\n" +
		" 259   1 nvme0n1p1 50 0 100 0 25 0 200 0 0\n" + // nvme partition -> drop
		" 179   0 mmcblk0 7 0 14 0 3 0 24 0 0\n" + // whole mmc disk -> keep (no p-partition sibling)
		" 252   0 dm-0 3 0 6 0 1 0 8 0 0\n" + // whole logical device -> keep
		"   7   0 loop0 1 0 1 0 1 0 1 0 0\n" // virtual -> skip

	got := parseDiskstats([]byte(fixture))
	want := map[string]bool{"sda": true, "nvme0n1": true, "mmcblk0": true, "dm-0": true}
	if len(got) != len(want) {
		t.Fatalf("device set = %v, want %v", keys(got), want)
	}
	for d := range want {
		if _, ok := got[d]; !ok {
			t.Errorf("expected whole device %q missing", d)
		}
	}
	for _, p := range []string{"sda1", "sda2", "nvme0n1p1", "loop0"} {
		if _, ok := got[p]; ok {
			t.Errorf("device %q should have been dropped", p)
		}
	}
}

func TestParseDiskstatsKeepsOrphanPartition(t *testing.T) {
	// A partition whose whole disk is absent from diskstats is kept, not hidden.
	got := parseDiskstats([]byte("   8   1 sdb1 5 0 10 0 2 0 20 0 0\n"))
	if _, ok := got["sdb1"]; !ok {
		t.Fatalf("orphan partition must be kept, got %v", keys(got))
	}
}

func TestParentDevice(t *testing.T) {
	// Real partition candidates that resolve to a real whole disk.
	cases := map[string]string{
		"sda1": "sda", "sda15": "sda", "vda2": "vda",
		"nvme0n1p1": "nvme0n1", "mmcblk0p2": "mmcblk0",
		// Names that can form no candidate at all.
		"sda": "", "dm-0": "",
		// Whole nvme/mmc disks form a lexical candidate that is never a real
		// device, so the data-driven filter keeps them (see parseDiskstats test).
		"nvme0n1": "nvme0n", "mmcblk0": "mmcblk",
	}
	for dev, want := range cases {
		if got := parentDevice(dev); got != want {
			t.Errorf("parentDevice(%q) = %q, want %q", dev, got, want)
		}
	}
}

func keys(m map[string]counters) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// itoa avoids strconv in the test body for readability.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
