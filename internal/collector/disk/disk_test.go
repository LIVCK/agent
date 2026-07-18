package disk

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/LIVCK/agent/internal/collector"
	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/platform/platformtest"
	"github.com/LIVCK/agent/pkg/wire"
)

// capEmitter records disk_full events for the hysteresis test.
type capEmitter struct{ full []map[string]string }

func (e *capEmitter) Emit(t wire.EventType, meta map[string]string) {
	if t == wire.EventType_EVENT_TYPE_DISK_FULL {
		e.full = append(e.full, meta)
	}
}

// atPct returns a statfs result whose df used_pct equals pct (Blocks=100, so
// used=100-free and avail=free give used/(used+avail) = pct).
func atPct(pct uint64) Statfs {
	free := 100 - pct
	return Statfs{Blocks: 100, Bfree: free, Bavail: free, Bsize: 4096, Files: 10, Ffree: 5}
}

type fakeStatfs struct {
	res   map[string]Statfs
	block map[string]chan struct{}
}

func (f *fakeStatfs) fn(path string) (Statfs, error) {
	if f.block != nil {
		if ch, ok := f.block[path]; ok {
			<-ch // hang like a dead network mount until released
		}
	}
	if r, ok := f.res[path]; ok {
		return r, nil
	}
	return Statfs{}, errors.New("no such mount")
}

func toMap(s []collector.Sample) map[string]float64 {
	m := map[string]float64{}
	for _, x := range s {
		m[x.Key] = x.Value
	}
	return m
}

const healthyMountinfo = "36 35 8:1 / / rw,relatime - ext4 /dev/sda1 rw\n" +
	"40 36 8:2 / /var rw,relatime - ext4 /dev/sda2 rw\n"

func TestDiskHealthyMountsEmitGauges(t *testing.T) {
	fs := platformtest.NewMemFS()
	_ = fs.WriteFileAtomic(mountinfoPath, []byte(healthyMountinfo), 0o644)
	stat := &fakeStatfs{res: map[string]Statfs{
		"/":    {Blocks: 1000, Bfree: 400, Bavail: 300, Bsize: 4096, Files: 100, Ffree: 50},
		"/var": {Blocks: 500, Bfree: 250, Bavail: 250, Bsize: 4096, Files: 200, Ffree: 100},
	}}
	c := NewWithStatfs(fs, config.Defaults, nil, stat.fn, 2*time.Second)

	got, err := c.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	m := toMap(got)
	// root: total=1000*4096, used=(1000-400)*4096, used_pct=600/(600+300)=66.67.
	if m["sys.disk._root.total_bytes"] != 1000*4096 {
		t.Fatalf("root total = %v", m["sys.disk._root.total_bytes"])
	}
	if m["sys.disk._root.used_bytes"] != 600*4096 {
		t.Fatalf("root used = %v", m["sys.disk._root.used_bytes"])
	}
	if !near(m["sys.disk._root.used_pct"], 66.666) {
		t.Fatalf("root used_pct = %v, want ~66.67", m["sys.disk._root.used_pct"])
	}
	if m["sys.disk._root.inodes_used_pct"] != 50 {
		t.Fatalf("root inodes_used_pct = %v, want 50", m["sys.disk._root.inodes_used_pct"])
	}
	if _, ok := m["sys.disk._var.used_pct"]; !ok {
		t.Fatalf("var mount missing: %v", m)
	}
	if m["sys.agent.stuck_mounts"] != 0 {
		t.Fatalf("stuck_mounts = %v, want 0", m["sys.agent.stuck_mounts"])
	}
}

func TestDiskStuckMountIsDroppedAndCounted(t *testing.T) {
	fs := platformtest.NewMemFS()
	_ = fs.WriteFileAtomic(mountinfoPath, []byte(
		"36 35 8:1 / / rw,relatime - ext4 /dev/sda1 rw\n"+
			"50 36 0:30 / /mnt/nfs rw,relatime - nfs 1.2.3.4:/export rw\n"), 0o644)
	block := make(chan struct{})
	t.Cleanup(func() { close(block) }) // release the hung probe goroutine
	stat := &fakeStatfs{
		res:   map[string]Statfs{"/": {Blocks: 100, Bfree: 50, Bavail: 50, Bsize: 4096, Files: 10, Ffree: 5}},
		block: map[string]chan struct{}{"/mnt/nfs": block},
	}
	c := NewWithStatfs(fs, config.Defaults, nil, stat.fn, 50*time.Millisecond)

	got, err := c.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	m := toMap(got)
	if _, ok := m["sys.disk._root.used_pct"]; !ok {
		t.Fatalf("healthy root mount must still emit: %v", m)
	}
	if _, ok := m["sys.disk._mnt_nfs.used_pct"]; ok {
		t.Fatal("stuck mount keys must be dropped (no zero-fake)")
	}
	if m["sys.agent.stuck_mounts"] != 1 {
		t.Fatalf("stuck_mounts = %v, want 1", m["sys.agent.stuck_mounts"])
	}
}

func TestDiskExcludeNetworkFS(t *testing.T) {
	fs := platformtest.NewMemFS()
	_ = fs.WriteFileAtomic(mountinfoPath, []byte(
		"36 35 8:1 / / rw,relatime - ext4 /dev/sda1 rw\n"+
			"50 36 0:30 / /mnt/nfs rw,relatime - nfs 1.2.3.4:/export rw\n"), 0o644)
	cfg := func() *config.Config {
		c := config.Defaults()
		c.Collectors.Disk.ExcludeNetworkFS = true
		return c
	}
	stat := &fakeStatfs{res: map[string]Statfs{"/": {Blocks: 100, Bfree: 50, Bavail: 50, Bsize: 4096, Files: 10, Ffree: 5}}}
	c := NewWithStatfs(fs, cfg, nil, stat.fn, 2*time.Second)

	got, err := c.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	m := toMap(got)
	if _, ok := m["sys.disk._mnt_nfs.used_pct"]; ok {
		t.Fatal("nfs mount must be excluded when exclude_network_fs is set")
	}
	if _, ok := m["sys.disk._root.used_pct"]; !ok {
		t.Fatal("root mount must remain")
	}
}

func TestDiskFullEdgeTriggeredWithHysteresis(t *testing.T) {
	fs := platformtest.NewMemFS()
	_ = fs.WriteFileAtomic(mountinfoPath, []byte("36 35 8:1 / / rw,relatime - ext4 /dev/sda1 rw\n"), 0o644)
	stat := &fakeStatfs{res: map[string]Statfs{"/": atPct(50)}}
	em := &capEmitter{}
	c := NewWithStatfs(fs, config.Defaults, em, stat.fn, 2*time.Second)

	step := func(pct uint64) {
		stat.res["/"] = atPct(pct)
		if _, err := c.Collect(context.Background()); err != nil {
			t.Fatal(err)
		}
	}

	step(50) // armed, below full
	if len(em.full) != 0 {
		t.Fatalf("no event below full, got %d", len(em.full))
	}
	step(100) // crosses into full -> exactly one event
	if len(em.full) != 1 {
		t.Fatalf("crossing 100%% must emit exactly one event, got %d", len(em.full))
	}
	if em.full[0]["mount"] != "/" || em.full[0]["used_pct"] != "100.0" {
		t.Fatalf("event meta wrong: %v", em.full[0])
	}
	step(100) // stays full -> no repeat
	step(97)  // between recover and full -> still latched, no repeat
	if len(em.full) != 1 {
		t.Fatalf("staying full must not repeat, got %d", len(em.full))
	}
	step(90) // recovers below 95 -> re-arm
	if len(em.full) != 1 {
		t.Fatalf("recovery must not emit, got %d", len(em.full))
	}
	step(100) // crosses again -> second event
	if len(em.full) != 2 {
		t.Fatalf("re-cross must emit a new event, got %d", len(em.full))
	}
}

const pseudoFsMountinfo = "36 35 8:1 / / rw,relatime - ext4 /dev/sda1 rw\n" +
	"40 36 0:5 / /dev rw,nosuid - devtmpfs devtmpfs rw\n" +
	"41 40 0:20 / /sys/fs/cgroup ro,nosuid - cgroup2 cgroup2 ro\n" +
	"42 40 0:21 / /sys/kernel/debug rw - debugfs debugfs rw\n" +
	"43 40 0:22 / /dev/mqueue rw - mqueue mqueue rw\n" +
	"45 40 0:24 / /run rw,nosuid - tmpfs tmpfs rw\n" +
	"44 40 0:23 / /boot/efi rw - vfat /dev/sda15 rw\n"

func diskFixtureStatfs() *fakeStatfs {
	return &fakeStatfs{res: map[string]Statfs{
		"/":         {Blocks: 100, Bfree: 50, Bavail: 50, Bsize: 4096, Files: 10, Ffree: 5},
		"/run":      {Blocks: 30, Bfree: 20, Bavail: 20, Bsize: 4096, Files: 6, Ffree: 4},
		"/boot/efi": {Blocks: 20, Bfree: 10, Bavail: 10, Bsize: 4096, Files: 4, Ffree: 2},
	}}
}

func TestDiskAlwaysExcludesPseudoFilesystems(t *testing.T) {
	fs := platformtest.NewMemFS()
	_ = fs.WriteFileAtomic(mountinfoPath, []byte(pseudoFsMountinfo), 0o644)
	// Empty exclude_fstypes proves the pseudo-fs filter is always-on, not config.
	cfg := func() *config.Config {
		c := config.Defaults()
		c.Collectors.Disk.ExcludeFstypes = nil
		return c
	}
	c := NewWithStatfs(fs, cfg, nil, diskFixtureStatfs().fn, 2*time.Second)

	got, err := c.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	m := toMap(got)
	// Real filesystems stay, including tmpfs (config-driven, not in empty list).
	for _, seg := range []string{"_root", "_boot_efi", "_run"} {
		if _, ok := m["sys.disk."+seg+".used_pct"]; !ok {
			t.Errorf("%q must be monitored with empty exclude_fstypes", seg)
		}
	}
	// Pure pseudo-filesystems are excluded regardless of config.
	for _, seg := range []string{"_dev", "_sys_fs_cgroup", "_sys_kernel_debug", "_dev_mqueue"} {
		if _, ok := m["sys.disk."+seg+".used_pct"]; ok {
			t.Errorf("pseudo-fs %q must be excluded even with empty config", seg)
		}
	}
}

func TestDiskTmpfsExcludableViaConfig(t *testing.T) {
	fs := platformtest.NewMemFS()
	_ = fs.WriteFileAtomic(mountinfoPath, []byte(pseudoFsMountinfo), 0o644)
	// The default config lists tmpfs, so /run is excluded here.
	c := NewWithStatfs(fs, config.Defaults, nil, diskFixtureStatfs().fn, 2*time.Second)

	got, err := c.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	m := toMap(got)
	if _, ok := m["sys.disk._run.used_pct"]; ok {
		t.Error("tmpfs /run must be excluded when exclude_fstypes lists tmpfs")
	}
	if _, ok := m["sys.disk._root.used_pct"]; !ok {
		t.Error("ext4 root must remain")
	}
}

func near(v, want float64) bool { return v >= want-0.01 && v <= want+0.01 }

// Broad-robustness: on Docker/k8s/LVM/timeshift hosts the same device is mounted at many paths
// (bind mounts) — df collapses those to one; so must we. And zero-capacity pseudo mounts (gvfs,
// portal) must never appear as "disks".
func TestDiskDedupesBindMountsByDeviceAndSkipsZeroCapacity(t *testing.T) {
	fs := platformtest.NewMemFS()
	_ = fs.WriteFileAtomic(mountinfoPath, []byte(
		"36 35 8:2 / / rw,relatime - ext4 /dev/sda2 rw\n"+
			"40 36 8:1 / /media/backups rw,relatime - ext4 /dev/sdb1 rw\n"+
			"41 36 8:1 /snap /run/timeshift/776583/backup rw,relatime - ext4 /dev/sdb1 rw\n"+
			"42 36 0:60 / /run/user/1000/gvfs rw,relatime - fuse.gvfsd-fuse gvfsd-fuse rw\n"), 0o644)
	stat := &fakeStatfs{res: map[string]Statfs{
		"/":                   {Blocks: 1000, Bfree: 400, Bavail: 300, Bsize: 4096, Files: 100, Ffree: 50},
		"/media/backups":      {Blocks: 2000, Bfree: 100, Bavail: 100, Bsize: 4096, Files: 100, Ffree: 90},
		"/run/user/1000/gvfs": {Blocks: 0, Bfree: 0, Bavail: 0, Bsize: 4096, Files: 0, Ffree: 0},
	}}
	c := NewWithStatfs(fs, config.Defaults, nil, stat.fn, 2*time.Second)

	got, err := c.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	m := toMap(got)

	if _, ok := m["sys.disk._media_backups.used_pct"]; !ok {
		t.Fatalf("canonical device mount /media/backups must be present: %v", m)
	}
	if _, ok := m["sys.disk._run_timeshift_776583_backup.used_pct"]; ok {
		t.Fatal("a bind mount of an already-listed device (8:1) must be collapsed")
	}
	if _, ok := m["sys.disk._run_user_1000_gvfs.used_pct"]; ok {
		t.Fatal("a zero-capacity pseudo mount (gvfs) must be dropped")
	}
}
