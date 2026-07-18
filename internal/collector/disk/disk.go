// Package disk reports per-mount space and inode usage. Mounts are classified
// from /proc/self/mountinfo (a non-blocking file read) so the exclude filters
// never touch statfs; only the surviving mounts are probed with statfs, and that
// probe runs off the sample path: at most one outstanding probe per mount, each
// bounded by a deadline. A mount whose probe does not return in time is marked
// stuck, its keys are dropped for that report (no zero-fake), and it is counted
// in sys.agent.stuck_mounts; no new probe starts until the hung one returns.
package disk

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/LIVCK/agent/internal/collector"
	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/platform"
	"github.com/LIVCK/agent/pkg/wire"
)

const mountinfoPath = "/proc/self/mountinfo"

// defaultStuckDeadline bounds how long Collect waits for a freshly started
// statfs probe before declaring the mount stuck (2-5s is a reasonable range).
const defaultStuckDeadline = 3 * time.Second

// disk_full is edge-triggered with hysteresis: it fires once when a mount
// crosses into full and re-arms only after it recovers below the lower
// threshold, so a mount sitting at 100% does not emit an event every report.
const (
	fullThreshold    = 100
	recoverThreshold = 95
)

// Emitter raises a disk_full lifecycle event. It matches internal/event.Emitter
// without importing it, mirroring the buffer and lifecycle packages. It may be
// nil (the collector then reports gauges only).
type Emitter interface {
	Emit(t wire.EventType, meta map[string]string)
}

// networkFstypes are treated as network filesystems for exclude_network_fs.
// fuse.* is matched by prefix separately.
var networkFstypes = map[string]bool{
	"nfs": true, "nfs4": true, "cifs": true, "smbfs": true,
	"ceph": true, "glusterfs": true, "9p": true,
}

// pseudoFstypes are the kernel's pure pseudo-filesystems: never real storage,
// always noise on a disk chart. They are excluded unconditionally (an always-on
// built-in filter, like the net collector's virtual-interface skip), NOT via the
// user-facing exclude_fstypes config, because a code-default change there would
// never reach an already-enrolled agent (it does not bump config_version) and no
// config should be able to un-exclude them. The user-overridable RAM/image
// filesystems (tmpfs, overlay, ...) stay in config.DefaultExcludeFstypes.
var pseudoFstypes = map[string]bool{
	"proc": true, "sysfs": true, "cgroup": true, "cgroup2": true,
	"bpf": true, "pstore": true, "tracefs": true, "configfs": true,
	"debugfs": true, "securityfs": true, "efivarfs": true, "mqueue": true,
	"hugetlbfs": true, "binfmt_misc": true, "devpts": true, "devtmpfs": true,
	"nsfs": true, "fusectl": true, "autofs": true,
	// Desktop userspace fuse mounts (GNOME gvfs, xdg-document-portal). They report the BACKING
	// filesystem's statfs (so a zero-capacity guard misses them) but are never real storage — they
	// would masquerade as a duplicate of the root disk. Real fuse storage (fuse.ntfs, fuse.exfat)
	// and network fuse (fuse.sshfs) keep their own fstype and are not listed here.
	"fuse.gvfsd-fuse": true, "fuse.portal": true, "fuse.gvfsd": true,
}

// mountMeta is what mountinfo tells us about a mount without touching statfs.
type mountMeta struct {
	path   string
	fstype string
	// dev is the mountinfo major:minor (st_dev). Two mounts sharing it are the same underlying
	// filesystem (a bind mount, a btrfs subvolume) — we keep only the canonical one, like df does.
	dev string
}

// probeResult carries one statfs outcome back from a probe goroutine.
type probeResult struct {
	path string
	res  Statfs
	err  error
}

// mountState tracks the outstanding probe and the last good result per mount.
// full is the disk_full hysteresis latch: true between crossing fullThreshold
// and recovering below recoverThreshold.
type mountState struct {
	pending bool
	stuck   bool
	have    bool
	full    bool
	last    Statfs
	lastErr error
}

// Collector reports disk usage with the stuck-mount guard.
type Collector struct {
	fs            platform.FS
	cfg           func() *config.Config
	statfs        StatfsFunc
	stuckDeadline time.Duration
	emitter       Emitter

	states  map[string]*mountState
	results chan probeResult
}

// New builds a disk collector using the real statfs syscall. emitter may be nil;
// when set it receives disk_full lifecycle events.
func New(fs platform.FS, cfg func() *config.Config, emitter Emitter) *Collector {
	return NewWithStatfs(fs, cfg, emitter, realStatfs, defaultStuckDeadline)
}

// NewWithStatfs builds a disk collector with an injectable statfs and deadline,
// used by tests to simulate a hung mount without a real filesystem.
func NewWithStatfs(fs platform.FS, cfg func() *config.Config, emitter Emitter, statfs StatfsFunc, stuckDeadline time.Duration) *Collector {
	if stuckDeadline <= 0 {
		stuckDeadline = defaultStuckDeadline
	}
	return &Collector{
		fs:            fs,
		cfg:           cfg,
		statfs:        statfs,
		stuckDeadline: stuckDeadline,
		emitter:       emitter,
		states:        map[string]*mountState{},
		results:       make(chan probeResult, 128),
	}
}

// Name returns "disk".
func (*Collector) Name() string { return "disk" }

// Available reports whether the disk collector is enabled in the current config.
func (c *Collector) Available() bool { return c.cfg().Collectors.Disk.Enabled }

// Collect classifies mounts, probes them off the sample path, and emits the
// space/inode gauges for every mount whose probe returned in time. Stuck mounts
// are dropped and counted.
func (c *Collector) Collect(ctx context.Context) ([]collector.Sample, error) {
	data, err := c.fs.ReadFile(mountinfoPath)
	if err != nil {
		return nil, err
	}
	dcfg := c.cfg().Collectors.Disk
	cur := c.selectMounts(data, dcfg)

	c.drainCompleted()
	c.reconcile(cur)
	fresh := c.startProbes(cur)
	c.waitForProbes(ctx, fresh)

	return c.buildSamples(cur), nil
}

// selectMounts parses mountinfo, applies the fstype/mount/network excludes, and
// caps the probe set to max_mounts. The cap is by mount-path depth then name
// (top-level mounts first) so it is deterministic without a statfs size, which
// we cannot know before probing.
func (c *Collector) selectMounts(data []byte, dcfg config.DiskConfig) map[string]mountMeta {
	excludeFstype := toSet(dcfg.ExcludeFstypes)
	excludeMount := toSet(dcfg.ExcludeMounts)

	var mounts []mountMeta
	seen := map[string]bool{}
	for _, line := range strings.Split(string(data), "\n") {
		m, ok := parseMountinfoLine(line)
		if !ok {
			continue
		}
		if seen[m.path] {
			continue // a bind mount can repeat a path; probe it once
		}
		if pseudoFstypes[m.fstype] {
			continue // always-on: pseudo-fs is never real storage
		}
		if excludeFstype[m.fstype] || excludeMount[m.path] {
			continue
		}
		if dcfg.ExcludeNetworkFS && isNetworkFS(m.fstype) {
			continue
		}
		seen[m.path] = true
		mounts = append(mounts, m)
	}

	sort.Slice(mounts, func(i, j int) bool {
		di, dj := strings.Count(mounts[i].path, "/"), strings.Count(mounts[j].path, "/")
		if di != dj {
			return di < dj
		}
		return mounts[i].path < mounts[j].path
	})

	// Collapse bind mounts / subvolumes: keep only the FIRST (shallowest, most canonical) mount of
	// each device, exactly as df dedupes by st_dev. Without this a Docker/k8s/timeshift host would
	// list the same filesystem many times (/, /var/lib/docker/…, /run/…/backup → all one device).
	// A device with an empty major:minor (should not happen from mountinfo) is never collapsed.
	seenDev := make(map[string]bool, len(mounts))
	deduped := mounts[:0]
	for _, m := range mounts {
		if m.dev != "" {
			if seenDev[m.dev] {
				continue
			}
			seenDev[m.dev] = true
		}
		deduped = append(deduped, m)
	}
	mounts = deduped

	if dcfg.MaxMounts > 0 && len(mounts) > dcfg.MaxMounts {
		mounts = mounts[:dcfg.MaxMounts]
	}

	out := make(map[string]mountMeta, len(mounts))
	for _, m := range mounts {
		out[m.path] = m
	}
	return out
}

// drainCompleted absorbs any probe results that arrived since the last cycle,
// clearing the pending/stuck flags for those mounts.
func (c *Collector) drainCompleted() {
	for {
		select {
		case pr := <-c.results:
			if st := c.states[pr.path]; st != nil && st.pending {
				st.pending = false
				st.stuck = false
				st.have = true
				st.last = pr.res
				st.lastErr = pr.err
			}
		default:
			return
		}
	}
}

// reconcile drops state for mounts that have gone away. A gone mount's probe (if
// any) still posts to the buffered results channel and is ignored on drain.
func (c *Collector) reconcile(cur map[string]mountMeta) {
	for path := range c.states {
		if _, ok := cur[path]; !ok {
			delete(c.states, path)
		}
	}
}

// startProbes launches one statfs probe per mount that has none in flight and
// returns how many were freshly started (the set Collect will wait on).
func (c *Collector) startProbes(cur map[string]mountMeta) int {
	fresh := 0
	for path := range cur {
		st := c.states[path]
		if st == nil {
			st = &mountState{}
			c.states[path] = st
		}
		if st.pending {
			continue // a probe is already outstanding (possibly stuck)
		}
		st.pending = true
		p := path
		go func() {
			res, err := c.statfs(p)
			c.results <- probeResult{path: p, res: res, err: err}
		}()
		fresh++
	}
	return fresh
}

// waitForProbes waits up to the stuck deadline for the freshly started probes.
// Known-stuck mounts (pending from an earlier cycle, not restarted) are not
// waited on. Whatever is still pending after this is treated as stuck by
// buildSamples.
func (c *Collector) waitForProbes(ctx context.Context, fresh int) {
	if fresh <= 0 {
		return
	}
	timer := time.NewTimer(c.stuckDeadline)
	defer timer.Stop()
	for fresh > 0 {
		select {
		case pr := <-c.results:
			if st := c.states[pr.path]; st != nil && st.pending {
				st.pending = false
				st.stuck = false
				st.have = true
				st.last = pr.res
				st.lastErr = pr.err
				fresh--
			}
		case <-timer.C:
			return
		case <-ctx.Done():
			return
		}
	}
}

// buildSamples emits the gauges for healthy mounts and counts stuck ones. Mounts
// are walked in a stable path order so key order is deterministic.
func (c *Collector) buildSamples(cur map[string]mountMeta) []collector.Sample {
	paths := make([]string, 0, len(cur))
	for p := range cur {
		paths = append(paths, p)
	}
	sort.Strings(paths)

	stuck := 0
	used := make(map[string]bool, len(paths))
	var samples []collector.Sample
	for _, path := range paths {
		st := c.states[path]
		if st == nil {
			continue
		}
		if st.pending {
			st.stuck = true
			stuck++
			continue
		}
		if !st.have || st.lastErr != nil {
			continue
		}
		if st.last.Blocks == 0 {
			continue // zero-capacity pseudo mount (gvfs, portal, …) — not real storage
		}
		seg := collector.DedupeSegment(collector.NormalizeSegment(path), used)
		samples = append(samples, spaceSamples(seg, st.last)...)
		c.checkFull(path, st)
	}
	samples = append(samples, collector.Sample{Key: "sys.agent.stuck_mounts", Value: float64(stuck)})
	return samples
}

// checkFull drives the disk_full edge/hysteresis latch for one healthy mount: it
// emits disk_full once when used_pct crosses fullThreshold and re-arms only after
// used_pct falls below recoverThreshold. Stuck mounts never reach here (no fresh
// used_pct). The event carries the raw mount path (matching fs_readonly): the
// event meta is human/API-facing, so it keeps the readable path rather than
// leaking the normalized wire encoding; the UI can derive the normalized
// sys.disk.{mount}.* series key from the raw path when it needs the correlation.
func (c *Collector) checkFull(path string, st *mountState) {
	up := usedPct(st.last)
	switch {
	case up >= fullThreshold:
		if !st.full {
			st.full = true
			if c.emitter != nil {
				c.emitter.Emit(wire.EventType_EVENT_TYPE_DISK_FULL, map[string]string{
					"mount":    path,
					"used_pct": strconv.FormatFloat(up, 'f', 1, 64),
				})
			}
		}
	case up < recoverThreshold:
		st.full = false
	}
}

// spaceSamples turns one statfs result into the four disk keys. inodes_used_pct
// is omitted when the filesystem reports zero inodes (btrfs, some tmpfs), which
// have no inode concept, rather than faking a zero.
func spaceSamples(seg string, s Statfs) []collector.Sample {
	base := "sys.disk." + seg + "."
	total := s.Blocks * s.Bsize
	usedBytes := (s.Blocks - min64(s.Bfree, s.Blocks)) * s.Bsize
	out := []collector.Sample{
		{Key: base + "total_bytes", Value: float64(total)},
		{Key: base + "used_bytes", Value: float64(usedBytes)},
		{Key: base + "used_pct", Value: usedPct(s)},
	}
	if s.Files > 0 {
		inodesUsed := s.Files - min64(s.Ffree, s.Files)
		out = append(out, collector.Sample{
			Key:   base + "inodes_used_pct",
			Value: collector.RatioPercent(float64(inodesUsed), float64(s.Files)),
		})
	}
	return out
}

// usedPct is the df-style used percentage: used / (used + available), where
// available is what an unprivileged user can write (Bavail). It reaches 100 when
// Bavail is 0 even if root-reserved blocks remain, which is the "disk full"
// condition a user hits. Working in blocks avoids the Bsize multiply.
func usedPct(s Statfs) float64 {
	used := s.Blocks - min64(s.Bfree, s.Blocks)
	return collector.RatioPercent(float64(used), float64(used+s.Bavail))
}

func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// parseMountinfoLine extracts the mount point and fstype from one mountinfo
// line. The optional fields before the " - " separator are variable in count,
// so the split on " - " is what makes the fstype position deterministic.
func parseMountinfoLine(line string) (mountMeta, bool) {
	sep := strings.Index(line, " - ")
	if sep < 0 {
		return mountMeta{}, false
	}
	left := strings.Fields(line[:sep])
	right := strings.Fields(line[sep+3:])
	if len(left) < 5 || len(right) < 1 {
		return mountMeta{}, false
	}
	return mountMeta{
		path:   unescapeOctal(left[4]),
		fstype: right[0],
		dev:    left[2], // "major:minor"
	}, true
}

// unescapeOctal decodes the \NNN octal escapes mountinfo uses for spaces, tabs
// and backslashes in mount points.
func unescapeOctal(s string) string {
	if !strings.ContainsRune(s, '\\') {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) && isOctal(s[i+1]) && isOctal(s[i+2]) && isOctal(s[i+3]) {
			v := (int(s[i+1]-'0') << 6) | (int(s[i+2]-'0') << 3) | int(s[i+3]-'0')
			b.WriteByte(byte(v))
			i += 3
			continue
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

func isOctal(c byte) bool { return c >= '0' && c <= '7' }

func isNetworkFS(fstype string) bool {
	return networkFstypes[fstype] || strings.HasPrefix(fstype, "fuse.")
}

func toSet(list []string) map[string]bool {
	m := make(map[string]bool, len(list))
	for _, s := range list {
		m[s] = true
	}
	return m
}

var _ collector.Collector = (*Collector)(nil)
