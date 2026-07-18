// Package lifecycle detects the discrete host lifecycle events the agent emits
// as the strong-delivery signal class: boot, unexpected_reboot, clean_shutdown,
// oom_kill and fs_readonly. It reads only unprivileged /proc files and persists
// two small 0600 files in the state directory: a state file (the last-seen
// boot_id and a periodically refreshed liveness timestamp) and a clean_shutdown
// marker written on SIGTERM. The marker is what makes the next boot a clean boot
// rather than an unexpected reboot.
//
// Events are emitted into the shared event queue. On a crash there is no live
// send; the safety net is the exit spool plus the boot backfill:
// events queued here are spooled on exit and delivered after the next start.
package lifecycle

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/LIVCK/agent/internal/platform"
	"github.com/LIVCK/agent/pkg/wire"
)

const (
	bootIDPath  = "/proc/sys/kernel/random/boot_id"
	vmstatPath  = "/proc/vmstat"
	statPath    = "/proc/stat"
	mountinfo   = "/proc/self/mountinfo"
	stateFile   = "lifecycle.json"
	markerFile  = "clean_shutdown"
	secretPerm  = 0o600
	flushEvery  = 60 * time.Second
	cleanReboot = "poweroff"
)

// Emitter raises a lifecycle event. It matches internal/event.Emitter without
// importing it, mirroring the buffer package.
type Emitter interface {
	Emit(t wire.EventType, meta map[string]string)
}

// persistState is the JSON persisted between runs. boot_id detects a reboot;
// last_alive_unix bounds the downtime estimate for an unexpected reboot.
type persistState struct {
	BootID    string `json:"boot_id"`
	LastAlive int64  `json:"last_alive_unix"`
}

// marker is the clean_shutdown marker written on SIGTERM.
type marker struct {
	At   int64  `json:"at_unix"`
	Type string `json:"type"`
}

// Detector holds the lifecycle detection state and the persistence paths.
type Detector struct {
	fs        platform.FS
	clock     platform.Clock
	emitter   Emitter
	statePath string
	mrkPath   string

	oomBaseline uint64
	oomSeeded   bool
	mountRO     map[string]bool
	lastFlush   time.Time
}

// New builds a Detector rooted at stateDir.
func New(fs platform.FS, clock platform.Clock, emitter Emitter, stateDir string) *Detector {
	return &Detector{
		fs:        fs,
		clock:     clock,
		emitter:   emitter,
		statePath: filepath.Join(stateDir, stateFile),
		mrkPath:   filepath.Join(stateDir, markerFile),
		mountRO:   map[string]bool{},
	}
}

// Startup runs once before the first report: it compares the current boot_id to
// the persisted one and emits boot or unexpected_reboot on a change, consumes
// the clean_shutdown marker, refreshes the persisted state, and seeds the oom
// and read-only-mount baselines so Tick reports only transitions.
func (d *Detector) Startup(context.Context) {
	current := d.readBootID()
	prev, hadState := d.loadState()
	mk, hadMarker := d.loadMarker()

	if hadState && prev.BootID != "" && current != "" && prev.BootID != current {
		bootTime := d.readBtime()
		if hadMarker {
			meta := map[string]string{}
			if current != "" {
				meta["boot_id"] = current
			}
			if bootTime > 0 && mk.At > 0 {
				meta["downtime_seconds"] = strconv.FormatInt(nonNeg(bootTime-mk.At), 10)
			}
			d.emitter.Emit(wire.EventType_EVENT_TYPE_BOOT, meta)
		} else {
			down := int64(0)
			if bootTime > 0 && prev.LastAlive > 0 {
				down = nonNeg(bootTime - prev.LastAlive)
			}
			d.emitter.Emit(wire.EventType_EVENT_TYPE_UNEXPECTED_REBOOT, map[string]string{
				"downtime_seconds": strconv.FormatInt(down, 10),
			})
		}
	}

	if hadMarker {
		_ = d.fs.Remove(d.mrkPath)
	}
	d.flush(current, d.clock.Now())

	if v, ok := d.readOOM(); ok {
		d.oomBaseline = v
		d.oomSeeded = true
	}
	d.seedMounts()
}

// Tick runs each collect interval: it emits oom_kill on a positive vmstat delta
// and fs_readonly when a previously writable mount has flipped read-only, and
// refreshes the liveness timestamp at most once per flushEvery.
func (d *Detector) Tick(context.Context) {
	d.checkOOM()
	d.checkReadonly()

	now := d.clock.Now()
	if now.Sub(d.lastFlush) >= flushEvery {
		d.flush(d.readBootID(), now)
	}
}

// Shutdown runs on SIGTERM: it emits clean_shutdown and persists the marker so
// the next boot is recognised as clean. It is best-effort and must not block.
func (d *Detector) Shutdown(context.Context) {
	now := d.clock.Now().Unix()
	d.emitter.Emit(wire.EventType_EVENT_TYPE_CLEAN_SHUTDOWN, map[string]string{"type": cleanReboot})
	if b, err := json.Marshal(marker{At: now, Type: cleanReboot}); err == nil {
		_ = d.fs.WriteFileAtomic(d.mrkPath, b, secretPerm)
	}
}

func (d *Detector) checkOOM() {
	cur, ok := d.readOOM()
	if !ok {
		return
	}
	if !d.oomSeeded {
		d.oomBaseline = cur
		d.oomSeeded = true
		return
	}
	delta, reset := counterDelta(cur, d.oomBaseline)
	d.oomBaseline = cur
	if reset || delta == 0 {
		return
	}
	d.emitter.Emit(wire.EventType_EVENT_TYPE_OOM_KILL, map[string]string{
		"count": strconv.FormatUint(delta, 10),
	})
}

func (d *Detector) checkReadonly() {
	cur := d.readMounts()
	for path, ro := range cur {
		was, known := d.mountRO[path]
		if known && !was && ro {
			d.emitter.Emit(wire.EventType_EVENT_TYPE_FS_READONLY, map[string]string{"mount": path})
		}
	}
	d.mountRO = cur
}

func (d *Detector) seedMounts() { d.mountRO = d.readMounts() }

// readMounts returns a path -> read-only map from /proc/self/mountinfo.
func (d *Detector) readMounts() map[string]bool {
	out := map[string]bool{}
	data, err := d.fs.ReadFile(mountinfo)
	if err != nil {
		return out
	}
	for _, line := range strings.Split(string(data), "\n") {
		sep := strings.Index(line, " - ")
		if sep < 0 {
			continue
		}
		f := strings.Fields(line[:sep])
		if len(f) < 6 {
			continue
		}
		path := f[4]
		ro := false
		for _, opt := range strings.Split(f[5], ",") {
			if opt == "ro" {
				ro = true
				break
			}
		}
		out[path] = ro
	}
	return out
}

func (d *Detector) readOOM() (uint64, bool) {
	data, err := d.fs.ReadFile(vmstatPath)
	if err != nil {
		return 0, false
	}
	for _, line := range strings.Split(string(data), "\n") {
		f := strings.Fields(line)
		if len(f) < 2 || f[0] != "oom_kill" {
			continue
		}
		v, err := strconv.ParseUint(f[1], 10, 64)
		if err != nil {
			return 0, false
		}
		return v, true
	}
	return 0, false
}

func (d *Detector) readBootID() string {
	data, err := d.fs.ReadFile(bootIDPath)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// readBtime returns the boot wall-clock time (unix seconds) from /proc/stat.
func (d *Detector) readBtime() int64 {
	data, err := d.fs.ReadFile(statPath)
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		f := strings.Fields(line)
		if len(f) < 2 || f[0] != "btime" {
			continue
		}
		v, err := strconv.ParseInt(f[1], 10, 64)
		if err != nil {
			return 0
		}
		return v
	}
	return 0
}

func (d *Detector) loadState() (persistState, bool) {
	data, err := d.fs.ReadFile(d.statePath)
	if err != nil {
		return persistState{}, false
	}
	var s persistState
	if err := json.Unmarshal(data, &s); err != nil {
		return persistState{}, false
	}
	return s, true
}

func (d *Detector) loadMarker() (marker, bool) {
	data, err := d.fs.ReadFile(d.mrkPath)
	if err != nil {
		return marker{}, false
	}
	var m marker
	if err := json.Unmarshal(data, &m); err != nil {
		return marker{}, false
	}
	return m, true
}

func (d *Detector) flush(bootID string, now time.Time) {
	b, err := json.Marshal(persistState{BootID: bootID, LastAlive: now.Unix()})
	if err != nil {
		return
	}
	if err := d.fs.WriteFileAtomic(d.statePath, b, secretPerm); err == nil {
		d.lastFlush = now
	}
}

// counterDelta mirrors collector.CounterDelta locally so lifecycle does not
// import the collector package for a single helper.
func counterDelta(cur, prev uint64) (uint64, bool) {
	if cur < prev {
		return 0, true
	}
	return cur - prev, false
}

func nonNeg(v int64) int64 {
	if v < 0 {
		return 0
	}
	return v
}
