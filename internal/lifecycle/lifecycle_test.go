package lifecycle

import (
	"context"
	"testing"
	"time"

	"github.com/LIVCK/agent/internal/platform/platformtest"
	"github.com/LIVCK/agent/pkg/wire"
)

type capEmitter struct {
	events []captured
}

type captured struct {
	typ  wire.EventType
	meta map[string]string
}

func (e *capEmitter) Emit(t wire.EventType, meta map[string]string) {
	e.events = append(e.events, captured{typ: t, meta: meta})
}

func (e *capEmitter) only(t testingT, want wire.EventType) captured {
	t.Helper()
	if len(e.events) != 1 {
		t.Fatalf("want exactly one %v event, got %d: %+v", want, len(e.events), e.events)
	}
	if e.events[0].typ != want {
		t.Fatalf("event type = %v, want %v", e.events[0].typ, want)
	}
	return e.events[0]
}

type testingT = *testing.T

const rwMount = "36 35 8:1 / / rw,relatime - ext4 /dev/sda1 rw\n"

func newFS(files map[string]string) *platformtest.MemFS {
	fs := platformtest.NewMemFS()
	for p, body := range files {
		_ = fs.WriteFileAtomic(p, []byte(body), 0o644)
	}
	return fs
}

func TestUnexpectedRebootWithoutMarker(t *testing.T) {
	fs := newFS(map[string]string{
		bootIDPath:              "new-boot\n",
		statPath:                "btime 2000\n",
		vmstatPath:              "oom_kill 0\n",
		mountinfo:               rwMount,
		"/state/lifecycle.json": `{"boot_id":"old-boot","last_alive_unix":1000}`,
	})
	em := &capEmitter{}
	d := New(fs, platformtest.NewClock(time.Unix(3000, 0)), em, "/state")

	d.Startup(context.Background())
	ev := em.only(t, wire.EventType_EVENT_TYPE_UNEXPECTED_REBOOT)
	if ev.meta["downtime_seconds"] != "1000" { // btime 2000 - last_alive 1000
		t.Fatalf("downtime = %q, want 1000", ev.meta["downtime_seconds"])
	}
}

func TestCleanBootWithMarker(t *testing.T) {
	fs := newFS(map[string]string{
		bootIDPath:              "new-boot\n",
		statPath:                "btime 2000\n",
		vmstatPath:              "oom_kill 0\n",
		mountinfo:               rwMount,
		"/state/lifecycle.json": `{"boot_id":"old-boot","last_alive_unix":1000}`,
		"/state/clean_shutdown": `{"at_unix":1900,"type":"poweroff"}`,
	})
	em := &capEmitter{}
	d := New(fs, platformtest.NewClock(time.Unix(3000, 0)), em, "/state")

	d.Startup(context.Background())
	ev := em.only(t, wire.EventType_EVENT_TYPE_BOOT)
	if ev.meta["downtime_seconds"] != "100" { // btime 2000 - marker 1900
		t.Fatalf("downtime = %q, want 100", ev.meta["downtime_seconds"])
	}
	if ev.meta["boot_id"] != "new-boot" {
		t.Fatalf("boot_id meta = %q", ev.meta["boot_id"])
	}
	// The marker is consumed so it cannot mislabel a later boot.
	if _, err := fs.ReadFile("/state/clean_shutdown"); err == nil {
		t.Fatal("clean_shutdown marker must be removed after a clean boot")
	}
}

func TestNoRebootWhenBootIDUnchanged(t *testing.T) {
	fs := newFS(map[string]string{
		bootIDPath:              "same-boot\n",
		statPath:                "btime 2000\n",
		vmstatPath:              "oom_kill 0\n",
		mountinfo:               rwMount,
		"/state/lifecycle.json": `{"boot_id":"same-boot","last_alive_unix":1000}`,
	})
	em := &capEmitter{}
	d := New(fs, platformtest.NewClock(time.Unix(3000, 0)), em, "/state")

	d.Startup(context.Background())
	if len(em.events) != 0 {
		t.Fatalf("no reboot event expected, got %+v", em.events)
	}
}

func TestOOMKillOnDelta(t *testing.T) {
	fs := newFS(map[string]string{
		bootIDPath:              "same-boot\n",
		statPath:                "btime 2000\n",
		vmstatPath:              "oom_kill 2\n",
		mountinfo:               rwMount,
		"/state/lifecycle.json": `{"boot_id":"same-boot","last_alive_unix":1000}`,
	})
	em := &capEmitter{}
	d := New(fs, platformtest.NewClock(time.Unix(3000, 0)), em, "/state")
	d.Startup(context.Background()) // seeds oom baseline 2

	_ = fs.WriteFileAtomic(vmstatPath, []byte("oom_kill 5\n"), 0o644)
	d.Tick(context.Background())
	ev := em.only(t, wire.EventType_EVENT_TYPE_OOM_KILL)
	if ev.meta["count"] != "3" {
		t.Fatalf("oom count = %q, want 3", ev.meta["count"])
	}
}

func TestFSReadonlyTransition(t *testing.T) {
	fs := newFS(map[string]string{
		bootIDPath:              "same-boot\n",
		statPath:                "btime 2000\n",
		vmstatPath:              "oom_kill 0\n",
		mountinfo:               rwMount,
		"/state/lifecycle.json": `{"boot_id":"same-boot","last_alive_unix":1000}`,
	})
	em := &capEmitter{}
	d := New(fs, platformtest.NewClock(time.Unix(3000, 0)), em, "/state")
	d.Startup(context.Background()) // seeds the mount as rw

	// The root mount flips read-only.
	_ = fs.WriteFileAtomic(mountinfo, []byte("36 35 8:1 / / ro,relatime - ext4 /dev/sda1 ro\n"), 0o644)
	d.Tick(context.Background())
	ev := em.only(t, wire.EventType_EVENT_TYPE_FS_READONLY)
	if ev.meta["mount"] != "/" {
		t.Fatalf("mount = %q, want /", ev.meta["mount"])
	}
}

func TestCleanShutdownEmitsAndPersistsMarker(t *testing.T) {
	fs := newFS(map[string]string{
		bootIDPath: "same-boot\n",
		statPath:   "btime 2000\n",
		vmstatPath: "oom_kill 0\n",
		mountinfo:  rwMount,
	})
	em := &capEmitter{}
	d := New(fs, platformtest.NewClock(time.Unix(3000, 0)), em, "/state")

	d.Shutdown(context.Background())
	em.only(t, wire.EventType_EVENT_TYPE_CLEAN_SHUTDOWN)
	if _, err := fs.ReadFile("/state/clean_shutdown"); err != nil {
		t.Fatal("clean_shutdown marker must be persisted so the next boot is clean")
	}
}
