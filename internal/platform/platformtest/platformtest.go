// Package platformtest provides deterministic fakes for the platform
// abstractions so buffer, sender, config and identity can be tested without a
// real clock or filesystem. It lives in its own package (not a _test.go file) so
// every package under internal can import it.
package platformtest

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/LIVCK/agent/internal/platform"
)

// Clock is a controllable platform.Clock. Now returns a fixed instant that only
// advances when the test calls Advance or when Sleep is allowed to move it.
// Sleep never blocks on real time: it records the requested duration, advances
// the clock by it, and returns immediately (honouring ctx cancellation). This
// keeps backoff tests instant and deterministic.
type Clock struct {
	mu     sync.Mutex
	now    time.Time
	sleeps []time.Duration
}

// NewClock returns a Clock anchored at start.
func NewClock(start time.Time) *Clock { return &Clock{now: start} }

// Now returns the current fake time.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the fake clock forward by d.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Sleep records d, advances the clock by d and returns immediately. If ctx is
// already done it returns ctx.Err() without advancing.
func (c *Clock) Sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	c.mu.Lock()
	c.sleeps = append(c.sleeps, d)
	if d > 0 {
		c.now = c.now.Add(d)
	}
	c.mu.Unlock()
	return nil
}

// Sleeps returns the durations passed to Sleep so far, in order.
func (c *Clock) Sleeps() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]time.Duration, len(c.sleeps))
	copy(out, c.sleeps)
	return out
}

// MemFS is an in-memory platform.FS. Writes are atomic by construction. It is
// safe for concurrent use.
type MemFS struct {
	mu    sync.Mutex
	files map[string][]byte
	perms map[string]os.FileMode
}

// NewMemFS returns an empty in-memory filesystem.
func NewMemFS() *MemFS {
	return &MemFS{files: map[string][]byte{}, perms: map[string]os.FileMode{}}
}

// ReadFile returns a copy of the stored bytes or an os.ErrNotExist wrapper.
func (m *MemFS) ReadFile(name string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.files[name]
	if !ok {
		return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrNotExist}
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out, nil
}

// WriteFileAtomic stores a copy of data under name with perm.
func (m *MemFS) WriteFileAtomic(name string, data []byte, perm os.FileMode) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	b := make([]byte, len(data))
	copy(b, data)
	m.files[name] = b
	m.perms[name] = perm
	return nil
}

// Remove deletes name. A missing file is not an error.
func (m *MemFS) Remove(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.files, name)
	delete(m.perms, name)
	return nil
}

// Stat reports whether name exists.
func (m *MemFS) Stat(name string) (os.FileInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.files[name]
	if !ok {
		return nil, &os.PathError{Op: "stat", Path: name, Err: os.ErrNotExist}
	}
	return memInfo{name: name, size: int64(len(b)), mode: m.perms[name]}, nil
}

// MkdirAll is a no-op: MemFS is flat and never reports a missing directory.
func (m *MemFS) MkdirAll(string, os.FileMode) error { return nil }

// ReadDir derives the immediate children of name from the flat file map: a
// stored path like /proc/123/stat implies the directory entry 123 under /proc.
// Entries are returned in name order so a scan is deterministic.
func (m *MemFS) ReadDir(name string) ([]os.DirEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	prefix := name
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	seen := map[string]bool{}
	var out []os.DirEntry
	for p := range m.files {
		if !strings.HasPrefix(p, prefix) {
			continue
		}
		rest := p[len(prefix):]
		if rest == "" {
			continue
		}
		child := rest
		isDir := false
		if i := strings.IndexByte(rest, '/'); i >= 0 {
			child = rest[:i]
			isDir = true
		}
		if seen[child] {
			continue
		}
		seen[child] = true
		out = append(out, memDirEntry{name: child, dir: isDir})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name() < out[j].Name() })
	return out, nil
}

// Perm returns the recorded mode for name, or false if absent.
func (m *MemFS) Perm(name string) (os.FileMode, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.perms[name]
	return p, ok
}

// Exec is a deterministic platform.Exec. LookPath resolves the binaries listed
// in Paths; Run returns the canned result registered for an exact argv, letting
// a test drive nvidia-smi / smartctl output without a real subprocess. An argv
// with no registered response returns ErrNotSupported, and a response may carry
// an error (for example context.DeadlineExceeded) to model a timeout or a
// non-zero exit.
type Exec struct {
	mu        sync.Mutex
	paths     map[string]string
	responses map[string]execResult
	calls     [][]string
}

type execResult struct {
	out []byte
	err error
}

// NewExec returns an Exec with no binaries and no canned responses.
func NewExec() *Exec {
	return &Exec{paths: map[string]string{}, responses: map[string]execResult{}}
}

// AddPath makes name resolvable via LookPath, resolving to /usr/bin/<name>.
func (e *Exec) AddPath(name string) *Exec {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.paths[name] = "/usr/bin/" + name
	return e
}

// SetResponse registers stdout (and an optional error) for an exact argv.
func (e *Exec) SetResponse(out []byte, err error, name string, args ...string) *Exec {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.responses[execKey(name, args)] = execResult{out: out, err: err}
	return e
}

// LookPath reports whether name was registered via AddPath.
func (e *Exec) LookPath(name string) (string, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p, ok := e.paths[name]
	return p, ok
}

// Run returns the response registered for the exact argv, honouring an
// already-cancelled ctx first. An unregistered argv returns ErrNotSupported.
func (e *Exec) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	call := append([]string{name}, args...)
	e.calls = append(e.calls, call)
	r, ok := e.responses[execKey(name, args)]
	if !ok {
		return nil, ErrNotSupported
	}
	// Mirror os/exec Cmd.Output: stdout is returned even when the process exits
	// non-zero, so a smartctl exit bitmask on a failing drive still yields JSON.
	out := make([]byte, len(r.out))
	copy(out, r.out)
	return out, r.err
}

// Calls returns the argvs Run was invoked with, in order.
func (e *Exec) Calls() [][]string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([][]string, len(e.calls))
	copy(out, e.calls)
	return out
}

func execKey(name string, args []string) string {
	return strings.Join(append([]string{name}, args...), "\x00")
}

var _ platform.FS = (*MemFS)(nil)
var _ platform.Clock = (*Clock)(nil)
var _ platform.Exec = (*Exec)(nil)

// ErrNotSupported is returned by fakes for operations they do not model.
var ErrNotSupported = errors.New("platformtest: operation not supported")

type memInfo struct {
	name string
	size int64
	mode os.FileMode
}

func (i memInfo) Name() string       { return i.name }
func (i memInfo) Size() int64        { return i.size }
func (i memInfo) Mode() os.FileMode  { return i.mode }
func (i memInfo) ModTime() time.Time { return time.Time{} }
func (i memInfo) IsDir() bool        { return false }
func (i memInfo) Sys() any           { return nil }

// memDirEntry is a synthetic os.DirEntry returned by MemFS.ReadDir.
type memDirEntry struct {
	name string
	dir  bool
}

func (e memDirEntry) Name() string { return e.name }
func (e memDirEntry) IsDir() bool  { return e.dir }
func (e memDirEntry) Type() fs.FileMode {
	if e.dir {
		return fs.ModeDir
	}
	return 0
}
func (e memDirEntry) Info() (fs.FileInfo, error) {
	mode := os.FileMode(0)
	if e.dir {
		mode = os.ModeDir
	}
	return memInfo{name: e.name, mode: mode}, nil
}
