package config

import (
	"context"
	"testing"
	"time"

	"github.com/LIVCK/agent/internal/platform/platformtest"
	"github.com/LIVCK/agent/pkg/wire"
)

// recordingEmitter captures emitted events for assertions.
type recordingEmitter struct {
	events []wire.EventType
}

func (r *recordingEmitter) Emit(t wire.EventType, _ map[string]string) {
	r.events = append(r.events, t)
}

func (r *recordingEmitter) has(t wire.EventType) bool {
	for _, e := range r.events {
		if e == t {
			return true
		}
	}
	return false
}

func newTestManager() (*Manager, *recordingEmitter, *platformtest.MemFS) {
	em := &recordingEmitter{}
	fs := platformtest.NewMemFS()
	m := NewManager(nil, em, fs, "/state/config.json", nil)
	return m, em, fs
}

func TestManagerDefaultsBeforeApply(t *testing.T) {
	m, _, _ := newTestManager()
	if m.Current().IntervalSeconds != DefaultIntervalSeconds {
		t.Fatalf("expected default interval, got %d", m.Current().IntervalSeconds)
	}
}

func TestManagerApplyValidSwapsAndPersists(t *testing.T) {
	m, em, fs := newTestManager()
	m.Apply([]byte(fullDoc))

	if m.Current().ConfigVersion != 7 {
		t.Fatalf("expected version 7 applied, got %d", m.Current().ConfigVersion)
	}
	if !em.has(wire.EventType_EVENT_TYPE_CONFIG_APPLIED) {
		t.Fatal("expected config_applied event")
	}
	if em.has(wire.EventType_EVENT_TYPE_CONFIG_ERROR) {
		t.Fatal("clean apply must not raise config_error")
	}
	if _, ok := fs.Perm("/state/config.json"); !ok {
		t.Fatal("last-good config was not persisted")
	}
}

func TestManagerApplyUnparseableKeepsLastGood(t *testing.T) {
	m, em, _ := newTestManager()
	m.Apply([]byte(fullDoc)) // establish version 7
	em.events = nil

	m.Apply([]byte(`{garbage`))
	if m.Current().ConfigVersion != 7 {
		t.Fatalf("keep-last-good broken: version is %d", m.Current().ConfigVersion)
	}
	if !em.has(wire.EventType_EVENT_TYPE_CONFIG_ERROR) {
		t.Fatal("expected config_error on unparseable document")
	}
}

func TestManagerApplyFieldErrorsAppliesButErrors(t *testing.T) {
	m, em, _ := newTestManager()
	doc := `{"config_version":9,"collectors":{"disk":{"exclude_network_fs":42}}}`
	m.Apply([]byte(doc))

	if m.Current().ConfigVersion != 9 {
		t.Fatalf("clamped config should still apply, version=%d", m.Current().ConfigVersion)
	}
	if !em.has(wire.EventType_EVENT_TYPE_CONFIG_ERROR) {
		t.Fatal("expected config_error for the coerced field")
	}
}

func TestManagerSeedsFromLastGood(t *testing.T) {
	fs := platformtest.NewMemFS()
	_ = fs.WriteFileAtomic("/state/config.json", []byte(fullDoc), 0o600)
	m := NewManager(nil, nil, fs, "/state/config.json", nil)
	if m.Current().ConfigVersion != 7 {
		t.Fatalf("expected seed from last-good, got version %d", m.Current().ConfigVersion)
	}
}

// stubFetcher scripts config pulls.
type stubFetcher struct {
	raw         []byte
	etag        string
	notModified bool
	err         error
	seenETag    string
}

func (f *stubFetcher) Fetch(_ context.Context, etag string) ([]byte, string, bool, error) {
	f.seenETag = etag
	return f.raw, f.etag, f.notModified, f.err
}

func TestManagerPullAppliesAndTracksETag(t *testing.T) {
	em := &recordingEmitter{}
	fs := platformtest.NewMemFS()
	f := &stubFetcher{raw: []byte(fullDoc), etag: `"v7"`}
	m := NewManager(f, em, fs, "/state/config.json", nil)

	m.Pull(context.Background())
	if m.Current().ConfigVersion != 7 {
		t.Fatalf("pull did not apply, version=%d", m.Current().ConfigVersion)
	}

	// Next pull sends the stored ETag and a 304 is a no-op.
	f.notModified = true
	m.Pull(context.Background())
	if f.seenETag != `"v7"` {
		t.Fatalf("expected stored ETag on second pull, got %q", f.seenETag)
	}
}

func TestManagerRunStopsOnContext(t *testing.T) {
	m, _, _ := newTestManager()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { m.Run(ctx, make(chan struct{}), 10*time.Millisecond); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not stop on context cancel")
	}
}
