package live

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/platform"
	"github.com/LIVCK/agent/internal/platform/platformtest"
)

// fakeConn records frames and optionally fails a write / notifies the test.
type fakeConn struct {
	mu        sync.Mutex
	frames    [][]byte
	failAfter int         // fail WriteText once this many frames have been written (0 = never)
	onWrite   func(n int) // called after each successful write with the running count
	closed    bool
}

func (f *fakeConn) WriteText(b []byte) error {
	f.mu.Lock()
	if f.failAfter > 0 && len(f.frames) >= f.failAfter {
		f.mu.Unlock()
		return errors.New("write fail")
	}
	f.frames = append(f.frames, append([]byte(nil), b...))
	n := len(f.frames)
	cb := f.onWrite
	f.mu.Unlock()
	if cb != nil {
		cb(n)
	}
	return nil
}

func (f *fakeConn) Close() error {
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
	return nil
}

func (f *fakeConn) Frames() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]byte(nil), f.frames...)
}

// fakeDialer builds a Conn per dial via make, counting dials.
type fakeDialer struct {
	dials int32
	make  func(dial int32) (Conn, error)
}

func (d *fakeDialer) Dial(_ context.Context, _, _ string) (Conn, error) {
	n := atomic.AddInt32(&d.dials, 1)
	return d.make(n)
}

type fakeSampler struct{ vals map[string]float64 }

func (f fakeSampler) Sample(context.Context) map[string]float64 { return f.vals }

func liveConfig(on bool) func() *config.Config {
	return func() *config.Config {
		c := config.Defaults()
		c.Features.Live = on
		return c
	}
}

func newStreamer(t *testing.T, opt Options) *Streamer {
	t.Helper()
	if opt.Clock == nil {
		opt.Clock = platformtest.NewClock(time.Unix(1000, 0))
	}
	if opt.TokenFn == nil {
		opt.TokenFn = func() string { return "tok" }
	}
	if opt.WSURL == "" {
		opt.WSURL = "wss://ingest.example/v1/live/agent"
	}
	if opt.Sampler == nil {
		opt.Sampler = fakeSampler{vals: map[string]float64{"sys.cpu.total_pct": 12.5}}
	}
	return New(opt)
}

func TestStreamerBurstsWhenLive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const target = 3
	fc := &fakeConn{onWrite: func(n int) {
		if n >= target {
			cancel()
		}
	}}
	dialer := &fakeDialer{make: func(int32) (Conn, error) { return fc, nil }}

	s := newStreamer(t, Options{Config: liveConfig(true), Dialer: dialer, Interval: time.Second})
	s.Run(ctx)

	if got := len(fc.Frames()); got < target {
		t.Fatalf("wrote %d frames, want >= %d", got, target)
	}
	if atomic.LoadInt32(&dialer.dials) != 1 {
		t.Fatalf("dials = %d, want 1", dialer.dials)
	}
	// Frames must be the agreed {t, m} shape carrying the sampler's values.
	var f frame
	if err := json.Unmarshal(fc.Frames()[0], &f); err != nil {
		t.Fatalf("frame is not valid JSON: %v", err)
	}
	if f.M["sys.cpu.total_pct"] != 12.5 || f.T == 0 {
		t.Fatalf("unexpected frame: %+v", f)
	}
	if !fc.closed {
		t.Fatal("connection was not closed on stop")
	}
}

func TestStreamerIdleWhenLiveOff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var calls int32
	cfg := func() *config.Config {
		if atomic.AddInt32(&calls, 1) >= 3 {
			cancel() // end the loop after a few idle polls
		}
		c := config.Defaults()
		c.Features.Live = false
		return c
	}
	dialer := &fakeDialer{make: func(int32) (Conn, error) {
		t.Fatal("must not dial while live is off")
		return nil, nil
	}}

	s := newStreamer(t, Options{Config: cfg, Dialer: dialer})
	s.Run(ctx)

	if atomic.LoadInt32(&dialer.dials) != 0 {
		t.Fatalf("dials = %d, want 0", dialer.dials)
	}
}

func TestStreamerReconnectsOnWriteError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dialer := &fakeDialer{make: func(dial int32) (Conn, error) {
		if dial == 1 {
			return &fakeConn{failAfter: 1}, nil // first frame ok, second fails → reconnect
		}
		return &fakeConn{onWrite: func(n int) {
			if n >= 2 {
				cancel()
			}
		}}, nil
	}}

	s := newStreamer(t, Options{Config: liveConfig(true), Dialer: dialer, Interval: time.Second, Backoff: time.Second})
	s.Run(ctx)

	if d := atomic.LoadInt32(&dialer.dials); d < 2 {
		t.Fatalf("dials = %d, want >= 2 (reconnect after write error)", d)
	}
}

func TestStreamerRetriesOnDialError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dialErr := errors.New("dial fail")
	dialer := &fakeDialer{make: func(dial int32) (Conn, error) {
		if dial < 3 {
			return nil, dialErr // first two dials fail
		}
		return &fakeConn{onWrite: func(int) { cancel() }}, nil
	}}

	s := newStreamer(t, Options{Config: liveConfig(true), Dialer: dialer, Interval: time.Second, Backoff: time.Second})
	s.Run(ctx)

	if d := atomic.LoadInt32(&dialer.dials); d < 3 {
		t.Fatalf("dials = %d, want >= 3 (retry after dial errors)", d)
	}
}

func TestStreamerStopsWhenLiveFlipsOff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var live atomic.Bool
	live.Store(true)
	cfg := func() *config.Config {
		c := config.Defaults()
		c.Features.Live = live.Load()
		return c
	}
	fc := &fakeConn{onWrite: func(n int) {
		if n == 1 {
			live.Store(false) // flip off after the first burst
		}
	}}
	dialer := &fakeDialer{make: func(int32) (Conn, error) { return fc, nil }}

	// Real clock so the idle poll actually blocks (the fake clock's instant Sleep
	// would busy-spin). A background timer ends the run after the flip so a
	// streamer that failed to stop bursting fails the frame-count assertion.
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()

	s := newStreamer(t, Options{
		Config: cfg, Dialer: dialer, Clock: platform.SystemClock{},
		Interval: 5 * time.Millisecond, IdlePoll: 5 * time.Millisecond, Backoff: 5 * time.Millisecond,
	})
	s.Run(ctx)

	if !fc.closed {
		t.Fatal("connection was not closed after live flipped off")
	}
	if got := len(fc.Frames()); got != 1 {
		t.Fatalf("frames = %d, want 1 (no burst after live flipped off)", got)
	}
}

func TestWSURL(t *testing.T) {
	cases := map[string]string{
		"https://ingest.livck.cloud":  "wss://ingest.livck.cloud/v1/live/agent",
		"http://localhost:15810":      "ws://localhost:15810/v1/live/agent",
		"https://ingest.livck.cloud/": "wss://ingest.livck.cloud/v1/live/agent",
		"  https://x.test  ":          "wss://x.test/v1/live/agent",
	}
	for in, want := range cases {
		if got := WSURL(in); got != want {
			t.Fatalf("WSURL(%q) = %q, want %q", in, got, want)
		}
	}
}
