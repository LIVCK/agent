package live

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/platform"
	"github.com/LIVCK/agent/internal/platform/platformtest"
)

// fakeConn records frames, feeds control frames back to the streamer and
// optionally fails a write / notifies the test.
type fakeConn struct {
	mu        sync.Mutex
	frames    [][]byte
	failAfter int         // fail WriteText once this many frames have been written (0 = never)
	onWrite   func(n int) // called after each successful write with the running count
	closed    bool
	recv      chan []byte
}

// newFakeConn returns a connection whose control channel is ready to take
// frames. The buffer is deep enough that Control never blocks a test, including
// from inside an onWrite callback (which runs on the streamer's goroutine).
func newFakeConn() *fakeConn { return &fakeConn{recv: make(chan []byte, 16)} }

// Control queues one Server→Agent control frame.
func (f *fakeConn) Control(js string) { f.recv <- []byte(js) }

func (f *fakeConn) Recv() <-chan []byte { return f.recv }

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

func (f *fakeConn) FrameCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.frames)
}

// IsClosed is the locked read for assertions made while the streamer runs.
func (f *fakeConn) IsClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
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

// countingSampler counts how often it was asked for a snapshot. Sampling is the
// expensive half of a burst, so tests that care about what a flood COSTS assert
// on this rather than on frames — a nil vals models a host whose collectors have
// nothing to report, where no frame is ever sent and the frame count would
// therefore stay silent about the work done.
type countingSampler struct {
	calls atomic.Int64
	vals  map[string]float64
}

func (c *countingSampler) Sample(context.Context) map[string]float64 {
	c.calls.Add(1)
	return c.vals
}

func (c *countingSampler) Calls() int64 { return c.calls.Load() }

func liveConfig(on bool) func() *config.Config {
	return func() *config.Config {
		c := config.Defaults()
		c.Features.Live = on
		return c
	}
}

// liveFor returns a features.live=true config source that ends the run after n
// reads. The streamer reads the config exactly once per loop iteration, so this
// doubles as an iteration budget for tests that have no frames to count, and as
// a hook (each) for assertions taken while the session is still open.
func liveFor(n int32, cancel context.CancelFunc, each func(iter int32)) func() *config.Config {
	var calls int32
	return func() *config.Config {
		i := atomic.AddInt32(&calls, 1)
		if each != nil {
			each(i)
		}
		if i >= n {
			cancel()
		}
		c := config.Defaults()
		c.Features.Live = true
		return c
	}
}

// streamOn is the control frame pulse sends while a viewer is watching; without
// rate_ms the agent keeps its configured cadence.
const (
	streamOn  = `{"t":"stream_on"}`
	streamOff = `{"t":"stream_off"}`
)

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
	fc := newFakeConn()
	fc.onWrite = func(n int) {
		if n >= target {
			cancel()
		}
	}
	fc.Control(streamOn) // pulse has a viewer: burst.
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

	// Every connection has to be armed separately: the keepalive state does not
	// survive a reconnect, pulse re-sends stream_on on the fresh channel.
	dialer := &fakeDialer{make: func(dial int32) (Conn, error) {
		fc := newFakeConn()
		fc.Control(streamOn)
		if dial == 1 {
			fc.failAfter = 1 // first frame ok, second fails → reconnect
			return fc, nil
		}
		fc.onWrite = func(n int) {
			if n >= 2 {
				cancel()
			}
		}
		return fc, nil
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
		fc := newFakeConn()
		fc.onWrite = func(int) { cancel() }
		fc.Control(streamOn)
		return fc, nil
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
	fc := newFakeConn()
	fc.onWrite = func(n int) {
		if n == 1 {
			live.Store(false) // flip off after the first burst
		}
	}
	fc.Control(streamOn)
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

func TestStreamerDoesNotBurstBeforeStreamOn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fc := newFakeConn()
	dialer := &fakeDialer{make: func(int32) (Conn, error) { return fc, nil }}

	// Ten loop iterations on the fake clock = ten seconds of connected session
	// without a single control frame.
	s := newStreamer(t, Options{Config: liveFor(10, cancel, nil), Dialer: dialer})
	s.Run(ctx)

	if got := fc.FrameCount(); got != 0 {
		t.Fatalf("frames = %d, want 0 — nothing may be sent until pulse says stream_on", got)
	}
	if d := atomic.LoadInt32(&dialer.dials); d != 1 {
		t.Fatalf("dials = %d, want 1 (the unarmed channel stays connected)", d)
	}
}

func TestStreamerClampsServerBurstRate(t *testing.T) {
	const configured = 7 * time.Second
	cases := []struct {
		name string
		ctrl string
		want time.Duration
	}{
		{"below the floor is clamped up", `{"t":"stream_on","rate_ms":10}`, minRateMS * time.Millisecond},
		{"above the ceiling is clamped down", `{"t":"stream_on","rate_ms":999999}`, maxRateMS * time.Millisecond},
		{"a missing rate keeps the configured cadence", streamOn, configured},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			const target = 3
			fc := newFakeConn()
			fc.onWrite = func(n int) {
				if n >= target {
					cancel()
				}
			}
			fc.Control(tc.ctrl)
			dialer := &fakeDialer{make: func(int32) (Conn, error) { return fc, nil }}
			clk := platformtest.NewClock(time.Unix(1000, 0))

			s := newStreamer(t, Options{Config: liveConfig(true), Dialer: dialer, Clock: clk, Interval: configured})
			s.Run(ctx)

			if got := fc.FrameCount(); got < target {
				t.Fatalf("frames = %d, want >= %d", got, target)
			}
			// A bursting session only ever waits the burst cadence, plus the coarse
			// idle tick it spent before the control frame landed.
			burstWaits := 0
			for _, d := range clk.Sleeps() {
				switch d {
				case tc.want:
					burstWaits++
				case defaultIdleTick:
				default:
					t.Fatalf("waited %v, want the clamped %v (or the %v idle tick)", d, tc.want, defaultIdleTick)
				}
			}
			if burstWaits == 0 {
				t.Fatalf("no burst wait of %v recorded: sleeps = %v", tc.want, clk.Sleeps())
			}
		})
	}
}

func TestStreamerStopsBurstOnStreamOffAndKeepsConnection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fc := newFakeConn()
	fc.onWrite = func(n int) {
		if n == 1 {
			fc.Control(streamOff) // the last viewer left
		}
	}
	fc.Control(streamOn)
	dialer := &fakeDialer{make: func(int32) (Conn, error) { return fc, nil }}

	// Fake clock, no wall-clock windows: the loop drains queued control frames
	// before it decides to burst, so the stream_off lands before the next frame
	// can no matter which select case won. The config read is one per iteration,
	// which is both the run budget and the point where the session must still be
	// connected — Close only ever happens on the way out.
	openWhileIdle := false
	cfg := liveFor(20, cancel, func(iter int32) {
		if iter == 20 {
			openWhileIdle = !fc.IsClosed()
		}
	})

	s := newStreamer(t, Options{Config: cfg, Dialer: dialer, Interval: time.Second})
	s.Run(ctx)

	if got := fc.FrameCount(); got != 1 {
		t.Fatalf("frames = %d, want 1 (no burst after stream_off)", got)
	}
	if !openWhileIdle {
		t.Fatal("connection was closed on stream_off — it must stay armed for the next viewer")
	}
	if d := atomic.LoadInt32(&dialer.dials); d != 1 {
		t.Fatalf("dials = %d, want 1 (stream_off must not end the session)", d)
	}
}

func TestStreamerStopsBurstWhenKeepaliveExpires(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fc := newFakeConn()
	fc.Control(streamOn) // armed once, never refreshed — pulse or the viewer vanished
	dialer := &fakeDialer{make: func(int32) (Conn, error) { return fc, nil }}

	// Fake clock at the burst ceiling: each wait advances time by exactly the
	// cadence, so the 60 s window fits bursts at +0s…+50s and the one that would
	// fall on +60s must not happen. Then 24 idle seconds prove it stays quiet.
	start := time.Unix(1000, 0)
	openAfterExpiry := false
	cfg := liveFor(30, cancel, func(iter int32) {
		if iter == 30 {
			openAfterExpiry = !fc.IsClosed()
		}
	})

	s := newStreamer(t, Options{
		Config: cfg, Dialer: dialer, Clock: platformtest.NewClock(start),
		Interval: maxRateMS * time.Millisecond,
	})
	s.Run(ctx)

	frames := fc.Frames()
	if len(frames) != 6 {
		t.Fatalf("frames = %d, want exactly the 6 that fit in a %v window at %v", len(frames), keepaliveWindow, maxRateMS*time.Millisecond)
	}
	last := start.Add(keepaliveWindow)
	for i, b := range frames {
		var f frame
		if err := json.Unmarshal(b, &f); err != nil {
			t.Fatalf("frame %d is not valid JSON: %v", i, err)
		}
		if at := time.UnixMilli(f.T); !at.Before(last) {
			t.Fatalf("frame %d sent at %v, past the keepalive deadline %v", i, at, last)
		}
	}
	if !openAfterExpiry {
		t.Fatal("connection was closed on keepalive expiry — it must stay armed for the next viewer")
	}
	if d := atomic.LoadInt32(&dialer.dials); d != 1 {
		t.Fatalf("dials = %d, want 1 (an expired keepalive must not end the session)", d)
	}
}

func TestStreamerResumesBurstAfterKeepaliveExpiry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fc := newFakeConn()
	fc.Control(streamOn)
	dialer := &fakeDialer{make: func(int32) (Conn, error) { return fc, nil }}

	// Same shape as the expiry test: 6 frames, then silence. At iteration 15 the
	// window is long gone and a new viewer shows up — the frame is queued from the
	// config hook, which runs at the top of the loop, so the drain picks it up in
	// that same iteration. No wall-clock timing anywhere.
	beforeRearm := -1
	cfg := liveFor(25, cancel, func(iter int32) {
		if iter == 15 {
			beforeRearm = fc.FrameCount()
			fc.Control(streamOn)
		}
	})

	s := newStreamer(t, Options{
		Config: cfg, Dialer: dialer, Clock: platformtest.NewClock(time.Unix(1000, 0)),
		Interval: maxRateMS * time.Millisecond,
	})
	s.Run(ctx)

	if beforeRearm != 6 {
		t.Fatalf("frames before the re-arm = %d, want the 6 of the first window", beforeRearm)
	}
	if got := fc.FrameCount(); got <= beforeRearm {
		t.Fatalf("frames %d → %d: a stream_on after the window expired must restart the burst", beforeRearm, got)
	}
	if d := atomic.LoadInt32(&dialer.dials); d != 1 {
		t.Fatalf("dials = %d, want 1 (re-arming must not need a reconnect)", d)
	}
}

// Flood parameters: ~100 control frames arrive in a window that fits ~7 bursts
// at the cadence, so a floor that works and one that does not are two orders of
// magnitude apart.
const (
	floodCadence = 40 * time.Millisecond
	floodFor     = 200 * time.Millisecond
	floodGap     = 2 * time.Millisecond
	floodMax     = 12 // generous ceiling on ~7 expected bursts
)

// floodResult is what one stream_on flood actually cost the host.
type floodResult struct {
	frames  int
	samples int64
	dials   int32
}

// runStreamOnFlood drives one session while control frames arrive far faster
// than the cadence, and reports both the frames sent and the samples taken.
func runStreamOnFlood(t *testing.T, vals map[string]float64) floodResult {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fc := newFakeConn()
	dialer := &fakeDialer{make: func(int32) (Conn, error) { return fc, nil }}
	sampler := &countingSampler{vals: vals}

	stop := make(chan struct{})
	go func() {
		tick := time.NewTicker(floodGap)
		defer tick.Stop()
		until := time.After(floodFor)
		for {
			select {
			case <-stop:
				return
			case <-until:
				time.Sleep(floodCadence) // let anything still due run, then end it
				cancel()
				return
			case <-tick.C:
				select {
				case fc.recv <- []byte(streamOn): // non-blocking, exactly like the real pump
				default:
				}
			}
		}
	}()

	s := newStreamer(t, Options{
		Config: liveConfig(true), Dialer: dialer, Clock: platform.SystemClock{},
		Sampler: sampler, Interval: floodCadence, IdlePoll: 5 * time.Millisecond,
	})
	s.Run(ctx)
	close(stop)

	return floodResult{frames: fc.FrameCount(), samples: sampler.Calls(), dials: atomic.LoadInt32(&dialer.dials)}
}

func TestStreamerFloorsBurstRateUnderStreamOnFlood(t *testing.T) {
	// The loop wakes on EVERY control frame, not just on the cadence timer, so the
	// rate clamp only means anything if the burst itself is floored. A pulse
	// looping on stream_on (bug, misconfig, compromised ingest) must not turn into
	// one sample+send per frame.
	t.Run("sends stay on the cadence", func(t *testing.T) {
		got := runStreamOnFlood(t, map[string]float64{"sys.cpu.total_pct": 12.5})
		if got.frames == 0 {
			t.Fatal("no frames at all — the flood should still have armed the burst")
		}
		if got.frames > floodMax {
			t.Fatalf("frames = %d for %v of stream_on every %v at a %v floor — the burst followed the frame rate, not the cadence", got.frames, floodFor, floodGap, floodCadence)
		}
		if got.dials != 1 {
			t.Fatalf("dials = %d, want 1 (a flood must not churn the connection)", got.dials)
		}
	})

	// The floor has to cap the WORK, not just the frames. On a host whose
	// collectors return nothing, no frame is ever sent — so a floor that only
	// advanced on a successful send would let the flood drive one full
	// Sampler.Sample() per control frame, unseen by any frame count.
	t.Run("sampling stays on the cadence even when nothing is sent", func(t *testing.T) {
		got := runStreamOnFlood(t, nil)
		if got.frames != 0 {
			t.Fatalf("frames = %d, want 0 (an empty snapshot must not be sent)", got.frames)
		}
		if got.samples == 0 {
			t.Fatal("never sampled — the flood should still have armed the burst")
		}
		if got.samples > floodMax {
			t.Fatalf("Sample() ran %d times for %v of stream_on every %v at a %v floor — the work followed the frame rate, not the cadence", got.samples, floodFor, floodGap, floodCadence)
		}
	})
}

func TestStreamerIgnoresUnusableControlFrames(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const target = 4
	fc := newFakeConn()
	fc.onWrite = func(n int) {
		if n == 1 {
			fc.Control(`{"t":`) // truncated JSON mid-burst must not stop anything
		}
		if n >= target {
			cancel()
		}
	}
	fc.Control(`not json at all`)
	fc.Control(`{"t":123,"m":{"sys.cpu.total_pct":1}}`) // the agent's OWN frame shape, numeric t
	fc.Control(`{"t":"ping"}`)                          // a type this agent version does not know
	fc.Control(streamOn)                                // ... and the burst still starts
	dialer := &fakeDialer{make: func(int32) (Conn, error) { return fc, nil }}

	s := newStreamer(t, Options{Config: liveConfig(true), Dialer: dialer, Interval: time.Second})
	s.Run(ctx)

	if got := fc.FrameCount(); got < target {
		t.Fatalf("frames = %d, want >= %d (junk frames must be dropped, not acted on)", got, target)
	}
	if d := atomic.LoadInt32(&dialer.dials); d != 1 {
		t.Fatalf("dials = %d, want 1 (junk must not kill the session)", d)
	}
}

func TestStreamerHintsWhenPulseNeverArmsTheChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var logs bytes.Buffer
	fc := newFakeConn()
	dialer := &fakeDialer{make: func(int32) (Conn, error) { return fc, nil }}

	// 90 idle iterations = 90 fake seconds, so the 60 s window is passed with room
	// to spare — the deploy-order hint must still be logged exactly once.
	s := newStreamer(t, Options{
		Config: liveFor(90, cancel, nil), Dialer: dialer,
		Log: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})),
	})
	s.Run(ctx)

	if got := strings.Count(logs.String(), "no stream_on received"); got != 1 {
		t.Fatalf("hint logged %d times, want exactly 1:\n%s", got, logs.String())
	}
}

func TestStreamerReconnectsWhenPeerClosesWhileIdle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// An unarmed session never writes, so a dead peer can only be noticed through
	// the closed Recv channel — without that it would sit on a corpse forever.
	dialer := &fakeDialer{make: func(dial int32) (Conn, error) {
		fc := newFakeConn()
		if dial == 1 {
			close(fc.recv)
			return fc, nil
		}
		fc.onWrite = func(int) { cancel() }
		fc.Control(streamOn)
		return fc, nil
	}}

	s := newStreamer(t, Options{Config: liveConfig(true), Dialer: dialer, Interval: time.Second, Backoff: time.Second})
	s.Run(ctx)

	if d := atomic.LoadInt32(&dialer.dials); d < 2 {
		t.Fatalf("dials = %d, want >= 2 (a peer that closed while idle must be redialled)", d)
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
