package sender

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"

	"github.com/LIVCK/agent/internal/buffer"
	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/event"
	"github.com/LIVCK/agent/internal/platform/platformtest"
	"github.com/LIVCK/agent/pkg/wire"
)

// scriptedServer returns programmed statuses and records the decoded batches.
type scriptedServer struct {
	mu       sync.Mutex
	statuses []int
	bodies   []string
	calls    int
	batches  []*wire.MetricBatch
	reached  chan int
}

func newScriptedServer(statuses []int, bodies []string) *scriptedServer {
	return &scriptedServer{statuses: statuses, bodies: bodies, reached: make(chan int, 256)}
}

func (s *scriptedServer) handler(dec *zstd.Decoder) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		plain, err := dec.DecodeAll(raw, nil)
		var batch wire.MetricBatch
		if err == nil {
			_ = proto.Unmarshal(plain, &batch)
		}
		s.mu.Lock()
		idx := s.calls
		s.calls++
		s.batches = append(s.batches, &batch)
		status := 202
		body := `{"status":"ok","config_version":1,"server_time_unix_ms":0}`
		if idx < len(s.statuses) {
			status = s.statuses[idx]
			if idx < len(s.bodies) {
				body = s.bodies[idx]
			}
		} else if len(s.statuses) > 0 {
			status = s.statuses[len(s.statuses)-1]
			if len(s.bodies) > 0 {
				body = s.bodies[len(s.bodies)-1]
			}
		}
		s.mu.Unlock()
		select {
		case s.reached <- idx + 1:
		default:
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}
}

func (s *scriptedServer) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *scriptedServer) allBatches() []*wire.MetricBatch {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*wire.MetricBatch, len(s.batches))
	copy(out, s.batches)
	return out
}

type senderFixture struct {
	sender *Sender
	ring   *buffer.Ring
	events *event.Queue
	clock  *platformtest.Clock
	server *scriptedServer
	http   *httptest.Server
}

func newFixture(t *testing.T, statuses []int, bodies []string, opts ...func(*Options)) *senderFixture {
	t.Helper()
	clk := platformtest.NewClock(time.Unix(1_700_000_000, 0))
	q := event.NewQueue(clk, seqID(), 64)
	ring := buffer.New(clk, q, buffer.DefaultMaxBytes, buffer.DefaultHorizon)

	dec, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(dec.Close)
	srv := newScriptedServer(statuses, bodies)
	httpSrv := httptest.NewServer(srv.handler(dec))
	t.Cleanup(httpSrv.Close)

	o := Options{
		Client:          httpSrv.Client(),
		URL:             httpSrv.URL,
		TokenFn:         func() string { return "lvk_test" },
		InstanceID:      "11111111-1111-4111-8111-111111111111",
		AgentVersion:    "0.0.0-test",
		Clock:           clk,
		Buffer:          ring,
		Events:          q,
		Config:          func() *config.Config { return config.Defaults() },
		Emitter:         q,
		NewID:           seqID(),
		Jitter:          func(int64) int64 { return 0 }, // deterministic, zero backoff
		QuarantineFloor: time.Millisecond,
	}
	for _, fn := range opts {
		fn(&o)
	}
	s, err := New(o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return &senderFixture{sender: s, ring: ring, events: q, clock: clk, server: srv, http: httpSrv}
}

func addReport(ring *buffer.Ring, ms int64) {
	ring.Add(&wire.Report{SampledAtUnixMs: ms, Metrics: map[string]float64{"sys.cpu.total_pct": 12.5}})
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", msg)
}

func TestSenderHappyPathRemovesOn202(t *testing.T) {
	f := newFixture(t, nil, nil)
	addReport(f.ring, f.clock.Now().UnixMilli())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.sender.Run(ctx)

	waitFor(t, func() bool { return f.ring.Len() == 0 }, "ring drained on 202")
	cancel()

	batches := f.server.allBatches()
	if len(batches) == 0 {
		t.Fatal("server received no batch")
	}
	if batches[0].IdempotencyKey == "" {
		t.Fatal("batch missing idempotency key")
	}
	if len(batches[0].Reports) != 1 {
		t.Fatalf("want 1 report in batch, got %d", len(batches[0].Reports))
	}
}

func TestSenderRetriesThenSucceedsWithStableKey(t *testing.T) {
	f := newFixture(t,
		[]int{503, 503, 202},
		[]string{`{"error":{"code":"RETRY_LATER"}}`, `{"error":{"code":"RETRY_LATER"}}`, `{"status":"ok"}`})
	addReport(f.ring, f.clock.Now().UnixMilli())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.sender.Run(ctx)

	waitFor(t, func() bool { return f.ring.Len() == 0 }, "ring drained after retries")
	cancel()

	batches := f.server.allBatches()
	if len(batches) < 3 {
		t.Fatalf("want at least 3 attempts, got %d", len(batches))
	}
	key := batches[0].IdempotencyKey
	for i, b := range batches[:3] {
		if b.IdempotencyKey != key {
			t.Fatalf("attempt %d changed idempotency key: %q != %q", i, b.IdempotencyKey, key)
		}
	}
	// Backoff actually slept between retries.
	if len(f.clock.Sleeps()) < 2 {
		t.Fatalf("want at least 2 backoff sleeps, got %d", len(f.clock.Sleeps()))
	}
}

func TestSenderTerminal4xxDiscards(t *testing.T) {
	f := newFixture(t, []int{400}, []string{`{"error":{"code":"MALFORMED_BODY","retryable":false}}`})
	addReport(f.ring, f.clock.Now().UnixMilli())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.sender.Run(ctx)

	waitFor(t, func() bool { return f.ring.Len() == 0 }, "ring drained by terminal 4xx discard")
	cancel()
}

func TestSenderQuarantineKeepsData(t *testing.T) {
	f := newFixture(t, []int{401}, []string{`{"error":{"code":"TOKEN_INVALID","retryable":false}}`})
	addReport(f.ring, f.clock.Now().UnixMilli())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.sender.Run(ctx)

	// Let it hammer a few times, then stop.
	waitFor(t, func() bool { return f.server.count() >= 3 }, "several quarantine retries")
	cancel()

	if f.ring.Len() != 1 {
		t.Fatalf("quarantine must never drop data, ring len = %d", f.ring.Len())
	}
}

func TestSenderConfigPiggybackTriggersPull(t *testing.T) {
	var triggered atomic.Bool
	f := newFixture(t,
		[]int{202},
		[]string{`{"status":"ok","config_version":5,"server_time_unix_ms":0}`},
		func(o *Options) {
			o.OnConfigVersion = func(v uint32) {
				if v == 5 {
					triggered.Store(true)
				}
			}
		})
	addReport(f.ring, f.clock.Now().UnixMilli())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.sender.Run(ctx)

	waitFor(t, func() bool { return triggered.Load() }, "config piggyback triggered a pull")
	cancel()
}

func TestSenderEventOnlyBatch(t *testing.T) {
	f := newFixture(t, nil, nil)
	f.events.Emit(wire.EventType_EVENT_TYPE_BOOT, map[string]string{"boot_id": "abc"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.sender.Run(ctx)

	waitFor(t, func() bool { return f.events.Len() == 0 }, "event delivered and removed on 202")
	cancel()

	batches := f.server.allBatches()
	if len(batches) == 0 || len(batches[0].Events) != 1 {
		t.Fatalf("want an event-only batch with 1 event, got %+v", batches)
	}
	if len(batches[0].Reports) != 0 {
		t.Fatal("event-only batch should carry no reports")
	}
}

// recEmitter records emitted lifecycle events without feeding the send queue, so
// an emission can be asserted without a drain race.
type recEmitter struct {
	mu    sync.Mutex
	types []wire.EventType
}

func (r *recEmitter) Emit(t wire.EventType, _ map[string]string) {
	r.mu.Lock()
	r.types = append(r.types, t)
	r.mu.Unlock()
}

func (r *recEmitter) has(t wire.EventType) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.types {
		if e == t {
			return true
		}
	}
	return false
}

func TestSenderEmitsClockSkewEvent(t *testing.T) {
	// The fake clock sits at 1_700_000_000s; the server reports a time two
	// minutes behind, well over the 30s event threshold.
	skewed := (int64(1_700_000_000) - 120) * 1000
	rec := &recEmitter{}
	f := newFixture(t,
		[]int{202},
		[]string{`{"status":"ok","config_version":1,"server_time_unix_ms":` + itoaTest(skewed) + `}`},
		func(o *Options) { o.Emitter = rec })
	addReport(f.ring, f.clock.Now().UnixMilli())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.sender.Run(ctx)

	waitFor(t, func() bool { return rec.has(wire.EventType_EVENT_TYPE_CLOCK_SKEW_DETECTED) }, "clock_skew_detected emitted")
	cancel()
}

func itoaTest(v int64) string {
	neg := v < 0
	if neg {
		v = -v
	}
	if v == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

func seqID() func() string {
	var n atomic.Int64
	return func() string {
		v := n.Add(1)
		return "key-" + time.Duration(v).String()
	}
}
