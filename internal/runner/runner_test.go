package runner

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
	"github.com/LIVCK/agent/internal/collector"
	collectorself "github.com/LIVCK/agent/internal/collector/self"
	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/event"
	"github.com/LIVCK/agent/internal/platform"
	"github.com/LIVCK/agent/internal/platform/platformtest"
	"github.com/LIVCK/agent/internal/sender"
	"github.com/LIVCK/agent/pkg/wire"
)

type stubHost struct{ name string }

func (h stubHost) Hostname() (string, error) { return h.name, nil }

type ingestRecorder struct {
	mu      sync.Mutex
	batches []*wire.MetricBatch
	status  int
	dec     *zstd.Decoder
}

func (r *ingestRecorder) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		raw, _ := io.ReadAll(req.Body)
		plain, err := r.dec.DecodeAll(raw, nil)
		if err == nil {
			var b wire.MetricBatch
			if proto.Unmarshal(plain, &b) == nil {
				r.mu.Lock()
				r.batches = append(r.batches, &b)
				r.mu.Unlock()
			}
		}
		w.WriteHeader(r.status)
		if r.status == http.StatusAccepted {
			_, _ = io.WriteString(w, `{"status":"ok","config_version":1,"server_time_unix_ms":0}`)
		} else {
			_, _ = io.WriteString(w, `{"error":{"code":"RETRY_LATER","retryable":true}}`)
		}
	}
}

func (r *ingestRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.batches)
}

func (r *ingestRecorder) first() *wire.MetricBatch {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.batches) == 0 {
		return nil
	}
	return r.batches[0]
}

type runnerFixture struct {
	runner *Runner
	rec    *ingestRecorder
	fs     *platformtest.MemFS
	ring   *buffer.Ring
	spool  *buffer.Spool
}

func newRunnerFixture(t *testing.T, status int) *runnerFixture {
	t.Helper()
	dec, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(dec.Close)
	rec := &ingestRecorder{status: status, dec: dec}
	srv := httptest.NewServer(rec.handler())
	t.Cleanup(srv.Close)

	plat := platform.Platform{
		Clock: platform.SystemClock{},
		FS:    platformtest.NewMemFS(),
		Host:  stubHost{name: "test-host"},
	}
	fs := plat.FS.(*platformtest.MemFS)

	var idc atomic.Int64
	newID := func() string { return "id-" + time.Duration(idc.Add(1)).String() }

	queue := event.NewQueue(plat.Clock, newID, event.DefaultCapacity)
	ring := buffer.New(plat.Clock, queue, buffer.DefaultMaxBytes, buffer.DefaultHorizon)
	spool := buffer.NewSpool(plat.FS, "/state/spool.pb", buffer.DefaultMaxBytes)
	cfg := config.NewManager(nil, queue, plat.FS, "/state/config.json", nil)

	registry := collector.NewRegistry()
	registry.Register(collectorself.New(plat.FS, plat.Clock))

	sndr, err := sender.New(sender.Options{
		Client:       srv.Client(),
		URL:          srv.URL,
		TokenFn:      func() string { return "lvk_test" },
		InstanceID:   "11111111-1111-4111-8111-111111111111",
		AgentVersion: "0.0.0-test",
		Clock:        plat.Clock,
		Buffer:       ring,
		Events:       queue,
		Config:       cfg.Current,
		Emitter:      queue,
		NewID:        newID,
		Jitter:       func(int64) int64 { return 0 },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sndr.Close)

	r := New(Options{
		Platform:      plat,
		Config:        cfg,
		Registry:      registry,
		Ring:          ring,
		Events:        queue,
		Sender:        sndr,
		Spool:         spool,
		ConfigTrigger: make(chan struct{}, 1),
		Meta:          map[string]string{"os": "linux", "hostname": "test-host"},
	})
	return &runnerFixture{runner: r, rec: rec, fs: fs, ring: ring, spool: spool}
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

func TestRunnerBootstrapSendsRealReport(t *testing.T) {
	f := newRunnerFixture(t, http.StatusAccepted)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = f.runner.Run(ctx); close(done) }()

	waitFor(t, func() bool { return f.rec.count() >= 1 }, "bootstrap report reached ingest")
	batch := f.rec.first()
	if len(batch.Reports) != 1 {
		t.Fatalf("bootstrap batch should carry exactly 1 report, got %d", len(batch.Reports))
	}
	rep := batch.Reports[0]
	if _, ok := rep.Metrics["sys.agent.rss_bytes"]; !ok {
		t.Fatal("self collector metric missing from bootstrap report")
	}
	if _, ok := rep.Metrics["sys.agent.buffer_fill_pct"]; !ok {
		t.Fatal("buffer-derived metric missing from bootstrap report")
	}
	if rep.Meta["hostname"] != "test-host" {
		t.Fatalf("bootstrap report missing host meta, got %+v", rep.Meta)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not shut down on cancel")
	}
}

func TestRunnerSpoolsUnackedOnExit(t *testing.T) {
	// The server refuses every batch, so the bootstrap report is never acked and
	// must survive shutdown in the exit spool.
	f := newRunnerFixture(t, http.StatusServiceUnavailable)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = f.runner.Run(ctx); close(done) }()

	waitFor(t, func() bool { return f.rec.count() >= 1 }, "sender attempted at least once")
	cancel()
	<-done

	reports, _, err := f.spool.Load()
	if err != nil {
		t.Fatalf("load spool: %v", err)
	}
	if len(reports) < 1 {
		t.Fatal("unacked bootstrap report was not spooled on exit")
	}
}
