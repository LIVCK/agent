// Package sender turns buffered reports and pending events into ingest requests
// and drives the retry matrix. One batch is held across all its retries so its
// idempotency_key stays stable (server-side report dedupe), while sent_at is
// refreshed on every attempt so a retried batch is not read as clock skew. On a
// 202 the batch's reports and events are removed from the buffer and queue
// (remove-on-202, at-least-once). The four outcomes map straight from the status
// code: 202 succeeds, 429/404/5xx back off, the terminal 4xx (400/413/422)
// discards the poison batch, and 401/403/409 quarantine without wiping.
package sender

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"

	"github.com/LIVCK/agent/internal/buffer"
	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/event"
	"github.com/LIVCK/agent/internal/platform"
	"github.com/LIVCK/agent/pkg/wire"
)

const (
	// maxBatchBytes keeps an uncompressed batch under the pulse 8 MB decode
	// guard with margin.
	maxBatchBytes = 7 * 1024 * 1024
	// maxEventsPerBatch is caps.max_events; overflow is dropped server-side.
	maxEventsPerBatch = 20
	// maxRespBody caps the response body the agent reads.
	maxRespBody = 64 * 1024
	// skewEventThreshold raises clock_skew_detected.
	skewEventThreshold = 30 * time.Second
	// idleTick is the fallback wake cadence when nothing signalled the sender.
	idleTick = 1 * time.Second
)

// Options configures a Sender. Buffer, Events, Config, Clock, URL, TokenFn and
// InstanceID are required; the rest have defaults.
type Options struct {
	Client       *http.Client
	URL          string
	TokenFn      func() string
	InstanceID   string
	AgentVersion string
	Clock        platform.Clock
	Buffer       *buffer.Ring
	Events       *event.Queue
	Config       func() *config.Config
	Emitter      event.Emitter
	NewID        func() string // fresh UUIDv4 for idempotency keys
	Log          *slog.Logger
	// Fingerprint is attached to batches until the first 202 (and re-attached
	// when it changes). May be nil.
	Fingerprint map[string]string
	// OnConfigVersion is called when a 202 advertises a newer config version.
	OnConfigVersion func(v uint32)

	// Tuning (zero uses defaults).
	BaseBackoff     time.Duration
	CapBackoff      time.Duration
	QuarantineFloor time.Duration
	Jitter          jitterFn
}

// Sender drives delivery.
type Sender struct {
	opt Options
	enc *encoder
	log *slog.Logger

	wake chan struct{}

	baseBackoff     time.Duration
	capBackoff      time.Duration
	quarantineFloor time.Duration
	jitter          jitterFn

	fpSent bool
}

// New builds a Sender.
func New(opt Options) (*Sender, error) {
	enc, err := newEncoder()
	if err != nil {
		return nil, err
	}
	log := opt.Log
	if log == nil {
		log = slog.Default()
	}
	if opt.Client == nil {
		opt.Client = http.DefaultClient
	}
	jit := opt.Jitter
	if jit == nil {
		jit = func(n int64) int64 { return rand.Int64N(n) }
	}
	base := opt.BaseBackoff
	if base <= 0 {
		base = DefaultBaseBackoff
	}
	capDur := opt.CapBackoff
	if capDur <= 0 {
		capDur = DefaultCapBackoff
	}
	qfloor := opt.QuarantineFloor
	if qfloor <= 0 {
		qfloor = DefaultQuarantineFloor
	}
	return &Sender{
		opt:             opt,
		enc:             enc,
		log:             log,
		wake:            make(chan struct{}, 1),
		baseBackoff:     base,
		capBackoff:      capDur,
		quarantineFloor: qfloor,
		jitter:          jit,
	}, nil
}

// Wake nudges the delivery loop to drain now (called after the collect loop
// enqueues a report). It never blocks.
func (s *Sender) Wake() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Close releases the encoder.
func (s *Sender) Close() { s.enc.close() }

// pending is one batch held across its retries.
type pending struct {
	idempotencyKey     string
	reports            []*wire.Report
	seqs               []uint64
	events             []event.Event
	eventIDs           []string
	carriedFingerprint bool
}

// Run drives delivery until ctx is done. It holds a batch across retries so the
// idempotency key is stable, and only releases it on a terminal outcome (202 or
// a discard). On ctx cancellation any in-flight reports and events stay in the
// buffer and queue for the exit spool.
func (s *Sender) Run(ctx context.Context) {
	var p *pending
	b := newBackoff(s.baseBackoff, s.capBackoff, s.jitter)

	for {
		if ctx.Err() != nil {
			return
		}
		if p == nil {
			p = s.buildPending()
			if p == nil {
				if !s.waitForWork(ctx) {
					return
				}
				continue
			}
		}

		resp, err := s.attempt(ctx, p)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.log.Warn("ingest attempt failed, backing off", "err", err.Error())
			if !s.sleep(ctx, &b, 0) {
				return
			}
			continue
		}

		switch resp.outcome {
		case outcomeOK:
			s.onAccepted(p, resp)
			p = nil
			b.reset()
		case outcomeDrop:
			s.log.Warn("batch rejected, discarding", "status", resp.status, "code", resp.errorCode)
			s.opt.Buffer.Remove(p.seqs)
			s.opt.Events.Remove(p.eventIDs)
			p = nil
			b.reset()
		case outcomeQuarantine:
			s.log.Warn("auth rejected, quarantine backoff, keeping data (no wipe)",
				"status", resp.status, "code", resp.errorCode)
			if !s.sleep(ctx, &b, s.quarantineFloor) {
				return
			}
		case outcomeRetry:
			s.log.Info("ingest retry", "status", resp.status, "code", resp.errorCode)
			if !s.sleep(ctx, &b, resp.retryAfter) {
				return
			}
		}
	}
}

// onAccepted handles a 202: remove the batch's reports and events, mark the
// fingerprint delivered, trigger a config pull if a newer version was
// advertised, and check clock skew.
func (s *Sender) onAccepted(p *pending, resp response) {
	s.opt.Buffer.Remove(p.seqs)
	s.opt.Events.Remove(p.eventIDs)
	if p.carriedFingerprint {
		s.fpSent = true
	}
	if resp.configVersion > uint32(s.opt.Config().ConfigVersion) && s.opt.OnConfigVersion != nil {
		s.opt.OnConfigVersion(resp.configVersion)
	}
	if resp.serverTimeMs > 0 {
		s.checkSkew(resp.serverTimeMs)
	}
}

func (s *Sender) checkSkew(serverTimeMs int64) {
	skew := s.opt.Clock.Now().Sub(time.UnixMilli(serverTimeMs))
	abs := skew
	if abs < 0 {
		abs = -abs
	}
	if abs > skewEventThreshold {
		s.log.Warn("clock skew against server", "skew_ms", skew.Milliseconds())
		if s.opt.Emitter != nil {
			s.opt.Emitter.Emit(wire.EventType_EVENT_TYPE_CLOCK_SKEW_DETECTED, map[string]string{
				"skew_ms": strconv.FormatInt(skew.Milliseconds(), 10),
			})
		}
	}
}

// buildPending pulls the newest reports and oldest events into a batch. It
// returns nil when there is nothing to send. A marshal failure is poison: the
// reports are dropped so they cannot wedge the loop.
func (s *Sender) buildPending() *pending {
	cfg := s.opt.Config()
	reports, seqs := s.opt.Buffer.TakeBatch(cfg.Limits.MaxReportsPerBatch, maxBatchBytes)
	events := s.opt.Events.Peek(maxEventsPerBatch)
	if len(reports) == 0 && len(events) == 0 {
		return nil
	}
	ids := make([]string, len(events))
	for i, e := range events {
		ids[i] = e.ID
	}
	p := &pending{
		idempotencyKey: s.opt.NewID(),
		reports:        reports,
		seqs:           seqs,
		events:         events,
		eventIDs:       ids,
	}
	if !s.fpSent && len(s.opt.Fingerprint) > 0 {
		p.carriedFingerprint = true
	}
	return p
}

// attempt performs one HTTP round-trip for p, rebuilding the batch with a fresh
// sent_at so a retried batch is not read as skew, while the idempotency key
// stays stable.
func (s *Sender) attempt(ctx context.Context, p *pending) (response, error) {
	batch := &wire.MetricBatch{
		SchemaVersion:        1,
		IdempotencyKey:       p.idempotencyKey,
		AgentVersion:         s.opt.AgentVersion,
		AgentInstanceId:      s.opt.InstanceID,
		SentAtUnixMs:         s.opt.Clock.Now().UnixMilli(),
		AppliedConfigVersion: uint32(s.opt.Config().ConfigVersion),
		Reports:              p.reports,
		Events:               toWireEvents(p.events),
	}
	if p.carriedFingerprint {
		batch.Fingerprint = s.opt.Fingerprint
	}
	body, err := s.enc.encode(batch)
	if err != nil {
		// Marshal failure: drop the poison reports so the loop cannot wedge.
		s.log.Error("encode failed, discarding batch", "err", err.Error())
		s.opt.Buffer.Remove(p.seqs)
		s.opt.Events.Remove(p.eventIDs)
		return response{outcome: outcomeDrop, status: 0}, nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.opt.URL, bytes.NewReader(body))
	if err != nil {
		return response{}, err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "zstd")
	req.Header.Set("Authorization", "Bearer "+s.opt.TokenFn())

	resp, err := s.opt.Client.Do(req)
	if err != nil {
		return response{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, maxRespBody))
	return parseResponse(resp.StatusCode, resp.Header, respBody), nil
}

// sleep waits for the greater of floor and the next backoff step, aborting on
// ctx cancellation. It returns false when the context ended.
func (s *Sender) sleep(ctx context.Context, b *backoff, floor time.Duration) bool {
	d := b.next()
	if floor > d {
		d = floor
	}
	return s.opt.Clock.Sleep(ctx, d) == nil
}

// waitForWork blocks until a wake signal, the idle tick, or ctx cancellation. It
// returns false when the context ended.
func (s *Sender) waitForWork(ctx context.Context) bool {
	t := time.NewTimer(idleTick)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-s.wake:
		return true
	case <-t.C:
		return true
	}
}

func toWireEvents(events []event.Event) []*wire.Event {
	if len(events) == 0 {
		return nil
	}
	out := make([]*wire.Event, len(events))
	for i, e := range events {
		out[i] = e.ToWire()
	}
	return out
}
