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
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sort"
	"strconv"
	"sync"
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
	// Meta is the INITIAL host profile (hostinfo.Meta). It decorates the first report
	// of every batch until a batch that carried it gets a 202 - delivery-confirmed,
	// unlike the old assemble-time flag, which lost the profile for the whole
	// process lifetime when the bootstrap report was evicted from the ring
	// during a long initial ingest outage. May be nil.
	//
	// After construction the profile is owned by the Sender: hand a re-read profile
	// to SetMeta, which re-arms delivery only when it actually differs.
	Meta map[string]string
	// OnConfigVersion is called when a 202 advertises a newer config version.
	OnConfigVersion func(v uint32)

	// Tuning (zero uses defaults).
	BaseBackoff     time.Duration
	CapBackoff      time.Duration
	QuarantineFloor time.Duration
	Jitter          jitterFn
}

// metaKeyRAMTotal is the profile key whose raw value is deliberately NOT compared
// verbatim; see MetaDigest.
const metaKeyRAMTotal = "ram_total_bytes"

// volatileMetaKeys are dropped from the change digest because they move without the
// host having changed, and re-sending the whole profile for them would defeat the
// point of sending on change at all.
//
// boot_time is read from /proc/stat btime, which the kernel derives from the current
// wall clock minus uptime. An NTP step or slew shifts it by a second or two on a host
// that has been idle for months. A genuine change to boot_time means the machine
// rebooted, and a reboot restarts the agent, which sends the profile from the
// first-report-after-start path regardless.
var volatileMetaKeys = map[string]bool{"boot_time": true}

// MetaDigest fingerprints a host profile for change detection: sorted key=value pairs
// hashed, volatile keys dropped, ram_total_bytes compared in whole GiB.
//
// The GiB rounding is not cosmetic. MemTotal is derived from totalram_pages, and a
// balloon driver (virtio_balloon on KVM/Proxmox, hv_balloon on Hyper-V) hands pages
// back and forth at runtime, so a ballooned guest reports a MemTotal that drifts by
// tens of MiB all day long. Nobody resizes a VM by less than a gigabyte, so whole GiB
// keeps every real change and drops the entire balloon noise floor.
//
// Exported so the wiring that feeds SetMeta can be tested against the same key the
// sender compares on.
func MetaDigest(meta map[string]string) string {
	keys := make([]string, 0, len(meta))
	for k := range meta {
		if !volatileMetaKeys[k] {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	h := sha256.New()
	for _, k := range keys {
		v := meta[k]
		if k == metaKeyRAMTotal {
			v = ramGiB(v)
		}
		// The NUL separators keep {"ab":"c"} and {"a":"bc"} from colliding.
		h.Write([]byte(k))
		h.Write([]byte{0})
		h.Write([]byte(v))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// ramGiB rounds a byte count to the nearest whole GiB. A value that is not a plain
// integer is passed through untouched so an unexpected format still compares exactly.
func ramGiB(v string) string {
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return v
	}
	const halfGiB = 1 << 29
	return strconv.FormatUint((n+halfGiB)>>30, 10)
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
	// skewWarned dedupes the 422 clock-skew alarm to one event per skew episode
	// (reset by the next accepted batch).
	skewWarned bool

	// mu guards the host profile, which is the one piece of sender state a second
	// goroutine writes: the collect loop calls SetMeta after re-reading the host
	// facts, while the delivery loop reads it when it builds and confirms a batch.
	mu         sync.Mutex
	meta       map[string]string
	metaDigest string
	metaSent   bool
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
		meta:            opt.Meta,
		metaDigest:      MetaDigest(opt.Meta),
	}, nil
}

// SetMeta installs a freshly collected host profile.
//
// The wire contract sends meta "only on change plus the first report after start"
// (SERVER-AGENT.md 4.1), so this is the "on change" half. An unchanged profile is a
// complete no-op: neither the stored map nor the delivery flag move, and the next
// batch stays profile-free. A changed profile re-arms the attach flag, so the next
// batch carries it and mark-after-202 confirms it exactly like the bootstrap profile.
//
// A restart needs no special case: a fresh Sender starts with metaSent false, so the
// first report of every process life carries the profile whether it changed or not.
func (s *Sender) SetMeta(meta map[string]string) {
	digest := MetaDigest(meta)

	s.mu.Lock()
	defer s.mu.Unlock()

	if digest == s.metaDigest {
		return
	}
	s.meta = meta
	s.metaDigest = digest
	s.metaSent = false
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
	carriedMeta        bool
	// meta and metaDigest are the profile snapshot this batch carries. They are
	// captured at build time so every retry re-encodes the same bytes, and so a
	// refresh that lands mid-flight is not mistaken for delivered by the 202.
	meta       map[string]string
	metaDigest string
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
			// A 422 CLOCK_SKEW discard is SILENT permanent data loss while the host
			// clock stays broken — raise it once per episode: a loud log line plus
			// the clock_skew_detected event (queued, so it reaches the timeline with
			// the first batch that goes through once the clock is fixed). The 202
			// path can never see this case (skew batches never get a 202).
			if resp.errorCode == "CLOCK_SKEW" && !s.skewWarned {
				s.skewWarned = true
				s.log.Error("host clock skew exceeds the ingest window — ALL metric batches are being discarded until the clock is fixed (check NTP/chrony)")
				if s.opt.Emitter != nil {
					s.opt.Emitter.Emit(wire.EventType_EVENT_TYPE_CLOCK_SKEW_DETECTED, map[string]string{
						"reason": "batch_rejected",
					})
				}
			}
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
	// A 202 can still shed data (cardinality caps, unknown keys). Without this
	// line a silently cut family (probes, SMART, …) is invisible everywhere:
	// the batch succeeds, the charts just thin out.
	if resp.dropped.any() {
		s.log.Warn("ingest accepted batch but dropped data",
			"reports", resp.dropped.Reports,
			"keys", resp.dropped.Keys,
			"events", resp.dropped.Events,
			"reasons", resp.dropReasons)
	}
	s.opt.Buffer.Remove(p.seqs)
	s.opt.Events.Remove(p.eventIDs)
	if p.carriedFingerprint {
		s.fpSent = true
	}
	if p.carriedMeta {
		// Only mark delivered when the profile that just landed is still the current
		// one. A SetMeta between build and 202 leaves the digests apart, so the newer
		// profile rides the next batch instead of being swallowed by this one's ack.
		s.mu.Lock()
		if p.metaDigest == s.metaDigest {
			s.metaSent = true
		}
		s.mu.Unlock()
	}
	s.skewWarned = false
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

	s.mu.Lock()
	if !s.metaSent && len(reports) > 0 && len(s.meta) > 0 {
		p.carriedMeta = true
		p.meta = s.meta
		p.metaDigest = s.metaDigest
	}
	s.mu.Unlock()

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
	if p.carriedMeta && len(p.reports) > 0 {
		// Decorate (idempotent under retries): the profile rides the first report
		// of the batch, mark-after-202 in onAccepted. MERGED under any meta the
		// report already carries (the runner's updater heartbeat) - report-level
		// keys win, and the stored profile map itself is never mutated. p.meta is
		// the snapshot taken at build time, so a refresh landing mid-retry cannot
		// change the bytes behind a live idempotency key.
		merged := make(map[string]string, len(p.meta)+len(p.reports[0].Meta))
		for k, v := range p.meta {
			merged[k] = v
		}
		for k, v := range p.reports[0].Meta {
			merged[k] = v
		}
		p.reports[0].Meta = merged
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
