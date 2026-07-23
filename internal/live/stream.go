// Package live implements the agent side of Live-Watch: a short-lived WSS signal
// channel to pulse that bursts an instantaneous metric snapshot every few seconds
// while a browser viewer is watching. It is armed by the features.live config
// flag and closes as soon as the flag clears, so it costs nothing on a host
// nobody is watching.
//
// The burst carries its OWN collector instances with their OWN delta state:
// CPU/net/diskio rates are deltas between consecutive reads, so sharing the
// report collectors would corrupt both loops' baselines. main.go builds a
// second registry for exactly this reason. The signal channel is never
// persisted, never buffered, and never retried — a dropped frame is simply the
// next frame's problem, 2 seconds later.
package live

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/rand"
	"strings"
	"time"

	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/platform"
)

const (
	// defaultInterval is the burst cadence — every few seconds while watched.
	defaultInterval = 2 * time.Second
	// defaultIdlePoll is how often the supervisor re-checks features.live while
	// the feature is off. Cheap: one atomic config read.
	defaultIdlePoll = 3 * time.Second
	// defaultBackoff is the FLOOR reconnect delay after a dial or write failure
	// while live stays on. Consecutive failures double it (full jitter) up to
	// backoffCap — live can be left enabled fleet-wide (auto-disable-on-last-
	// viewer is deferred), so a flat 2 s would have every affected agent redial
	// TLS every 2 s for the whole duration of a pulse outage.
	defaultBackoff = 2 * time.Second
	// backoffCap bounds the exponential reconnect ladder.
	backoffCap = 60 * time.Second
	// sessionResetAfter: a session that stayed up this long counts as healthy,
	// so the next failure starts the ladder from the floor again.
	sessionResetAfter = 30 * time.Second
)

// Conn is one open signal channel to pulse. WriteText sends a burst frame; a
// non-nil error means the connection is dead and the caller reconnects.
type Conn interface {
	WriteText(b []byte) error
	Close() error
}

// Dialer opens a signal channel. The real implementation (ws.go) speaks
// WebSocket to pulse's /v1/live/agent, Bearer-authenticated with the managed
// token; tests substitute a fake.
type Dialer interface {
	Dial(ctx context.Context, url, token string) (Conn, error)
}

// Sampler produces one instantaneous snapshot of the cheap collectors. It holds
// its own delta state, independent of the report loop.
type Sampler interface {
	Sample(ctx context.Context) map[string]float64
}

// Options bundles the streamer's dependencies. Config, Sampler, Dialer, WSURL,
// TokenFn and Clock are required; the cadences default when zero.
type Options struct {
	Config   func() *config.Config
	Sampler  Sampler
	Dialer   Dialer
	WSURL    string
	TokenFn  func() string
	Clock    platform.Clock
	Log      *slog.Logger
	Interval time.Duration
	IdlePoll time.Duration
	Backoff  time.Duration
	// Jitter maps an exclusive ceiling to a random value in [0, n) — full-jitter
	// backoff (same contract as the sender's). Defaults to math/rand.
	Jitter func(n int64) int64
}

// Streamer supervises the signal channel lifecycle against features.live.
type Streamer struct {
	opt Options
	log *slog.Logger
}

// frame is one burst: sampled-at unix ms and the instantaneous metric values.
// pulse relays this JSON text frame verbatim to the browser viewer.
type frame struct {
	T int64              `json:"t"`
	M map[string]float64 `json:"m"`
}

// New builds a Streamer, filling cadence defaults.
func New(opt Options) *Streamer {
	if opt.Log == nil {
		opt.Log = slog.Default()
	}
	if opt.Interval <= 0 {
		opt.Interval = defaultInterval
	}
	if opt.IdlePoll <= 0 {
		opt.IdlePoll = defaultIdlePoll
	}
	if opt.Backoff <= 0 {
		opt.Backoff = defaultBackoff
	}
	if opt.Jitter == nil {
		opt.Jitter = rand.Int63n
	}
	return &Streamer{opt: opt, log: opt.Log}
}

// nextBackoff returns the full-jitter delay for the given failure streak and
// advances it: uniform in [0, min(Backoff<<attempt, backoffCap)].
func (s *Streamer) nextBackoff(attempt *uint) time.Duration {
	ceiling := s.opt.Backoff << *attempt
	if ceiling <= 0 || ceiling > backoffCap {
		ceiling = backoffCap
	}
	if *attempt < 16 {
		*attempt++
	}
	return time.Duration(s.opt.Jitter(int64(ceiling) + 1))
}

// Run supervises the channel until ctx is done. While features.live is off it
// idles (never dials); while on it holds one session open and reconnects with a
// backoff after any failure. A clean live-off transition closes the channel and
// returns to idle without an error.
func (s *Streamer) Run(ctx context.Context) {
	var attempt uint
	for {
		if ctx.Err() != nil {
			return
		}
		if !s.live() {
			// Idle: re-check on the poll cadence. A cancelled ctx ends the loop.
			if err := s.opt.Clock.Sleep(ctx, s.opt.IdlePoll); err != nil {
				return
			}
			continue
		}
		started := s.opt.Clock.Now()
		if err := s.session(ctx); err != nil && ctx.Err() == nil {
			if s.opt.Clock.Now().Sub(started) >= sessionResetAfter {
				attempt = 0
			}
			delay := s.nextBackoff(&attempt)
			s.log.Debug("live session ended, will reconnect", "err", err.Error(), "delay", delay.String())
			if berr := s.opt.Clock.Sleep(ctx, delay); berr != nil {
				return
			}
		} else {
			attempt = 0
		}
	}
}

// session dials once and bursts until live clears, ctx ends, or a write fails.
// It returns nil on a clean stop (live off / ctx done) and the transport error
// on a failure the supervisor should back off and reconnect from.
func (s *Streamer) session(ctx context.Context) error {
	conn, err := s.opt.Dialer.Dial(ctx, s.opt.WSURL, s.opt.TokenFn())
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	s.log.Info("live stream connected", "url", s.opt.WSURL)

	for {
		if ctx.Err() != nil || !s.live() {
			return nil
		}
		snap := s.opt.Sampler.Sample(ctx)
		if len(snap) > 0 {
			if b, mErr := json.Marshal(frame{T: s.opt.Clock.Now().UnixMilli(), M: snap}); mErr == nil {
				if wErr := conn.WriteText(b); wErr != nil {
					return wErr
				}
			}
		}
		if err := s.opt.Clock.Sleep(ctx, s.opt.Interval); err != nil {
			return nil // ctx cancelled: clean stop.
		}
	}
}

func (s *Streamer) live() bool {
	c := s.opt.Config()
	return c != nil && c.Features.Live
}

// WSURL derives the agent signal-channel URL from the ingest base URL: the
// scheme is upgraded to WebSocket (https→wss, http→ws) and the fixed
// /v1/live/agent path is appended. A base without a recognised scheme is
// returned with the path appended unchanged (dev/test convenience).
func WSURL(ingestBase string) string {
	u := strings.TrimRight(strings.TrimSpace(ingestBase), "/")
	switch {
	case strings.HasPrefix(u, "https://"):
		u = "wss://" + strings.TrimPrefix(u, "https://")
	case strings.HasPrefix(u, "http://"):
		u = "ws://" + strings.TrimPrefix(u, "http://")
	}
	return u + "/v1/live/agent"
}
