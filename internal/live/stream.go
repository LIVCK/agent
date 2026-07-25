// Package live implements the agent side of Live-Watch: a WSS signal channel to
// pulse that bursts an instantaneous metric snapshot every few seconds while a
// browser viewer is watching.
//
// Two independent switches gate the burst and BOTH must be on:
//
//   - features.live (config, pull-based) decides whether the channel is DIALLED
//     at all. Off ⇒ the supervisor idles and never opens a socket, so the whole
//     feature costs nothing on a host nobody watches.
//   - a stream_on control frame from pulse (push-based) decides whether the open
//     channel BURSTS. pulse owns the viewer-presence flag and repeats stream_on
//     every 15 s while somebody is watching; keepaliveWindow without one and the
//     agent stops bursting on its own.
//
// The second switch exists because the first one is both too coarse and too
// slow: Laravel sets features.live on the first ticket mint and the agent only
// learns of a change on its next config pull, so a single Live-Watch session
// used to leave a customer host streaming every 2 s forever. A stream_off keeps
// the connection OPEN and only stops the burst — that is the point: the channel
// stays armed, so the next viewer gets a picture in ~1-3 s instead of waiting
// for a config round-trip.
//
// DEPLOY ORDER: pulse first, agent second. An agent of this version talking to a
// pulse that does not send control frames NEVER bursts — it waits for a
// stream_on that never arrives and Live-Watch looks dead. That is the accepted
// trade-off of the server-first rollout (SERVER-AGENT.md §14a). A session that
// has been up for keepaliveWindow without a single stream_on logs a one-off hint
// naming exactly this cause.
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
	// defaultInterval is the burst cadence — every few seconds while watched. It
	// is the fallback when pulse's stream_on carries no rate_ms.
	defaultInterval = 2 * time.Second
	// defaultIdlePoll is how often the supervisor re-checks features.live while
	// the feature is off. Cheap: one atomic config read.
	defaultIdlePoll = 3 * time.Second
	// keepaliveWindow is the agent's self-protection: pulse repeats stream_on
	// every 15 s while a viewer is present, so a burst that has not seen one for
	// this long is orphaned — pulse gone, control frame lost, viewer flag expired
	// — and stops itself (SERVER-AGENT.md §7.5, PULSE.md §11.3).
	keepaliveWindow = 60 * time.Second
	// defaultIdleTick is the coarse re-check cadence of a connected-but-idle
	// session. It only bounds how late the keepalive expiry is noticed; arming
	// itself is event-driven (the loop wakes on the control frame), so this must
	// stay coarse — a connected idle session must not poll hot.
	defaultIdleTick = time.Second
	// minRateMS/maxRateMS clamp the server-supplied burst cadence. pulse decides
	// how fast to burst, but a server bug must never make the agent hammer the
	// customer's host (floor) or stall a viewer's chart forever (ceiling) — same
	// defensive stance the config package takes on every server-supplied number.
	minRateMS = 1000
	maxRateMS = 10000
	// defaultBackoff is the FLOOR reconnect delay after a dial or write failure
	// while live stays on. Consecutive failures double it (full jitter) up to
	// backoffCap — the config flag is never reset once set, so live stays enabled
	// fleet-wide and a flat 2 s would have every affected agent redial TLS every
	// 2 s for the whole duration of a pulse outage.
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
	// Recv delivers the control frames pulse pushes down the same socket. The
	// channel is CLOSED when the connection dies — an idle session never writes,
	// so a closed Recv is its only way to notice a dead peer. Implementations
	// must never block on it: frames are dropped when the streamer lags.
	Recv() <-chan []byte
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
// pulse relays this JSON text frame verbatim to the browser viewer, so its shape
// is frozen by useAgentLiveWatch.ts on the other end — do not touch it.
type frame struct {
	T int64              `json:"t"`
	M map[string]float64 `json:"m"`
}

// Control frame types pulse pushes down the same socket.
const (
	ctrlStreamOn  = "stream_on"
	ctrlStreamOff = "stream_off"
)

// control is one Server→Agent control frame: {"t":"stream_on","rate_ms":2000} or
// {"t":"stream_off"}.
//
// Note the deliberate asymmetry: "t" is a STRING here and a NUMBER (unix ms) in
// the agent's own frames above. The two directions are disjoint — pulse never
// reads what it sends and the agent never sends what it reads — so the tag can
// mean different things per direction. A frame that does not fit this shape (a
// numeric t, malformed JSON, a type this agent version does not know) is ignored
// silently, which is what lets a newer pulse add frame types without upsetting
// agents in the field.
type control struct {
	T      string `json:"t"`
	RateMS int64  `json:"rate_ms"`
}

// sessionState is the per-connection burst state. It lives on the session's
// single goroutine — no locking.
type sessionState struct {
	// armedUntil is the keepalive deadline: burst while Now is before it. Zero
	// means idle, and a stream_off resets it to zero (which is how the stop
	// reason is told apart from a keepalive expiry when logging).
	armedUntil time.Time
	interval   time.Duration // current burst cadence
	// lastBurstAt is when the burst last RAN — stamped whether or not the sample
	// had anything to send. It is the floor that makes the rate clamp real: the
	// loop wakes on EVERY control frame, not just on the cadence timer, so
	// without it a pulse repeating stream_on in a tight loop would drive one
	// sample per frame and the clamp would bound nothing. Stamping only on a
	// successful send would leave exactly that hole open for a host whose
	// collectors return nothing — the floor caps the WORK, not just the frames,
	// and a defensive rule with an exception is not one. Zero lets the first
	// frame after a stream_on go out immediately: that first-frame latency is the
	// whole point of keeping the channel armed.
	lastBurstAt time.Time
	bursting    bool // last observed burst state, for transition logs
	sawOn       bool // any stream_on seen on this connection
	warned      bool // compat hint already logged for this connection
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

// session dials once and then holds the channel open until live clears, ctx
// ends, the peer dies or a write fails. It bursts only while pulse keeps the
// keepalive alive; the rest of the time it sits idle on the open socket. It
// returns nil on a clean stop (live off / ctx done) and the transport error on a
// failure the supervisor should back off and reconnect from.
func (s *Streamer) session(ctx context.Context) error {
	conn, err := s.opt.Dialer.Dial(ctx, s.opt.WSURL, s.opt.TokenFn())
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	started := s.opt.Clock.Now()
	recv := conn.Recv()
	st := sessionState{interval: s.opt.Interval}
	s.log.Info("live stream connected", "url", s.opt.WSURL)

	for {
		if ctx.Err() != nil || !s.live() {
			return nil
		}
		if s.drainControl(&st, recv) {
			return errConnClosed
		}
		now := s.opt.Clock.Now()
		// Not armed — yet, or any more: sit on the open socket. Dialling is the
		// expensive part, so the channel stays up and only the burst stops.
		wait := defaultIdleTick
		if armed := s.trackBurst(&st, now); armed {
			due := st.lastBurstAt.Add(st.interval)
			if st.lastBurstAt.IsZero() || !now.Before(due) {
				// Stamp before sampling: this iteration has consumed the slot even if
				// the snapshot turns out empty. See lastBurstAt.
				st.lastBurstAt = now
				if wErr := s.burst(ctx, conn, now); wErr != nil {
					return wErr
				}
				wait = st.interval
			} else {
				// Woken early by a control frame and not due yet: do not sample, do
				// not send, just wait out the rest of the cadence.
				wait = due.Sub(now)
			}
			// Wake exactly at the keepalive deadline when it falls inside that wait,
			// so a 10 s burst rate cannot overshoot the window by 10 s.
			if rem := st.armedUntil.Sub(now); rem > 0 && rem < wait {
				wait = rem
			}
		} else {
			s.hintIfNeverArmed(&st, now.Sub(started))
		}

		b, ev := s.await(ctx, wait, recv)
		switch ev {
		case evStop:
			return nil
		case evClosed:
			// The peer went away while we were idle — no write would ever have
			// noticed. Report it as a failure so the supervisor backs off instead
			// of redialling in a tight loop.
			return errConnClosed
		case evFrame:
			s.applyControl(b, &st, s.opt.Clock.Now())
		}
	}
}

// burst samples once and writes one frame. An empty snapshot writes nothing —
// there is no point spending a frame on no metrics — and the caller has already
// consumed the cadence slot either way, so nothing needs reporting back.
func (s *Streamer) burst(ctx context.Context, conn Conn, now time.Time) error {
	snap := s.opt.Sampler.Sample(ctx)
	if len(snap) == 0 {
		return nil
	}
	b, err := json.Marshal(frame{T: now.UnixMilli(), M: snap})
	if err != nil {
		return nil // Unmarshalable snapshot: skip this frame, not the session.
	}
	return conn.WriteText(b)
}

// trackBurst reports whether the keepalive still permits bursting and logs the
// transitions. Only stream_off zeroes armedUntil, so a zero deadline at the
// moment of the transition tells the two stop reasons apart.
func (s *Streamer) trackBurst(st *sessionState, now time.Time) bool {
	burst := now.Before(st.armedUntil)
	if burst != st.bursting {
		if burst {
			s.log.Info("live burst started", "interval", st.interval.String())
		} else {
			reason := "keepalive_expired"
			if st.armedUntil.IsZero() {
				reason = "stream_off"
			}
			s.log.Info("live burst stopped, channel stays open", "reason", reason)
		}
		st.bursting = burst
	}
	return burst
}

// drainControl applies everything pulse has already said before the loop decides
// what to do, and reports a closed channel (the dead peer) to the caller.
//
// This is correctness, not tidiness: when a stream_off is queued at the same
// instant a burst comes due, both the control channel and the cadence timer are
// ready in the same select, and Go picks between ready cases at random. Draining
// first makes the pending stop win every time, so the agent never spends one more
// sample+send on a viewer who is already gone. The drain is cheap and bounded —
// one JSON unmarshal per frame, no sampling, no writes — and the read pump's
// queue is bounded, so a talkative pulse cannot turn it into work.
func (s *Streamer) drainControl(st *sessionState, recv <-chan []byte) (closed bool) {
	for {
		select {
		case b, ok := <-recv:
			if !ok {
				return true
			}
			s.applyControl(b, st, s.opt.Clock.Now())
		default:
			return false
		}
	}
}

// applyControl folds one inbound control frame into the session state. Anything
// it cannot make sense of is dropped with a debug line and leaves the running
// state untouched — see the control type for why that is the contract.
func (s *Streamer) applyControl(b []byte, st *sessionState, now time.Time) {
	var c control
	if err := json.Unmarshal(b, &c); err != nil {
		s.log.Debug("live control frame ignored: unparseable", "bytes", len(b))
		return
	}
	switch c.T {
	case ctrlStreamOn:
		// Every stream_on is also a keepalive: pulse repeats it every 15 s while
		// a viewer watches, and each one pushes the deadline out.
		st.armedUntil = now.Add(keepaliveWindow)
		st.interval = s.burstInterval(c.RateMS)
		st.sawOn = true
	case ctrlStreamOff:
		st.armedUntil = time.Time{}
	default:
		s.log.Debug("live control frame ignored: unknown type", "type", c.T)
	}
}

// burstInterval turns the server-supplied rate_ms into a cadence, clamped to
// [minRateMS, maxRateMS]. A missing or zero rate keeps the configured default.
func (s *Streamer) burstInterval(rateMS int64) time.Duration {
	if rateMS <= 0 {
		return s.opt.Interval
	}
	if rateMS < minRateMS {
		rateMS = minRateMS
	}
	if rateMS > maxRateMS {
		rateMS = maxRateMS
	}
	return time.Duration(rateMS) * time.Millisecond
}

// hintIfNeverArmed logs once per connection when a session has been up for a
// whole keepalive window without ever seeing a stream_on. On a current pulse
// that just means nobody is watching; on an outdated one it is the single
// visible symptom of the server-first deploy order, so the line names it.
func (s *Streamer) hintIfNeverArmed(st *sessionState, up time.Duration) {
	if st.warned || st.sawOn || up < keepaliveWindow {
		return
	}
	st.warned = true
	s.log.Info("live channel connected but no stream_on received — is pulse up to date?", "up", up.String())
}

// event is what ended an await.
type event int

const (
	evTick   event = iota // the requested duration elapsed
	evFrame               // a control frame arrived
	evClosed              // the connection died
	evStop                // ctx ended
)

// await blocks until d elapses, a control frame arrives, the connection dies or
// ctx ends — whichever comes first.
//
// The timer runs in a goroutine because platform.Clock only offers a blocking
// Sleep (that is the seam the fake clock hooks into), while the loop must stay
// selectable on the control channel: a stream_off has to stop the burst now, not
// whenever the current cadence happens to run out. The goroutine always ends —
// Sleep aborts on the derived context — so this leaks nothing per iteration.
func (s *Streamer) await(ctx context.Context, d time.Duration, recv <-chan []byte) ([]byte, event) {
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	elapsed := make(chan struct{})
	go func() {
		defer close(elapsed)
		_ = s.opt.Clock.Sleep(wctx, d)
	}()

	select {
	case <-ctx.Done():
		return nil, evStop
	case b, ok := <-recv:
		if !ok {
			return nil, evClosed
		}
		return b, evFrame
	case <-elapsed:
		// A cancelled ctx also ends the sleep, and then both cases are ready.
		if ctx.Err() != nil {
			return nil, evStop
		}
		return nil, evTick
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
