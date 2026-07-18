package sender

import "time"

// Backoff parameters: full-jitter exponential backoff from a 2s base to a 10min
// cap for retryable outcomes.
const (
	DefaultBaseBackoff = 2 * time.Second
	DefaultCapBackoff  = 10 * time.Minute
	// DefaultQuarantineFloor is the minimum wait after a persistent 401. The
	// agent never hammers and never wipes: it backs off long and keeps its data,
	// so a re-enroll or an external token rotation can recover it.
	DefaultQuarantineFloor = 5 * time.Minute
)

// jitterFn returns a value in [0, n). It is injected so tests are deterministic.
type jitterFn func(n int64) int64

// backoff implements full-jitter exponential backoff. next returns a duration in
// [0, min(cap, base<<attempt)] and advances the attempt counter; reset returns
// to the base.
type backoff struct {
	base    time.Duration
	cap     time.Duration
	jitter  jitterFn
	attempt uint
}

func newBackoff(base, capDur time.Duration, jitter jitterFn) backoff {
	if base <= 0 {
		base = DefaultBaseBackoff
	}
	if capDur <= 0 {
		capDur = DefaultCapBackoff
	}
	return backoff{base: base, cap: capDur, jitter: jitter}
}

// next returns the next full-jitter delay and advances the attempt counter.
func (b *backoff) next() time.Duration {
	ceiling := b.exponential()
	b.attempt++
	if ceiling <= 0 {
		return 0
	}
	return time.Duration(b.jitter(int64(ceiling) + 1))
}

// exponential returns min(cap, base<<attempt), guarding against shift overflow.
func (b *backoff) exponential() time.Duration {
	if b.attempt >= 62 {
		return b.cap
	}
	d := b.base << b.attempt
	if d <= 0 || d > b.cap {
		return b.cap
	}
	return d
}

func (b *backoff) reset() { b.attempt = 0 }
