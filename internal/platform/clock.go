package platform

import (
	"context"
	"time"
)

// Clock abstracts wall-clock time and cancellable sleeping. The sender's backoff
// and the buffer's time horizon read it instead of the time package so tests can
// drive time deterministically.
type Clock interface {
	// Now returns the current wall-clock time.
	Now() time.Time
	// Sleep blocks for d or until ctx is done, whichever comes first. It returns
	// ctx.Err() if the context ended first, otherwise nil.
	Sleep(ctx context.Context, d time.Duration) error
}

// SystemClock is the production Clock backed by the time package.
type SystemClock struct{}

// Now returns time.Now.
func (SystemClock) Now() time.Time { return time.Now() }

// Sleep waits for d using a timer, aborting early if ctx is cancelled.
func (SystemClock) Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		// Still honour an already-cancelled context.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
