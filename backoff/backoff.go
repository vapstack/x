// Package backoff provides a simple exponential backoff implementation
// with optional symmetric jitter.
//
// The backoff grows exponentially from a minimum duration, is capped by
// a maximum duration (if specified), and can apply random jitter to avoid
// synchronized retries (thundering herd).
//
// Backoff is not thread-safe.
package backoff

import (
	"math"
	"math/rand/v2"
	"time"
)

// New returns a new exponential Backoff.
//
// The backoff starts at minDur and grows exponentially by the given factor
// on each call to Duration, up to maxDur if maxDur is positive.
// If jitter is non-zero, a symmetric random jitter is applied to the
// returned duration to reduce retry synchronization.
//
// The returned backoff starts in a reset state; the first call to Duration
// returns minDur (with jitter applied, if enabled).
func New(minDur, maxDur time.Duration, factor, jitter float64) *Backoff {
	return &Backoff{
		min:    max(minDur, 0),
		max:    max(maxDur, 0),
		factor: factor,
		jitter: math.Abs(jitter),
		next:   0,
	}
}

// Backoff implements an exponential backoff with optional jitter.
//
// Each call to Duration advances the internal state. The base delay grows
// exponentially by multiplying the previous value by factor, starting from
// min and capped at max (if max > 0).
//
// Jitter, if enabled, is applied only to the returned duration and does not
// affect the underlying exponential progression.
type Backoff struct {
	min    time.Duration
	max    time.Duration
	factor float64
	jitter float64 // 0.2 is +/-20%
	next   time.Duration
}

// Duration returns the next backoff duration.
//
// On the first call, it returns the minimum duration. On subsequent calls,
// the duration grows exponentially by the configured factor and is capped
// by the maximum duration if specified.
//
// If jitter is enabled, the returned value is randomly adjusted within
// +/-jitter of the base duration.
func (b *Backoff) Duration() time.Duration {
	if b.next == 0 {
		b.next = b.min
	} else {
		b.next = time.Duration(float64(b.next) * b.factor)
		if b.max > 0 && b.next > b.max {
			b.next = b.max
		}
	}
	if b.jitter > 0 && b.next > 0 {
		return b.applyJitter(b.next)
	}
	return b.next
}

func (b *Backoff) applyJitter(base time.Duration) time.Duration {
	mult := 1 - b.jitter + (2*b.jitter)*rand.Float64()
	d := time.Duration(float64(base) * mult)
	if b.max > 0 && d > b.max {
		d = b.max
	}
	return d
}

// After returns a channel that will receive current time after the duration
// has elapsed.
// Each call advances the backoff state in the same way as Duration.
func (b *Backoff) After() <-chan time.Time {
	return time.After(b.Duration())
}

// Wait blocks for the duration returned by Duration.
// Each call advances the backoff state.
func (b *Backoff) Wait() {
	time.Sleep(b.Duration())
}

// Reset resets the backoff to its initial state.
// The next call to Duration will return the minimum duration.
func (b *Backoff) Reset() {
	b.next = 0
}
