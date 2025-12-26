// Package keyrate provides a rate.Limiter per key with bounded retention.
//
// KeyLimiter lazily creates a golang.org/x/time/rate.Limiter for each key and
// keeps limiters in an internal two-generation map. The active generation is
// stored in cur; the previous generation is kept in prev and discarded on the
// next swap. This provides a simple, allocation-friendly way to avoid
// unbounded growth when the set of keys is large and mostly ephemeral.
//
// KeyLimiter is safe for concurrent use.
package keyrate

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// NewKeyLimiter returns a KeyLimiter that applies the same rate limit to each key.
//
// The dur parameter is interpreted as the minimum time between events for a
// single key (i.e. one token is added every dur); burst specifies the maximum
// burst size for that key, following the semantics of x/time/rate.
//
// Limiters are created on demand and are retained for approximately two swap
// intervals. Swap interval is equal to dur.
//
// If dur is negative it is treated as zero. A zero dur results in an unlimited
// limiter (rate.Inf) for each key.
func NewKeyLimiter[K comparable](dur time.Duration, burst int, avgUnique int) *KeyLimiter[K] {
	if avgUnique <= 0 {
		avgUnique = 2048
	}
	if dur < 0 {
		dur = 0
	}
	l := &KeyLimiter[K]{
		cur:       make(map[K]*rate.Limiter, avgUnique),
		prev:      make(map[K]*rate.Limiter, avgUnique),
		dur:       dur,
		burst:     burst,
		nextSwap:  time.Now().Add(max(dur, 5*time.Second)),
		avgUnique: avgUnique,
	}
	return l
}

// KeyLimiter provides a per-key token-bucket limiter.
//
// All exported methods delegate to the underlying per-key *rate.Limiter.
// Limiters are created lazily and reused across swaps while the key remains
// active.
type KeyLimiter[K comparable] struct {
	cur       map[K]*rate.Limiter
	prev      map[K]*rate.Limiter
	dur       time.Duration
	burst     int
	nextSwap  time.Time
	avgUnique int
	mu        sync.Mutex
}

func (l *KeyLimiter[K]) get(key K) *rate.Limiter {

	now := time.Now()

	l.mu.Lock()

	if l.nextSwap.Before(now) {
		l.prev = l.cur
		l.cur = make(map[K]*rate.Limiter, l.avgUnique)
		l.nextSwap = now.Add(max(l.dur, 5*time.Second))
	}

	lim, ok := l.cur[key]
	if !ok {
		if lim, ok = l.prev[key]; ok {
			l.cur[key] = lim
		} else {
			lim = rate.NewLimiter(rate.Every(l.dur), l.burst)
			l.cur[key] = lim
		}
	}

	l.mu.Unlock()

	return lim
}

func (l *KeyLimiter[K]) Allow(key K) bool {
	return l.get(key).Allow()
}
func (l *KeyLimiter[K]) Limit(key K) rate.Limit {
	return l.get(key).Limit()
}
func (l *KeyLimiter[K]) Burst(key K) int {
	return l.get(key).Burst()
}
func (l *KeyLimiter[K]) TokensAt(key K, t time.Time) float64 {
	return l.get(key).TokensAt(t)
}
func (l *KeyLimiter[K]) Tokens(key K) float64 {
	return l.get(key).Tokens()
}
func (l *KeyLimiter[K]) AllowN(key K, t time.Time, n int) bool {
	return l.get(key).AllowN(t, n)
}
func (l *KeyLimiter[K]) Reserve(key K) *rate.Reservation {
	return l.get(key).Reserve()
}
func (l *KeyLimiter[K]) ReserveN(key K, t time.Time, n int) *rate.Reservation {
	return l.get(key).ReserveN(t, n)
}
func (l *KeyLimiter[K]) Wait(key K, ctx context.Context) (err error) {
	return l.get(key).Wait(ctx)
}
func (l *KeyLimiter[K]) WaitN(key K, ctx context.Context, n int) (err error) {
	return l.get(key).WaitN(ctx, n)
}
