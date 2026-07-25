// Package ratelimit is a token bucket, and nothing else.
//
// It deliberately knows nothing about chat, connections or policy. What the
// limits should be is somebody else's decision — see internal/hook — and
// this package only enforces whatever it is handed.
package ratelimit

import (
	"sync"
	"time"
)

// Bucket is a token bucket: burst tokens to spend, refilled one per
// interval.
//
// A nil *Bucket allows everything. That is not a convenience, it is the
// point: unlimited is the default, and a server with no limits configured
// should pay a nil check on the hot path rather than a mutex and a clock
// read. The same goes for a Bucket built with a non-positive burst or
// interval — "no limit" and "a limit of nothing" would otherwise be one
// typo apart.
type Bucket struct {
	// Immutable after New, so Unlimited can be checked without the lock.
	interval time.Duration
	burst    float64

	mu     sync.Mutex
	tokens float64
	last   time.Time // zero until the first call
}

// New returns a bucket allowing burst messages immediately and one more
// every interval after that. A non-positive burst or interval means
// unlimited, and returns nil.
func New(burst int, interval time.Duration) *Bucket {
	if burst < 1 || interval <= 0 {
		return nil
	}
	return &Bucket{
		interval: interval,
		burst:    float64(burst),
		tokens:   float64(burst), // start full: a new connection may talk
	}
}

// Unlimited reports whether this bucket enforces anything.
func (b *Bucket) Unlimited() bool { return b == nil }

// Allow spends a token if there is one.
func (b *Bucket) Allow() bool { return b.AllowAt(time.Now()) }

// AllowAt is Allow at an explicit time. Tests use it to avoid sleeping:
// a rate limiter tested with real time is a test that fails on a loaded
// machine.
func (b *Bucket) AllowAt(now time.Time) bool {
	if b == nil {
		return true
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.refillLocked(now)

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// refillLocked adds the tokens that have accrued since the last call. A
// clock that went backwards refills nothing rather than draining the
// bucket, and does not move `last` back.
func (b *Bucket) refillLocked(now time.Time) {
	if b.last.IsZero() {
		b.last = now
	}
	elapsed := now.Sub(b.last)
	if elapsed <= 0 {
		return
	}
	b.tokens += float64(elapsed) / float64(b.interval)
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	b.last = now
}

// Idle reports whether the bucket has refilled completely — that is,
// whether it holds anything a brand new bucket would not.
//
// It is what makes a shared bucket safe to forget. A bucket that has
// refilled is indistinguishable from one that never existed, so dropping
// it loses nothing, where dropping a half-spent one would hand somebody a
// fresh budget for reconnecting.
func (b *Bucket) Idle(now time.Time) bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refillLocked(now)
	return b.tokens >= b.burst
}

// Tokens reports what is left, for logging and tests.
func (b *Bucket) Tokens() float64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.tokens
}
