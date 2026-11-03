// Package ratelimit is a token bucket; the policy is the Limiter hook's.
package ratelimit

import (
	"sync"
	"time"
)

// Bucket is nil when unlimited, so the default costs a nil check, not a mutex.
type Bucket struct {
	// Immutable after New, so Unlimited can be checked without the lock.
	interval time.Duration
	burst    float64

	mu     sync.Mutex
	tokens float64
	last   time.Time // zero until the first call
}

// New returns nil, unlimited, for a non-positive burst or interval.
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

func (b *Bucket) Unlimited() bool { return b == nil }

func (b *Bucket) Allow() bool { return b.AllowAt(time.Now()) }

// AllowAt takes the time so tests need not sleep.
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

// A clock that went backwards refills nothing and does not move last back.
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

// Idle means full again, so forgetting the bucket hands nobody a free refill.
func (b *Bucket) Idle(now time.Time) bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refillLocked(now)
	return b.tokens >= b.burst
}

func (b *Bucket) Tokens() float64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.tokens
}
