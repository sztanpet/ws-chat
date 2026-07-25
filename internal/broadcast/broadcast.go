// Package broadcast fans one message out to many subscribers.
//
// This is the chat server's hot path: one message arrives in a channel and
// has to reach every member of it, where the members are wildly different
// speeds and any one of them may be a dead TCP connection that has not
// noticed yet. Every implementation in this package obeys the same two
// rules, which is what makes them interchangeable and comparable:
//
//   - A send to a subscriber NEVER blocks. A subscriber whose buffer is
//     full is dropped, not waited on.
//   - Nothing on the receiving side can stall the sender. Backpressure
//     stops at the subscription; it never reaches the caller.
//
// MapChan is the reference implementation — a map of subscribers behind an
// RWMutex, one buffered channel each. It is the obvious solution and the
// baseline every alternative has to beat in bench_test.go.
package broadcast

import (
	"sync"
	"sync/atomic"
)

// DefaultBuffer is the outbound buffer used when Subscribe is given a
// non-positive size. Roughly a couple of seconds of a busy channel: long
// enough that a subscriber hitting it is genuinely gone rather than briefly
// descheduled.
const DefaultBuffer = 256

// Broadcaster fans messages out to a set of subscribers.
//
// Implementations must be safe for concurrent use by multiple goroutines.
// The msg passed to Broadcast is shared by every subscriber: it must not be
// modified after the call.
type Broadcaster interface {
	// Subscribe registers a new subscriber with an outbound buffer of buf
	// messages (DefaultBuffer if buf < 1). Subscribing to a closed
	// Broadcaster returns an already-finished Sub rather than failing.
	Subscribe(buf int) *Sub

	// Unsubscribe removes s and closes its channel. It is idempotent and
	// safe to call on a Sub that was already dropped.
	Unsubscribe(s *Sub)

	// Broadcast delivers msg to every subscriber, dropping the ones whose
	// buffer is full, and reports how many it dropped.
	Broadcast(msg []byte) (dropped int)

	// Len reports the current subscriber count.
	Len() int

	// Close removes every subscriber and rejects new ones. Idempotent.
	Close()
}

// Sub is a single subscription. The consumer ranges over C until it closes;
// once it has closed, Dropped reports why.
type Sub struct {
	ch      chan []byte
	dropped atomic.Bool
	once    sync.Once
}

// C is the subscriber's message channel. It is closed when the subscription
// ends, whether by Unsubscribe, by Close, or by being dropped for falling
// behind.
func (s *Sub) C() <-chan []byte { return s.ch }

// Dropped reports whether the subscription ended because the subscriber
// could not keep up, as opposed to an orderly Unsubscribe or Close. It is
// meaningful once C has closed.
func (s *Sub) Dropped() bool { return s.dropped.Load() }

// finish ends the subscription. The caller must hold the broadcaster's
// write lock, and s must already be unreachable to any concurrent send —
// that is what makes closing ch safe.
func (s *Sub) finish(dropped bool) {
	s.once.Do(func() {
		s.dropped.Store(dropped)
		close(s.ch)
	})
}

// MapChan is the reference implementation: map[*Sub]struct{} behind an
// RWMutex, with a buffered channel per subscriber. Broadcast holds the read
// lock, so concurrent broadcasts to the same set run in parallel and only
// membership changes serialize.
type MapChan struct {
	mu     sync.RWMutex
	subs   map[*Sub]struct{}
	closed bool
}

// NewMapChan returns an empty MapChan.
func NewMapChan() *MapChan {
	return &MapChan{subs: make(map[*Sub]struct{})}
}

func (b *MapChan) Subscribe(buf int) *Sub {
	if buf < 1 {
		buf = DefaultBuffer
	}
	s := &Sub{ch: make(chan []byte, buf)}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		s.finish(false)
		return s
	}
	b.subs[s] = struct{}{}
	return s
}

func (b *MapChan) Unsubscribe(s *Sub) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.subs[s]; !ok {
		return // already dropped, unsubscribed, or from another broadcaster
	}
	delete(b.subs, s)
	s.finish(false)
}

func (b *MapChan) Broadcast(msg []byte) int {
	var slow []*Sub

	b.mu.RLock()
	for s := range b.subs {
		select {
		case s.ch <- msg:
		default:
			slow = append(slow, s)
		}
	}
	b.mu.RUnlock()

	if len(slow) == 0 {
		return 0
	}

	// The drop is deliberately deferred to a second pass under the write
	// lock. Closing a subscriber's channel inline, while the read lock is
	// held, would race with another goroutine's Broadcast sending into that
	// same channel — a send on a closed channel panics. Taking the write
	// lock guarantees no send is in flight.
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, s := range slow {
		if _, ok := b.subs[s]; !ok {
			continue // lost the race to another dropper or an Unsubscribe
		}
		delete(b.subs, s)
		s.finish(true)
		n++
	}
	return n
}

func (b *MapChan) Len() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subs)
}

func (b *MapChan) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	for s := range b.subs {
		delete(b.subs, s)
		s.finish(false)
	}
}
