// Package broadcast fans one message out to many subscribers.
//
// This is the chat server's hot path: one message arrives in a channel and
// has to reach every member of it, where the members are wildly different
// speeds and any one of them may be a dead TCP connection that has not
// noticed yet. Every implementation in this package obeys the same two
// rules, which is what makes them interchangeable and comparable:
//
//   - A send to a subscriber NEVER blocks. A subscriber that cannot keep up
//     is dropped, not waited on.
//   - Nothing on the receiving side can stall the sender. Backpressure
//     stops at the subscription; it never reaches the caller.
//
// Receiving is batch-oriented on purpose. The benchmarks say roughly two
// thirds of the cost of a broadcast is waking the receiver goroutines
// rather than finding them, so Recv hands back everything that has piled up
// since the last call. A write pump that wakes once and writes twenty
// frames costs a twentieth of the wakeups of one that wakes twenty times,
// and it can coalesce them into a single socket write.
//
// MapChan is the reference implementation — a map of subscribers behind an
// RWMutex, one buffered channel each. It is the obvious solution and the
// baseline every alternative has to beat in bench_test.go.
package broadcast

import (
	"errors"
	"sync"
	"sync/atomic"
)

// DefaultBuffer is the capacity used when a constructor is given a
// non-positive size. Roughly a couple of seconds of a busy channel: long
// enough that a subscriber hitting it is genuinely gone rather than briefly
// descheduled.
const DefaultBuffer = 256

// The two ways a subscription ends. Recv reports one of them once, after
// which the Sub is finished.
var (
	// ErrClosed means the subscription ended in an orderly way: the
	// subscriber called Close, or the whole Broadcaster was closed.
	ErrClosed = errors.New("broadcast: subscription closed")

	// ErrLagged means the subscriber could not keep up and was dropped.
	// In the server this is the disconnect reason for a client that
	// outran its socket.
	ErrLagged = errors.New("broadcast: subscriber fell behind")
)

// Broadcaster fans messages out to a set of subscribers.
//
// Implementations must be safe for concurrent use by multiple goroutines.
// The msg passed to Broadcast is shared by every subscriber and must not be
// modified after the call.
type Broadcaster interface {
	// Subscribe registers a new subscriber. Subscribing to a closed
	// Broadcaster returns a Sub whose Recv reports ErrClosed rather than
	// failing.
	Subscribe() Sub

	// Broadcast delivers msg to every subscriber, dropping the ones that
	// cannot keep up. It never blocks.
	Broadcast(msg []byte)

	// Drops reports the total number of subscribers dropped for falling
	// behind over the lifetime of the Broadcaster. It only grows.
	//
	// This is a counter rather than a Broadcast return value because an
	// implementation is free to notice a lagging subscriber lazily, on the
	// receiving side, instead of checking every subscriber on every send.
	Drops() int64

	// Len reports the current subscriber count. Implementations that drop
	// lazily only remove a lagging subscriber once it notices, so this can
	// briefly overcount.
	Len() int

	// Close ends every subscription and rejects new ones. Idempotent.
	Close()
}

// Sub is a single subscription.
type Sub interface {
	// Recv blocks until at least one message is available, copies up to
	// len(dst) messages into dst, and returns how many it copied. The
	// message slices are shared with every other subscriber and must not be
	// modified.
	//
	// A subscription ends exactly once, with 0 and either ErrClosed or
	// ErrLagged. Messages already handed to the subscriber may be returned
	// before that error. Calling Recv with an empty dst returns 0, nil.
	Recv(dst [][]byte) (int, error)

	// Close ends this subscription. It is idempotent, and safe to call on a
	// subscription that has already ended.
	Close()
}

// MapChan is the reference implementation: map[*mapSub]struct{} behind an
// RWMutex, with a buffered channel per subscriber. Broadcast holds the read
// lock, so concurrent broadcasts to the same set run in parallel and only
// membership changes serialize.
type MapChan struct {
	buf int

	mu     sync.RWMutex
	subs   map[*mapSub]struct{}
	closed bool

	drops atomic.Int64
}

// NewMapChan returns an empty MapChan whose subscribers each get an
// outbound buffer of buf messages.
func NewMapChan(buf int) *MapChan {
	if buf < 1 {
		buf = DefaultBuffer
	}
	return &MapChan{buf: buf, subs: make(map[*mapSub]struct{})}
}

type mapSub struct {
	b      *MapChan
	ch     chan []byte
	lagged atomic.Bool
	once   sync.Once
}

func (b *MapChan) Subscribe() Sub {
	s := &mapSub{b: b, ch: make(chan []byte, b.buf)}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		s.finish(false)
		return s
	}
	b.subs[s] = struct{}{}
	return s
}

func (b *MapChan) Broadcast(msg []byte) {
	var slow []*mapSub

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
		return
	}

	// The drop is deliberately deferred to a second pass under the write
	// lock. Closing a subscriber's channel inline, while the read lock is
	// held, would race with another goroutine's Broadcast sending into that
	// same channel — a send on a closed channel panics. Taking the write
	// lock guarantees no send is in flight.
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, s := range slow {
		if _, ok := b.subs[s]; !ok {
			continue // lost the race to another dropper or a Close
		}
		delete(b.subs, s)
		s.finish(true)
		b.drops.Add(1)
	}
}

func (b *MapChan) Drops() int64 { return b.drops.Load() }

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

// remove unregisters s, if it is still registered.
func (b *MapChan) remove(s *mapSub) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.subs[s]; !ok {
		return
	}
	delete(b.subs, s)
	s.finish(false)
}

// finish ends the subscription. The caller must hold the broadcaster's
// write lock, and s must already be unreachable to any concurrent send —
// that is what makes closing ch safe.
func (s *mapSub) finish(lagged bool) {
	s.once.Do(func() {
		s.lagged.Store(lagged)
		close(s.ch)
	})
}

func (s *mapSub) Close() { s.b.remove(s) }

func (s *mapSub) Recv(dst [][]byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}

	// Block for the first message, then take whatever else has piled up
	// without blocking. One wakeup, up to len(dst) frames.
	msg, ok := <-s.ch
	if !ok {
		return 0, s.err()
	}
	dst[0] = msg

	n := 1
	for n < len(dst) {
		select {
		case msg, ok := <-s.ch:
			if !ok {
				return n, nil // report the end on the next call
			}
			dst[n] = msg
			n++
		default:
			return n, nil
		}
	}
	return n, nil
}

func (s *mapSub) err() error {
	if s.lagged.Load() {
		return ErrLagged
	}
	return ErrClosed
}
