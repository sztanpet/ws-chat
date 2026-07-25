package broadcast

import (
	"sync"
	"sync/atomic"
)

// Ring is a shared ring buffer: one copy of the message history that every
// subscriber reads from at its own position, instead of one queue per
// subscriber.
//
// The point is what Broadcast does NOT do. It writes the message into one
// slot, bumps a sequence number, and returns — it never walks the
// subscribers, so the cost of a broadcast is O(1) in the number of them.
// Nobody is checked for lagging on the send path either: a subscriber
// discovers it was lapped the next time it reads, which keeps that cost off
// the writer as well.
//
// Wakeups work the same way. Subscribers that are busy draining are not
// waiting on anything, so a broadcast that arrives while they work costs no
// wakeup at all and simply gets picked up by their next Recv, batched with
// everything else that landed meanwhile. Only subscribers that have caught
// up and gone to sleep need waking, and they share a single notify channel:
// the writer closes it and installs a fresh one, waking all of them with
// one operation rather than N sends.
//
// The tradeoff against MapChan is memory and fairness. There is one buffer
// for the whole broadcaster rather than one per subscriber, so a slow
// subscriber is lapped based on the shared capacity — the slowest reader
// decides how much history everyone gets. And the ring pins the last
// capacity messages until they are overwritten, where a channel drops its
// reference as soon as the last subscriber reads it.
type Ring struct {
	buf  [][]byte
	mask uint64

	mu     sync.RWMutex
	seq    uint64        // total messages ever written
	notify chan struct{} // closed and replaced to wake sleeping subscribers
	closed bool

	// waiters is the count of subscribers asleep in Recv. It is atomic
	// rather than plain because subscribers register while holding the READ
	// lock — see the ordering argument in Broadcast.
	waiters atomic.Int64

	subs  atomic.Int64
	drops atomic.Int64
}

// NewRing returns a Ring whose shared history holds capacity messages,
// rounded up to a power of two. A subscriber that falls more than capacity
// messages behind is lapped and dropped.
func NewRing(capacity int) *Ring {
	if capacity < 1 {
		capacity = DefaultBuffer
	}
	capacity = roundPow2(capacity)
	return &Ring{
		buf:    make([][]byte, capacity),
		mask:   uint64(capacity - 1),
		notify: make(chan struct{}),
	}
}

func roundPow2(n int) int {
	c := 1
	for c < n {
		c <<= 1
	}
	return c
}

func (r *Ring) Broadcast(msg []byte) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.buf[r.seq&r.mask] = msg
	r.seq++

	// Only bother with a wakeup if somebody is actually asleep. Under load
	// the subscribers are busy draining, this is false, and a broadcast
	// costs nothing beyond the slot write — no allocation, no close, no
	// scheduler work. That is the whole design.
	//
	// There is no lost-wakeup race here even though waiters is atomic: a
	// subscriber increments it while holding the read lock, and this
	// decision is made under the write lock. So either the subscriber
	// registered before this call (and is counted here, and the channel it
	// captured is the one closed below), or it has not yet re-read seq
	// (and will see the message it would have slept through). The two
	// cannot interleave.
	var wake chan struct{}
	if r.waiters.Load() > 0 {
		wake = r.notify
		r.notify = make(chan struct{})
	}
	r.mu.Unlock()

	if wake != nil {
		close(wake)
	}
}

func (r *Ring) Subscribe() Sub {
	s := &ringSub{r: r, done: make(chan struct{})}

	r.mu.Lock()
	defer r.mu.Unlock()
	// Count first, unconditionally: finish is what decrements, and it runs
	// at most once per subscriber, so the closed case nets out to zero
	// without a second code path.
	r.subs.Add(1)
	if r.closed {
		s.finish(ErrClosed)
		return s
	}
	// Start at the write cursor: a new subscriber sees what happens next,
	// not the history still sitting in the ring.
	s.pos = r.seq
	return s
}

func (r *Ring) Drops() int64 { return r.drops.Load() }

// Len reports the subscriber count. A lapped subscriber is still counted
// until it next calls Recv and notices, since nothing on the send path
// looks at subscribers at all.
//
// A closed Ring reports zero rather than waiting for every subscriber to
// wake up and notice: they are all finished the moment it closes, whether
// or not their goroutine has been scheduled to find out.
func (r *Ring) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return 0
	}
	return int(r.subs.Load())
}

func (r *Ring) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	wake := r.notify
	r.notify = make(chan struct{})
	r.mu.Unlock()

	close(wake) // wakes every sleeping subscriber; they re-read r.closed
}

type ringSub struct {
	r    *Ring
	pos  uint64 // next sequence to read; touched only by the reader
	done chan struct{}

	once sync.Once
	err  atomic.Pointer[error]
}

// finish records how the subscription ended, drops it from the count, and
// wakes its reader. It runs at most once.
func (s *ringSub) finish(err error) {
	s.once.Do(func() {
		s.err.Store(&err)
		s.r.subs.Add(-1)
		close(s.done)
	})
}

func (s *ringSub) Close() { s.finish(ErrClosed) }

func (s *ringSub) Recv(dst [][]byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	r := s.r

	for {
		if err := s.err.Load(); err != nil {
			return 0, *err
		}

		r.mu.RLock()
		seq, closed := r.seq, r.closed

		// Lapped: the writer has overwritten the slot this subscriber was
		// due to read next. Sequence k lives in the ring until k+capacity
		// is written, so it survives while seq-pos <= capacity.
		if seq-s.pos > uint64(len(r.buf)) {
			r.mu.RUnlock()
			r.drops.Add(1)
			s.finish(ErrLagged)
			return 0, ErrLagged
		}

		n := 0
		for s.pos < seq && n < len(dst) {
			dst[n] = r.buf[s.pos&r.mask]
			s.pos++
			n++
		}

		var notify chan struct{}
		if n == 0 && !closed {
			// Register as a waiter before dropping the read lock, so a
			// concurrent Broadcast either counts us or is seen by us.
			notify = r.notify
			r.waiters.Add(1)
		}
		r.mu.RUnlock()

		if n > 0 {
			return n, nil
		}
		if closed {
			s.finish(ErrClosed)
			return 0, ErrClosed
		}

		select {
		case <-notify:
		case <-s.done:
		}
		r.waiters.Add(-1)
	}
}
