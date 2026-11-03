package broadcast

import (
	"sync"
	"sync/atomic"
)

// Ring is one history each subscriber reads at its own pace: Broadcast is O(1).
type Ring struct {
	buf  [][]byte
	mask uint64

	mu     sync.RWMutex
	seq    uint64        // total messages ever written
	notify chan struct{} // closed and replaced to wake sleeping subscribers
	closed bool

	// Atomic: subscribers register under the read lock, not the write lock.
	waiters atomic.Int64

	subs  atomic.Int64
	drops atomic.Int64
}

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

	// No lost wakeup: waiters register under the read lock, this runs under the
	// write lock, so each is counted here or re-reads seq and finds the message.
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
	// Counted even when closed: finish decrements, once, so it nets out.
	r.subs.Add(1)
	if r.closed {
		s.finish(ErrClosed)
		return s
	}
	// A new subscriber sees what happens next, not what the ring still holds.
	s.pos = r.seq
	return s
}

func (r *Ring) Drops() int64 { return r.drops.Load() }

// Len is zero once closed: every subscriber is finished, noticed or not.
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

	close(wake)
}

type ringSub struct {
	r    *Ring
	pos  uint64 // next sequence to read; touched only by the reader
	done chan struct{}

	once sync.Once
	err  atomic.Pointer[error]
}

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

		// Lapped: sequence k survives until k+capacity is written.
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
			// Before unlocking: a concurrent Broadcast counts us or is seen by us.
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
