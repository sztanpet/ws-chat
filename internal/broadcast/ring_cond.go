package broadcast

import (
	"sync"
	"sync/atomic"
)

// CondRing is Ring with the wakeup mechanism swapped: a sync.Cond in place
// of a notify channel that is closed and replaced.
//
// It exists to answer one question with numbers instead of opinions. Ring
// allocates a channel per broadcast whenever anyone is asleep — which is
// the COMMON case for a chat server, since a quiet room is a room where
// every write pump is parked in Recv — and sync.Cond allocates nothing.
// Against that, sync.Cond cannot wake one specific waiter: there is no
// select, and Signal wakes an arbitrary one, so ending a single
// subscription has to Broadcast and wake every sleeper in the room to
// re-check a predicate and go back to sleep.
//
// Everything else — the shared buffer, the sequence number, the lazy lap
// detection, the O(1) send path — is identical to Ring on purpose. The only
// variable is how sleepers are woken.
//
// The lock is an RWMutex and the Cond is built on its RLocker, so readers
// still run concurrently. Using a plain Mutex would have handicapped this
// variant for a reason that has nothing to do with sync.Cond.
type CondRing struct {
	buf  [][]byte
	mask uint64

	mu     sync.RWMutex
	cond   *sync.Cond
	seq    uint64
	closed bool

	subs  atomic.Int64
	drops atomic.Int64
}

// NewCondRing returns a CondRing holding capacity messages, rounded up to a
// power of two.
func NewCondRing(capacity int) *CondRing {
	if capacity < 1 {
		capacity = DefaultBuffer
	}
	capacity = roundPow2(capacity)

	r := &CondRing{
		buf:  make([][]byte, capacity),
		mask: uint64(capacity - 1),
	}
	r.cond = sync.NewCond(r.mu.RLocker())
	return r
}

func (r *CondRing) Broadcast(msg []byte) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.buf[r.seq&r.mask] = msg
	r.seq++
	r.mu.Unlock()

	// No "is anybody waiting" check here, unlike Ring: notifyListNotifyAll
	// has its own early-out on two atomic loads when the list is empty. That
	// is the tidy part of this design — there is no bookkeeping to get
	// wrong, and no ordering argument to write down, because Wait enqueues
	// the waiter before it releases the lock.
	r.cond.Broadcast()
}

func (r *CondRing) Subscribe() Sub {
	s := &condSub{r: r}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.subs.Add(1)
	if r.closed {
		err := error(ErrClosed)
		s.err.Store(&err)
		r.subs.Add(-1)
		return s
	}
	s.pos = r.seq
	return s
}

func (r *CondRing) Drops() int64 { return r.drops.Load() }

func (r *CondRing) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return 0
	}
	return int(r.subs.Load())
}

func (r *CondRing) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	r.mu.Unlock()

	r.cond.Broadcast()
}

type condSub struct {
	r   *CondRing
	pos uint64 // next sequence to read; touched only by the reader

	once sync.Once
	err  atomic.Pointer[error]
}

// finish records how the subscription ended and wakes its reader — along
// with every other sleeper in the broadcaster, because sync.Cond has no way
// to wake one.
//
// The write lock is taken and released around the store rather than skipped.
// A waiter checks its error and enqueues itself while holding the read lock,
// so without excluding that window the store could land between the check
// and the enqueue, the Broadcast would find an empty list, and the waiter
// would park forever.
func (s *condSub) finish(err error) {
	s.once.Do(func() {
		s.r.mu.Lock()
		s.err.Store(&err)
		s.r.subs.Add(-1)
		s.r.mu.Unlock()

		s.r.cond.Broadcast()
	})
}

func (s *condSub) Close() { s.finish(ErrClosed) }

func (s *condSub) Recv(dst [][]byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	r := s.r

	r.mu.RLock()
	for {
		if err := s.err.Load(); err != nil {
			r.mu.RUnlock()
			return 0, *err
		}

		// Lapped: the writer has overwritten the slot this subscriber was
		// due to read next.
		if r.seq-s.pos > uint64(len(r.buf)) {
			r.mu.RUnlock()
			r.drops.Add(1)
			s.finish(ErrLagged) // takes the write lock, so not while holding RLock
			return 0, ErrLagged
		}

		n := 0
		for s.pos < r.seq && n < len(dst) {
			dst[n] = r.buf[s.pos&r.mask]
			s.pos++
			n++
		}
		if n > 0 {
			r.mu.RUnlock()
			return n, nil
		}

		if r.closed {
			r.mu.RUnlock()
			s.finish(ErrClosed)
			return 0, ErrClosed
		}

		// Wait releases the read lock, parks, and takes it again on wake.
		r.cond.Wait()
	}
}
