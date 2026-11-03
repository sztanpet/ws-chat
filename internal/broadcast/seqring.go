package broadcast

import (
	"sync"
	"sync/atomic"
)

// SeqRing is Ring without the read lock: a seqlock validates each batch.
type SeqRing struct {
	// Pointers, not headers: the GC is the grace period; a torn header is a race.
	buf  []atomic.Pointer[[]byte]
	mask uint64

	wmu    sync.Mutex
	closed atomic.Bool

	// begin is stored before the slot write and published after: the seqlock.
	begin     atomic.Uint64
	published atomic.Uint64

	// waiters lets the busy case skip swapping notify, and its allocation.
	notify  atomic.Value
	waiters atomic.Int64

	subs  atomic.Int64
	drops atomic.Int64
}

func NewSeqRing(capacity int) *SeqRing {
	if capacity < 1 {
		capacity = DefaultBuffer
	}
	capacity = roundPow2(capacity)
	r := &SeqRing{
		buf:  make([]atomic.Pointer[[]byte], capacity),
		mask: uint64(capacity - 1),
	}
	r.notify.Store(make(chan struct{}))
	return r
}

func (r *SeqRing) Broadcast(msg []byte) {
	r.wmu.Lock()
	if r.closed.Load() {
		r.wmu.Unlock()
		return
	}
	s := r.published.Load()
	r.begin.Store(s + 1)
	r.buf[s&r.mask].Store(&msg)
	r.published.Store(s + 1)

	// No lost wakeup: a sleeper bumps waiters before re-checking published, and
	// this stores published before loading waiters, so one sees the other.
	var wake chan struct{}
	if r.waiters.Load() > 0 {
		wake = r.notify.Swap(make(chan struct{})).(chan struct{})
	}
	r.wmu.Unlock()

	if wake != nil {
		close(wake)
	}
}

func (r *SeqRing) Subscribe() Sub {
	s := &seqSub{r: r, done: make(chan struct{})}
	// Counted even when closed: finish decrements, once, so it nets out.
	r.subs.Add(1)
	if r.closed.Load() {
		s.finish(ErrClosed)
		return s
	}
	// From the write cursor; a Close since the check above is caught by Recv.
	s.pos = r.published.Load()
	return s
}

func (r *SeqRing) Drops() int64 { return r.drops.Load() }

// Len is zero once closed: every subscriber is finished, noticed or not.
func (r *SeqRing) Len() int {
	if r.closed.Load() {
		return 0
	}
	return int(r.subs.Load())
}

func (r *SeqRing) Close() {
	r.wmu.Lock()
	if r.closed.Load() {
		r.wmu.Unlock()
		return
	}
	r.closed.Store(true)
	// Unconditionally: a sleeper's waiters bump may not be visible yet, and one
	// that captures the fresh channel re-checks closed after, and sees it.
	wake := r.notify.Swap(make(chan struct{})).(chan struct{})
	r.wmu.Unlock()

	close(wake)
}

type seqSub struct {
	r    *SeqRing
	pos  uint64 // next sequence to read; touched only by the reader
	done chan struct{}

	once sync.Once
	err  atomic.Pointer[error]
}

func (s *seqSub) finish(err error) {
	s.once.Do(func() {
		s.err.Store(&err)
		s.r.subs.Add(-1)
		close(s.done)
	})
}

func (s *seqSub) Close() { s.finish(ErrClosed) }

// Returns 0: what was copied into dst may be from the wrong lap.
func (s *seqSub) lag() (int, error) {
	s.r.drops.Add(1)
	s.finish(ErrLagged)
	return 0, ErrLagged
}

func (s *seqSub) Recv(dst [][]byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	r := s.r

	for {
		if err := s.err.Load(); err != nil {
			return 0, *err
		}

		pub := r.published.Load()
		if pub == s.pos {
			if r.closed.Load() {
				s.finish(ErrClosed)
				return 0, ErrClosed
			}
			// Register, then re-check: see Broadcast for why no wakeup is lost.
			notify := r.notify.Load().(chan struct{})
			r.waiters.Add(1)
			if r.published.Load() != s.pos || r.closed.Load() {
				r.waiters.Add(-1)
				continue
			}
			select {
			case <-notify:
			case <-s.done:
			}
			r.waiters.Add(-1)
			continue
		}

		// Lapped: sequence k is gone once k+capacity begins being written.
		if pub-s.pos > uint64(len(r.buf)) {
			return s.lag()
		}

		start := s.pos
		n := 0
		for s.pos < pub && n < len(dst) {
			dst[n] = *r.buf[s.pos&r.mask].Load()
			s.pos++
			n++
		}

		// Seqlock check. Failing retries rather than drops, keeping Ring's threshold;
		// it is one store from becoming the lap check's drop, so it cannot spin.
		if r.begin.Load()-start > uint64(len(r.buf)) {
			s.pos = start
			continue
		}
		return n, nil
	}
}
