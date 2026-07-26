package broadcast

import (
	"sync"
	"sync/atomic"
)

// SeqRing is Ring with the lock taken off the read path. Same shared
// buffer, same lazy lap detection, same swap-and-close wakeup channel —
// what changes is how readers and the writer stay out of each other's way.
//
// Ring guards everything with an RWMutex. Correct and simple, but an RLock
// is still a read-modify-write on one shared word: at ten thousand
// subscribers every Recv batch bounces that cache line across the machine,
// and every Broadcast takes the write half against all of them. The lock is
// not protecting much — a reader only wants "how far has the writer got"
// and one slot per message — so here both sides get what they need from
// atomics alone:
//
//   - A slot holds a POINTER to the message header, not the header itself.
//     The writer replaces the pointer and never mutates what it points to,
//     so a reader that loaded it may hold it as long as it likes — the GC
//     is the grace period. That is the RCU shape: reads are plain loads,
//     reclamation is free.
//
//   - Publication is a pair of counters bracketing the slot write: begin
//     before, published after. A reader loads published, copies pointers,
//     then loads begin ONCE for the whole batch. If begin has not reached
//     into what it just read, no slot it copied was mid-replacement; if it
//     has, the reader was lapped and is dropped — which is what was about
//     to happen to it anyway. This is a seqlock, validated per batch
//     instead of per message.
//
// A Recv batch is three atomic loads plus a pointer load per message, with
// no read-modify-write anywhere on the path. Writers serialize on a plain
// Mutex readers never touch; the server already serializes broadcasts per
// channel (message ids are assigned under its own lock), so writer
// concurrency was never the contended axis — reader concurrency is.
//
// The cost against Ring is one small allocation per Broadcast: storing
// &msg makes the 24-byte slice header escape. The zero-allocation version
// of this design stores the header words in place and reads them torn,
// which is exactly the data race the race detector exists to refuse — and
// reconstructing a possibly-torn pointer/length pair with unsafe.Slice is
// rejected by checkptr, which -race turns on. The indirection is what
// keeps the design safe, and the encoder upstream allocates an order of
// magnitude more per message than it costs.
type SeqRing struct {
	buf  []atomic.Pointer[[]byte]
	mask uint64

	// Writers serialize here; readers never touch it.
	wmu    sync.Mutex
	closed atomic.Bool

	// begin counts sequences a writer has STARTED writing, published the
	// ones it has finished. Stored in that order around the slot write,
	// which is what lets a reader prove after the fact that its batch was
	// not overwritten under it.
	begin     atomic.Uint64
	published atomic.Uint64

	// notify holds the current chan struct{}, swapped for a fresh one and
	// closed to wake every sleeper at once. waiters counts sleepers so the
	// common busy case skips the swap and its allocation entirely.
	notify  atomic.Value
	waiters atomic.Int64

	subs  atomic.Int64
	drops atomic.Int64
}

// NewSeqRing returns a SeqRing whose shared history holds capacity
// messages, rounded up to a power of two. A subscriber that falls more than
// capacity messages behind is lapped and dropped.
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

	// Wake sleepers only if there are any. The ordering argument is the
	// same one Ring makes with its RWMutex, carried by sequential
	// consistency instead: a sleeper increments waiters BEFORE its final
	// re-check of published, and the writer stores published before
	// loading waiters. So either this load sees the sleeper — and the
	// channel it captured is a generation this swap (or a later one)
	// closes — or the sleeper's re-check sees this message and never
	// sleeps. The two cannot interleave any third way.
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
	// Count first, unconditionally: finish is what decrements, and it runs
	// at most once per subscriber, so the closed case nets out to zero.
	r.subs.Add(1)
	if r.closed.Load() {
		s.finish(ErrClosed)
		return s
	}
	// Start at the write cursor: a new subscriber sees what happens next,
	// not the history still sitting in the ring. If Close lands between
	// the check above and here, the first Recv notices closed.
	s.pos = r.published.Load()
	return s
}

func (r *SeqRing) Drops() int64 { return r.drops.Load() }

// Len reports the subscriber count. A lapped subscriber is still counted
// until it next calls Recv and notices; a closed SeqRing reports zero
// rather than waiting for every subscriber to be scheduled and find out.
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
	// Swap unconditionally: Close must wake sleepers whether or not the
	// waiter count is visible yet. A sleeper that captured the fresh
	// channel instead necessarily captured it after this swap, so its
	// re-check of closed — which happens after the capture — sees true.
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

// finish records how the subscription ended, drops it from the count, and
// wakes its reader. It runs at most once.
func (s *seqSub) finish(err error) {
	s.once.Do(func() {
		s.err.Store(&err)
		s.r.subs.Add(-1)
		close(s.done)
	})
}

func (s *seqSub) Close() { s.finish(ErrClosed) }

// lag ends the subscription as lapped. Any pointers already copied into the
// caller's dst may be from the wrong lap of the ring, which is why Recv
// returns 0 with the error rather than a partial batch.
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
			// The sleep dance: capture the channel, register, then
			// re-check. A broadcast that saw waiters as zero must have
			// published before the increment, so the re-check sees it; a
			// broadcast that saw the increment closes a channel generation
			// no older than the one captured here. Either way no wakeup is
			// lost. Close swaps unconditionally, so the same holds for it.
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

		// Lapped before even starting: sequence k lives in the ring until
		// k+capacity begins being written, so it is gone once the write
		// cursor is more than capacity ahead.
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

		// The seqlock validation, once per batch: if a writer has begun
		// overwriting the OLDEST slot this batch read, some pointer above
		// may be from the wrong lap. begin never trails published, so a
		// clean check here proves every slot copied was the one meant.
		//
		// Failing it is a RETRY, not a drop. Rewinding to where the batch
		// started costs nothing — the reader's position is its own — and it
		// is what keeps the drop threshold exactly Ring's: a subscriber
		// dies when the writer has genuinely lapped it, decided by the
		// check at the top of the loop, not because it happened to be
		// reading the slot the writer reached. Dropping here instead would
		// make the effective capacity one slot smaller than Ring's and turn
		// a near miss into a disconnection, which would make every capacity
		// number measured for Ring inapplicable to this one.
		//
		// The retry terminates. Validation fails only once the writer has
		// begun sequence start+capacity, so published reaches
		// start+capacity+1 as soon as that store lands and the next pass
		// drops the subscriber. The spin is bounded by one store, not by
		// the writer's rate.
		if r.begin.Load()-start > uint64(len(r.buf)) {
			s.pos = start
			continue
		}
		return n, nil
	}
}
