// Package broadcast fans out; a subscriber that lags is dropped, never waited on.
package broadcast

import (
	"errors"
	"sync"
	"sync/atomic"
)

// DefaultBuffer is seconds of a busy channel: a subscriber that fills it is gone.
const DefaultBuffer = 256

var (
	ErrClosed = errors.New("broadcast: subscription closed")

	ErrLagged = errors.New("broadcast: subscriber fell behind")
)

// Broadcaster shares one msg among all subscribers: neither side may modify it.
type Broadcaster interface {
	// Subscribing to a closed one is not an error: Recv reports ErrClosed.
	Subscribe() Sub

	Broadcast(msg []byte)

	// A counter, not a Broadcast result: a lag may be noticed later, by the reader.
	Drops() int64

	// May briefly overcount: a lazily dropped subscriber leaves when it notices.
	Len() int

	Close()
}

type Sub interface {
	// Recv returns all that piled up: the wakeup is most of what a broadcast costs.
	Recv(dst [][]byte) (int, error)

	Close()
}

// MapChan is the obvious design, kept as the baseline to measure against.
type MapChan struct {
	buf int

	mu     sync.RWMutex
	subs   map[*mapSub]struct{}
	closed bool

	drops atomic.Int64
}

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

	// Not under the read lock: a concurrent Broadcast's send would panic.
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

func (b *MapChan) remove(s *mapSub) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.subs[s]; !ok {
		return
	}
	delete(b.subs, s)
	s.finish(false)
}

// The caller holds the write lock, so no send to ch can be in flight.
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
