package broadcast

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// size is a lagging subscriber's slack, however each one keeps it.
var impls = []struct {
	name string
	new  func(size int) Broadcaster
}{
	{"mapchan", func(size int) Broadcaster { return NewMapChan(size) }},
	{"ring", func(size int) Broadcaster { return NewRing(size) }},
	{"seqring", func(size int) Broadcaster { return NewSeqRing(size) }},
}

const testSize = 8

func eachImpl(t *testing.T, fn func(t *testing.T, b Broadcaster)) {
	t.Helper()
	for _, impl := range impls {
		t.Run(impl.name, func(t *testing.T) {
			b := impl.new(testSize)
			defer b.Close()
			fn(t, b)
		})
	}
}

func recvWithin(t *testing.T, s Sub, dst [][]byte, d time.Duration) (int, error) {
	t.Helper()
	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		n, err := s.Recv(dst)
		done <- result{n, err}
	}()
	select {
	case r := <-done:
		return r.n, r.err
	case <-time.After(d):
		t.Fatal("Recv timed out")
		return 0, nil
	}
}

func recvOne(t *testing.T, s Sub) []byte {
	t.Helper()
	dst := make([][]byte, 1)
	n, err := recvWithin(t, s, dst, 2*time.Second)
	if err != nil {
		t.Fatalf("Recv: %v, wanted a message", err)
	}
	if n != 1 {
		t.Fatalf("Recv returned %d messages, want 1", n)
	}
	return dst[0]
}

func expectEnd(t *testing.T, s Sub, want error) {
	t.Helper()
	dst := make([][]byte, 4)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, err := recvWithin(t, s, dst, 2*time.Second)
		if err == nil {
			continue
		}
		if !errors.Is(err, want) {
			t.Fatalf("subscription ended with %v, want %v", err, want)
		}
		return
	}
	t.Fatal("timed out waiting for the subscription to end")
}

func TestBroadcastDelivers(t *testing.T) {
	eachImpl(t, func(t *testing.T, b Broadcaster) {
		subs := []Sub{b.Subscribe(), b.Subscribe(), b.Subscribe()}
		if got := b.Len(); got != 3 {
			t.Fatalf("Len() = %d, want 3", got)
		}

		for i := range 5 {
			b.Broadcast([]byte{byte(i)})
		}
		if got := b.Drops(); got != 0 {
			t.Fatalf("Drops() = %d, want 0", got)
		}

		for i, s := range subs {
			for want := range 5 {
				if got := recvOne(t, s)[0]; got != byte(want) {
					t.Fatalf("sub %d: got %d, want %d", i, got, want)
				}
			}
		}
	})
}

func TestRecvBatches(t *testing.T) {
	eachImpl(t, func(t *testing.T, b Broadcaster) {
		s := b.Subscribe()
		for i := range 4 {
			b.Broadcast([]byte{byte(i)})
		}

		dst := make([][]byte, 8)
		n, err := recvWithin(t, s, dst, 2*time.Second)
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if n != 4 {
			t.Fatalf("Recv returned %d messages, want all 4 in one call", n)
		}
		for i := range 4 {
			if dst[i][0] != byte(i) {
				t.Fatalf("message %d = %d, want %d", i, dst[i][0], i)
			}
		}
	})
}

func TestRecvRespectsDstLen(t *testing.T) {
	eachImpl(t, func(t *testing.T, b Broadcaster) {
		s := b.Subscribe()
		for i := range 4 {
			b.Broadcast([]byte{byte(i)})
		}

		dst := make([][]byte, 2)
		n, err := recvWithin(t, s, dst, 2*time.Second)
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if n != 2 {
			t.Fatalf("Recv returned %d messages, want 2", n)
		}
		if dst[0][0] != 0 || dst[1][0] != 1 {
			t.Fatalf("got messages %d,%d want 0,1", dst[0][0], dst[1][0])
		}

		if got := recvOne(t, s)[0]; got != 2 {
			t.Fatalf("next message = %d, want 2", got)
		}
	})
}

func TestRecvEmptyDst(t *testing.T) {
	eachImpl(t, func(t *testing.T, b Broadcaster) {
		s := b.Subscribe()
		n, err := s.Recv(nil)
		if n != 0 || err != nil {
			t.Fatalf("Recv(nil) = %d, %v; want 0, nil", n, err)
		}
	})
}

func TestBroadcastToNobody(t *testing.T) {
	eachImpl(t, func(t *testing.T, b Broadcaster) {
		b.Broadcast([]byte("into the void"))
		if got := b.Drops(); got != 0 {
			t.Fatalf("Drops() = %d, want 0", got)
		}
	})
}

func TestBroadcastNeverBlocks(t *testing.T) {
	eachImpl(t, func(t *testing.T, b Broadcaster) {
		lagging := b.Subscribe()

		done := make(chan struct{})
		go func() {
			defer close(done)
			for i := range testSize * 4 {
				b.Broadcast([]byte{byte(i)})
			}
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("Broadcast blocked on a subscriber that stopped reading")
		}

		expectEnd(t, lagging, ErrLagged)
		if got := b.Drops(); got != 1 {
			t.Fatalf("Drops() = %d, want 1", got)
		}
		if got := b.Len(); got != 0 {
			t.Fatalf("Len() = %d, want 0", got)
		}
	})
}

// Reading once per broadcast keeps the healthy one in step with no sleeps.
func TestLaggingSubscriberDoesNotAffectOthers(t *testing.T) {
	eachImpl(t, func(t *testing.T, b Broadcaster) {
		lagging := b.Subscribe()
		healthy := b.Subscribe()

		const sent = testSize * 4
		dst := make([][]byte, sent)
		got := 0
		for i := range sent {
			b.Broadcast([]byte{byte(i)})

			n, err := healthy.Recv(dst)
			if err != nil {
				t.Fatalf("healthy subscriber ended with %v after %d messages", err, got)
			}
			for j := range n {
				if want := byte(got + j); dst[j][0] != want {
					t.Fatalf("message %d = %d, want %d", got+j, dst[j][0], want)
				}
			}
			got += n
		}
		if got != sent {
			t.Fatalf("healthy subscriber got %d messages, want %d", got, sent)
		}

		expectEnd(t, lagging, ErrLagged)
		if got := b.Drops(); got != 1 {
			t.Fatalf("Drops() = %d, want 1", got)
		}
		if got := b.Len(); got != 1 {
			t.Fatalf("Len() = %d, want 1 (only the healthy subscriber left)", got)
		}
	})
}

// Capacity numbers measured on one implementation only transfer if all drop here.
func TestDropThresholdIsExactlyTheSlack(t *testing.T) {
	t.Run("survives at the limit", func(t *testing.T) {
		eachImpl(t, func(t *testing.T, b Broadcaster) {
			s := b.Subscribe()
			for i := range testSize {
				b.Broadcast([]byte{byte(i)})
			}

			dst := make([][]byte, testSize)
			for got := 0; got < testSize; {
				n, err := recvWithin(t, s, dst[:testSize-got], 2*time.Second)
				if err != nil {
					t.Fatalf("dropped at exactly %d messages behind after %d: %v", testSize, got, err)
				}
				for j := range n {
					if want := byte(got + j); dst[j][0] != want {
						t.Fatalf("message %d = %d, want %d", got+j, dst[j][0], want)
					}
				}
				got += n
			}
			if got := b.Drops(); got != 0 {
				t.Fatalf("Drops() = %d, want 0 at exactly the limit", got)
			}
		})
	})

	t.Run("dropped one past it", func(t *testing.T) {
		eachImpl(t, func(t *testing.T, b Broadcaster) {
			s := b.Subscribe()
			for i := range testSize + 1 {
				b.Broadcast([]byte{byte(i)})
			}

			expectEnd(t, s, ErrLagged)
			if got := b.Drops(); got != 1 {
				t.Fatalf("Drops() = %d, want 1 one past the limit", got)
			}
		})
	})
}

func TestSubClose(t *testing.T) {
	eachImpl(t, func(t *testing.T, b Broadcaster) {
		s := b.Subscribe()
		stay := b.Subscribe()

		s.Close()
		if got := b.Len(); got != 1 {
			t.Fatalf("Len() = %d, want 1", got)
		}
		expectEnd(t, s, ErrClosed)

		b.Broadcast([]byte("after"))
		if got := string(recvOne(t, stay)); got != "after" {
			t.Fatalf("got %q, want %q", got, "after")
		}
		if got := b.Drops(); got != 0 {
			t.Fatalf("Drops() = %d, want 0", got)
		}
	})
}

func TestSubCloseIsIdempotent(t *testing.T) {
	eachImpl(t, func(t *testing.T, b Broadcaster) {
		s := b.Subscribe()
		s.Close()
		s.Close()
		expectEnd(t, s, ErrClosed)
	})
}

// The server's own order: a pump sees ErrLagged, then closes its Sub.
func TestCloseAfterLagged(t *testing.T) {
	eachImpl(t, func(t *testing.T, b Broadcaster) {
		s := b.Subscribe()
		for i := range testSize * 4 {
			b.Broadcast([]byte{byte(i)})
		}
		expectEnd(t, s, ErrLagged)
		s.Close()
		if _, err := recvWithin(t, s, make([][]byte, 1), 2*time.Second); !errors.Is(err, ErrLagged) {
			t.Fatalf("Recv after Close = %v, want ErrLagged", err)
		}
	})
}

func TestBroadcasterClose(t *testing.T) {
	eachImpl(t, func(t *testing.T, b Broadcaster) {
		subs := []Sub{b.Subscribe(), b.Subscribe()}
		b.Close()
		b.Close()

		if got := b.Len(); got != 0 {
			t.Fatalf("Len() = %d, want 0", got)
		}
		for _, s := range subs {
			expectEnd(t, s, ErrClosed)
		}

		after := b.Subscribe()
		expectEnd(t, after, ErrClosed)
		if got := b.Len(); got != 0 {
			t.Fatalf("Len() after late Subscribe = %d, want 0", got)
		}
		b.Broadcast([]byte("nobody home"))
	})
}

// For the race detector: it asserts little itself.
func TestConcurrentChurn(t *testing.T) {
	eachImpl(t, func(t *testing.T, b Broadcaster) {
		const (
			senders    = 4
			churners   = 4
			perGoro    = 500
			subscribed = 32
		)

		var drainWG, sendWG, churnWG sync.WaitGroup
		stop := make(chan struct{})

		// Drained, or the fan-out would drop everyone and exercise nothing.
		for range subscribed {
			s := b.Subscribe()
			drainWG.Go(func() {
				dst := make([][]byte, 16)
				for {
					if _, err := s.Recv(dst); err != nil {
						return
					}
				}
			})
		}

		for range senders {
			sendWG.Go(func() {
				msg := []byte("hello")
				for range perGoro {
					b.Broadcast(msg)
				}
			})
		}

		// Never read, so some are dropped mid-broadcast: the race worth hunting.
		for range churners {
			churnWG.Go(func() {
				for {
					select {
					case <-stop:
						return
					default:
					}
					s := b.Subscribe()
					b.Broadcast([]byte("churn"))
					s.Close()
				}
			})
		}

		waitOrFail(t, &sendWG, "senders")
		close(stop)
		waitOrFail(t, &churnWG, "churners")
		b.Close()
		waitOrFail(t, &drainWG, "drainers")
	})
}

func waitOrFail(t *testing.T, wg *sync.WaitGroup, what string) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); wg.Wait() }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatalf("timed out waiting for the %s to finish", what)
	}
}
