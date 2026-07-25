package broadcast

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// impls is the table every test and benchmark runs over. Adding an
// alternative implementation means adding one line here, nothing else.
//
// The size argument means "how much slack a subscriber gets before it is
// dropped for lagging" — a per-subscriber buffer for MapChan, the shared
// ring capacity for Ring. It is not the same mechanism, but it is the same
// promise, which is what makes them comparable.
var impls = []struct {
	name string
	new  func(size int) Broadcaster
}{
	{"mapchan", func(size int) Broadcaster { return NewMapChan(size) }},
	{"ring", func(size int) Broadcaster { return NewRing(size) }},
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

// recvWithin runs Recv with a deadline, so a broken implementation fails
// the test instead of hanging until the package timeout.
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

// recvOne reads a single message, requiring one to arrive.
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

// expectEnd drains until the subscription ends and asserts how it ended.
func expectEnd(t *testing.T, s Sub, want error) {
	t.Helper()
	dst := make([][]byte, 4)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, err := recvWithin(t, s, dst, 2*time.Second)
		if err == nil {
			continue // messages already in flight; keep draining
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

		// Every subscriber sees every message, in order.
		for i, s := range subs {
			for want := range 5 {
				if got := recvOne(t, s)[0]; got != byte(want) {
					t.Fatalf("sub %d: got %d, want %d", i, got, want)
				}
			}
		}
	})
}

// Recv returns everything that piled up since the last call, which is the
// property the whole batch API exists for.
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

// A batch is capped by the destination slice, and the remainder stays
// queued for the next call.
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

// Half the point of the package: a subscriber that stops reading must not
// stall the sender.
func TestBroadcastNeverBlocks(t *testing.T) {
	eachImpl(t, func(t *testing.T, b Broadcaster) {
		lagging := b.Subscribe() // never read

		// Overrun whatever slack the implementation gives a subscriber,
		// several times over.
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

// The other half: dropping the subscriber that fell behind must not cost
// the ones that did not.
//
// The healthy subscriber reads once per broadcast, which can never block
// because a message was just sent, and never lets it fall more than one
// message behind. That keeps the test deterministic — no sleeps, no
// reliance on a drainer goroutine being scheduled promptly.
func TestLaggingSubscriberDoesNotAffectOthers(t *testing.T) {
	eachImpl(t, func(t *testing.T, b Broadcaster) {
		lagging := b.Subscribe() // never read
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

		// The lagging one is gone, and only it.
		expectEnd(t, lagging, ErrLagged)
		if got := b.Drops(); got != 1 {
			t.Fatalf("Drops() = %d, want 1", got)
		}
		if got := b.Len(); got != 1 {
			t.Fatalf("Len() = %d, want 1 (only the healthy subscriber left)", got)
		}
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
		s.Close() // must not panic on the already-ended subscription
		expectEnd(t, s, ErrClosed)
	})
}

// A lagging subscriber that the consumer then closes is the normal shutdown
// order in the server: the write pump sees ErrLagged and cleans up. It must
// not double-close, and it must keep reporting the real reason.
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
		b.Close() // idempotent

		if got := b.Len(); got != 0 {
			t.Fatalf("Len() = %d, want 0", got)
		}
		for _, s := range subs {
			expectEnd(t, s, ErrClosed)
		}

		// Subscribing to a closed broadcaster yields a finished Sub, not a
		// live one that would never receive anything.
		after := b.Subscribe()
		expectEnd(t, after, ErrClosed)
		if got := b.Len(); got != 0 {
			t.Fatalf("Len() after late Subscribe = %d, want 0", got)
		}
		b.Broadcast([]byte("nobody home"))
	})
}

// The race detector is the point of this one: broadcasters, subscribers
// churning, and consumers draining, all at once.
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

		// Long-lived subscribers with drainers, so the fan-out actually
		// delivers instead of instantly dropping everyone. They exit when
		// Close ends their subscriptions.
		for range subscribed {
			s := b.Subscribe()
			drainWG.Add(1)
			go func() {
				defer drainWG.Done()
				dst := make([][]byte, 16)
				for {
					if _, err := s.Recv(dst); err != nil {
						return
					}
				}
			}()
		}

		for range senders {
			sendWG.Add(1)
			go func() {
				defer sendWG.Done()
				msg := []byte("hello")
				for range perGoro {
					b.Broadcast(msg)
				}
			}()
		}

		// Subscribe/close churn against the same state the senders are
		// walking. These subscribers are never read, so some get dropped
		// mid-broadcast, which is exactly the race worth hunting.
		for range churners {
			churnWG.Add(1)
			go func() {
				defer churnWG.Done()
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
			}()
		}

		// Shut down in dependency order, no sleeps: the senders bound
		// themselves, the churners stop on the signal, and Close is what
		// releases the drainers.
		waitOrFail(t, &sendWG, "senders")
		close(stop)
		waitOrFail(t, &churnWG, "churners")
		b.Close()
		waitOrFail(t, &drainWG, "drainers")
	})
}

// waitOrFail waits for wg, failing instead of hanging the whole test binary
// until the package timeout if something deadlocked.
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
