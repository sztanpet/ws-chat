package broadcast

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// impls is the table every test and benchmark runs over. Adding an
// alternative implementation means adding one line here, nothing else.
var impls = []struct {
	name string
	new  func() Broadcaster
}{
	{"mapchan", func() Broadcaster { return NewMapChan() }},
}

func eachImpl(t *testing.T, fn func(t *testing.T, b Broadcaster)) {
	t.Helper()
	for _, impl := range impls {
		t.Run(impl.name, func(t *testing.T) {
			b := impl.new()
			defer b.Close()
			fn(t, b)
		})
	}
}

// recv reads one message, failing the test rather than hanging forever if
// nothing arrives.
func recv(t *testing.T, s *Sub) []byte {
	t.Helper()
	select {
	case msg, ok := <-s.C():
		if !ok {
			t.Fatal("subscription closed, wanted a message")
		}
		return msg
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a message")
		return nil
	}
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

// expectClosed asserts the subscription ended, and why.
func expectClosed(t *testing.T, s *Sub, wantDropped bool) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-s.C():
			if ok {
				continue // drain whatever was already buffered
			}
			if got := s.Dropped(); got != wantDropped {
				t.Fatalf("Dropped() = %v, want %v", got, wantDropped)
			}
			return
		case <-deadline:
			t.Fatal("timed out waiting for the subscription to close")
		}
	}
}

func TestBroadcastDelivers(t *testing.T) {
	eachImpl(t, func(t *testing.T, b Broadcaster) {
		subs := []*Sub{b.Subscribe(8), b.Subscribe(8), b.Subscribe(8)}
		if got := b.Len(); got != 3 {
			t.Fatalf("Len() = %d, want 3", got)
		}

		for i := range 5 {
			if dropped := b.Broadcast([]byte{byte(i)}); dropped != 0 {
				t.Fatalf("message %d dropped %d subscribers", i, dropped)
			}
		}

		// Every subscriber sees every message, in order.
		for i, s := range subs {
			for want := range 5 {
				if got := recv(t, s)[0]; got != byte(want) {
					t.Fatalf("sub %d: got %d, want %d", i, got, want)
				}
			}
		}
	})
}

func TestBroadcastToNobody(t *testing.T) {
	eachImpl(t, func(t *testing.T, b Broadcaster) {
		if dropped := b.Broadcast([]byte("into the void")); dropped != 0 {
			t.Fatalf("dropped = %d, want 0", dropped)
		}
	})
}

// The whole point of the package: one subscriber that stops reading must
// not affect anyone else, and must not block the sender.
func TestSlowSubscriberIsDroppedNotWaitedOn(t *testing.T) {
	eachImpl(t, func(t *testing.T, b Broadcaster) {
		slow := b.Subscribe(1) // never drained
		fast := b.Subscribe(64)

		if dropped := b.Broadcast([]byte("one")); dropped != 0 {
			t.Fatalf("first broadcast dropped %d, want 0 (buffer had room)", dropped)
		}

		// slow's buffer is now full; the next send finds it full and drops it.
		done := make(chan int, 1)
		go func() { done <- b.Broadcast([]byte("two")) }()
		select {
		case dropped := <-done:
			if dropped != 1 {
				t.Fatalf("dropped = %d, want 1", dropped)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Broadcast blocked on a full subscriber")
		}

		expectClosed(t, slow, true)

		if got := b.Len(); got != 1 {
			t.Fatalf("Len() = %d, want 1 (only the fast subscriber left)", got)
		}
		if got := string(recv(t, fast)); got != "one" {
			t.Fatalf("fast sub got %q, want %q", got, "one")
		}
		if got := string(recv(t, fast)); got != "two" {
			t.Fatalf("fast sub got %q, want %q", got, "two")
		}
	})
}

func TestUnsubscribe(t *testing.T) {
	eachImpl(t, func(t *testing.T, b Broadcaster) {
		s := b.Subscribe(8)
		stay := b.Subscribe(8)

		b.Unsubscribe(s)
		if got := b.Len(); got != 1 {
			t.Fatalf("Len() = %d, want 1", got)
		}
		expectClosed(t, s, false)

		if dropped := b.Broadcast([]byte("after")); dropped != 0 {
			t.Fatalf("dropped = %d, want 0", dropped)
		}
		if got := string(recv(t, stay)); got != "after" {
			t.Fatalf("got %q, want %q", got, "after")
		}
	})
}

func TestUnsubscribeIsIdempotent(t *testing.T) {
	eachImpl(t, func(t *testing.T, b Broadcaster) {
		s := b.Subscribe(8)
		b.Unsubscribe(s)
		b.Unsubscribe(s) // must not panic on the already-closed channel
		expectClosed(t, s, false)
	})
}

// A dropped subscriber that the consumer then unsubscribes is the normal
// shutdown order in the server: the write pump notices its channel closed
// and cleans up. It must not double-close.
func TestUnsubscribeAfterDrop(t *testing.T) {
	eachImpl(t, func(t *testing.T, b Broadcaster) {
		s := b.Subscribe(1)
		b.Broadcast([]byte("fills the buffer"))
		if dropped := b.Broadcast([]byte("drops it")); dropped != 1 {
			t.Fatalf("dropped = %d, want 1", dropped)
		}
		b.Unsubscribe(s)
		expectClosed(t, s, true) // still reports the real reason
	})
}

func TestClose(t *testing.T) {
	eachImpl(t, func(t *testing.T, b Broadcaster) {
		subs := []*Sub{b.Subscribe(8), b.Subscribe(8)}
		b.Close()
		b.Close() // idempotent

		if got := b.Len(); got != 0 {
			t.Fatalf("Len() = %d, want 0", got)
		}
		for i, s := range subs {
			t.Run(fmt.Sprint(i), func(t *testing.T) { expectClosed(t, s, false) })
		}

		// Subscribing to a closed broadcaster yields a finished Sub, not a
		// live one that would never receive anything.
		after := b.Subscribe(8)
		expectClosed(t, after, false)
		if got := b.Len(); got != 0 {
			t.Fatalf("Len() after late Subscribe = %d, want 0", got)
		}
		if dropped := b.Broadcast([]byte("nobody home")); dropped != 0 {
			t.Fatalf("dropped = %d, want 0", dropped)
		}
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
		// Close closes their channels.
		for range subscribed {
			s := b.Subscribe(64)
			drainWG.Add(1)
			go func() {
				defer drainWG.Done()
				for range s.C() {
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

		// Subscribe/unsubscribe churn against the same map the senders are
		// walking. These subscribers are never drained, so some get dropped
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
					s := b.Subscribe(1)
					b.Broadcast([]byte("churn"))
					b.Unsubscribe(s)
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
