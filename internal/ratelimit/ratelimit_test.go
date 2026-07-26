package ratelimit

import (
	"sync"
	"testing"
	"time"
)

// Every test drives the clock explicitly. A rate limiter tested against
// real time is a test that fails on a busy machine.
var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// Unlimited is the default, so it is the first thing that has to work.
func TestNilBucketAllowsEverything(t *testing.T) {
	var b *Bucket

	if !b.Unlimited() {
		t.Error("a nil bucket is not unlimited")
	}
	for i := range 1000 {
		if !b.AllowAt(epoch) {
			t.Fatalf("a nil bucket refused call %d", i)
		}
	}
}

// "No limit" and "a limit of nothing" must not be one typo apart.
func TestNonPositiveMeansUnlimited(t *testing.T) {
	for _, tt := range []struct {
		burst    int
		interval time.Duration
	}{
		{0, time.Second},
		{-1, time.Second},
		{5, 0},
		{5, -time.Second},
		{0, 0},
	} {
		if b := New(tt.burst, tt.interval); b != nil {
			t.Errorf("New(%d, %v) enforces a limit, want unlimited", tt.burst, tt.interval)
		}
	}
}

func TestBurstThenRefill(t *testing.T) {
	b := New(3, time.Second)

	// The burst is available immediately: a new connection may talk.
	for i := range 3 {
		if !b.AllowAt(epoch) {
			t.Fatalf("call %d refused inside the burst", i)
		}
	}
	if b.AllowAt(epoch) {
		t.Fatal("the fourth call was allowed with an empty bucket")
	}

	// Half an interval is not a token.
	if b.AllowAt(epoch.Add(500 * time.Millisecond)) {
		t.Fatal("half an interval bought a token")
	}
	// A whole one is.
	if !b.AllowAt(epoch.Add(time.Second)) {
		t.Fatal("a full interval did not refill a token")
	}
	if b.AllowAt(epoch.Add(time.Second)) {
		t.Fatal("one interval refilled more than one token")
	}
}

func TestRefillCapsAtBurst(t *testing.T) {
	b := New(2, time.Second)

	// Drain, then wait far longer than it takes to refill.
	b.AllowAt(epoch)
	b.AllowAt(epoch)
	later := epoch.Add(time.Hour)

	// Both calls have to happen: the point is that an hour buys exactly
	// two tokens back, so they are spent one at a time and then checked.
	first, second := b.AllowAt(later), b.AllowAt(later)
	if !first || !second {
		t.Fatal("the bucket did not refill")
	}
	if b.AllowAt(later) {
		t.Fatal("an hour of idling banked more than the burst")
	}
}

func TestSustainedRate(t *testing.T) {
	b := New(1, 100*time.Millisecond)

	// One per interval, exactly, over a stretch.
	now := epoch
	for i := range 10 {
		if !b.AllowAt(now) {
			t.Fatalf("call %d refused at the sustained rate", i)
		}
		if b.AllowAt(now) {
			t.Fatalf("call %d got a second token in the same instant", i)
		}
		now = now.Add(100 * time.Millisecond)
	}
}

// A clock that jumps backwards must not drain the bucket or strand it.
func TestClockGoingBackwards(t *testing.T) {
	b := New(2, time.Second)

	if !b.AllowAt(epoch) {
		t.Fatal("first call refused")
	}
	if !b.AllowAt(epoch.Add(-time.Hour)) {
		t.Fatal("a backwards clock refused a token that was available")
	}
	// And the bucket still refills normally afterwards.
	if !b.AllowAt(epoch.Add(time.Second)) {
		t.Fatal("the bucket stopped refilling after the clock went backwards")
	}
}

func TestTokens(t *testing.T) {
	var nilBucket *Bucket
	if got := nilBucket.Tokens(); got != 0 {
		t.Errorf("nil bucket has %v tokens, want 0", got)
	}

	b := New(2, time.Second)
	if got := b.Tokens(); got != 2 {
		t.Errorf("a new bucket has %v tokens, want 2", got)
	}
	b.AllowAt(epoch)
	if got := b.Tokens(); got != 1 {
		t.Errorf("after one call, %v tokens, want 1", got)
	}
}

// One bucket, many senders: the total handed out must be exactly the burst,
// not "about" it.
func TestConcurrentAllowIsExact(t *testing.T) {
	const (
		burst   = 50
		callers = 8
		each    = 100
	)
	b := New(burst, time.Hour) // an hour: nothing refills during the test

	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed := 0

	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := 0
			for range each {
				if b.AllowAt(epoch) {
					local++
				}
			}
			mu.Lock()
			allowed += local
			mu.Unlock()
		}()
	}
	wg.Wait()

	if allowed != burst {
		t.Fatalf("%d calls allowed, want exactly %d", allowed, burst)
	}
}
