package moderation

import (
	"sync"
	"testing"
	"time"
)

func TestEmptyStore(t *testing.T) {
	s := New()
	if muted, _ := s.Muted("main", "id:u1"); muted {
		t.Error("a fresh store has somebody muted")
	}
	if banned, _ := s.Banned(Global, "id:u1"); banned {
		t.Error("a fresh store has somebody banned")
	}
	// The zero value works too.
	var z Store
	if muted, _ := z.Muted("main", "id:u1"); muted {
		t.Error("the zero Store has somebody muted")
	}
}

// A channel mute silences somebody there and nowhere else. This is the
// whole point of the scope.
func TestChannelMuteIsLocal(t *testing.T) {
	s := New()
	s.Mute("main", "id:u1", time.Time{})

	if muted, _ := s.Muted("main", "id:u1"); !muted {
		t.Error("the mute did not take in its own channel")
	}
	if muted, _ := s.Muted("other", "id:u1"); muted {
		t.Error("a mute in one channel silenced another")
	}
	if muted, _ := s.Muted(Global, "id:u1"); muted {
		t.Error("a channel mute counted as a global one")
	}
	if muted, _ := s.Muted("main", "id:u2"); muted {
		t.Error("muting one person muted another")
	}
}

// A global mute counts everywhere, without having to be written into each
// channel.
func TestGlobalMuteAppliesEverywhere(t *testing.T) {
	s := New()
	s.Mute(Global, "id:u1", time.Time{})

	for _, scope := range []string{Global, "main", "other", "somewhere new"} {
		if muted, _ := s.Muted(scope, "id:u1"); !muted {
			t.Errorf("a global mute does not apply in %q", scope)
		}
	}
}

func TestGlobalBanAppliesEverywhere(t *testing.T) {
	s := New()
	s.Ban(Global, "id:u1", time.Time{})

	if banned, _ := s.Banned("main", "id:u1"); !banned {
		t.Error("a global ban does not apply in a channel")
	}
	if banned, _ := s.Banned(Global, "id:u1"); !banned {
		t.Error("a global ban does not apply globally")
	}
	// A ban is not a mute.
	if muted, _ := s.Muted("main", "id:u1"); muted {
		t.Error("a ban also muted")
	}
}

func TestChannelBanIsLocal(t *testing.T) {
	s := New()
	s.Ban("main", "id:u1", time.Time{})

	if banned, _ := s.Banned("main", "id:u1"); !banned {
		t.Error("the ban did not take")
	}
	if banned, _ := s.Banned("other", "id:u1"); banned {
		t.Error("a ban in one channel barred another")
	}
	if banned, _ := s.Banned(Global, "id:u1"); banned {
		t.Error("a channel ban barred the server")
	}
}

// Lifting one scope leaves the other alone: they were separate decisions.
func TestUnmuteIsPerScope(t *testing.T) {
	s := New()
	s.Mute(Global, "id:u1", time.Time{})
	s.Mute("main", "id:u1", time.Time{})

	if !s.Unmute("main", "id:u1") {
		t.Error("Unmute reported nothing to lift")
	}
	if muted, _ := s.Muted("main", "id:u1"); !muted {
		t.Error("lifting the channel mute also lifted the global one")
	}

	if !s.Unmute(Global, "id:u1") {
		t.Error("Unmute reported nothing global to lift")
	}
	if muted, _ := s.Muted("main", "id:u1"); muted {
		t.Error("the mute survived both being lifted")
	}
	if s.Unmute("main", "id:u1") {
		t.Error("Unmute reported lifting something twice")
	}
}

func TestExpiry(t *testing.T) {
	s := New()

	s.Mute("main", "id:past", time.Now().Add(-time.Second))
	if muted, _ := s.Muted("main", "id:past"); muted {
		t.Error("an expired mute still reads as active")
	}

	s.Mute("main", "id:future", time.Now().Add(time.Hour))
	muted, until := s.Muted("main", "id:future")
	if !muted {
		t.Error("a mute with an hour left is not active")
	}
	if until.IsZero() {
		t.Error("a timed mute reported no expiry")
	}
}

// An expired channel mute must not hide a global one that is still live.
func TestExpiredScopeFallsThroughToGlobal(t *testing.T) {
	s := New()
	s.Mute("main", "id:u1", time.Now().Add(-time.Second)) // expired
	s.Mute(Global, "id:u1", time.Time{})                  // still standing

	if muted, _ := s.Muted("main", "id:u1"); !muted {
		t.Fatal("an expired channel mute hid a live global one")
	}
}

func TestMuteReplacesDeadline(t *testing.T) {
	s := New()
	s.Mute("main", "id:u1", time.Now().Add(-time.Second))
	s.Mute("main", "id:u1", time.Now().Add(time.Hour))

	if muted, _ := s.Muted("main", "id:u1"); !muted {
		t.Fatal("re-muting did not extend an expired mute")
	}
}

func TestSweep(t *testing.T) {
	s := New()
	s.Mute("main", "id:gone", time.Now().Add(-time.Second))
	s.Ban(Global, "id:alsogone", time.Now().Add(-time.Second))
	s.Mute("main", "id:stays", time.Now().Add(time.Hour))
	s.Ban(Global, "id:permanent", time.Time{})

	if removed := s.Sweep(); removed != 2 {
		t.Fatalf("Sweep removed %d, want 2", removed)
	}
	mutes, bans := s.Counts()
	if mutes != 1 || bans != 1 {
		t.Fatalf("after sweeping: %d mutes and %d bans, want 1 each", mutes, bans)
	}
	if banned, _ := s.Banned(Global, "id:permanent"); !banned {
		t.Error("Sweep removed a permanent ban")
	}
}

// The same person in two channels is two entries, not one.
func TestScopesDoNotCollide(t *testing.T) {
	s := New()
	s.Mute("main", "id:u1", time.Time{})
	s.Mute("other", "id:u1", time.Time{})

	if mutes, _ := s.Counts(); mutes != 2 {
		t.Fatalf("counted %d mutes, want 2", mutes)
	}
	s.Unmute("main", "id:u1")
	if muted, _ := s.Muted("other", "id:u1"); !muted {
		t.Error("lifting one channel's mute lifted another's")
	}
}

func TestConcurrentUse(t *testing.T) {
	s := New()
	var wg sync.WaitGroup

	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 500 {
				scope := []string{Global, "main", "other"}[i%3]
				s.Mute(scope, "id:u1", time.Now().Add(time.Hour))
				s.Muted("main", "id:u1")
				s.Banned(scope, "id:u1")
				s.Unmute(scope, "id:u1")
				s.Sweep()
			}
		}()
	}
	wg.Wait()
}
