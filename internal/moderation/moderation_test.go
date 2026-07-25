package moderation

import (
	"sync"
	"testing"
	"time"
)

func TestEmptyStore(t *testing.T) {
	s := New()
	if muted, _ := s.Muted("id:u1"); muted {
		t.Error("a fresh store has somebody muted")
	}
	if banned, _ := s.Banned("id:u1"); banned {
		t.Error("a fresh store has somebody banned")
	}
	// The zero value works too.
	var z Store
	if muted, _ := z.Muted("id:u1"); muted {
		t.Error("the zero Store has somebody muted")
	}
}

func TestMuteAndUnmute(t *testing.T) {
	s := New()
	s.Mute("id:u1", time.Time{})

	muted, until := s.Muted("id:u1")
	if !muted {
		t.Fatal("the mute did not take")
	}
	if !until.IsZero() {
		t.Errorf("until = %v, want zero for a permanent mute", until)
	}
	if muted, _ := s.Muted("id:u2"); muted {
		t.Error("muting one key muted another")
	}
	// A mute is not a ban.
	if banned, _ := s.Banned("id:u1"); banned {
		t.Error("a mute also banned")
	}

	if !s.Unmute("id:u1") {
		t.Error("Unmute reported nothing to lift")
	}
	if muted, _ := s.Muted("id:u1"); muted {
		t.Error("the mute survived being lifted")
	}
	if s.Unmute("id:u1") {
		t.Error("Unmute reported lifting a mute twice")
	}
}

func TestBanAndUnban(t *testing.T) {
	s := New()
	s.Ban("id:u1", time.Time{})

	if banned, _ := s.Banned("id:u1"); !banned {
		t.Fatal("the ban did not take")
	}
	if muted, _ := s.Muted("id:u1"); muted {
		t.Error("a ban also muted")
	}
	if !s.Unban("id:u1") {
		t.Error("Unban reported nothing to lift")
	}
	if banned, _ := s.Banned("id:u1"); banned {
		t.Error("the ban survived being lifted")
	}
}

func TestExpiry(t *testing.T) {
	s := New()

	s.Mute("id:past", time.Now().Add(-time.Second))
	if muted, _ := s.Muted("id:past"); muted {
		t.Error("an expired mute still reads as active")
	}

	s.Mute("id:future", time.Now().Add(time.Hour))
	muted, until := s.Muted("id:future")
	if !muted {
		t.Error("a mute with an hour left is not active")
	}
	if until.IsZero() {
		t.Error("a timed mute reported no expiry")
	}
}

// Re-muting replaces the deadline rather than keeping the older one.
func TestMuteReplacesDeadline(t *testing.T) {
	s := New()
	s.Mute("id:u1", time.Now().Add(-time.Second)) // already expired
	s.Mute("id:u1", time.Now().Add(time.Hour))

	if muted, _ := s.Muted("id:u1"); !muted {
		t.Fatal("re-muting did not extend an expired mute")
	}
}

func TestSweep(t *testing.T) {
	s := New()
	s.Mute("id:gone", time.Now().Add(-time.Second))
	s.Ban("id:alsogone", time.Now().Add(-time.Second))
	s.Mute("id:stays", time.Now().Add(time.Hour))
	s.Ban("id:permanent", time.Time{})

	if removed := s.Sweep(); removed != 2 {
		t.Fatalf("Sweep removed %d, want 2", removed)
	}
	mutes, bans := s.Counts()
	if mutes != 1 || bans != 1 {
		t.Fatalf("after sweeping: %d mutes and %d bans, want 1 each", mutes, bans)
	}
	// A permanent entry is not "expired at the zero time".
	if banned, _ := s.Banned("id:permanent"); !banned {
		t.Error("Sweep removed a permanent ban")
	}
}

func TestConcurrentUse(t *testing.T) {
	s := New()
	var wg sync.WaitGroup

	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := "id:u1"
			for range 500 {
				s.Mute(key, time.Now().Add(time.Hour))
				s.Muted(key)
				s.Banned(key)
				s.Unmute(key)
				s.Sweep()
			}
		}()
		_ = i
	}
	wg.Wait()
}
