// Package moderation holds who is muted and who is banned.
//
// It is state and expiry and nothing else: it does not decide who may
// moderate (that is hook.Authorizer), it does not persist anything (that is
// hook.Recorder), and it does not know what a channel or a connection is.
// It answers one question — is this key muted or banned right now — and it
// answers it on the hot path, so it answers it under a read lock.
package moderation

import (
	"sync"
	"time"
)

// Store is the set of active mutes and bans, keyed by hook.Identity.Key.
// The zero value is ready to use and has nobody in it.
type Store struct {
	mu    sync.RWMutex
	mutes map[string]time.Time
	bans  map[string]time.Time
}

// New returns an empty Store.
func New() *Store { return &Store{} }

// Mute silences key until the given time. A zero time never expires.
func (s *Store) Mute(key string, until time.Time) { s.set(&s.mutes, key, until) }

// Ban bars key until the given time. A zero time never expires.
func (s *Store) Ban(key string, until time.Time) { s.set(&s.bans, key, until) }

// Unmute lifts a mute, reporting whether there was one.
func (s *Store) Unmute(key string) bool { return s.clear(&s.mutes, key) }

// Unban lifts a ban, reporting whether there was one.
func (s *Store) Unban(key string) bool { return s.clear(&s.bans, key) }

// Muted reports whether key is silenced, and until when.
func (s *Store) Muted(key string) (bool, time.Time) { return s.active(s.mutes, key) }

// Banned reports whether key is barred, and until when.
func (s *Store) Banned(key string) (bool, time.Time) { return s.active(s.bans, key) }

func (s *Store) set(m *map[string]time.Time, key string, until time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if *m == nil {
		*m = make(map[string]time.Time)
	}
	(*m)[key] = until
}

func (s *Store) clear(m *map[string]time.Time, key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := (*m)[key]; !ok {
		return false
	}
	delete(*m, key)
	return true
}

// active checks one map. Expiry is lazy: an entry that has run out reads as
// absent and is left for the next sweep to remove.
//
// Lazily on purpose. The alternative is a timer per mute, which is a
// goroutine per mute and a cancellation problem per unmute, to avoid
// holding a few map entries that nobody is looking at.
func (s *Store) active(m map[string]time.Time, key string) (bool, time.Time) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	until, ok := m[key]
	if !ok {
		return false, time.Time{}
	}
	if !until.IsZero() && !time.Now().Before(until) {
		return false, time.Time{}
	}
	return true, until
}

// Sweep drops expired entries and reports how many it removed. Nothing
// depends on it running — it exists so a long-lived server does not
// accumulate the entries lazy expiry leaves behind.
func (s *Store) Sweep() int {
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	removed := 0
	for _, m := range []map[string]time.Time{s.mutes, s.bans} {
		for key, until := range m {
			if !until.IsZero() && !now.Before(until) {
				delete(m, key)
				removed++
			}
		}
	}
	return removed
}

// Counts reports the number of live mutes and bans, for logging and tests.
func (s *Store) Counts() (mutes, bans int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.mutes), len(s.bans)
}
