// Package moderation holds who is muted and who is banned, and where.
//
// It is state and expiry and nothing else: it does not decide who may
// moderate (that is hook.Authorizer), it does not persist anything (that is
// hook.Recorder), and it does not know what a connection is. It answers one
// question — is this key silenced in this channel right now — and it
// answers it on the hot path, so it answers it under a read lock.
//
// Every entry has a SCOPE: a channel name, or Global for one that applies
// everywhere. A lookup checks both, so a global mute silences somebody in
// every channel without having to be written into each of them, and lifting
// a channel mute does not lift a global one.
package moderation

import (
	"sync"
	"time"
)

// Global is the scope of an action that applies everywhere rather than in
// one channel.
const Global = ""

// Store is the set of active mutes and bans, keyed by scope and by
// hook.Identity.Key. The zero value is ready to use and has nobody in it.
type Store struct {
	mu    sync.RWMutex
	mutes map[entry]time.Time
	bans  map[entry]time.Time
}

// entry is what an action is filed under. A struct key rather than a joined
// string, so a channel name containing whatever a client typed cannot
// collide with another scope.
type entry struct {
	scope string
	key   string
}

// New returns an empty Store.
func New() *Store { return &Store{} }

// Mute silences key in a scope until the given time. A zero time never
// expires; a scope of Global silences them everywhere.
func (s *Store) Mute(scope, key string, until time.Time) {
	s.set(&s.mutes, entry{scope, key}, until)
}

// Ban bars key from a scope until the given time.
func (s *Store) Ban(scope, key string, until time.Time) {
	s.set(&s.bans, entry{scope, key}, until)
}

// Unmute lifts a mute in one scope, reporting whether there was one.
// Lifting a channel mute does not lift a global one: they were separate
// decisions and undoing one should not quietly undo the other.
func (s *Store) Unmute(scope, key string) bool {
	return s.clear(&s.mutes, entry{scope, key})
}

// Unban lifts a ban in one scope, reporting whether there was one.
func (s *Store) Unban(scope, key string) bool {
	return s.clear(&s.bans, entry{scope, key})
}

// Muted reports whether key is silenced in a scope, and until when. A
// global mute counts in every scope.
func (s *Store) Muted(scope, key string) (bool, time.Time) {
	return s.active(&s.mutes, scope, key)
}

// Banned reports whether key is barred from a scope, and until when. A
// global ban counts in every scope.
func (s *Store) Banned(scope, key string) (bool, time.Time) {
	return s.active(&s.bans, scope, key)
}

func (s *Store) set(m *map[entry]time.Time, e entry, until time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if *m == nil {
		*m = make(map[entry]time.Time)
	}
	(*m)[e] = until
}

func (s *Store) clear(m *map[entry]time.Time, e entry) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := (*m)[e]; !ok {
		return false
	}
	delete(*m, e)
	return true
}

// active checks the scope and then the global entry, and reports the first
// that is live.
//
// Expiry is lazy: an entry that has run out reads as absent and is left for
// the next sweep to remove. Lazily on purpose — the alternative is a timer
// per mute, which is a goroutine per mute and a cancellation problem per
// unmute, to avoid holding a few map entries nobody is looking at.
func (s *Store) active(m *map[entry]time.Time, scope, key string) (bool, time.Time) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	scopes := []string{scope}
	if scope != Global {
		scopes = append(scopes, Global)
	}
	for _, sc := range scopes {
		until, ok := (*m)[entry{sc, key}]
		if !ok {
			continue
		}
		if !until.IsZero() && !time.Now().Before(until) {
			continue // expired; a global entry behind it may still stand
		}
		return true, until
	}
	return false, time.Time{}
}

// Sweep drops expired entries and reports how many it removed. Nothing
// depends on it running — every read already treats an expired entry as
// absent — so it only exists to stop a long-lived server accumulating them.
func (s *Store) Sweep() int {
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	removed := 0
	for _, m := range []map[entry]time.Time{s.mutes, s.bans} {
		for e, until := range m {
			if !until.IsZero() && !now.Before(until) {
				delete(m, e)
				removed++
			}
		}
	}
	return removed
}

// Counts reports the number of live mutes and bans across every scope, for
// logging and metrics.
func (s *Store) Counts() (mutes, bans int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.mutes), len(s.bans)
}
