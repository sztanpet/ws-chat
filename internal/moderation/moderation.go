// Package moderation is who is muted and banned, and where: state, not policy.
package moderation

import (
	"sync"
	"time"
)

const Global = ""

// Store is usable as its zero value.
type Store struct {
	mu    sync.RWMutex
	mutes map[entry]time.Time
	bans  map[entry]time.Time
}

// A struct, not a joined string: a client-chosen channel name cannot collide.
type entry struct {
	scope string
	key   string
}

func New() *Store { return &Store{} }

func (s *Store) Mute(scope, key string, until time.Time) {
	s.set(&s.mutes, entry{scope, key}, until)
}

func (s *Store) Ban(scope, key string, until time.Time) {
	s.set(&s.bans, entry{scope, key}, until)
}

// Unmute lifts only this scope's mute: a global one was a separate decision.
func (s *Store) Unmute(scope, key string) bool {
	return s.clear(&s.mutes, entry{scope, key})
}

func (s *Store) Unban(scope, key string) bool {
	return s.clear(&s.bans, entry{scope, key})
}

func (s *Store) Muted(scope, key string) (bool, time.Time) {
	return s.active(&s.mutes, scope, key)
}

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

// Expiry is lazy: a timer per mute would be a goroutine each, cancelled on unmute.
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

// Sweep is only housekeeping: every read already ignores what it removes.
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

func (s *Store) Counts() (mutes, bans int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.mutes), len(s.bans)
}
