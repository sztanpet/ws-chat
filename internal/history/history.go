// Package history implements the default replay window: the last N
// messages of a channel, kept in memory.
//
// This is what a server gets when it installs no History hook, and it is
// what the server did before the hook existed. Anything more — history that
// survives a restart, history older than the window, history a client can
// page back through — is a hook.History implementation and does not belong
// here.
package history

import (
	"context"
	"slices"
	"sync"

	"github.com/sztanpet/ws-chat/internal/hook"
)

// Memory keeps the last max messages of each channel.
//
// A non-positive max keeps nothing, which is how the backlog is switched
// off: Append does nothing and Recent returns nothing, rather than every
// caller having to check first.
type Memory struct {
	max int

	mu       sync.RWMutex
	channels map[string][]hook.Message
}

// NewMemory returns a window holding max messages per channel.
func NewMemory(max int) *Memory {
	return &Memory{max: max, channels: make(map[string][]hook.Message)}
}

// Append adds a message, dropping the oldest if the window is full.
//
// It runs under the lock that orders the fan-out, so it does the least it
// can get away with: a shift and an append, and no allocation at all once
// the window has reached its size.
func (m *Memory) Append(ctx context.Context, channel string, msg hook.Message) {
	if m.max < 1 {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	window := m.channels[channel]
	if len(window) == m.max {
		copy(window, window[1:])
		window = window[:len(window)-1]
	}
	m.channels[channel] = append(window, msg)
}

// Recent returns up to n messages in the order they were said.
func (m *Memory) Recent(ctx context.Context, channel string, n int) ([]hook.Message, error) {
	if n < 1 {
		return nil, nil
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	window := m.channels[channel]
	if len(window) == 0 {
		return nil, nil
	}
	if n > len(window) {
		n = len(window)
	}
	// Cloned: the caller gets a snapshot it can hold on to while the window
	// moves underneath it.
	return slices.Clone(window[len(window)-n:]), nil
}

// Len reports how many messages are held for a channel.
func (m *Memory) Len(channel string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.channels[channel])
}
