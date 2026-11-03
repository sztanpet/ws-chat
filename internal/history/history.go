// Package history is the default replay window: the last N messages, in memory.
package history

import (
	"context"
	"slices"
	"sync"

	"github.com/sztanpet/ws-chat/internal/hook"
)

// Memory with a non-positive max keeps nothing: that is the backlog switched off.
type Memory struct {
	max int

	mu       sync.RWMutex
	channels map[string][]hook.Message
}

func NewMemory(max int) *Memory {
	return &Memory{max: max, channels: make(map[string][]hook.Message)}
}

// Append runs under the fan-out lock: once full, it no longer allocates.
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
	// Cloned: the caller holds it while the window moves on.
	return slices.Clone(window[len(window)-n:]), nil
}

func (m *Memory) Len(channel string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.channels[channel])
}
