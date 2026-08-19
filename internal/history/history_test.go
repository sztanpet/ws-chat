package history

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sztanpet/ws-chat/internal/hook"
)

func msg(id uint64, data string) hook.Message {
	return hook.Message{
		ID:   id,
		From: hook.Identity{ID: "u1", Nick: "someone"},
		Data: data,
		At:   time.Unix(0, int64(id)),
	}
}

func fill(m *Memory, n int) {
	for i := range n {
		m.Append(context.Background(), "main", msg(uint64(i+1), fmt.Sprintf("m%d", i)))
	}
}

func TestEmpty(t *testing.T) {
	m := NewMemory(10)
	got, err := m.Recent(context.Background(), "main", 5)
	if err != nil {
		t.Fatalf("Recent: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("a fresh window returned %d messages", len(got))
	}
}

func TestOrderIsOldestFirst(t *testing.T) {
	m := NewMemory(10)
	fill(m, 3)

	got, err := m.Recent(context.Background(), "main", 10)
	if err != nil {
		t.Fatalf("Recent: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d messages, want 3", len(got))
	}
	for i, entry := range got {
		if want := fmt.Sprintf("m%d", i); entry.Data != want {
			t.Errorf("[%d] = %q, want %q — order is oldest first", i, entry.Data, want)
		}
	}
}

func TestWindowDropsOldest(t *testing.T) {
	m := NewMemory(3)
	fill(m, 6)

	if got := m.Len("main"); got != 3 {
		t.Fatalf("the window holds %d, want its max of 3", got)
	}

	got, _ := m.Recent(context.Background(), "main", 10)
	for i, entry := range got {
		if want := fmt.Sprintf("m%d", i+3); entry.Data != want {
			t.Errorf("[%d] = %q, want %q — the LAST three should survive", i, entry.Data, want)
		}
	}
}

func TestRecentCaps(t *testing.T) {
	m := NewMemory(10)
	fill(m, 6)

	got, _ := m.Recent(context.Background(), "main", 2)
	if len(got) != 2 {
		t.Fatalf("asked for 2, got %d", len(got))
	}
	// The most recent two, not the oldest two.
	if got[0].Data != "m4" || got[1].Data != "m5" {
		t.Fatalf("got %q and %q, want m4 and m5", got[0].Data, got[1].Data)
	}
}

func TestRecentOfNothing(t *testing.T) {
	m := NewMemory(10)
	fill(m, 3)

	for _, n := range []int{0, -1} {
		if got, _ := m.Recent(context.Background(), "main", n); len(got) != 0 {
			t.Errorf("Recent(n=%d) returned %d messages", n, len(got))
		}
	}
}

// A non-positive max is how the backlog is switched off.
func TestZeroMaxKeepsNothing(t *testing.T) {
	for _, max := range []int{0, -1} {
		m := NewMemory(max)
		fill(m, 5)

		if got := m.Len("main"); got != 0 {
			t.Errorf("NewMemory(%d) kept %d messages", max, got)
		}
		if got, _ := m.Recent(context.Background(), "main", 10); len(got) != 0 {
			t.Errorf("NewMemory(%d) replayed %d messages", max, len(got))
		}
	}
}

// Channels get their own windows, which is the whole reason the key is
// there before channels exist.
func TestChannelsAreSeparate(t *testing.T) {
	m := NewMemory(10)
	m.Append(context.Background(), "one", msg(1, "in one"))
	m.Append(context.Background(), "two", msg(2, "in two"))

	got, _ := m.Recent(context.Background(), "one", 10)
	if len(got) != 1 || got[0].Data != "in one" {
		t.Fatalf("channel one has %v", got)
	}
	got, _ = m.Recent(context.Background(), "two", 10)
	if len(got) != 1 || got[0].Data != "in two" {
		t.Fatalf("channel two has %v", got)
	}
	if got, _ := m.Recent(context.Background(), "three", 10); len(got) != 0 {
		t.Fatalf("an untouched channel has %v", got)
	}
}

// The caller holds the snapshot while the window keeps moving.
func TestRecentIsASnapshot(t *testing.T) {
	m := NewMemory(3)
	fill(m, 3)

	got, _ := m.Recent(context.Background(), "main", 3)
	fill(m, 3) // pushes every one of them out

	if got[0].Data != "m0" {
		t.Fatalf("the snapshot changed underneath the caller: [0] = %q", got[0].Data)
	}
}

func TestConcurrentUse(t *testing.T) {
	m := NewMemory(64)
	var wg sync.WaitGroup

	for w := range 8 {
		wg.Go(func() {
			for i := range 500 {
				m.Append(context.Background(), "main", msg(uint64(i), fmt.Sprintf("w%d-%d", w, i)))
				_, _ = m.Recent(context.Background(), "main", 16)
				m.Len("main")
			}
		})
	}
	wg.Wait()

	if got := m.Len("main"); got != 64 {
		t.Fatalf("the window holds %d, want its max of 64", got)
	}
}
