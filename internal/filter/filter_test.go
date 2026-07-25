package filter

import (
	"context"
	"strings"
	"testing"

	"github.com/sztanpet/ws-chat/internal/hook"
)

func allow(t *testing.T, f hook.Filter, data string) (bool, string) {
	t.Helper()
	return f.Allow(context.Background(), hook.Identity{Nick: "someone"}, data)
}

func TestUTF8(t *testing.T) {
	valid := []string{
		"",
		"plain ascii",
		"ünïcödé is fine",
		"日本語も",
		"emoji 🎉 too",
		"�", // the replacement character itself is valid text
	}
	for _, s := range valid {
		if ok, reason := allow(t, UTF8{}, s); !ok {
			t.Errorf("UTF8 refused %q as %s", s, reason)
		}
	}

	// Byte sequences no encoder should produce, which is exactly why a
	// binary-framed client can send them.
	invalid := []string{
		"\xff",
		"\xc3\x28",         // bad continuation
		"lead \xe2\x82",    // truncated three-byte sequence
		"\xed\xa0\x80",     // surrogate half
		"ok then \xf0\x28", // valid prefix, invalid tail
	}
	for _, s := range invalid {
		ok, reason := allow(t, UTF8{}, s)
		if ok {
			t.Errorf("UTF8 accepted %q", s)
		} else if reason != ReasonInvalidUTF8 {
			t.Errorf("UTF8 refused %q as %q, want %q", s, reason, ReasonInvalidUTF8)
		}
	}
}

func TestZalgo(t *testing.T) {
	const combining = "́" // combining acute accent

	tests := []struct {
		name string
		data string
		want bool
	}{
		{"plain", "hello", true},
		{"one mark", "e" + combining, true},
		{"five marks", "e" + strings.Repeat(combining, 5), true},
		{"six marks", "e" + strings.Repeat(combining, 6), false},
		{"far too many", "e" + strings.Repeat(combining, 200), false},

		// The count is per run, so a long message that uses marks normally
		// throughout is fine.
		{"many separate marks", strings.Repeat("e"+combining+" ", 100), true},
		{"two short runs", "e" + strings.Repeat(combining, 3) + "a" + strings.Repeat(combining, 3), true},

		// Real text that stacks marks legitimately.
		{"vietnamese", "Tiếng Việt", true},
		{"hebrew with niqqud", "בְּרֵאשִׁית", true},
		{"thai", "ภาษาไทย", true},
		{"devanagari", "हिन्दी", true},

		// A run at the very end still counts.
		{"trailing run", "ok" + strings.Repeat(combining, 9), false},
	}

	z := Zalgo{Max: 5}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ok, reason := allow(t, z, tt.data)
			if ok != tt.want {
				t.Fatalf("Allow(%q) = %v, want %v (reason %q)", tt.data, ok, tt.want, reason)
			}
			if !ok && reason != ReasonZalgo {
				t.Fatalf("reason = %q, want %q", reason, ReasonZalgo)
			}
		})
	}
}

func TestZalgoDisabled(t *testing.T) {
	for _, max := range []int{0, -1} {
		z := Zalgo{Max: max}
		if ok, _ := allow(t, z, "e"+strings.Repeat("́", 500)); !ok {
			t.Errorf("Zalgo{Max: %d} filtered something with the filter off", max)
		}
	}
}

// The boundary is "more than Max", so exactly Max is allowed.
func TestZalgoBoundary(t *testing.T) {
	for max := 1; max <= 8; max++ {
		z := Zalgo{Max: max}
		at := "e" + strings.Repeat("́", max)
		over := "e" + strings.Repeat("́", max+1)

		if ok, _ := allow(t, z, at); !ok {
			t.Errorf("Max=%d refused exactly %d marks", max, max)
		}
		if ok, _ := allow(t, z, over); ok {
			t.Errorf("Max=%d allowed %d marks", max, max+1)
		}
	}
}

type refuse struct{ reason string }

func (r refuse) Allow(context.Context, hook.Identity, string) (bool, string) {
	return false, r.reason
}

type count struct{ n *int }

func (c count) Allow(context.Context, hook.Identity, string) (bool, string) {
	*c.n++
	return true, ""
}

func TestChainStopsAtFirstRefusal(t *testing.T) {
	after := 0
	c := Chain(refuse{"first"}, count{&after})

	ok, reason := allow(t, c, "anything")
	if ok {
		t.Fatal("the chain allowed a message its first filter refused")
	}
	if reason != "first" {
		t.Fatalf("reason = %q, want the first filter's %q", reason, "first")
	}
	if after != 0 {
		t.Fatalf("the filter after the refusal ran %d times", after)
	}
}

func TestChainRunsAllWhenAllowed(t *testing.T) {
	a, b := 0, 0
	c := Chain(count{&a}, count{&b})

	if ok, _ := allow(t, c, "fine"); !ok {
		t.Fatal("the chain refused a message every filter allowed")
	}
	if a != 1 || b != 1 {
		t.Fatalf("filters ran %d and %d times, want 1 each", a, b)
	}
}

// A chain is built with an optional hook on the end, so nils have to be
// skipped rather than panic.
func TestChainSkipsNils(t *testing.T) {
	n := 0
	c := Chain(nil, count{&n}, nil)
	if ok, _ := allow(t, c, "fine"); !ok {
		t.Fatal("the chain refused")
	}
	if n != 1 {
		t.Fatalf("the real filter ran %d times, want 1", n)
	}
}

// An empty chain is nil, so the caller can skip the call entirely rather
// than paying for a loop over nothing on every message.
func TestEmptyChainIsNil(t *testing.T) {
	if c := Chain(); c != nil {
		t.Errorf("Chain() = %v, want nil", c)
	}
	if c := Chain(nil, nil); c != nil {
		t.Errorf("Chain(nil, nil) = %v, want nil", c)
	}
}

// The order the server installs them in: text validity before anything
// that assumes valid text.
func TestDefaultOrder(t *testing.T) {
	c := Chain(UTF8{}, Zalgo{Max: 5})

	ok, reason := allow(t, c, "\xff"+strings.Repeat("́", 50))
	if ok {
		t.Fatal("the chain allowed invalid UTF-8")
	}
	if reason != ReasonInvalidUTF8 {
		t.Fatalf("reason = %q, want %q — UTF8 must run first", reason, ReasonInvalidUTF8)
	}
}
