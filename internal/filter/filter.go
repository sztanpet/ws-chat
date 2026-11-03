// Package filter is the built-in message filters: pure functions on the hot path.
package filter

import (
	"context"
	"unicode"
	"unicode/utf8"

	"github.com/sztanpet/ws-chat/internal/hook"
)

// Sent to clients as ERR codes, so they are stable.
const (
	ReasonInvalidUTF8 = "invalidutf8"
	ReasonZalgo       = "zalgo"
)

// Chain skips nils, so an optional hook can go on the end unchecked.
func Chain(filters ...hook.Filter) hook.Filter {
	kept := make(chain, 0, len(filters))
	for _, f := range filters {
		if f != nil {
			kept = append(kept, f)
		}
	}
	if len(kept) == 0 {
		return nil // the caller skips a nil filter entirely
	}
	return kept
}

type chain []hook.Filter

func (c chain) Allow(ctx context.Context, from hook.Identity, data string) (bool, string) {
	for _, f := range c {
		if ok, reason := f.Allow(ctx, from, data); !ok {
			return false, reason
		}
	}
	return true, ""
}

// UTF8 is needed: MessagePack rides binary frames, which nothing validates.
type UTF8 struct{}

func (UTF8) Allow(ctx context.Context, from hook.Identity, data string) (bool, string) {
	if !utf8.ValidString(data) {
		return false, ReasonInvalidUTF8
	}
	return true, ""
}

// Zalgo counts marks in a run, as some scripts stack up to four legitimately.
type Zalgo struct {
	Max int
}

func (z Zalgo) Allow(ctx context.Context, from hook.Identity, data string) (bool, string) {
	if z.Max < 1 {
		return true, ""
	}

	run := 0
	for _, r := range data {
		if !unicode.Is(unicode.M, r) {
			run = 0
			continue
		}
		run++
		if run > z.Max {
			return false, ReasonZalgo
		}
	}
	return true, ""
}
