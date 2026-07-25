// Package filter implements the message filters the server runs in front
// of every message, and the chain that runs them.
//
// A filter is a hook.Filter, so there is nothing special about the ones in
// here: the built-in filters and a filter supplied by a deployment are the
// same kind of thing and run through the same chain. What makes these
// built in is only that the server installs them by default, because they
// are protocol hygiene rather than policy — a message that is not valid
// text is a message that breaks clients, whatever anybody's moderation
// opinions are.
//
// Every filter runs on the sender's read pump, in front of every message,
// so all of them are pure functions over the message text. None of them
// may allocate much, block, or look anything up.
package filter

import (
	"context"
	"unicode"
	"unicode/utf8"

	"github.com/sztanpet/ws-chat/internal/hook"
)

// The refusal codes these filters produce. They end up as the ERR
// description a client is sent, so they are stable machine-readable
// tokens.
const (
	ReasonInvalidUTF8 = "invalidutf8"
	ReasonZalgo       = "zalgo"
)

// Chain runs filters in order and stops at the first refusal, whose reason
// is the one the client is told. Nil filters are skipped, so a chain can be
// built with an optional hook on the end without the caller checking.
//
// Order is not an accident: put the cheap, universal checks first, so a
// message that is not even valid text never reaches a filter that was
// written assuming it was.
func Chain(filters ...hook.Filter) hook.Filter {
	kept := make(chain, 0, len(filters))
	for _, f := range filters {
		if f != nil {
			kept = append(kept, f)
		}
	}
	if len(kept) == 0 {
		return nil // nothing to run; the caller skips the call entirely
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

// UTF8 refuses text that is not valid UTF-8.
//
// This is not redundant with the WebSocket layer, which is the tempting
// assumption. RFC 6455 requires TEXT frames to be valid UTF-8 and
// coder/websocket enforces it, and a JSON client's invalid bytes would
// anyway be turned into U+FFFD by encoding/json rather than rejected. But
// a MessagePack client sends BINARY frames, nothing validates them, and a
// msgpack string is just bytes — so invalid UTF-8 reaches the server
// intact and would be fanned out to every other client, including the JSON
// ones, whose frames would then be invalid on the wire.
type UTF8 struct{}

func (UTF8) Allow(ctx context.Context, from hook.Identity, data string) (bool, string) {
	if !utf8.ValidString(data) {
		return false, ReasonInvalidUTF8
	}
	return true, ""
}

// Zalgo refuses text that stacks more than Max combining marks on one
// character — the "h̸̡̪̯ͨ͊̽̅̾̎" trick, which is not a message so much as a way to
// take up somebody else's screen.
//
// The count is of CONSECUTIVE marks, because that is what stacking is: a
// base character followed by a run of Unicode Mark-category runes, all
// rendering on top of it. Counting marks in the message as a whole would
// refuse a long sentence in any script that uses them normally.
//
// Max is deliberately generous for that reason. Devanagari, Thai, Hebrew
// with niqqud and cantillation, and Vietnamese all legitimately stack
// marks; Hebrew can reach three or four. Five is well clear of anything
// real and well under anything decorative. A non-positive Max disables the
// filter.
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
