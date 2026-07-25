// Package proto implements the wire format: one command per frame, in the
// form
//
//	VERB {"json":"payload"}
//
// An uppercase verb, a single space, and a JSON object. A frame with no
// payload is the bare verb. There is no batching and no trailing newline —
// one frame is one command, which is what makes a client's parser three
// lines long.
package proto

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Verbs the server understands from a client, and the ones it sends.
const (
	VerbMsg     = "MSG"     // both directions: a chat message
	VerbPriv    = "PRIVMSG" // both directions: a message to one person
	VerbReady   = "READY"   // server -> client, once, on connect
	VerbBacklog = "BACKLOG" // server -> client, once, after READY
	VerbPing    = "PING"    // client -> server
	VerbPong    = "PONG"    // server -> client
	VerbErr     = "ERR"     // server -> client

	// Moderation. The four commands come from a client with the standing
	// to use them; the server answers all of them with one MOD frame to
	// the whole channel, so a client needs one handler rather than four.
	VerbMute   = "MUTE"
	VerbUnmute = "UNMUTE"
	VerbBan    = "BAN"
	VerbUnban  = "UNBAN"
	VerbMod    = "MOD"
)

// Moderation actions, as they appear in a MOD frame.
const (
	ActionMute   = "mute"
	ActionUnmute = "unmute"
	ActionBan    = "ban"
	ActionUnban  = "unban"
)

// Error descriptions. These are stable machine-readable codes, not prose:
// a client should be able to switch on them without parsing English.
const (
	ErrProtocol      = "protocol"         // unparseable frame or payload
	ErrUnknown       = "unknown"          // verb the server does not implement
	ErrEmpty         = "empty"            // message with no content
	ErrTooLong       = "toolong"          // message body over the configured limit
	ErrFraming       = "framing"          // wrong WebSocket message type for the negotiated codec
	ErrNoSuch        = "nosuchnick"       // private message to nobody
	ErrBacklog       = "recipientbusy"    // recipient is not draining its queue
	ErrSelf          = "self"             // private message to yourself
	ErrThrottled     = "throttled"        // you are sending too fast
	ErrChanThrottled = "channelthrottled" // the channel as a whole is

	// Moderation.
	ErrMuted       = "muted"       // you may not speak here at the moment
	ErrBanned      = "banned"      // you may not be here at all
	ErrForbidden   = "forbidden"   // that command is not yours to use
	ErrBadDuration = "badduration" // unparseable duration on a command
)

// ErrMalformed is returned by Split for anything that is not a well-formed
// frame.
var ErrMalformed = errors.New("proto: malformed frame")

// maxVerbLen bounds the verb so a garbage frame cannot make the server
// allocate or log something enormous.
const maxVerbLen = 16

// Msg is a chat message as the server sends it. Every server-originated
// message carries the channel-scoped id and the server's timestamp, so a
// client never has to trust its own clock or invent ordering.
type Msg struct {
	ID        uint64 `json:"id" msgpack:"id"`
	Nick      string `json:"nick" msgpack:"nick"`
	Data      string `json:"data" msgpack:"data"`
	Timestamp int64  `json:"timestamp" msgpack:"timestamp"` // unix milliseconds

	// Roles and Attrs are whatever the directory layer attached to the
	// sender, carried on every message they send.
	//
	// Repeating them per message rather than sending them once and having
	// clients keep a table is a deliberate trade: a client can render any
	// message on its own, with no state and no ordering problem when
	// somebody's roles change mid-conversation. It costs bytes on every
	// message, which is part of why MessagePack is the preferred codec.
	// Both are omitted entirely when empty, so a server with no directory
	// installed pays nothing.
	Roles []string          `json:"roles,omitempty" msgpack:"roles,omitempty"`
	Attrs map[string]string `json:"attrs,omitempty" msgpack:"attrs,omitempty"`
}

// In is a chat message as a client sends it: the body and nothing else.
// Everything the client might claim about itself is assigned by the server.
type In struct {
	Data string `json:"data" msgpack:"data"`
}

// Priv is a private message as the server sends it. Both parties get one:
// the recipient's copy names the sender, and the sender's own echo names the
// recipient and sets Sent, so a client can render its own outgoing messages
// from the same frame it renders incoming ones.
type Priv struct {
	ID        uint64 `json:"id" msgpack:"id"`
	Nick      string `json:"nick" msgpack:"nick"`
	Data      string `json:"data" msgpack:"data"`
	Timestamp int64  `json:"timestamp" msgpack:"timestamp"`
	Sent      bool   `json:"sent,omitempty" msgpack:"sent,omitempty"`

	// Roles and Attrs describe the other party, the same way Msg carries
	// them for the sender.
	Roles []string          `json:"roles,omitempty" msgpack:"roles,omitempty"`
	Attrs map[string]string `json:"attrs,omitempty" msgpack:"attrs,omitempty"`
}

// InPriv is a private message as a client sends it: who it is for, and the
// body.
type InPriv struct {
	Nick string `json:"nick" msgpack:"nick"`
	Data string `json:"data" msgpack:"data"`
}

// Ready is the first frame the server sends. It tells the client the name
// it was given, and — more importantly — that it is subscribed: a client
// that starts talking before this arrives can miss the replies, because the
// WebSocket handshake completes before the server has finished wiring the
// connection up.
type Ready struct {
	Nick string `json:"nick" msgpack:"nick"`
}

// Backlog is the recent history of the channel, sent once on connect so a
// client arrives with context instead of an empty window.
//
// It is one frame with an array rather than a burst of MSG frames. A client
// can then tell "here is what you missed" from "this just happened" without
// a per-message flag, and render the block in one pass.
//
// A client MUST ignore a live message whose id it already has from the
// backlog. The server subscribes a connection to the channel before it
// reads the history, so a message sent in between arrives twice: once in
// the backlog and once live. Doing it the other way round would lose that
// message instead, and a duplicate a client can drop by id is strictly
// better than a gap it cannot detect at all. Ids are monotonic, so the
// check is a comparison against the last id in the backlog.
type Backlog struct {
	Messages []Msg `json:"messages" msgpack:"messages"`
}

// InMod is a moderation command from a client.
type InMod struct {
	Nick string `json:"nick" msgpack:"nick"`

	// Duration is a Go duration string ("10m", "24h"). Empty means the
	// action does not expire.
	Duration string `json:"duration,omitempty" msgpack:"duration,omitempty"`

	Reason string `json:"reason,omitempty" msgpack:"reason,omitempty"`
}

// Mod is a moderation action as the server announces it to the channel.
//
// It goes to everybody, including the person it is about. Moderation that
// happens invisibly gets re-litigated in the channel by people guessing at
// what happened.
type Mod struct {
	ID        uint64 `json:"id" msgpack:"id"`
	Action    string `json:"action" msgpack:"action"`
	Nick      string `json:"nick" msgpack:"nick"`
	By        string `json:"by" msgpack:"by"`
	Timestamp int64  `json:"timestamp" msgpack:"timestamp"`

	// Until is when the action expires, in unix milliseconds. Zero means it
	// does not.
	Until  int64  `json:"until,omitempty" msgpack:"until,omitempty"`
	Reason string `json:"reason,omitempty" msgpack:"reason,omitempty"`
}

// Err is a refusal. Description is one of the codes above.
type Err struct {
	Description string `json:"description" msgpack:"description"`
}

// Split separates a text frame into its verb and its raw JSON payload. The
// payload is nil for a bare verb; it is not parsed here, only carried. It
// is the framing half of the JSON codec.
func Split(frame []byte) (verb string, payload []byte, err error) {
	if len(frame) == 0 {
		return "", nil, fmt.Errorf("%w: empty", ErrMalformed)
	}

	sp := -1
	for i, b := range frame {
		if b == ' ' {
			sp = i
			break
		}
	}

	if sp < 0 {
		verb = string(frame)
	} else {
		verb = string(frame[:sp])
		payload = frame[sp+1:]
	}

	if !validVerb(verb) {
		return "", nil, fmt.Errorf("%w: bad verb", ErrMalformed)
	}
	if sp >= 0 && len(payload) == 0 {
		return "", nil, fmt.Errorf("%w: trailing space with no payload", ErrMalformed)
	}
	return verb, payload, nil
}

func validVerb(v string) bool {
	if v == "" || len(v) > maxVerbLen {
		return false
	}
	for _, c := range []byte(v) {
		if c < 'A' || c > 'Z' {
			return false
		}
	}
	return true
}

// Format builds a text frame. A nil payload produces the bare verb. It is
// the framing half of the JSON codec.
func Format(verb string, payload any) ([]byte, error) {
	if !validVerb(verb) {
		return nil, fmt.Errorf("%w: bad verb %q", ErrMalformed, verb)
	}
	if payload == nil {
		return []byte(verb), nil
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	frame := make([]byte, 0, len(verb)+1+len(body))
	frame = append(frame, verb...)
	frame = append(frame, ' ')
	frame = append(frame, body...)
	return frame, nil
}
