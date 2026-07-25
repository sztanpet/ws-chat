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
	VerbMsg   = "MSG"     // both directions: a chat message
	VerbPriv  = "PRIVMSG" // both directions: a message to one person
	VerbReady = "READY"   // server -> client, once, on connect
	VerbPing  = "PING"    // client -> server
	VerbPong  = "PONG"    // server -> client
	VerbErr   = "ERR"     // server -> client
)

// Error descriptions. These are stable machine-readable codes, not prose:
// a client should be able to switch on them without parsing English.
const (
	ErrProtocol = "protocol"      // unparseable frame or payload
	ErrUnknown  = "unknown"       // verb the server does not implement
	ErrEmpty    = "empty"         // message with no content
	ErrTooLong  = "toolong"       // message body over the configured limit
	ErrBinary   = "binary"        // binary frame; this protocol is text only
	ErrNoSuch   = "nosuchnick"    // private message to nobody
	ErrBacklog  = "recipientbusy" // recipient is not draining its queue
	ErrSelf     = "self"          // private message to yourself
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
	ID        uint64 `json:"id"`
	Nick      string `json:"nick"`
	Data      string `json:"data"`
	Timestamp int64  `json:"timestamp"` // unix milliseconds
}

// In is a chat message as a client sends it: the body and nothing else.
// Everything the client might claim about itself is assigned by the server.
type In struct {
	Data string `json:"data"`
}

// Priv is a private message as the server sends it. Both parties get one:
// the recipient's copy names the sender, and the sender's own echo names the
// recipient and sets Sent, so a client can render its own outgoing messages
// from the same frame it renders incoming ones.
type Priv struct {
	ID        uint64 `json:"id"`
	Nick      string `json:"nick"`
	Data      string `json:"data"`
	Timestamp int64  `json:"timestamp"`
	Sent      bool   `json:"sent,omitempty"`
}

// InPriv is a private message as a client sends it: who it is for, and the
// body.
type InPriv struct {
	Nick string `json:"nick"`
	Data string `json:"data"`
}

// Ready is the first frame the server sends. It tells the client the name
// it was given, and — more importantly — that it is subscribed: a client
// that starts talking before this arrives can miss the replies, because the
// WebSocket handshake completes before the server has finished wiring the
// connection up.
type Ready struct {
	Nick string `json:"nick"`
}

// Err is a refusal. Description is one of the codes above.
type Err struct {
	Description string `json:"description"`
}

// Split separates a frame into its verb and its raw JSON payload. The
// payload is nil for a bare verb; it is not parsed here, only carried.
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

// Format builds a frame. A nil payload produces the bare verb.
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
