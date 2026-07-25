package proto

import (
	"errors"
	"fmt"
)

// Codec is the wire format. Everything above it deals in verbs and Go
// structs; everything below it deals in bytes, and the two do not know
// about each other.
//
// This exists because the framing is not the protocol. What the verbs mean,
// who may send them and what the server does about it is the protocol; how
// a frame is spelled on the wire is a detail a client should be able to
// choose. A browser wants JSON it can read in devtools. A bot moving a lot
// of traffic wants MessagePack. Neither should force the other.
//
// Because the verb lives inside the document, a codec is now just its
// encoder: Marshal one way, Unmarshal the other, and an inbound frame costs
// exactly one pass over the bytes.
//
// Implementations must be safe for concurrent use — one codec serves every
// connection that negotiated it.
type Codec interface {
	// Name is the WebSocket subprotocol that selects this codec.
	Name() string

	// Binary reports whether frames are binary WebSocket messages rather
	// than text. The server uses it to pick the message type it writes and
	// the one it will accept.
	Binary() bool

	// Encode builds a frame. The payload carries its own verb, which is
	// what the Outbound interface is for.
	Encode(payload Outbound) ([]byte, error)

	// Decode parses an inbound frame in a single pass.
	Decode(frame []byte, cmd *Command) error
}

// ErrUnsupported is returned for a subprotocol nothing implements.
var ErrUnsupported = errors.New("proto: unsupported codec")

// ErrNoVerb is returned for an outbound payload with no verb set. It cannot
// happen through the New* constructors and is a programming error if it
// does.
var ErrNoVerb = errors.New("proto: outbound frame has no verb")

// The subprotocol names clients negotiate with.
const (
	NameJSON    = "chat.json"
	NameMsgPack = "chat.msgpack"
)

// Codecs are the supported wire formats, in the order the server offers
// them. MessagePack is first because it is the better one to use — smaller
// frames and no number-to-string round trip — and JSON is the fallback that
// anything can speak, including a browser console and curl.
func Codecs() []Codec { return []Codec{MsgPack{}, JSON{}} }

// Names returns the subprotocols to advertise, in preference order.
func Names() []string {
	codecs := Codecs()
	names := make([]string, len(codecs))
	for i, c := range codecs {
		names[i] = c.Name()
	}
	return names
}

// Default is what a client that negotiates nothing gets. JSON, because a
// client that did not ask is a client that is being written by hand.
func Default() Codec { return JSON{} }

// ByName returns the codec a subprotocol selects. An empty name is the
// default.
func ByName(name string) (Codec, error) {
	if name == "" {
		return Default(), nil
	}
	for _, c := range Codecs() {
		if c.Name() == name {
			return c, nil
		}
	}
	return nil, fmt.Errorf("%w: %q", ErrUnsupported, name)
}

// checkVerb guards the one mistake the Outbound interface exists to catch.
func checkVerb(payload Outbound) error {
	if payload == nil || payload.frameVerb() == "" {
		return ErrNoVerb
	}
	return nil
}
