package proto

import (
	"errors"
	"fmt"
)

// Codec implementations must be safe for concurrent use: connections share one.
type Codec interface {
	Name() string

	Binary() bool

	Encode(payload Outbound) ([]byte, error)

	Decode(frame []byte, cmd *Command) error
}

var ErrUnsupported = errors.New("proto: unsupported codec")

var ErrNoVerb = errors.New("proto: outbound frame has no verb")

const (
	NameJSON    = "chat.json"
	NameMsgPack = "chat.msgpack"
)

// Codecs is in preference order: MessagePack is smaller and cheaper to parse.
func Codecs() []Codec { return []Codec{MsgPack{}, JSON{}} }

func Subprotocols() []string {
	codecs := Codecs()
	names := make([]string, len(codecs))
	for i, c := range codecs {
		names[i] = c.Name()
	}
	return names
}

// Default is JSON: a client that negotiates nothing is being written by hand.
func Default() Codec { return JSON{} }

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

func checkVerb(payload Outbound) error {
	if payload == nil || payload.frameVerb() == "" {
		return ErrNoVerb
	}
	return nil
}
