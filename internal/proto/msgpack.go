package proto

import (
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

// MsgPack is the binary wire format. A frame is a two-element array,
// [verb, payload], which keeps the same shape as the text format — a verb
// and a body the framing layer does not look inside — while costing a
// fraction of the bytes and none of the number parsing.
//
// The payload stays a msgpack.RawMessage through decoding for exactly the
// reason the text codec keeps its payload as bytes: what a verb contains is
// not the framing layer's business.
type MsgPack struct{}

func (MsgPack) Name() string { return NameMsgPack }

func (MsgPack) Binary() bool { return true }

// msgpackNil is how the encoder writes a nil payload, and what a bare verb
// therefore decodes to.
var msgpackNil = []byte{0xc0}

func (MsgPack) Encode(verb string, payload any) ([]byte, error) {
	if !validVerb(verb) {
		return nil, fmt.Errorf("%w: bad verb %q", ErrMalformed, verb)
	}

	body := msgpack.RawMessage(msgpackNil)
	if payload != nil {
		encoded, err := msgpack.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = encoded
	}

	return msgpack.Marshal([]any{verb, body})
}

func (MsgPack) Decode(frame []byte) (string, []byte, error) {
	var parts []msgpack.RawMessage
	if err := msgpack.Unmarshal(frame, &parts); err != nil {
		return "", nil, fmt.Errorf("%w: %s", ErrMalformed, err)
	}
	if len(parts) != 2 {
		return "", nil, fmt.Errorf("%w: want a [verb, payload] pair, got %d elements", ErrMalformed, len(parts))
	}

	var verb string
	if err := msgpack.Unmarshal(parts[0], &verb); err != nil {
		return "", nil, fmt.Errorf("%w: verb is not a string", ErrMalformed)
	}
	if !validVerb(verb) {
		return "", nil, fmt.Errorf("%w: bad verb", ErrMalformed)
	}

	payload := []byte(parts[1])
	if len(payload) == 1 && payload[0] == msgpackNil[0] {
		payload = nil // a bare verb
	}
	return verb, payload, nil
}

func (MsgPack) Unmarshal(payload []byte, v any) error {
	if len(payload) == 0 {
		return fmt.Errorf("%w: no payload", ErrMalformed)
	}
	return msgpack.Unmarshal(payload, v)
}
