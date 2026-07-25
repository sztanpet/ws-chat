package proto

import (
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

// MsgPack is the binary wire format: the same document as the text one, at
// a fraction of the bytes and none of the number parsing.
type MsgPack struct{}

func (MsgPack) Name() string { return NameMsgPack }

func (MsgPack) Binary() bool { return true }

func (MsgPack) Encode(payload Outbound) ([]byte, error) {
	if err := checkVerb(payload); err != nil {
		return nil, err
	}
	return msgpack.Marshal(payload)
}

func (MsgPack) Decode(frame []byte, cmd *Command) error {
	if err := msgpack.Unmarshal(frame, cmd); err != nil {
		return fmt.Errorf("%w: %s", ErrMalformed, err)
	}
	if cmd.Verb == "" {
		return fmt.Errorf("%w: no verb", ErrMalformed)
	}
	return nil
}
