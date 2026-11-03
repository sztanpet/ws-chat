package proto

import (
	"encoding/json/v2"
	"fmt"
)

// JSON ignores unknown members: they are from clients newer than the server.
type JSON struct{}

func (JSON) Name() string { return NameJSON }

func (JSON) Binary() bool { return false }

func (JSON) Encode(payload Outbound) ([]byte, error) {
	if err := checkVerb(payload); err != nil {
		return nil, err
	}
	return json.Marshal(payload)
}

func (JSON) Decode(frame []byte, cmd *Command) error {
	if err := json.Unmarshal(frame, cmd); err != nil {
		return fmt.Errorf("%w: %s", ErrMalformed, err)
	}
	if cmd.Verb == "" {
		return fmt.Errorf("%w: no verb", ErrMalformed)
	}
	return nil
}
