package proto

import (
	"encoding/json/v2"
	"fmt"
)

// JSON is the text wire format. It is the fallback every client can speak,
// and the one you can type by hand into a console.
//
// It is encoding/json/v2, which is why the whole module builds with
// GOEXPERIMENT=jsonv2 (see the Makefile). What that buys, on the frames in
// bench_test.go: decoding a command is about twice as fast and allocates
// half of what it did, which matters because it happens once per frame
// anybody sends.
//
// Three behaviour changes come with it, all of them in the direction a wire
// protocol wants:
//
//   - Invalid UTF-8 in a string is refused rather than quietly replaced
//     with U+FFFD. Unreachable here — coder/websocket validates TEXT frames
//     — but the filter chain no longer has to be the only thing standing
//     between a bad byte and a client.
//   - Duplicate object members are refused rather than last-one-wins.
//   - HTML characters are no longer escaped, so a message containing "<"
//     is spelled with a "<" instead of <. Same document, fewer bytes.
//
// Unmarshaling stays lenient about unknown members, which is the one thing
// that must not change: a client sending a field this server does not know
// about is a client written against a newer one.
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
