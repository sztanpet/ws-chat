package proto

import (
	"encoding/json"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// mustMarshal encodes a value the way a client speaking this codec would.
// It is not Codec.Encode, which only takes outbound frames.
func mustMarshal(t *testing.T, c Codec, v any) []byte {
	t.Helper()

	var frame []byte
	var err error
	if c.Binary() {
		frame, err = msgpack.Marshal(v)
	} else {
		frame, err = json.Marshal(v)
	}
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return frame
}

func mustUnmarshal(t *testing.T, c Codec, frame []byte, v any) {
	t.Helper()

	var err error
	if c.Binary() {
		err = msgpack.Unmarshal(frame, v)
	} else {
		err = json.Unmarshal(frame, v)
	}
	if err != nil {
		t.Fatalf("unmarshal %q: %v", frame, err)
	}
}
