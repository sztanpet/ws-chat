package proto

import (
	"encoding/json/v2"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// marshalAs encodes a value the way a client speaking this codec would. It
// is not Codec.Encode, which only takes outbound frames.
func marshalAs(c Codec, v any) ([]byte, error) {
	if c.Binary() {
		return msgpack.Marshal(v)
	}
	return json.Marshal(v)
}

func mustMarshal(t *testing.T, c Codec, v any) []byte {
	t.Helper()

	frame, err := marshalAs(c, v)
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
