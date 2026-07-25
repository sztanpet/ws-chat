package proto

import (
	"errors"
	"testing"
)

// Both codecs answer to the same tests. That is the entire point of the
// interface: if one of them needs its own test to pass, they are not
// interchangeable and a client cannot rely on choosing either.
func eachCodec(t *testing.T, fn func(t *testing.T, c Codec)) {
	t.Helper()
	for _, c := range Codecs() {
		t.Run(c.Name(), func(t *testing.T) { fn(t, c) })
	}
}

func TestCodecRoundTrip(t *testing.T) {
	eachCodec(t, func(t *testing.T, c Codec) {
		want := Msg{ID: 42, Nick: "someone", Data: "hello world", Timestamp: 1700000000000}

		frame, err := c.Encode(VerbMsg, want)
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}

		verb, payload, err := c.Decode(frame)
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if verb != VerbMsg {
			t.Fatalf("verb = %q, want %q", verb, VerbMsg)
		}

		var got Msg
		if err := c.Unmarshal(payload, &got); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		if got != want {
			t.Fatalf("round trip gave %+v, want %+v", got, want)
		}
	})
}

func TestCodecBareVerb(t *testing.T) {
	eachCodec(t, func(t *testing.T, c Codec) {
		frame, err := c.Encode(VerbPong, nil)
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		verb, payload, err := c.Decode(frame)
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if verb != VerbPong {
			t.Fatalf("verb = %q, want %q", verb, VerbPong)
		}
		if len(payload) != 0 {
			t.Fatalf("payload = %q, want none", payload)
		}
	})
}

func TestCodecAwkwardBodies(t *testing.T) {
	eachCodec(t, func(t *testing.T, c Codec) {
		for _, data := range []string{"a b", "line\nbreak", `has "quotes"`, "MSG {\"nested\":1}", "  ", "ünïcödé 🎉"} {
			frame, err := c.Encode(VerbMsg, Msg{Data: data})
			if err != nil {
				t.Fatalf("Encode(%q): %v", data, err)
			}
			_, payload, err := c.Decode(frame)
			if err != nil {
				t.Fatalf("Decode(%q): %v", data, err)
			}
			var got Msg
			if err := c.Unmarshal(payload, &got); err != nil {
				t.Fatalf("Unmarshal(%q): %v", data, err)
			}
			if got.Data != data {
				t.Fatalf("data survived as %q, want %q", got.Data, data)
			}
		}
	})
}

func TestCodecRejectsGarbage(t *testing.T) {
	eachCodec(t, func(t *testing.T, c Codec) {
		for _, frame := range [][]byte{nil, {}, []byte("not a frame at all"), {0xff, 0xff, 0xff}} {
			if verb, _, err := c.Decode(frame); err == nil {
				t.Errorf("Decode(%q) = %q with no error", frame, verb)
			}
		}
	})
}

func TestCodecRejectsBadVerb(t *testing.T) {
	eachCodec(t, func(t *testing.T, c Codec) {
		if _, err := c.Encode("lower", nil); err == nil {
			t.Error("Encode accepted a lowercase verb")
		}
		if _, err := c.Encode("has space", nil); err == nil {
			t.Error("Encode accepted a verb with a space")
		}
	})
}

// A frame from one codec must not be readable as the other's, or a
// mis-negotiated connection would silently half-work.
func TestCodecsDoNotOverlap(t *testing.T) {
	jsonFrame, err := (JSON{}).Encode(VerbMsg, Msg{Data: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := (MsgPack{}).Decode(jsonFrame); err == nil {
		t.Error("the msgpack codec accepted a JSON frame")
	}

	packed, err := (MsgPack{}).Encode(VerbMsg, Msg{Data: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := (JSON{}).Decode(packed); err == nil {
		t.Error("the JSON codec accepted a msgpack frame")
	}
}

func TestByName(t *testing.T) {
	if c, err := ByName(""); err != nil || c.Name() != NameJSON {
		t.Errorf("ByName(empty) = %v, %v; want the JSON default", c, err)
	}
	for _, want := range Codecs() {
		got, err := ByName(want.Name())
		if err != nil {
			t.Fatalf("ByName(%q): %v", want.Name(), err)
		}
		if got.Name() != want.Name() {
			t.Errorf("ByName(%q) returned %q", want.Name(), got.Name())
		}
	}
	if _, err := ByName("chat.xml"); !errors.Is(err, ErrUnsupported) {
		t.Errorf("ByName of an unknown codec = %v, want ErrUnsupported", err)
	}
}

// MessagePack is offered first, and it had better actually be smaller.
func TestMsgPackIsSmaller(t *testing.T) {
	msg := Msg{ID: 4294967295, Nick: "someone", Data: "hello world", Timestamp: 1700000000000}

	jsonFrame, err := (JSON{}).Encode(VerbMsg, msg)
	if err != nil {
		t.Fatal(err)
	}
	packed, err := (MsgPack{}).Encode(VerbMsg, msg)
	if err != nil {
		t.Fatal(err)
	}
	if len(packed) >= len(jsonFrame) {
		t.Fatalf("msgpack frame is %d bytes, json is %d", len(packed), len(jsonFrame))
	}
	t.Logf("msgpack %d bytes, json %d bytes", len(packed), len(jsonFrame))
}
