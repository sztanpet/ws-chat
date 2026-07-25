package proto

import (
	"errors"
	"reflect"
	"strings"
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

// A command is one document, so it decodes in one pass — the verb and its
// arguments come out together.
func TestCommandRoundTrip(t *testing.T) {
	eachCodec(t, func(t *testing.T, c Codec) {
		for _, want := range []Command{
			{Verb: VerbMsg, Data: "hello world"},
			{Verb: VerbPriv, Nick: "someone", Data: "just for you"},
			{Verb: VerbPing},
			{Verb: VerbMute, Nick: "someone", Duration: "10m", Reason: "spam"},
			{Verb: VerbUnban, Nick: "someone"},
			{Verb: VerbJoin, Channel: "main"},
			{Verb: VerbPart, Channel: "main"},
			{Verb: VerbNames, Channel: "main"},
			{Verb: VerbMsg, Channel: "other", Data: "in another room"},
		} {
			// A client encodes the same struct the server decodes into.
			frame := mustMarshal(t, c, want)

			var got Command
			if err := c.Decode(frame, &got); err != nil {
				t.Fatalf("Decode(%q): %v", frame, err)
			}
			if got != want {
				t.Fatalf("round trip gave %+v, want %+v", got, want)
			}
		}
	})
}

// Every outbound type survives the trip and arrives naming itself.
func TestOutboundRoundTrip(t *testing.T) {
	eachCodec(t, func(t *testing.T, c Codec) {
		msg := NewMsg(Msg{
			ID: 42, Nick: "someone", Data: "hello", Timestamp: 1700000000000,
			Roles: []string{"mod"}, Attrs: map[string]string{"colour": "red"},
		})
		frame, err := c.Encode(msg)
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}

		var got Msg
		mustUnmarshal(t, c, frame, &got)
		if !reflect.DeepEqual(got, msg) {
			t.Fatalf("round trip gave %+v, want %+v", got, msg)
		}
		if got.Verb != VerbMsg {
			t.Fatalf("verb = %q, want %q", got.Verb, VerbMsg)
		}
	})
}

// Every outbound frame carries a verb, and the constructors are what makes
// that true.
func TestEveryOutboundNamesItself(t *testing.T) {
	outbound := []struct {
		want    string
		payload Outbound
	}{
		{VerbMsg, NewMsg(Msg{Data: "x"})},
		{VerbPriv, NewPriv(Priv{Data: "x"})},
		{VerbReady, NewReady("someone")},
		{VerbBacklog, NewBacklog("main", nil)},
		{VerbJoin, NewJoin(Join{Channel: "main", Nick: "someone"})},
		{VerbPart, NewPart("main", "someone")},
		{VerbNames, NewNames("main", []string{"someone"}, 1)},
		{VerbMod, NewMod(Mod{Action: ActionMute})},
		{VerbPong, NewPong()},
		{VerbErr, NewErr(ErrProtocol)},
	}

	eachCodec(t, func(t *testing.T, c Codec) {
		for _, tt := range outbound {
			frame, err := c.Encode(tt.payload)
			if err != nil {
				t.Fatalf("Encode(%T): %v", tt.payload, err)
			}
			var header struct {
				Verb string `json:"verb" msgpack:"verb"`
			}
			mustUnmarshal(t, c, frame, &header)
			if header.Verb != tt.want {
				t.Errorf("%T encoded verb %q, want %q", tt.payload, header.Verb, tt.want)
			}
		}
	})
}

// A payload built without a constructor has no verb, and encoding it is a
// programming error rather than a frame no client can dispatch.
func TestEncodeRefusesAVerblessFrame(t *testing.T) {
	eachCodec(t, func(t *testing.T, c Codec) {
		if _, err := c.Encode(Msg{Data: "no verb here"}); !errors.Is(err, ErrNoVerb) {
			t.Fatalf("Encode of a verbless frame = %v, want ErrNoVerb", err)
		}
	})
}

func TestDecodeRejectsGarbage(t *testing.T) {
	eachCodec(t, func(t *testing.T, c Codec) {
		for _, frame := range [][]byte{nil, {}, []byte("not a frame at all"), {0xff, 0xff, 0xff}} {
			var cmd Command
			if err := c.Decode(frame, &cmd); err == nil {
				t.Errorf("Decode(%q) succeeded with %+v", frame, cmd)
			}
		}
	})
}

// A well-formed document with no verb is not a command.
func TestDecodeRejectsAVerblessCommand(t *testing.T) {
	eachCodec(t, func(t *testing.T, c Codec) {
		frame := mustMarshal(t, c, Command{Data: "who am i for"})

		var cmd Command
		if err := c.Decode(frame, &cmd); !errors.Is(err, ErrMalformed) {
			t.Fatalf("Decode of a verbless command = %v, want ErrMalformed", err)
		}
	})
}

func TestAwkwardBodiesSurvive(t *testing.T) {
	eachCodec(t, func(t *testing.T, c Codec) {
		for _, data := range []string{"a b", "line\nbreak", `has "quotes"`, `{"nested":1}`, "  ", "ünïcödé 🎉"} {
			frame := mustMarshal(t, c, Command{Verb: VerbMsg, Data: data})

			var got Command
			if err := c.Decode(frame, &got); err != nil {
				t.Fatalf("Decode(%q): %v", data, err)
			}
			if got.Data != data {
				t.Fatalf("data survived as %q, want %q", got.Data, data)
			}
		}
	})
}

// A frame from one codec must not be readable as the other's, or a
// mis-negotiated connection would silently half-work.
func TestCodecsDoNotOverlap(t *testing.T) {
	jsonFrame := mustMarshal(t, JSON{}, Command{Verb: VerbMsg, Data: "hi"})
	packed := mustMarshal(t, MsgPack{}, Command{Verb: VerbMsg, Data: "hi"})

	var cmd Command
	if err := (MsgPack{}).Decode(jsonFrame, &cmd); err == nil {
		t.Error("the msgpack codec accepted a JSON frame")
	}
	if err := (JSON{}).Decode(packed, &cmd); err == nil {
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
	msg := NewMsg(Msg{ID: 4294967295, Nick: "someone", Data: "hello world", Timestamp: 1700000000000})

	jsonFrame, err := (JSON{}).Encode(msg)
	if err != nil {
		t.Fatal(err)
	}
	packed, err := (MsgPack{}).Encode(msg)
	if err != nil {
		t.Fatal(err)
	}
	if len(packed) >= len(jsonFrame) {
		t.Fatalf("msgpack frame is %d bytes, json is %d", len(packed), len(jsonFrame))
	}
	t.Logf("msgpack %d bytes, json %d bytes", len(packed), len(jsonFrame))
}

// The JSON form is meant to be readable by a person, which is most of the
// reason it is the default.
func TestJSONFrameIsReadable(t *testing.T) {
	frame, err := (JSON{}).Encode(NewErr(ErrTooLong))
	if err != nil {
		t.Fatal(err)
	}
	if got := string(frame); !strings.Contains(got, `"verb":"ERR"`) {
		t.Fatalf("frame = %s, want the verb visible in it", got)
	}
}
