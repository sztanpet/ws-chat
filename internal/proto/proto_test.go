package proto

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestSplit(t *testing.T) {
	tests := []struct {
		name        string
		frame       string
		wantVerb    string
		wantPayload string
		wantErr     bool
	}{
		{name: "verb and payload", frame: `MSG {"data":"hi"}`, wantVerb: "MSG", wantPayload: `{"data":"hi"}`},
		{name: "bare verb", frame: "PING", wantVerb: "PING"},
		{name: "payload with spaces", frame: `MSG {"data":"a b c"}`, wantVerb: "MSG", wantPayload: `{"data":"a b c"}`},
		{name: "long verb", frame: "PRIVMSG {}", wantVerb: "PRIVMSG", wantPayload: "{}"},

		{name: "empty", frame: "", wantErr: true},
		{name: "lowercase", frame: `msg {}`, wantErr: true},
		{name: "mixed case", frame: `Msg {}`, wantErr: true},
		{name: "digits", frame: `MSG2 {}`, wantErr: true},
		{name: "leading space", frame: ` MSG {}`, wantErr: true},
		{name: "trailing space only", frame: "MSG ", wantErr: true},
		{name: "json only", frame: `{"data":"hi"}`, wantErr: true},
		{name: "verb too long", frame: "AAAAAAAAAAAAAAAAA {}", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			verb, payload, err := Split([]byte(tt.frame))
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Split(%q) = %q, %q; want an error", tt.frame, verb, payload)
				}
				if !errors.Is(err, ErrMalformed) {
					t.Fatalf("error %v does not wrap ErrMalformed", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Split(%q): %v", tt.frame, err)
			}
			if verb != tt.wantVerb {
				t.Errorf("verb = %q, want %q", verb, tt.wantVerb)
			}
			if string(payload) != tt.wantPayload {
				t.Errorf("payload = %q, want %q", payload, tt.wantPayload)
			}
		})
	}
}

func TestFormat(t *testing.T) {
	frame, err := Format(VerbMsg, Msg{ID: 7, Nick: "someone", Data: "hi", Timestamp: 1})
	if err != nil {
		t.Fatalf("Format: %v", err)
	}

	verb, payload, err := Split(frame)
	if err != nil {
		t.Fatalf("Format produced something Split rejects (%q): %v", frame, err)
	}
	if verb != VerbMsg {
		t.Fatalf("verb = %q, want %q", verb, VerbMsg)
	}
	if len(payload) == 0 {
		t.Fatal("no payload")
	}
}

func TestFormatBareVerb(t *testing.T) {
	frame, err := Format(VerbPong, nil)
	if err != nil {
		t.Fatalf("Format: %v", err)
	}
	if string(frame) != VerbPong {
		t.Fatalf("frame = %q, want %q", frame, VerbPong)
	}
}

func TestFormatRejectsBadVerb(t *testing.T) {
	if _, err := Format("not a verb", nil); err == nil {
		t.Fatal("Format accepted a verb with spaces")
	}
}

// A message body containing a space, a newline, or a quote must survive the
// round trip: the framing is space-delimited only for the verb.
func TestRoundTripAwkwardBodies(t *testing.T) {
	for _, data := range []string{"a b", "line\nbreak", `has "quotes"`, "MSG {\"nested\":1}", "  "} {
		frame, err := Format(VerbMsg, Msg{Data: data})
		if err != nil {
			t.Fatalf("Format(%q): %v", data, err)
		}
		verb, payload, err := Split(frame)
		if err != nil {
			t.Fatalf("Split(%q): %v", frame, err)
		}
		if verb != VerbMsg {
			t.Fatalf("verb = %q, want %q", verb, VerbMsg)
		}
		var got Msg
		if err := json.Unmarshal(payload, &got); err != nil {
			t.Fatalf("payload %q: %v", payload, err)
		}
		if got.Data != data {
			t.Fatalf("data survived as %q, want %q", got.Data, data)
		}
	}
}
