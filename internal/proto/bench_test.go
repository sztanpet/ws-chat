package proto

import "testing"

// What a codec costs, per frame, on the shapes the server actually moves.
//
// Both families run over Codecs(), so a new wire format is measured against
// the others the moment it is added to that table — and so is a change to
// how an existing one is implemented.
//
// The two directions are separate benchmarks because they are paid by
// different people. Encoding a broadcast happens once per codec per
// message, however many members are in the room; decoding happens once per
// frame a client sends. A change that trades one for the other has to be
// visible as such.

// benchMsg is the frame that dominates a busy server: a chat message with a
// sender who has roles and attrs attached, which is the expensive case
// because both ride on every message.
var benchMsg = NewMsg(Msg{
	Channel:   "main",
	ID:        4294967295,
	Nick:      "someone",
	Data:      "hello everyone, this is roughly what a chat message looks like",
	Timestamp: 1700000000000,
	Roles:     []string{"moderator", "subscriber"},
	Attrs:     map[string]string{"colour": "#ff0000"},
})

// benchCmd is the inbound side: one command, the common one.
var benchCmd = Command{Verb: VerbMsg, Channel: "main", Data: "hello everyone"}

// benchPlain is the same message from a server with no directory
// installed: no roles, no attrs, nothing omitted-but-present. Both shapes
// are measured because the difference between them is what the roles and
// attrs on every message actually cost.
var benchPlain = NewMsg(Msg{
	Channel:   "main",
	ID:        4294967295,
	Nick:      "someone",
	Data:      "hello everyone, this is roughly what a chat message looks like",
	Timestamp: 1700000000000,
})

func BenchmarkEncode(b *testing.B) {
	for _, tt := range []struct {
		name string
		msg  Msg
	}{
		{"plain", benchPlain},
		{"roles", benchMsg},
	} {
		for _, c := range Codecs() {
			b.Run(tt.name+"/"+c.Name(), func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					frame, err := c.Encode(tt.msg)
					if err != nil {
						b.Fatal(err)
					}
					sink = len(frame)
				}
			})
		}
	}
}

func BenchmarkDecode(b *testing.B) {
	for _, c := range Codecs() {
		frame, err := marshalAs(c, benchCmd)
		if err != nil {
			b.Fatal(err)
		}

		b.Run(c.Name(), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				var cmd Command
				if err := c.Decode(frame, &cmd); err != nil {
					b.Fatal(err)
				}
				sink = len(cmd.Data)
			}
		})
	}
}

// sink keeps the compiler from deciding none of the above happened.
var sink int
