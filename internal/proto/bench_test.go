package proto

import "testing"

var benchMsg = NewMsg(Msg{
	Channel:   "main",
	ID:        4294967295,
	Nick:      "someone",
	Data:      "hello everyone, this is roughly what a chat message looks like",
	Timestamp: 1700000000000,
	Roles:     []string{"moderator", "subscriber"},
	Attrs:     map[string]string{"colour": "#ff0000"},
})

var benchCmd = Command{Verb: VerbMsg, Channel: "main", Data: "hello everyone"}

// Against benchMsg, what roles and attrs on every message cost.
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
