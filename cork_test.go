package main

import (
	"net"
	"slices"
	"testing"
	"time"
)

// recordingConn is a socket that writes down what it is asked to do.
type recordingConn struct {
	net.Conn
	log []string
}

func (r *recordingConn) Write(p []byte) (int, error) {
	r.log = append(r.log, "write "+string(p))
	return len(p), nil
}

func (r *recordingConn) SetWriteDeadline(t time.Time) error {
	if t.IsZero() {
		r.log = append(r.log, "clear")
	} else {
		r.log = append(r.log, "deadline")
	}
	return nil
}

// Whatever is written while anybody is corked reaches the socket as one
// write when the last of them uncorks, and the deadline is cleared only
// after it. How many writes a frame took cannot be seen from the wire.
func TestCorkSendsOneWriteForEveryone(t *testing.T) {
	rec := &recordingConn{}
	s := &corkConn{Conn: rec}
	write := func(p string) {
		t.Helper()
		if _, err := s.Write([]byte(p)); err != nil {
			t.Fatal(err)
		}
	}
	uncork := func() {
		t.Helper()
		if err := s.uncork(); err != nil {
			t.Fatal(err)
		}
	}

	s.cork(time.Second)
	write("a")
	s.cork(time.Second)
	write("b")
	uncork()
	write("c")
	uncork()
	write("d") // nobody is corked: straight through

	want := []string{"deadline", "deadline", "write abc", "clear", "write d"}
	if !slices.Equal(rec.log, want) {
		t.Fatalf("socket saw %q, want %q", rec.log, want)
	}
}
