package main

import (
	"net"
	"slices"
	"testing"
	"time"
)

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

// A unit test: the wire cannot show how many writes there were.
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
	write("d")

	want := []string{"deadline", "deadline", "write abc", "clear", "write d"}
	if !slices.Equal(rec.log, want) {
		t.Fatalf("socket saw %q, want %q", rec.log, want)
	}
}
