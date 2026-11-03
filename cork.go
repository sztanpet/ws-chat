package main

import (
	"bufio"
	"net"
	"net/http"
	"sync"
	"time"
)

// The library hijacks for itself; this is how a corkConn gets under it.
type hijackCapture struct {
	http.ResponseWriter
	sock *corkConn
}

func (h *hijackCapture) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	sock, brw, err := http.NewResponseController(h.ResponseWriter).Hijack()
	if err != nil {
		return sock, brw, err
	}
	h.sock = &corkConn{Conn: sock}
	// net/http hands back a new, empty writer, so Reset discards nothing.
	brw.Writer.Reset(h.sock)
	return h.sock, brw, nil
}

// The library flushes every message; holding them makes a batch one write(2).
type corkConn struct {
	net.Conn

	mu      sync.Mutex
	writers int
	held    *[]byte
}

// Pooled, so an idle connection holds no buffer.
var heldBufs = sync.Pool{New: func() any { return new([]byte) }}

func (s *corkConn) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.writers == 0 {
		return s.Conn.Write(p)
	}
	if s.held == nil {
		s.held = heldBufs.Get().(*[]byte)
	}
	*s.held = append(*s.held, p...)
	return len(p), nil
}

// On the socket, not a context, which would cost an allocation per frame.
func (s *corkConn) cork(timeout time.Duration) {
	s.mu.Lock()
	s.writers++
	_ = s.Conn.SetWriteDeadline(time.Now().Add(timeout))
	s.mu.Unlock()
}

// Its error may belong to another writer: the last one out flushes for all.
func (s *corkConn) uncork() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.writers--; s.writers > 0 {
		return nil
	}
	var err error
	if s.held != nil {
		_, err = s.Conn.Write(*s.held)
		*s.held = (*s.held)[:0]
		heldBufs.Put(s.held)
		s.held = nil
	}
	_ = s.Conn.SetWriteDeadline(time.Time{})
	return err
}
