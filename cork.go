package main

import (
	"bufio"
	"net"
	"net/http"
	"sync"
	"time"
)

// hijackCapture is what websocket.Accept is handed instead of the
// ResponseWriter, so that the connection the library hijacks comes back with
// a corkConn in front of it.
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
	// The library writes through brw.Writer, which net/http hands back new
	// and empty, so pointing it at the cork loses nothing.
	brw.Writer.Reset(h.sock)
	return h.sock, brw, nil
}

// corkConn is the socket under the library. coder/websocket flushes every
// message, one frame each, so a pump draining sixteen frames cost sixteen
// write(2)s. While anybody is corked, what the library writes is held, and
// the last writer to uncork sends it in one.
//
// The writers share the socket's write deadline as well: each sets it on the
// way in, and the last one out clears it once what they held is on the wire.
type corkConn struct {
	net.Conn

	mu      sync.Mutex
	writers int
	held    *[]byte
}

// heldBufs keeps the buffers between writes, so that a connection with
// nothing to say holds none.
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

// cork starts a write that has to be on the wire within timeout.
func (s *corkConn) cork(timeout time.Duration) {
	s.mu.Lock()
	s.writers++
	_ = s.Conn.SetWriteDeadline(time.Now().Add(timeout))
	s.mu.Unlock()
}

// uncork ends one. The last writer out sends what all of them wrote, so the
// error it gets back may be about somebody else's frames.
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
