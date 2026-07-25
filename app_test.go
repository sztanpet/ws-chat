package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/sztanpet/ws-chat/internal/config"
	"github.com/sztanpet/ws-chat/internal/hook"
	"github.com/sztanpet/ws-chat/internal/proto"
)

// Tests here are integration-first on purpose: a real httptest.Server, a
// real WebSocket client dialing it, and assertions on what the CLIENT
// received. Nothing reaches into the server's internals to check delivery,
// because a test that does cannot tell the difference between "fanned out"
// and "actually written to the socket".

// testApp starts the server on a random port and tears it down with the
// test.
type testApp struct {
	*app
	srv *httptest.Server
	t   *testing.T
}

func newTestApp(t *testing.T, tweak ...func(*config.Config)) *testApp {
	t.Helper()
	return newTestAppWith(t, hook.Hooks{}, tweak...)
}

// newTestAppWith is the same with layers installed.
func newTestAppWith(t *testing.T, hooks hook.Hooks, tweak ...func(*config.Config)) *testApp {
	t.Helper()

	cfg := config.Default()
	cfg.LogLevel = "error" // a passing test should be quiet
	for _, fn := range tweak {
		fn(&cfg)
	}

	a, err := newAppWithConfig(cfg, hooks)
	if err != nil {
		t.Fatalf("newAppWithConfig: %v", err)
	}

	srv := httptest.NewServer(a.mux)
	t.Cleanup(func() {
		a.close()
		srv.Close()
	})
	return &testApp{app: a, srv: srv, t: t}
}

// client is a connected WebSocket with the assertions the tests need.
type client struct {
	t    *testing.T
	ws   *websocket.Conn
	nick string // as assigned by the server, from the READY frame
}

func (ta *testApp) dial(t *testing.T) *client {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	url := "ws" + strings.TrimPrefix(ta.srv.URL, "http") + "/ws"
	ws, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { ws.CloseNow() })

	c := &client{t: t, ws: ws}

	// Waiting for READY is not politeness, it is what makes the test
	// deterministic: the handshake returns before the server has subscribed
	// the connection, so a client that talks immediately can miss its own
	// message.
	verb, payload := c.recv()
	if verb != proto.VerbReady {
		t.Fatalf("first frame was %s, want %s", verb, proto.VerbReady)
	}
	var ready proto.Ready
	if err := json.Unmarshal(payload, &ready); err != nil {
		t.Fatalf("bad READY payload %q: %v", payload, err)
	}
	if ready.Nick == "" {
		t.Fatal("READY carried no nick")
	}
	c.nick = ready.Nick
	return c
}

func (c *client) send(frame string) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.ws.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
		c.t.Fatalf("write %q: %v", frame, err)
	}
}

// recv reads one frame, failing the test rather than hanging.
func (c *client) recv() (verb string, payload []byte) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	typ, frame, err := c.ws.Read(ctx)
	if err != nil {
		c.t.Fatalf("read: %v", err)
	}
	if typ != websocket.MessageText {
		c.t.Fatalf("got a %v frame, want text", typ)
	}
	verb, payload, err = proto.Split(frame)
	if err != nil {
		c.t.Fatalf("server sent an unparseable frame %q: %v", frame, err)
	}
	return verb, payload
}

// expectMsg reads one frame and requires it to be a MSG with this body.
func (c *client) expectMsg(nick, data string) proto.Msg {
	c.t.Helper()
	verb, payload := c.recv()
	if verb != proto.VerbMsg {
		c.t.Fatalf("got %s, want %s", verb, proto.VerbMsg)
	}
	var msg proto.Msg
	if err := json.Unmarshal(payload, &msg); err != nil {
		c.t.Fatalf("bad MSG payload %q: %v", payload, err)
	}
	if msg.Data != data {
		c.t.Fatalf("data = %q, want %q", msg.Data, data)
	}
	if nick != "" && msg.Nick != nick {
		c.t.Fatalf("nick = %q, want %q", msg.Nick, nick)
	}
	return msg
}

// expectPriv reads one frame and requires it to be a PRIVMSG with this body.
func (c *client) expectPriv(data string) proto.Priv {
	c.t.Helper()
	verb, payload := c.recv()
	if verb != proto.VerbPriv {
		c.t.Fatalf("got %s %s, want %s", verb, payload, proto.VerbPriv)
	}
	var msg proto.Priv
	if err := json.Unmarshal(payload, &msg); err != nil {
		c.t.Fatalf("bad PRIVMSG payload %q: %v", payload, err)
	}
	if msg.Data != data {
		c.t.Fatalf("data = %q, want %q", msg.Data, data)
	}
	return msg
}

// expectErr reads one frame and requires it to be an ERR with this code.
func (c *client) expectErr(description string) {
	c.t.Helper()
	verb, payload := c.recv()
	if verb != proto.VerbErr {
		c.t.Fatalf("got %s %s, want %s %s", verb, payload, proto.VerbErr, description)
	}
	var e proto.Err
	if err := json.Unmarshal(payload, &e); err != nil {
		c.t.Fatalf("bad ERR payload %q: %v", payload, err)
	}
	if e.Description != description {
		c.t.Fatalf("ERR description = %q, want %q", e.Description, description)
	}
}

// contextWithTimeout is the deadline every test operation gets.
func contextWithTimeout() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

func mustUnmarshal(t *testing.T, data []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(data, v); err != nil {
		t.Fatalf("bad payload %q: %v", data, err)
	}
}

// expectClosed requires the connection to be closed by the server.
func (c *client) expectClosed() websocket.StatusCode {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for {
		_, _, err := c.ws.Read(ctx)
		if err == nil {
			continue // drain whatever was already in flight
		}
		var ce websocket.CloseError
		if errors.As(err, &ce) {
			return ce.Code
		}
		c.t.Fatalf("connection ended with %v, want a close frame", err)
	}
}
