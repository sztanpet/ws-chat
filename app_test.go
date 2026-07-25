package main

import (
	"context"
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

// client is a connected WebSocket with the assertions the tests need. It
// speaks whichever codec it negotiated, so the same assertions work over
// either wire format.
type client struct {
	t       *testing.T
	ws      *websocket.Conn
	codec   proto.Codec
	nick    string      // as assigned by the server, from the READY frame
	backlog []proto.Msg // what it was replayed on connect
}

// dial connects with the default (JSON) codec.
func (ta *testApp) dial(t *testing.T) *client {
	t.Helper()
	return ta.dialCodec(t, proto.Default())
}

// dialCodec connects asking for a specific wire format.
func (ta *testApp) dialCodec(t *testing.T, codec proto.Codec) *client {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	url := "ws" + strings.TrimPrefix(ta.srv.URL, "http") + "/ws"
	ws, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		Subprotocols: []string{codec.Name()},
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { ws.CloseNow() })

	if got := ws.Subprotocol(); got != codec.Name() {
		t.Fatalf("negotiated %q, want %q", got, codec.Name())
	}

	c := &client{t: t, ws: ws, codec: codec}

	// Waiting for READY is not politeness, it is what makes the test
	// deterministic: the handshake returns before the server has subscribed
	// the connection, so a client that talks immediately can miss its own
	// message.
	verb, payload := c.recv()
	if verb != proto.VerbReady {
		t.Fatalf("first frame was %s, want %s", verb, proto.VerbReady)
	}
	var ready proto.Ready
	if err := c.codec.Unmarshal(payload, &ready); err != nil {
		t.Fatalf("bad READY payload %q: %v", payload, err)
	}
	if ready.Nick == "" {
		t.Fatal("READY carried no nick")
	}
	c.nick = ready.Nick

	// Then the backlog, unless it is switched off. Always exactly one
	// frame, so the harness never has to guess.
	if ta.cfg.Backlog > 0 {
		verb, payload := c.recv()
		if verb != proto.VerbBacklog {
			t.Fatalf("second frame was %s, want %s", verb, proto.VerbBacklog)
		}
		var backlog proto.Backlog
		if err := c.codec.Unmarshal(payload, &backlog); err != nil {
			t.Fatalf("bad BACKLOG payload %q: %v", payload, err)
		}
		c.backlog = backlog.Messages
	}
	return c
}

// msgType is the WebSocket message type this client's codec uses.
func (c *client) msgType() websocket.MessageType {
	if c.codec.Binary() {
		return websocket.MessageBinary
	}
	return websocket.MessageText
}

// send encodes and sends a command.
func (c *client) send(verb string, payload any) {
	c.t.Helper()
	frame, err := c.codec.Encode(verb, payload)
	if err != nil {
		c.t.Fatalf("encode %s: %v", verb, err)
	}
	c.sendRaw(frame, c.msgType())
}

// sendRaw sends bytes as-is, for the tests that are about malformed input.
func (c *client) sendRaw(frame []byte, typ websocket.MessageType) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.ws.Write(ctx, typ, frame); err != nil {
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
	if typ != c.msgType() {
		c.t.Fatalf("got a %v frame, want %v", typ, c.msgType())
	}
	verb, payload, err = c.codec.Decode(frame)
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
	if err := c.codec.Unmarshal(payload, &msg); err != nil {
		c.t.Fatalf("bad MSG payload %q: %v", payload, err)
	}
	if data != "" && msg.Data != data {
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
	if err := c.codec.Unmarshal(payload, &msg); err != nil {
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
	if err := c.codec.Unmarshal(payload, &e); err != nil {
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

func mustUnmarshal(t *testing.T, c *client, data []byte, v any) {
	t.Helper()
	if err := c.codec.Unmarshal(data, v); err != nil {
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
