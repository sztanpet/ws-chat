package main

import (
	"context"
	"encoding/json/v2"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/vmihailenco/msgpack/v5"

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
	joined  []string    // channels it was put into on connect
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
	c.decode(payload, &ready)
	if ready.Nick == "" {
		t.Fatal("READY carried no nick")
	}
	c.nick = ready.Nick
	c.settle(ta)
	return c
}

// settle consumes what a connection is sent on arrival: a JOIN and a
// BACKLOG for each channel it was put into.
//
// It stops at the first frame that is neither, which is the point — a test
// that dials and then reads gets live traffic, not setup.
func (c *client) settle(ta *testApp) {
	c.t.Helper()

	// Each channel the server tried to put it in produces exactly one
	// outcome — a JOIN, or an ERR saying why not — with a BACKLOG along the
	// way if the backlog is on.
	//
	// Counted rather than read in a fixed order, because only the ordering
	// WITHIN a channel is guaranteed. Its backlog is written directly while
	// its JOIN comes through its pump, so with two channels the second
	// backlog can overtake the first join.
	outcomes := 0
	for want := len(ta.app.autojoin(context.Background(), hook.Identity{})); outcomes < want; {
		verb, payload := c.recv()
		switch verb {
		case proto.VerbErr:
			outcomes++ // refused; that channel produces nothing else
		case proto.VerbJoin:
			var join proto.Join
			c.decode(payload, &join)
			c.joined = append(c.joined, join.Channel)
			outcomes++
		case proto.VerbBacklog:
			var backlog proto.Backlog
			c.decode(payload, &backlog)
			c.backlog = append(c.backlog, backlog.Messages...)
		default:
			c.t.Fatalf("unexpected %s frame during connect", verb)
		}
	}
}

// msgType is the WebSocket message type this client's codec uses.
func (c *client) msgType() websocket.MessageType {
	if c.codec.Binary() {
		return websocket.MessageBinary
	}
	return websocket.MessageText
}

// send encodes and sends a command. The verb is a field in the command, so
// there is one document and one encode.
func (c *client) send(cmd proto.Command) {
	c.t.Helper()

	var frame []byte
	var err error
	if c.codec.Binary() {
		frame, err = msgpack.Marshal(cmd)
	} else {
		frame, err = json.Marshal(cmd)
	}
	if err != nil {
		c.t.Fatalf("encode %s: %v", cmd.Verb, err)
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

// recv reads one frame and reports its verb along with the whole frame.
//
// The frame is returned intact rather than split, because there is nothing
// to split any more: the verb is a field inside the document. A test that
// wants the rest decodes the same bytes into the type the verb named. A
// real client would decode once into a union struct of its own, the way the
// server does with proto.Command.
func (c *client) recv() (verb string, frame []byte) {
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

	var header struct {
		Verb string `json:"verb" msgpack:"verb"`
	}
	c.decode(frame, &header)
	if header.Verb == "" {
		c.t.Fatalf("server sent a frame with no verb: %q", frame)
	}
	return header.Verb, frame
}

// decode unmarshals a frame with this client's codec.
func (c *client) decode(frame []byte, v any) {
	c.t.Helper()
	var err error
	if c.codec.Binary() {
		err = msgpack.Unmarshal(frame, v)
	} else {
		err = json.Unmarshal(frame, v)
	}
	if err != nil {
		c.t.Fatalf("cannot decode %q: %v", frame, err)
	}
}

// nextInteresting reads until it finds a frame that is not presence.
//
// Presence is noise for a test that is about something else: every client
// already in a room sees a JOIN when the next one dials, and a PART when
// anybody leaves. Skipping it here rather than in recv keeps the presence
// tests honest — they use recv and see everything.
func (c *client) nextInteresting() (verb string, payload []byte) {
	c.t.Helper()
	for {
		verb, payload = c.recv()
		if verb != proto.VerbJoin && verb != proto.VerbPart {
			return verb, payload
		}
	}
}

// sync waits until the server has finished handling everything this client
// has sent.
//
// Commands are read and handled one at a time on this connection's read
// pump, so a PONG proves the ones before it are done. Any test that asserts
// on server STATE rather than on what a client received needs this: a frame
// can reach a client before the handler that sent it has finished its work,
// and parting is the case that bites — the leaving client is told directly,
// on purpose, before its membership is torn down.
func (c *client) sync() {
	c.t.Helper()
	c.send(proto.Command{Verb: proto.VerbPing})
	if verb, payload := c.nextInteresting(); verb != proto.VerbPong {
		c.t.Fatalf("got %s %s, want %s", verb, payload, proto.VerbPong)
	}
}

// expectMsg reads one frame and requires it to be a MSG with this body.
func (c *client) expectMsg(nick, data string) proto.Msg {
	c.t.Helper()
	verb, payload := c.nextInteresting()
	if verb != proto.VerbMsg {
		c.t.Fatalf("got %s, want %s", verb, proto.VerbMsg)
	}
	var msg proto.Msg
	c.decode(payload, &msg)
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
	verb, payload := c.nextInteresting()
	if verb != proto.VerbPriv {
		c.t.Fatalf("got %s %s, want %s", verb, payload, proto.VerbPriv)
	}
	var msg proto.Priv
	c.decode(payload, &msg)
	if msg.Data != data {
		c.t.Fatalf("data = %q, want %q", msg.Data, data)
	}
	return msg
}

// expectAll requires every client to receive the same message. Clients that
// are in the room receive everything said in it, so a test with more than
// two of them has to drain all of them or the next assertion reads a
// message it was not looking for.
func expectAll(clients []*client, nick, data string) {
	for _, c := range clients {
		c.t.Helper()
		c.expectMsg(nick, data)
	}
}

// expectErr reads one frame and requires it to be an ERR with this code.
func (c *client) expectErr(description string) {
	c.t.Helper()
	verb, payload := c.nextInteresting()
	if verb != proto.VerbErr {
		c.t.Fatalf("got %s %s, want %s %s", verb, payload, proto.VerbErr, description)
	}
	var e proto.Err
	c.decode(payload, &e)
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
	c.decode(data, v)
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
