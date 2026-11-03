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

type testApp struct {
	*app
	srv *httptest.Server
	t   *testing.T
}

func newTestApp(t *testing.T, tweak ...func(*config.Config)) *testApp {
	t.Helper()
	return newTestAppWith(t, hook.Hooks{}, tweak...)
}

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

type client struct {
	t       *testing.T
	ws      *websocket.Conn
	codec   proto.Codec
	nick    string
	joined  []string
	backlog []proto.Msg
}

func (ta *testApp) dial(t *testing.T) *client {
	t.Helper()
	return ta.dialCodec(t, proto.Default())
}

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
	t.Cleanup(func() { _ = ws.CloseNow() })

	if got := ws.Subprotocol(); got != codec.Name() {
		t.Fatalf("negotiated %q, want %q", got, codec.Name())
	}

	c := &client{t: t, ws: ws, codec: codec}

	// READY, or a client talking at once can miss its own message.
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

func (c *client) settle(ta *testApp) {
	c.t.Helper()

	// Counted, not in order: a second backlog can overtake the first JOIN.
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

func (c *client) msgType() websocket.MessageType {
	if c.codec.Binary() {
		return websocket.MessageBinary
	}
	return websocket.MessageText
}

func (c *client) send(cmd proto.Command) {
	c.t.Helper()
	c.sendRaw(c.encode(cmd), c.msgType())
}

// For writing from other goroutines: encode here, where Fatalf is allowed.
func (c *client) encode(cmd proto.Command) []byte {
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
	return frame
}

func (c *client) sendRaw(frame []byte, typ websocket.MessageType) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.ws.Write(ctx, typ, frame); err != nil {
		c.t.Fatalf("write %q: %v", frame, err)
	}
}

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

// Presence is skipped here, not in recv, so the presence tests see everything.
func (c *client) nextInteresting() (verb string, payload []byte) {
	c.t.Helper()
	for {
		verb, payload = c.recv()
		if verb != proto.VerbJoin && verb != proto.VerbPart {
			return verb, payload
		}
	}
}

// Commands are handled in order, so the PONG proves the rest are done; a frame
// can arrive before its handler finishes, as a PART does before the teardown.
func (c *client) sync() {
	c.t.Helper()
	c.send(proto.Command{Verb: proto.VerbPing})
	if verb, payload := c.nextInteresting(); verb != proto.VerbPong {
		c.t.Fatalf("got %s %s, want %s", verb, payload, proto.VerbPong)
	}
}

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

func expectAll(clients []*client, nick, data string) {
	for _, c := range clients {
		c.t.Helper()
		c.expectMsg(nick, data)
	}
}

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

func contextWithTimeout() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

func mustUnmarshal(t *testing.T, c *client, data []byte, v any) {
	t.Helper()
	c.decode(data, v)
}

func (c *client) expectClosed() websocket.StatusCode {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for {
		_, _, err := c.ws.Read(ctx)
		if err == nil {
			continue // drain whatever was already in flight
		}
		if ce, ok := errors.AsType[websocket.CloseError](err); ok {
			return ce.Code
		}
		c.t.Fatalf("connection ended with %v, want a close frame", err)
	}
}
