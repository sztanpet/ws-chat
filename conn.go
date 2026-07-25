package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/sztanpet/ws-chat/internal/broadcast"
	"github.com/sztanpet/ws-chat/internal/hook"
	"github.com/sztanpet/ws-chat/internal/proto"
)

// conn is one client. Three goroutines run per connection: a read pump that
// parses what the client sends, a write pump that feeds it the broadcast
// stream, and a private pump that delivers messages addressed to it alone.
//
// All three may write to the socket, which is safe because
// coder/websocket serializes writes internally ("all methods may be called
// concurrently except for Reader and Read"). That is worth a small
// deviation from a single-writer rule, because the alternative — one merge
// channel in front of the socket — would put a channel hop back in front of
// every broadcast message and undo the fan-out work in
// internal/broadcast.
//
// The private pump is the interesting one. A private message arrives on
// SOMEBODY ELSE'S read pump, and that goroutine must not be made to wait on
// this client's socket: one person who has stopped reading would otherwise
// stall everyone who messages them. So the sender only queues, never
// writes, and a recipient that lets its queue fill has its messages refused
// rather than allowed to become the sender's problem.
//
// The cost is that a private message and a broadcast have no relative
// order, since they arrive on two different streams. Each stream is ordered
// within itself, which is what a client actually needs.
type conn struct {
	app  *app
	ws   *websocket.Conn
	sub  broadcast.Sub
	priv chan []byte
	id   hook.Identity
	log  *slog.Logger
}

// nick is the connection's display name, as decided at connect time.
func (c *conn) nick() string { return c.id.Nick }

func (a *app) handleWS(w http.ResponseWriter, r *http.Request) {
	// Identity is resolved once, before the upgrade, so a refusal is a
	// plain HTTP error the client can actually read rather than a
	// WebSocket that opens and immediately closes.
	id, err := a.identify(r.Context(), r)
	switch {
	case errors.Is(err, hook.ErrUnauthorized):
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	case err != nil:
		a.log.Error("authentication failed", "err", err, "remote", r.RemoteAddr)
		http.Error(w, "authentication unavailable", http.StatusInternalServerError)
		return
	}

	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns: a.cfg.AllowedOrigins,
	})
	if err != nil {
		// Accept has already written the error response.
		a.log.Debug("websocket upgrade rejected", "err", err, "remote", r.RemoteAddr)
		return
	}
	ws.SetReadLimit(a.cfg.MaxFrameSize)

	c := &conn{
		app:  a,
		ws:   ws,
		priv: make(chan []byte, a.cfg.PrivBuffer),
		id:   id,
	}
	c.log = a.log.With("nick", c.nick(), "remote", r.RemoteAddr)

	// Deliberately not r.Context(): the connection is hijacked, and its
	// lifetime is now the server's shutdown, not the request's.
	c.serve(a.ctx)
}

// serve runs the connection until either side gives up.
func (c *conn) serve(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	c.sub = c.app.bc.Subscribe()
	c.app.register(c)
	c.log.Debug("connected")

	// Only now is the connection real: subscribed, addressable, and able to
	// receive. The handshake finished before any of that, so a client that
	// talks first would race its own setup.
	ready, err := proto.Format(proto.VerbReady, proto.Ready{Nick: c.nick()})
	if err != nil {
		c.log.Error("cannot format READY", "err", err)
		c.ws.CloseNow()
		c.app.unregister(c)
		c.sub.Close()
		return
	}
	if !c.write(ctx, ready) {
		c.app.unregister(c)
		c.sub.Close()
		return
	}

	var pumps sync.WaitGroup
	pumps.Add(2)
	go func() { defer pumps.Done(); c.writePump(ctx) }()
	go func() { defer pumps.Done(); c.privPump(ctx) }()

	rerr := c.readPump(ctx)

	// Take the connection out of the directory first, so nobody queues a
	// private message that will never be written.
	c.app.unregister(c)

	// Ending the subscription unblocks the write pump, which is parked in
	// Recv; cancelling the context unblocks the private pump. Closing the
	// socket is what unblocks a read pump instead, which is why the write
	// pump does that when it is the one to fail.
	c.sub.Close()
	cancel()
	pumps.Wait()

	c.ws.CloseNow()
	c.log.Debug("disconnected", "err", rerr)
}

// privPump writes the messages addressed to this client alone.
func (c *conn) privPump(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case frame := <-c.priv:
			if !c.write(ctx, frame) {
				return
			}
		}
	}
}

// deliver queues a frame for this client alone, reporting whether it fit.
// It never blocks: it runs on the SENDER's goroutine, and a recipient that
// has stopped reading must not become the sender's problem.
func (c *conn) deliver(frame []byte) bool {
	select {
	case c.priv <- frame:
		return true
	default:
		return false
	}
}

// writePump feeds the client the broadcast stream. It is the only place a
// fanned-out message is written.
func (c *conn) writePump(ctx context.Context) {
	// One Recv per wakeup, up to WriteBatch frames. The batching is what
	// keeps a client that has fallen behind from costing one scheduler
	// wakeup per message; the frames still go out one WebSocket message
	// each, because the protocol is one command per frame.
	batch := make([][]byte, c.app.cfg.WriteBatch)

	for {
		n, err := c.sub.Recv(batch)

		for _, frame := range batch[:n] {
			if !c.write(ctx, frame) {
				return
			}
		}

		switch {
		case err == nil:
			continue
		case errors.Is(err, broadcast.ErrLagged):
			// The client could not keep up. Say so on the way out rather
			// than dropping the socket silently: a client that reconnects
			// blind cannot tell this from a network failure.
			c.log.Info("dropped: too slow")
			c.close(websocket.StatusPolicyViolation, "too slow")
			return
		default:
			return // orderly close
		}
	}
}

// readPump parses client frames until the client goes away or misbehaves.
func (c *conn) readPump(ctx context.Context) error {
	for {
		// A connection that says nothing at all is disconnected. Clients
		// are expected to PING; a silent one is either gone or broken, and
		// there is no way to tell the difference from this side.
		rctx, cancel := context.WithTimeout(ctx, c.app.cfg.IdleTimeout.Duration())
		typ, frame, err := c.ws.Read(rctx)
		cancel()
		if err != nil {
			return err
		}

		if typ != websocket.MessageText {
			c.reply(ctx, proto.ErrBinary)
			continue
		}

		verb, payload, err := proto.Split(frame)
		if err != nil {
			c.reply(ctx, proto.ErrProtocol)
			continue
		}

		switch verb {
		case proto.VerbMsg:
			c.handleMsg(ctx, payload)
		case proto.VerbPriv:
			c.handlePriv(ctx, payload)
		case proto.VerbPing:
			c.send(ctx, []byte(proto.VerbPong))
		default:
			c.reply(ctx, proto.ErrUnknown)
		}
	}
}

func (c *conn) handleMsg(ctx context.Context, payload []byte) {
	var in proto.In
	if err := json.Unmarshal(payload, &in); err != nil {
		c.reply(ctx, proto.ErrProtocol)
		return
	}
	switch {
	case in.Data == "":
		c.reply(ctx, proto.ErrEmpty)
		return
	case len(in.Data) > c.app.cfg.MaxMessage:
		c.reply(ctx, proto.ErrTooLong)
		return
	}

	if ok, reason := c.app.allow(ctx, c.id, in.Data); !ok {
		c.reply(ctx, reason)
		return
	}

	// The id and the timestamp are the server's to assign: a client's clock
	// is not evidence, and ordering has to come from one place.
	id := c.app.seq.Add(1)
	at := time.Now()

	frame, err := proto.Format(proto.VerbMsg, proto.Msg{
		ID:        id,
		Nick:      c.nick(),
		Data:      in.Data,
		Timestamp: at.UnixMilli(),
	})
	if err != nil {
		c.log.Error("cannot format outgoing message", "err", err)
		c.reply(ctx, proto.ErrProtocol)
		return
	}

	// Deliver first, persist after. A store having a bad day must cost
	// history, never delivery.
	c.app.bc.Broadcast(frame)
	c.app.recordMessage(hook.Message{ID: id, From: c.id, Data: in.Data, At: at})
}

// handlePriv delivers a message to one named client.
func (c *conn) handlePriv(ctx context.Context, payload []byte) {
	var in proto.InPriv
	if err := json.Unmarshal(payload, &in); err != nil {
		c.reply(ctx, proto.ErrProtocol)
		return
	}
	switch {
	case in.Data == "":
		c.reply(ctx, proto.ErrEmpty)
		return
	case len(in.Data) > c.app.cfg.MaxMessage:
		c.reply(ctx, proto.ErrTooLong)
		return
	case in.Nick == c.nick():
		c.reply(ctx, proto.ErrSelf)
		return
	}

	if ok, reason := c.app.allow(ctx, c.id, in.Data); !ok {
		c.reply(ctx, reason)
		return
	}

	target, ok := c.app.lookup(in.Nick)
	if !ok {
		c.reply(ctx, proto.ErrNoSuch)
		return
	}

	id := c.app.seq.Add(1)
	at := time.Now()
	now := at.UnixMilli()

	// The recipient's copy names the sender.
	frame, err := proto.Format(proto.VerbPriv, proto.Priv{
		ID: id, Nick: c.nick(), Data: in.Data, Timestamp: now,
	})
	if err != nil {
		c.log.Error("cannot format private message", "err", err)
		c.reply(ctx, proto.ErrProtocol)
		return
	}
	if !target.deliver(frame) {
		// The recipient is not draining. Refuse rather than wait, and say
		// which it was: a sender that gets silence cannot tell a full
		// queue from a delivered message.
		c.reply(ctx, proto.ErrBacklog)
		return
	}

	// The sender's echo names the recipient, so a client can render both
	// halves of a conversation from the same frame type.
	echo, err := proto.Format(proto.VerbPriv, proto.Priv{
		ID: id, Nick: in.Nick, Data: in.Data, Timestamp: now, Sent: true,
	})
	if err != nil {
		c.log.Error("cannot format private message echo", "err", err)
		return
	}
	c.send(ctx, echo)

	// Recorded after delivery, like everything else. Both identities are
	// passed on: a store wants stable ids, not display names.
	c.app.recordPrivate(hook.Private{
		ID: id, From: c.id, To: target.id, Data: in.Data, At: at,
	})
}

// reply sends an ERR to this client alone.
func (c *conn) reply(ctx context.Context, description string) {
	frame, err := proto.Format(proto.VerbErr, proto.Err{Description: description})
	if err != nil {
		c.log.Error("cannot format ERR", "description", description, "err", err)
		return
	}
	c.send(ctx, frame)
}

// send writes a frame from this connection's own read pump. Failures are
// left for the next read to notice, so they are only logged.
func (c *conn) send(ctx context.Context, frame []byte) {
	c.write(ctx, frame)
}

// write puts one frame on the socket, reporting whether it worked. Every
// write is bounded: a client that stops reading must not wedge the
// goroutine writing to it.
func (c *conn) write(ctx context.Context, frame []byte) bool {
	wctx, cancel := context.WithTimeout(ctx, c.app.cfg.WriteTimeout.Duration())
	defer cancel()

	if err := c.ws.Write(wctx, websocket.MessageText, frame); err != nil {
		c.log.Debug("write failed", "err", err)
		c.ws.CloseNow() // unblocks the read pump
		return false
	}
	return true
}

// close sends a close frame with a reason, falling back to dropping the
// socket: a client being dropped for lagging is by definition not reading,
// so the polite version may not land.
func (c *conn) close(status websocket.StatusCode, reason string) {
	if err := c.ws.Close(status, reason); err != nil {
		c.ws.CloseNow()
	}
}
