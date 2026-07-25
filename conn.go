package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/sztanpet/ws-chat/internal/broadcast"
	"github.com/sztanpet/ws-chat/internal/hook"
	"github.com/sztanpet/ws-chat/internal/proto"
	"github.com/sztanpet/ws-chat/internal/ratelimit"
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
	app   *app
	ws    *websocket.Conn
	sub   broadcast.Sub
	priv  chan []byte
	id    hook.Identity
	codec proto.Codec

	// limit is this connection's own bucket. Nil means unlimited, which is
	// the default.
	limit *ratelimit.Bucket

	log *slog.Logger
}

// msgType is the WebSocket message type this connection's codec speaks.
func (c *conn) msgType() websocket.MessageType {
	if c.codec.Binary() {
		return websocket.MessageBinary
	}
	return websocket.MessageText
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

	// The wire format is negotiated, not configured: a browser wants JSON
	// it can read in devtools, a bot moving traffic wants MessagePack, and
	// the server does not have to care which.
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns: a.cfg.AllowedOrigins,
		Subprotocols:   proto.Names(),
	})
	if err != nil {
		// Accept has already written the error response.
		a.log.Debug("websocket upgrade rejected", "err", err, "remote", r.RemoteAddr)
		return
	}
	ws.SetReadLimit(a.cfg.MaxFrameSize)

	// A client that asked for nothing gets the default. A client that asked
	// for something we do not have never gets here: Accept leaves the
	// subprotocol empty, which is the same as not asking.
	codec, err := proto.ByName(ws.Subprotocol())
	if err != nil {
		a.log.Error("negotiated an unknown subprotocol", "subprotocol", ws.Subprotocol(), "err", err)
		ws.Close(websocket.StatusProtocolError, "unsupported subprotocol")
		return
	}

	c := &conn{
		app:   a,
		ws:    ws,
		priv:  make(chan []byte, a.cfg.PrivBuffer),
		id:    id,
		codec: codec,
		limit: a.clientLimiter(r.Context(), id),
	}
	c.log = a.log.With("nick", c.nick(), "remote", r.RemoteAddr, "codec", codec.Name())

	// Deliberately not r.Context(): the connection is hijacked, and its
	// lifetime is now the server's shutdown, not the request's.
	c.serve(a.ctx)
}

// serve runs the connection until either side gives up.
func (c *conn) serve(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	c.sub = c.app.bcs[c.codec.Name()].Subscribe()
	c.app.register(c)
	c.log.Debug("connected")

	// Only now is the connection real: subscribed, addressable, and able to
	// receive. The handshake finished before any of that, so a client that
	// talks first would race its own setup.
	ready, err := c.codec.Encode(proto.VerbReady, proto.Ready{Nick: c.nick()})
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

		if typ != c.msgType() {
			c.reply(ctx, proto.ErrFraming)
			continue
		}

		verb, payload, err := c.codec.Decode(frame)
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
			c.sendVerb(ctx, proto.VerbPong, nil)
		default:
			c.reply(ctx, proto.ErrUnknown)
		}
	}
}

func (c *conn) handleMsg(ctx context.Context, payload []byte) {
	var in proto.In
	if err := c.codec.Unmarshal(payload, &in); err != nil {
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

	// Limits before the filter: the check is a nil comparison or a mutex,
	// where the filter may be a lookup, and a throttled client should not
	// cost the filter anything at all.
	//
	// Both are spent whether or not the message survives what comes after.
	// A rate limit that only counts successful messages is not a rate
	// limit — sending garbage would be free.
	if !c.limit.Allow() {
		c.reply(ctx, proto.ErrThrottled)
		return
	}
	if !c.app.chanLimit.Allow() {
		c.reply(ctx, proto.ErrChanThrottled)
		return
	}

	if ok, reason := c.app.allow(ctx, c.id, in.Data); !ok {
		c.reply(ctx, reason)
		return
	}

	// The id and the timestamp are the server's to assign: a client's clock
	// is not evidence, and ordering has to come from one place.
	at := time.Now()
	id, err := c.app.broadcast(proto.VerbMsg, func(id uint64) any {
		return proto.Msg{
			ID:        id,
			Nick:      c.nick(),
			Data:      in.Data,
			Timestamp: at.UnixMilli(),
		}
	})
	if err != nil {
		c.log.Error("cannot encode outgoing message", "err", err)
		c.reply(ctx, proto.ErrProtocol)
		return
	}

	// Delivered first, persisted after. A store having a bad day must cost
	// history, never delivery.
	c.app.recordMessage(hook.Message{ID: id, From: c.id, Data: in.Data, At: at})
}

// handlePriv delivers a message to one named client.
func (c *conn) handlePriv(ctx context.Context, payload []byte) {
	var in proto.InPriv
	if err := c.codec.Unmarshal(payload, &in); err != nil {
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

	// The client's own limit applies; the channel's does not, because a
	// private message is not in the channel and must not be able to starve
	// it — or be starved by it.
	if !c.limit.Allow() {
		c.reply(ctx, proto.ErrThrottled)
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

	// The recipient's copy names the sender, and is encoded with the
	// RECIPIENT's codec: the two ends of a private message negotiated
	// separately and need not agree. This is the one place a frame crosses
	// from one connection to another, which is why it is the one place that
	// has to think about it — broadcasts cannot, and that is a real
	// constraint on channels, noted in CLAUDE.md.
	frame, err := target.codec.Encode(proto.VerbPriv, proto.Priv{
		ID: id, Nick: c.nick(), Data: in.Data, Timestamp: now,
	})
	if err != nil {
		c.log.Error("cannot encode private message", "err", err)
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
	echo, err := c.codec.Encode(proto.VerbPriv, proto.Priv{
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
	c.sendVerb(ctx, proto.VerbErr, proto.Err{Description: description})
}

// sendVerb encodes and sends a frame to this client alone.
func (c *conn) sendVerb(ctx context.Context, verb string, payload any) {
	frame, err := c.codec.Encode(verb, payload)
	if err != nil {
		c.log.Error("cannot encode frame", "verb", verb, "err", err)
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

	if err := c.ws.Write(wctx, c.msgType(), frame); err != nil {
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
