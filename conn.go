package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/sztanpet/ws-chat/internal/hook"
	"github.com/sztanpet/ws-chat/internal/moderation"
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
	app  *app
	ws   *websocket.Conn
	priv chan []byte

	// memberships is the channels this connection is in, each with its own
	// subscription and its own write pump.
	membersMu   sync.RWMutex
	memberships map[string]*membership

	id    hook.Identity
	codec proto.Codec

	// limit is this connection's bucket — its own, or one shared with the
	// other connections of the same account. Nil means unlimited, which is
	// the default. releaseLimit hands a shared one back and is nil when
	// there is nothing to hand back.
	limit        *ratelimit.Bucket
	releaseLimit func()

	log *slog.Logger
}

// labels names one of this connection's goroutines for the profiler. The
// codec goes on every one of them because it is the one thing that varies
// between two connections doing identical work — a msgpack client and a
// JSON one encode and decode differently, and a profile that cannot tell
// them apart cannot say which.
//
// Both values come from a closed set, for the reason metric labels do: a
// nick or a channel name is client-controlled, and the profiler keeps a
// map of every label set it has seen.
func (c *conn) labels(task string) pprof.LabelSet {
	return pprof.Labels(labelTask, task, "codec", c.codec.Name())
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
		a.metrics.connectionsFailed.With("unauthorized").Inc()
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	case err != nil:
		a.metrics.connectionsFailed.With("authfailed").Inc()
		a.log.Error("authentication failed", "err", err, "remote", r.RemoteAddr)
		http.Error(w, "authentication unavailable", http.StatusInternalServerError)
		return
	}

	// The wire format is negotiated, not configured: a browser wants JSON
	// it can read in devtools, a bot moving traffic wants MessagePack, and
	// the server does not have to care which.
	// Barred before the handshake: a banned client gets an HTTP status it
	// can read rather than a socket that opens and shuts.
	if a.banned(id) {
		a.metrics.connectionsFailed.With("banned").Inc()
		refuseBanned(w)
		return
	}

	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns: a.cfg.AllowedOrigins,
		Subprotocols:   proto.Subprotocols(),
	})
	if err != nil {
		// Accept has already written the error response.
		a.metrics.connectionsFailed.With("upgrade").Inc()
		a.log.Debug("websocket upgrade rejected", "err", err, "remote", r.RemoteAddr)
		return
	}
	ws.SetReadLimit(a.cfg.MaxFrameSize)

	// A client that asked for nothing gets the default. A client that asked
	// for something we do not have never gets here: Accept leaves the
	// subprotocol empty, which is the same as not asking.
	codec, err := proto.ByName(ws.Subprotocol())
	if err != nil {
		a.metrics.connectionsFailed.With("subprotocol").Inc()
		a.log.Error("negotiated an unknown subprotocol", "subprotocol", ws.Subprotocol(), "err", err)
		ws.Close(websocket.StatusProtocolError, "unsupported subprotocol")
		return
	}
	a.metrics.connectionsTotal.Inc()
	a.metrics.codecs.With(codec.Name()).Inc()

	c := &conn{
		app:         a,
		ws:          ws,
		priv:        make(chan []byte, a.cfg.PrivBuffer),
		id:          id,
		codec:       codec,
		memberships: make(map[string]*membership),
	}
	c.limit, c.releaseLimit = a.clientLimiter(r.Context(), id)
	c.log = a.log.With("nick", c.nick(), "remote", r.RemoteAddr, "codec", codec.Name())

	// Deliberately not r.Context(): the connection is hijacked, and its
	// lifetime is now the server's shutdown, not the request's.
	//
	// This goroutine is net/http's, borrowed for as long as the connection
	// lasts, so the label replaces the listener's for that whole time. The
	// pumps started inside inherit it through the context and label
	// themselves over the top.
	pprof.Do(a.ctx, c.labels(taskConn), c.serve)
}

// serve runs the connection until either side gives up.
func (c *conn) serve(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	// Whatever happens below, a shared bucket goes back. Every early return
	// in here is a connection that never really started.
	if c.releaseLimit != nil {
		defer c.releaseLimit()
	}

	c.app.register(c)
	c.log.Debug("connected")

	// Only now is the connection real: addressable, and able to receive.
	// The handshake finished before any of that, so a client that talks
	// first would race its own setup.
	ready, err := c.codec.Encode(proto.NewReady(c.nick()))
	if err != nil {
		c.log.Error("cannot format READY", "err", err)
		_ = c.ws.CloseNow()
		c.app.unregister(c)
		return
	}
	if !c.write(ctx, ready) {
		c.app.unregister(c)
		return
	}

	var pumps sync.WaitGroup
	pumps.Add(1)
	go func() {
		defer pumps.Done()
		pprof.Do(ctx, c.labels(taskPrivPump), c.privPump)
	}()

	// Then wherever the connection belongs. Each join announces itself and
	// replays that channel's history, so a client's first frames are READY,
	// then a JOIN and a BACKLOG for each channel it landed in.
	for _, name := range c.app.autojoin(ctx, c.id) {
		if code := c.join(ctx, name, true); code != "" {
			// Told, not just logged. A client that was put somewhere by the
			// server and silently was not would spend the connection
			// wondering why the room is empty; an ERR names the reason it
			// is not there.
			c.reply(ctx, code)
			c.log.Warn("autojoin refused", "channel", name, "code", code)
		}
	}

	rerr := c.readPump(ctx)

	// Take the connection out of the directory first, so nobody queues a
	// private message that will never be written.
	c.app.unregister(c)

	// Leaving every channel ends the subscriptions its write pumps are
	// parked in and tells the rooms; cancelling the context unblocks the
	// private pump. Closing the socket is what unblocks a read pump
	// instead, which is why a write pump does that when it is the one to
	// fail.
	c.leaveAll(ctx)
	cancel()
	pumps.Wait()

	_ = c.ws.CloseNow()
	c.log.Debug("disconnected", "err", rerr)
}

// sendBacklog replays the recent history to a client that has just
// arrived.
//
// The frame is sent even when the history is empty, so the sequence a
// client sees is always READY, BACKLOG, then live traffic. A conditional
// frame would make "the room is new" and "the backlog is disabled" look
// the same to a client, and would make every client's first-frame handling
// a guess.
func (c *conn) sendBacklog(ctx context.Context, channel string) {
	if c.app.cfg.Backlog < 1 {
		return // disabled: no frame at all, which is the one other case
	}
	c.sendFrame(ctx, proto.NewBacklog(channel, c.app.backlog(ctx, channel)))
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

		// One decode for the verb and its arguments together. Dispatching
		// on a prefix and then parsing the rest would read the same bytes
		// twice, on every message anybody sends.
		var cmd proto.Command
		if err := c.codec.Decode(frame, &cmd); err != nil {
			c.reply(ctx, proto.ErrProtocol)
			continue
		}
		c.app.metrics.commandsTotal.With(cmd.Verb).Inc()

		switch cmd.Verb {
		case proto.VerbMsg:
			c.handleMsg(ctx, cmd)
		case proto.VerbPriv:
			c.handlePriv(ctx, cmd)
		case proto.VerbJoin:
			c.handleJoin(ctx, cmd)
		case proto.VerbPart:
			c.handlePart(ctx, cmd)
		case proto.VerbNames:
			c.handleNames(ctx, cmd)
		case proto.VerbMute, proto.VerbUnmute, proto.VerbBan, proto.VerbUnban:
			c.handleMod(ctx, cmd)
		case proto.VerbPing:
			c.sendFrame(ctx, proto.NewPong())
		default:
			c.reply(ctx, proto.ErrUnknown)
		}
	}
}

func (c *conn) handleMsg(ctx context.Context, cmd proto.Command) {
	switch {
	case cmd.Data == "":
		c.reply(ctx, proto.ErrEmpty)
		return
	case len(cmd.Data) > c.app.cfg.MaxMessage:
		c.reply(ctx, proto.ErrTooLong)
		return
	}

	// A message goes to a channel this connection is in. Speaking into a
	// room you are not in is refused rather than silently joining you: a
	// client that thinks it is somewhere it is not should find out.
	name := c.app.normalise(cmd.Channel)
	m, ok := c.joined(name)
	if !ok {
		c.reply(ctx, proto.ErrNotJoined)
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
	if !m.ch.limit.Allow() {
		c.reply(ctx, proto.ErrChanThrottled)
		return
	}
	if muted, _ := c.app.mod.Muted(name, c.id.Key()); muted {
		c.reply(ctx, proto.ErrMuted)
		return
	}

	if ok, reason := c.app.allow(ctx, c.id, cmd.Data); !ok {
		c.reply(ctx, reason)
		return
	}

	// The id and the timestamp are the server's to assign: a client's clock
	// is not evidence, and ordering has to come from one place.
	at := time.Now()
	id, err := c.app.broadcastMsg(m.ch, c.id, cmd.Data, at)
	if err != nil {
		c.log.Error("cannot encode outgoing message", "err", err)
		c.reply(ctx, proto.ErrProtocol)
		return
	}

	// Delivered first, persisted after. A store having a bad day must cost
	// history, never delivery.
	c.app.metrics.messagesTotal.Inc()
	c.app.recordMessage(hook.Message{ID: id, From: c.id, Data: cmd.Data, At: at})
}

// handlePriv delivers a message to one named client.
func (c *conn) handlePriv(ctx context.Context, cmd proto.Command) {
	switch {
	case cmd.Data == "":
		c.reply(ctx, proto.ErrEmpty)
		return
	case len(cmd.Data) > c.app.cfg.MaxMessage:
		c.reply(ctx, proto.ErrTooLong)
		return
	case cmd.Nick == c.nick():
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
	// Only a SERVER-WIDE mute reaches private messages. Being silenced in
	// one room is about that room; it is not a statement that somebody may
	// not talk to anybody at all, and treating it as one would make a
	// channel mute a bigger punishment than the moderator asked for.
	if muted, _ := c.app.mod.Muted(moderation.Global, c.id.Key()); muted {
		c.reply(ctx, proto.ErrMuted)
		return
	}

	if ok, reason := c.app.allow(ctx, c.id, cmd.Data); !ok {
		c.reply(ctx, reason)
		return
	}

	target, ok := c.app.lookup(cmd.Nick)
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
	frame, err := target.codec.Encode(proto.NewPriv(proto.Priv{
		ID: id, Nick: c.nick(), Data: cmd.Data, Timestamp: now,
		Roles: c.id.Roles, Attrs: c.id.Attrs,
	}))
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

	// Counted here rather than at the end, because the message has been
	// delivered here and the echo below is a blocking socket write. A
	// counter that lands after it can be scraped by somebody who is already
	// holding the message, which makes the metric look like it lost one.
	c.app.metrics.privateTotal.Inc()

	// The sender's echo names the recipient, so a client can render both
	// halves of a conversation from the same frame type.
	echo, err := c.codec.Encode(proto.NewPriv(proto.Priv{
		ID: id, Nick: cmd.Nick, Data: cmd.Data, Timestamp: now, Sent: true,
		Roles: target.id.Roles, Attrs: target.id.Attrs,
	}))
	if err != nil {
		c.log.Error("cannot format private message echo", "err", err)
		return
	}
	c.send(ctx, echo)

	// Recorded after delivery, like everything else. Both identities are
	// passed on: a store wants stable ids, not display names.
	c.app.recordPrivate(hook.Private{
		ID: id, From: c.id, To: target.id, Data: cmd.Data, At: at,
	})
}

// reply sends an ERR to this client alone.
// reply sends an ERR to this client alone, and counts it. Counting here
// rather than at each call site is what stops the metric drifting from what
// clients are actually told.
func (c *conn) reply(ctx context.Context, description string) {
	c.app.refused(description)
	c.sendFrame(ctx, proto.NewErr(description))
}

// sendFrame encodes and sends a frame to this client alone.
func (c *conn) sendFrame(ctx context.Context, payload proto.Outbound) {
	frame, err := c.codec.Encode(payload)
	if err != nil {
		c.log.Error("cannot encode frame", "err", err)
		return
	}
	c.send(ctx, frame)
}

// send writes a frame from this connection's own read pump. Failures are
// left for the next read to notice, so they are only logged.
func (c *conn) send(ctx context.Context, frame []byte) {
	c.write(ctx, frame)
}

// write puts one frame on the socket under a deadline of its own,
// reporting whether it worked. A client that stops reading must not wedge
// the goroutine writing to it.
//
// ctx is deliberately unused while a deadline does the bounding. It stays in
// the signature because putting the context back is what reverting to the
// released coder/websocket means — see the TODO in go.mod.
func (c *conn) write(ctx context.Context, frame []byte) bool {
	c.armDeadline()
	defer c.disarmDeadline()
	return c.writeTo(frame)
}

// armDeadline bounds every frame written until disarmDeadline by
// WriteTimeout, using the connection's own deadline rather than a context.
//
// An instant costs one atomic store. A context costs a WithTimeout here and
// a context.AfterFunc inside the library, per frame — which for a broadcast
// is per member, and measured at ~700 bytes of garbage per delivered
// message, none of it the message. See state/loadgen.md.
//
// TODO: Conn.SetWriteDeadline is not in a released coder/websocket yet, so
// go.mod pins a fork. The revert instructions are the TODO beside that
// replace directive.
func (c *conn) armDeadline() {
	_ = c.ws.SetWriteDeadline(time.Now().Add(c.app.cfg.WriteTimeout.Duration()))
}

func (c *conn) disarmDeadline() {
	_ = c.ws.SetWriteDeadline(time.Time{})
}

// writeBatch writes a wakeup's worth of frames under ONE deadline.
//
// It bounds a wakeup rather than a write, which is the stricter of the two
// where it matters. A batch of sixteen frames would otherwise be allowed
// sixteen times WriteTimeout to drain, so a client that reads just fast
// enough to keep restarting the clock could hold its pump indefinitely.
//
// What originally forced one deadline per batch was its cost — a context, a
// timer and three allocations, paid once per member of the room. That cost
// is gone now that a deadline is an instant (see armDeadline), so this is
// kept for the stricter bound alone, which is the better reason anyway.
//
// ctx is unused here for the same reason it is unused in write.
func (c *conn) writeBatch(ctx context.Context, frames [][]byte) bool {
	if len(frames) == 0 {
		return true
	}

	c.armDeadline()
	defer c.disarmDeadline()

	for _, frame := range frames {
		if !c.writeTo(frame) {
			return false
		}
	}
	return true
}

// writeTo puts one frame on the socket under the deadline armDeadline set.
//
// The context is deliberately context.Background(). A cancellable one makes
// the library arm a context.AfterFunc per frame, which is the allocation the
// deadline exists to avoid. Nothing is lost: the write is bounded by the
// deadline instead, and shutdown still unblocks a stuck write, because it
// closes the connection and every wait inside the library selects on that.
func (c *conn) writeTo(frame []byte) bool {
	if err := c.ws.Write(context.Background(), c.msgType(), frame); err != nil {
		c.log.Debug("write failed", "err", err)
		_ = c.ws.CloseNow() // unblocks the read pump
		return false
	}
	return true
}

// close sends a close frame with a reason, falling back to dropping the
// socket: a client being dropped for lagging is by definition not reading,
// so the polite version may not land.
func (c *conn) close(status websocket.StatusCode, reason string) {
	if err := c.ws.Close(status, reason); err != nil {
		_ = c.ws.CloseNow()
	}
}

// handleJoin puts the connection into a channel it asked for.
func (c *conn) handleJoin(ctx context.Context, cmd proto.Command) {
	if code := c.join(ctx, cmd.Channel, false); code != "" {
		c.reply(ctx, code)
	}
}

// handlePart takes it out of one.
func (c *conn) handlePart(ctx context.Context, cmd proto.Command) {
	if code := c.part(ctx, cmd.Channel); code != "" {
		c.reply(ctx, code)
	}
}

// handleNames answers with who is in a channel.
//
// Membership is required: who is in a room is the room's business, and a
// server that answers it for anybody is a server that can be enumerated by
// anybody.
func (c *conn) handleNames(ctx context.Context, cmd proto.Command) {
	name := c.app.normalise(cmd.Channel)
	m, ok := c.joined(name)
	if !ok {
		c.reply(ctx, proto.ErrNotJoined)
		return
	}

	nicks, total := m.ch.names()
	c.sendFrame(ctx, proto.NewNames(name, nicks, total))
}
