package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/sztanpet/ws-chat/internal/hook"
	"github.com/sztanpet/ws-chat/internal/moderation"
	"github.com/sztanpet/ws-chat/internal/proto"
	"github.com/sztanpet/ws-chat/internal/ratelimit"
)

// A private message and a broadcast have no relative order: they are two streams.
type conn struct {
	app  *app
	ws   *websocket.Conn
	priv chan []byte

	sock *corkConn

	membersMu   sync.RWMutex
	memberships map[string]*membership

	id    hook.Identity
	codec proto.Codec

	limit        *ratelimit.Bucket
	releaseLimit func()

	log *slog.Logger
}

// Closed-set values only: the profiler keeps every label set it has seen.
func (c *conn) labels(task string) pprof.LabelSet {
	return pprof.Labels(labelTask, task, "codec", c.codec.Name())
}

func (c *conn) msgType() websocket.MessageType {
	if c.codec.Binary() {
		return websocket.MessageBinary
	}
	return websocket.MessageText
}

func (c *conn) nick() string { return c.id.Nick }

func (a *app) handleWS(w http.ResponseWriter, r *http.Request) {
	// Before the upgrade, so a refusal is an HTTP status the client can read.
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

	// Before the handshake, so a ban is an HTTP status, not a socket that shuts.
	if a.banned(id) {
		a.metrics.connectionsFailed.With("banned").Inc()
		refuseBanned(w)
		return
	}

	hc := &hijackCapture{ResponseWriter: w}
	ws, err := websocket.Accept(hc, r, &websocket.AcceptOptions{
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

	// Empty also when the client offered only codecs we lack: Accept drops them.
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
		sock:        hc.sock,
		priv:        make(chan []byte, a.cfg.PrivBuffer),
		id:          id,
		codec:       codec,
		memberships: make(map[string]*membership),
	}
	c.limit, c.releaseLimit = a.clientLimiter(r.Context(), id)
	c.log = a.log.With("nick", c.nick(), "remote", r.RemoteAddr, "codec", codec.Name())

	// Not r.Context(): once hijacked, the connection lives as long as the server.
	pprof.Do(a.ctx, c.labels(taskConn), c.serve)
}

func (c *conn) serve(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	if c.releaseLimit != nil {
		defer c.releaseLimit()
	}

	c.app.register(c)
	c.log.Debug("connected")

	// READY only now that it is registered: before, a client would race its setup.
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
	pumps.Go(func() {
		pprof.Do(ctx, c.labels(taskPrivPump), c.privPump)
	})

	for _, name := range c.app.autojoin(ctx, c.id) {
		if code := c.join(ctx, name, true); code != "" {
			// Told, not just logged, or the client wonders why the room is empty.
			c.reply(ctx, code)
			c.log.Warn("autojoin refused", "channel", name, "code", code)
		}
	}

	rerr := c.readPump(ctx)

	// First, so nobody queues a private message that will never be written.
	c.app.unregister(c)

	c.leaveAll(ctx)
	cancel()
	pumps.Wait()

	_ = c.ws.CloseNow()
	c.log.Debug("disconnected", "err", rerr)
}

// Sent even when empty, so an empty room and a disabled backlog differ.
func (c *conn) sendBacklog(ctx context.Context, channel string) {
	if c.app.cfg.Backlog < 1 {
		return // disabled: the one case with no frame
	}
	c.sendFrame(ctx, proto.NewBacklog(channel, c.app.backlog(ctx, channel)))
}

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

// Runs on the sender's goroutine, so it never blocks on the recipient.
func (c *conn) deliver(frame []byte) bool {
	select {
	case c.priv <- frame:
		return true
	default:
		return false
	}
}

func (c *conn) readPump(ctx context.Context) error {
	for {
		// Clients must PING: a silent one is gone or broken, and we cannot tell which.
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

	// Refused, not joined: a client wrong about where it is should find out.
	name := c.app.normalise(cmd.Channel)
	m, ok := c.joined(name)
	if !ok {
		c.reply(ctx, proto.ErrNotJoined)
		return
	}

	// Spent even on messages refused later, or sending garbage would be free.
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

	at := time.Now()
	id, err := c.app.broadcastMsg(m.ch, c.id, cmd.Data, at)
	if err != nil {
		c.log.Error("cannot encode outgoing message", "err", err)
		c.reply(ctx, proto.ErrProtocol)
		return
	}

	// After delivery: a bad day for the store must cost history, never delivery.
	c.app.metrics.messagesTotal.Inc()
	c.app.recordMessage(hook.Message{ID: id, From: c.id, Data: cmd.Data, At: at})
}

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

	// Not the channel's limit: a private message must not starve it, or be starved.
	if !c.limit.Allow() {
		c.reply(ctx, proto.ErrThrottled)
		return
	}
	// Only a server-wide mute: a channel mute is about that channel.
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

	// In the recipient's codec: the two ends negotiated separately.
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
		// Said, not silent: a sender cannot otherwise tell a full queue from delivery.
		c.reply(ctx, proto.ErrBacklog)
		return
	}

	// Before the echo, a blocking write: the recipient may already hold the message.
	c.app.metrics.privateTotal.Inc()

	echo, err := c.codec.Encode(proto.NewPriv(proto.Priv{
		ID: id, Nick: cmd.Nick, Data: cmd.Data, Timestamp: now, Sent: true,
		Roles: target.id.Roles, Attrs: target.id.Attrs,
	}))
	if err != nil {
		c.log.Error("cannot format private message echo", "err", err)
		return
	}
	c.send(ctx, echo)

	c.app.recordPrivate(hook.Private{
		ID: id, From: c.id, To: target.id, Data: cmd.Data, At: at,
	})
}

// Counted where it is sent, so the metric cannot drift from what clients hear.
func (c *conn) reply(ctx context.Context, description string) {
	c.app.refused(description)
	c.sendFrame(ctx, proto.NewErr(description))
}

func (c *conn) sendFrame(ctx context.Context, payload proto.Outbound) {
	frame, err := c.codec.Encode(payload)
	if err != nil {
		c.log.Error("cannot encode frame", "err", err)
		return
	}
	c.send(ctx, frame)
}

// The result is ignored: a failed write closed the socket, and the read sees it.
func (c *conn) send(ctx context.Context, frame []byte) {
	c.write(ctx, frame)
}

func (c *conn) write(ctx context.Context, frame []byte) bool {
	c.cork()
	return c.uncork(c.writeTo(frame))
}

func (c *conn) cork() {
	c.sock.cork(c.app.cfg.WriteTimeout.Duration())
}

// Uncorked before closing, or a held close frame would go down with the socket.
func (c *conn) uncork(err error) bool {
	if serr := c.sock.uncork(); err == nil {
		err = serr
	}
	if err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded) {
			// A drop like ErrLagged's, but blind: no close frame fits a full socket.
			c.app.metrics.writeTimeouts.Inc()
			c.log.Info("dropped: write timed out")
		} else {
			c.log.Debug("write failed", "err", err)
		}
		_ = c.ws.CloseNow() // unblocks the read pump
		return false
	}
	return true
}

// One deadline per batch, not per frame, so a slow reader cannot restart it.
func (c *conn) writeBatch(ctx context.Context, frames [][]byte) bool {
	if len(frames) == 0 {
		return true
	}

	c.cork()
	var err error
	for _, frame := range frames {
		if err = c.writeTo(frame); err != nil {
			break
		}
	}
	return c.uncork(err)
}

// Background, not ctx: a cancellable one costs an AfterFunc per frame.
func (c *conn) writeTo(frame []byte) error {
	return c.ws.Write(context.Background(), c.msgType(), frame)
}

// Falls back to dropping the socket: a lagging client may never read the close.
func (c *conn) close(status websocket.StatusCode, reason string) {
	if err := c.ws.Close(status, reason); err != nil {
		_ = c.ws.CloseNow()
	}
}

func (c *conn) handleJoin(ctx context.Context, cmd proto.Command) {
	if code := c.join(ctx, cmd.Channel, false); code != "" {
		c.reply(ctx, code)
	}
}

func (c *conn) handlePart(ctx context.Context, cmd proto.Command) {
	if code := c.part(ctx, cmd.Channel); code != "" {
		c.reply(ctx, code)
	}
}

// Members only, or anybody could enumerate any room.
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
