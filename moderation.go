package main

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/coder/websocket"

	"github.com/sztanpet/ws-chat/internal/hook"
	"github.com/sztanpet/ws-chat/internal/proto"
)

// Moderation: the four commands, what they do, and how they are announced.
//
// Every one of them ends in the same place — a MOD frame to the whole
// channel, including the person it is about. Moderation that happens
// invisibly gets re-litigated in the room by people guessing at what
// happened, and a client that is silently ignored will assume the server
// is broken and reconnect in a loop.

// handleMod runs one of MUTE, UNMUTE, BAN or UNBAN.
func (c *conn) handleMod(ctx context.Context, cmd proto.Command) {
	if !c.app.canModerate(ctx, c.id) {
		c.reply(ctx, proto.ErrForbidden)
		return
	}
	if cmd.Nick == "" {
		c.reply(ctx, proto.ErrProtocol)
		return
	}

	until, err := deadline(cmd.Duration)
	if err != nil {
		c.reply(ctx, proto.ErrBadDuration)
		return
	}

	// Moderation names a person, and the only people the server knows about
	// are the ones connected. Acting on somebody who has already left needs
	// the directory to resolve a name to a key, which is a hook away and
	// noted in state/server.md.
	target, ok := c.app.lookup(cmd.Nick)
	if !ok {
		c.reply(ctx, proto.ErrNoSuch)
		return
	}
	key := target.id.Key()

	var action string
	switch cmd.Verb {
	case proto.VerbMute:
		c.app.mod.Mute(key, until)
		action = proto.ActionMute
	case proto.VerbUnmute:
		c.app.mod.Unmute(key)
		action = proto.ActionUnmute
		until = time.Time{}
	case proto.VerbBan:
		c.app.mod.Ban(key, until)
		action = proto.ActionBan
	case proto.VerbUnban:
		c.app.mod.Unban(key)
		action = proto.ActionUnban
		until = time.Time{}
	default:
		c.reply(ctx, proto.ErrUnknown)
		return
	}

	at := time.Now()
	id, err := c.app.broadcast(func(id uint64) proto.Outbound {
		return proto.NewMod(proto.Mod{
			ID:        id,
			Action:    action,
			Nick:      cmd.Nick,
			By:        c.nick(),
			Timestamp: at.UnixMilli(),
			Until:     millis(until),
			Reason:    cmd.Reason,
		})
	})
	if err != nil {
		c.log.Error("cannot encode moderation action", "action", action, "err", err)
		c.reply(ctx, proto.ErrProtocol)
		return
	}

	c.app.recordModeration(hook.Moderation{
		ID:     id,
		Action: action,
		By:     c.id,
		Target: cmd.Nick,
		Key:    key,
		Reason: cmd.Reason,
		Until:  until,
		At:     at,
	})

	// A ban ends the connection. The announcement went to the room first,
	// but the banned client will usually NOT see it: its write pump has to
	// be scheduled to drain the ring, and the socket is closing now. That
	// is why the close frame carries a reason of its own — waiting for the
	// write pump instead would let a slow client delay its own ban.
	if action == proto.ActionBan {
		c.app.disconnect(target, "banned")
	}

	c.log.Info("moderation", "action", action, "target", cmd.Nick, "until", until, "reason", cmd.Reason)
}

// deadline turns a duration string into an absolute time. An empty string
// means the action does not expire.
func deadline(d string) (time.Time, error) {
	if d == "" {
		return time.Time{}, nil
	}
	parsed, err := time.ParseDuration(d)
	if err != nil {
		return time.Time{}, err
	}
	if parsed <= 0 {
		return time.Time{}, errBadDuration
	}
	return time.Now().Add(parsed), nil
}

var errBadDuration = errors.New("duration must be positive")

// millis is the unix-millisecond form of a deadline, or zero for one that
// never expires.
func millis(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

// disconnect closes somebody else's connection with a reason.
func (a *app) disconnect(target *conn, reason string) {
	target.close(websocket.StatusPolicyViolation, reason)
}

// banned reports whether this identity is barred, for the check at connect
// time.
func (a *app) banned(id hook.Identity) bool {
	barred, _ := a.mod.Banned(id.Key())
	return barred
}

// refuseBanned answers a connection attempt from somebody barred. It
// happens before the upgrade, so the client gets an HTTP status rather than
// a socket that opens and immediately shuts.
func refuseBanned(w http.ResponseWriter) {
	http.Error(w, "banned", http.StatusForbidden)
}
