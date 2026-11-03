package main

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/coder/websocket"

	"github.com/sztanpet/ws-chat/internal/hook"
	"github.com/sztanpet/ws-chat/internal/moderation"
	"github.com/sztanpet/ws-chat/internal/proto"
)

// The command's channel, empty meaning server-wide.
type modScope = string

func (c *conn) handleMod(ctx context.Context, cmd proto.Command) {
	// Not normalised: empty is server-wide here, not the default channel.
	scope := cmd.Channel

	if !c.app.canModerate(ctx, c.id, scope) {
		c.reply(ctx, proto.ErrForbidden)
		return
	}
	if cmd.Nick == "" {
		c.reply(ctx, proto.ErrProtocol)
		return
	}
	if scope != moderation.Global && !validChannel(scope) {
		c.reply(ctx, proto.ErrNoChannel)
		return
	}

	until, err := deadline(cmd.Duration)
	if err != nil {
		c.reply(ctx, proto.ErrBadDuration)
		return
	}

	target, id, ok := c.app.resolve(ctx, cmd.Nick)
	if !ok {
		c.reply(ctx, proto.ErrNoSuch)
		return
	}
	key := id.Key()

	var action string
	switch cmd.Verb {
	case proto.VerbMute:
		c.app.mod.Mute(scope, key, until)
		action = proto.ActionMute
	case proto.VerbUnmute:
		c.app.mod.Unmute(scope, key)
		action = proto.ActionUnmute
		until = time.Time{}
	case proto.VerbBan:
		c.app.mod.Ban(scope, key, until)
		action = proto.ActionBan
	case proto.VerbUnban:
		c.app.mod.Unban(scope, key)
		action = proto.ActionUnban
		until = time.Time{}
	default:
		c.reply(ctx, proto.ErrUnknown)
		return
	}

	// Removed before the announcement: after, the MOD would sit in a closing
	// subscription and need a direct copy, which some would then see twice.
	if action == proto.ActionBan && scope != moderation.Global && target != nil {
		target.part(ctx, scope)
	}

	at := time.Now()
	mod := proto.Mod{
		Scope:     scope,
		Action:    action,
		Nick:      cmd.Nick,
		By:        c.nick(),
		Timestamp: at.UnixMilli(),
		Until:     millis(until),
		Reason:    cmd.Reason,
	}
	announced, err := c.app.announceModeration(target, scope, mod)
	if err != nil {
		c.log.Error("cannot encode moderation action", "action", action, "err", err)
		c.reply(ctx, proto.ErrProtocol)
		return
	}

	c.app.recordSanction(hook.Moderation{
		ID:     announced,
		Action: action,
		Scope:  scope,
		By:     c.id,
		Target: cmd.Nick,
		Key:    key,
		Reason: cmd.Reason,
		Until:  until,
		At:     at,
	})

	if action == proto.ActionBan && target != nil {
		mod.ID, mod.Channel = announced, scope
		c.app.enforceBan(ctx, target, scope, mod)
	}

	c.app.metrics.moderationTotal.With(action).Inc()
	c.log.Info("moderation", "action", action, "scope", scope, "target", cmd.Nick,
		"until", until, "reason", cmd.Reason)
}

// A server-wide ban closes with a reason, as its MOD races the closing socket;
// a channel ban sends the MOD directly, the room's copy no longer reaching them.
func (a *app) enforceBan(ctx context.Context, target *conn, scope modScope, mod proto.Mod) {
	if scope == moderation.Global {
		target.close(websocket.StatusPolicyViolation, "banned")
		return
	}
	target.sendFrame(ctx, proto.NewMod(mod))
}

// Server-wide goes to every channel the target is in; absent, to none.
func (a *app) announceModeration(target *conn, scope modScope, mod proto.Mod) (uint64, error) {
	var channels []*channel

	if scope != moderation.Global {
		ch, ok := a.lookupChannel(scope)
		if !ok {
			// Nobody to tell, but the action stands: it is filed against the scope.
			return a.seq.Add(1), nil
		}
		channels = []*channel{ch}
	} else if target != nil {
		target.membersMu.RLock()
		for _, m := range target.memberships {
			channels = append(channels, m.ch)
		}
		target.membersMu.RUnlock()
	}

	var last uint64
	for _, ch := range channels {
		id, err := ch.broadcastTo(a, func(id uint64) proto.Outbound {
			mod.ID = id
			mod.Channel = ch.name
			return proto.NewMod(mod)
		}, nil)
		if err != nil {
			return 0, err
		}
		last = id
	}
	if last == 0 {
		last = a.seq.Add(1) // nothing announced, but the action still has an id
	}
	return last, nil
}

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

func millis(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

func (a *app) banned(id hook.Identity) bool {
	barred, _ := a.mod.Banned(moderation.Global, id.Key())
	return barred
}

func refuseBanned(w http.ResponseWriter) {
	http.Error(w, "banned", http.StatusForbidden)
}
