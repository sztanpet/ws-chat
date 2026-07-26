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

// Moderation: the four commands, what they do, and how they are announced.
//
// Every one of them ends in a MOD frame, because moderation that happens
// invisibly gets re-litigated in the room by people guessing at what
// happened, and a client that is silently ignored will assume the server is
// broken and reconnect in a loop.
//
// The command's channel is the SCOPE. Naming one mutes or bans somebody
// there and nowhere else; leaving it empty does it server-wide. That is the
// whole of the distinction, and it falls out of a field the protocol
// already had:
//
//	MUTE {"nick":"someone","channel":"main"}   silenced in main
//	MUTE {"nick":"someone"}                    silenced everywhere
//
// Authorization is asked per scope, so running one room is not the same
// permission as silencing somebody across the server.
type modScope = string

// handleMod runs one of MUTE, UNMUTE, BAN or UNBAN.
func (c *conn) handleMod(ctx context.Context, cmd proto.Command) {
	// Deliberately NOT normalised: an empty channel means server-wide here,
	// where on a MSG it means the default channel. Running it through
	// normalise would turn every server-wide command into a main-channel
	// one, silently.
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

	// Moderation names a person, connected or not. The connection
	// directory answers for anybody here; the Directory hook answers for
	// everybody else, which is what makes a ban liftable — the person it
	// barred is by definition not connected.
	//
	// target is nil when they are not here. Everything below has to cope
	// with that: there is nobody to remove from a channel and, for a
	// server-wide action, no room to announce it in.
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

	// A channel ban removes the target BEFORE the announcement, and the
	// order is deliberate. Announcing first and removing after means the
	// frame is in the channel's ring while the target's subscription is
	// being closed — which discards it — so it would have to be written
	// directly as well, and would then arrive twice for anybody whose pump
	// had already drained it. Removing first means one copy of everything:
	// the room sees PART then MOD, and so does the person it happened to.
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

// enforceBan finishes what a ban started.
//
// A server-wide ban ends the connection. The announcement went out first,
// though a disconnected client will usually not see it — its write pump is
// racing its own socket closing, which is why the close frame carries a
// reason of its own.
//
// A channel ban only takes them out of that channel, which is the
// difference the scope buys, and that removal has already happened by the
// time this runs. All that is left is telling them why: they are no longer
// subscribed, so the room's copy of the announcement will not reach them
// and this direct one is the only copy they get.
func (a *app) enforceBan(ctx context.Context, target *conn, scope modScope, mod proto.Mod) {
	if scope == moderation.Global {
		target.close(websocket.StatusPolicyViolation, "banned")
		return
	}
	target.sendFrame(ctx, proto.NewMod(mod))
}

// announceModeration puts a MOD frame where the people who need it are.
//
// A channel action is announced in that channel, whether or not the person
// it names is connected — the room is entitled to know somebody was banned
// from it after they left. A server-wide one is announced in every channel
// the target is in, because that is where the people who saw what they did
// are; if they are not connected there is nowhere to announce it, and the
// action is recorded and applies the moment they come back.
func (a *app) announceModeration(target *conn, scope modScope, mod proto.Mod) (uint64, error) {
	var channels []*channel

	if scope != moderation.Global {
		ch, ok := a.lookupChannel(scope)
		if !ok {
			// Nobody is in it, so there is nobody to tell. The action still
			// stands: it is filed against the scope, not against a channel
			// object, and it applies when somebody next tries to join.
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

// banned reports whether this identity is barred from the server, for the
// check at connect time. A channel ban is checked when they try to join it.
func (a *app) banned(id hook.Identity) bool {
	barred, _ := a.mod.Banned(moderation.Global, id.Key())
	return barred
}

// refuseBanned answers a connection attempt from somebody barred. It
// happens before the upgrade, so the client gets an HTTP status rather than
// a socket that opens and immediately shuts.
func refuseBanned(w http.ResponseWriter) {
	http.Error(w, "banned", http.StatusForbidden)
}
