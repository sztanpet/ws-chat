package main

import (
	"context"
	"errors"

	"github.com/coder/websocket"

	"github.com/sztanpet/ws-chat/internal/broadcast"
	"github.com/sztanpet/ws-chat/internal/proto"
)

// Membership: joining, leaving, and the goroutine that feeds a connection
// one channel's traffic.
//
// There is a write pump per membership rather than one multiplexing over
// all of them, because Recv blocks and cannot be selected on. A merge
// channel in front of the socket would fix that and would also put a
// channel hop back in front of every broadcast message, which is the thing
// internal/broadcast exists to avoid. So a connection in five channels runs
// five pumps, and coder/websocket serializes their writes.

// membership is one connection's place in one channel.
type membership struct {
	ch   *channel
	sub  broadcast.Sub
	done chan struct{} // closed when its pump has stopped
}

// join puts a connection into a channel, announces it, and starts feeding
// it. It reports the error code to send back, or "" on success.
//
// force skips the CanJoin hook and is used for autojoin: a layer that put
// somebody somewhere has already decided they may be there.
func (c *conn) join(ctx context.Context, name string, force bool) string {
	name = c.app.normalise(name)
	if !validChannel(name) {
		return proto.ErrNoChannel
	}

	c.membersMu.Lock()
	if _, ok := c.memberships[name]; ok {
		c.membersMu.Unlock()
		return proto.ErrAlreadyJoined
	}
	if len(c.memberships) >= c.app.cfg.MaxChannelsPerConn {
		c.membersMu.Unlock()
		return proto.ErrTooManyChans
	}
	c.membersMu.Unlock()

	if !force {
		if ok, reason := c.app.canJoin(ctx, c.id, name); !ok {
			if reason == "" {
				reason = proto.ErrForbidden
			}
			return reason
		}
	}

	ch, ok := c.app.channelFor(ctx, name)
	if !ok {
		return proto.ErrTooManyChans
	}

	m := &membership{ch: ch, sub: ch.bcs[c.codec.Name()].Subscribe(), done: make(chan struct{})}

	c.membersMu.Lock()
	if _, exists := c.memberships[name]; exists {
		// Somebody else's JOIN won the race. Only one read pump per
		// connection sends commands, so this can only happen between an
		// autojoin and a JOIN in flight; give up the subscription rather
		// than leak it.
		c.membersMu.Unlock()
		m.sub.Close()
		close(m.done)
		return proto.ErrAlreadyJoined
	}
	c.memberships[name] = m
	c.membersMu.Unlock()

	ch.add(c)

	// The order of the next three matters, and it is why the pump starts
	// last.
	//
	// The subscription is already open, so nothing said from here on can be
	// missed. The backlog is written directly, while no pump is running for
	// this channel, so nothing can interleave with it. Then the JOIN is
	// broadcast and the pump starts, which delivers that JOIN and
	// everything after it in order.
	//
	// A client therefore sees exactly: BACKLOG, its own JOIN, then live
	// traffic. Starting the pump earlier makes those first two race, which
	// they did.
	c.sendBacklog(ctx, name)

	// Told to the channel, including the person who asked — which is how
	// they learn it worked, and how everybody else learns they are here.
	if _, err := ch.broadcastTo(c.app, func(uint64) proto.Outbound {
		return proto.NewJoin(proto.Join{
			Channel: name, Nick: c.nick(), Roles: c.id.Roles, Attrs: c.id.Attrs,
		})
	}, nil); err != nil {
		c.log.Error("cannot encode JOIN", "channel", name, "err", err)
	}

	go c.channelPump(ctx, m)

	c.app.metrics.joinsTotal.Inc()
	c.log.Debug("joined", "channel", name)
	return ""
}

// part takes a connection out of a channel and announces it.
func (c *conn) part(ctx context.Context, name string) string {
	name = c.app.normalise(name)

	c.membersMu.Lock()
	m, ok := c.memberships[name]
	if ok {
		delete(c.memberships, name)
	}
	c.membersMu.Unlock()

	if !ok {
		return proto.ErrNotJoined
	}

	// The parting client is told directly, before it stops receiving.
	// Broadcasting and hoping is not enough: ending the subscription
	// discards whatever it had not read yet, so its own PART would be the
	// message most likely to be lost. Everybody else hears it from the
	// channel, which is why this is not simply a broadcast.
	c.sendFrame(ctx, proto.NewPart(name, c.nick()))
	c.leave(m)

	if _, err := m.ch.broadcastTo(c.app, func(uint64) proto.Outbound {
		return proto.NewPart(name, c.nick())
	}, nil); err != nil {
		c.log.Error("cannot encode PART", "channel", name, "err", err)
	}

	c.app.metrics.partsTotal.Inc()
	c.log.Debug("parted", "channel", name)
	return ""
}

// leave ends a membership: out of the member list, subscription closed,
// pump stopped.
func (c *conn) leave(m *membership) {
	m.ch.remove(c)
	m.sub.Close() // unblocks the pump, which is parked in Recv
	<-m.done
}

// leaveAll takes a connection out of everything, on disconnect.
//
// The PART frames go out because the room is entitled to know somebody
// left, whether they asked to or their socket did.
func (c *conn) leaveAll(ctx context.Context) {
	c.membersMu.Lock()
	memberships := make(map[string]*membership, len(c.memberships))
	for name, m := range c.memberships {
		memberships[name] = m
	}
	c.memberships = make(map[string]*membership)
	c.membersMu.Unlock()

	for name, m := range memberships {
		c.leave(m)
		if _, err := m.ch.broadcastTo(c.app, func(uint64) proto.Outbound {
			return proto.NewPart(name, c.nick())
		}, nil); err != nil {
			c.log.Debug("cannot encode PART on teardown", "channel", name, "err", err)
		}
	}
}

// joined reports the membership for a channel.
func (c *conn) joined(name string) (*membership, bool) {
	c.membersMu.RLock()
	defer c.membersMu.RUnlock()
	m, ok := c.memberships[name]
	return m, ok
}

// channelPump feeds this connection one channel's traffic.
func (c *conn) channelPump(ctx context.Context, m *membership) {
	defer close(m.done)

	batch := make([][]byte, c.app.cfg.WriteBatch)
	for {
		n, err := m.sub.Recv(batch)

		for _, frame := range batch[:n] {
			if !c.write(ctx, frame) {
				return
			}
		}

		switch {
		case err == nil:
			continue
		case errors.Is(err, broadcast.ErrLagged):
			// The client could not keep up with this channel. Say so on the
			// way out rather than dropping the socket silently: a client
			// that reconnects blind cannot tell this from a network
			// failure.
			c.app.metrics.laggedTotal.Inc()
			c.log.Info("dropped: too slow", "channel", m.ch.name)
			c.close(websocket.StatusPolicyViolation, "too slow")
			return
		default:
			return // orderly close
		}
	}
}
