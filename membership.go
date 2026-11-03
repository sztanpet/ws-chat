package main

import (
	"context"
	"errors"
	"runtime/pprof"

	"github.com/coder/websocket"

	"github.com/sztanpet/ws-chat/internal/broadcast"
	"github.com/sztanpet/ws-chat/internal/proto"
)

// A pump each: Recv cannot be selected on, and a merge channel costs a hop.
type membership struct {
	ch   *channel
	sub  broadcast.Sub
	done chan struct{} // closed when its pump has stopped
}

// force skips CanJoin for autojoin, whose layer has already decided.
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

	// Even on autojoin: a layer putting somebody here does not overrule a ban.
	if banned, _ := c.app.mod.Banned(name, c.id.Key()); banned {
		return proto.ErrBanned
	}

	if !force {
		if ok, reason := c.app.canJoin(ctx, c.id, name); !ok {
			if reason == "" {
				reason = proto.ErrForbidden
			}
			return reason
		}
	}

	ch, ok := c.app.channelFor(ctx, name, c)
	if !ok {
		return proto.ErrTooManyChans
	}

	// Only this goroutine adds memberships, so the name is still free.
	m := &membership{ch: ch, sub: ch.bcs[c.codec.Name()].Subscribe(), done: make(chan struct{})}
	c.membersMu.Lock()
	c.memberships[name] = m
	c.membersMu.Unlock()

	// Again: a ban and a membership are each stored before looking for the
	// other, so whichever lands second sees the first.
	if banned, _ := c.app.mod.Banned(name, c.id.Key()); banned {
		c.membersMu.Lock()
		mine := c.memberships[name] == m
		if mine {
			delete(c.memberships, name)
		}
		c.membersMu.Unlock()
		if mine {
			ch.remove(c)
			m.sub.Close()
		}
		close(m.done) // no pump was started
		return proto.ErrBanned
	}

	// Pump last: the backlog goes out directly while none runs, and the JOIN is
	// the first thing it delivers: BACKLOG, then the JOIN, then live traffic.
	c.sendBacklog(ctx, name)

	if _, err := ch.broadcastTo(c.app, func(uint64) proto.Outbound {
		return proto.NewJoin(proto.Join{
			Channel: name, Nick: c.nick(), Roles: c.id.Roles, Attrs: c.id.Attrs,
		})
	}, nil); err != nil {
		c.log.Error("cannot encode JOIN", "channel", name, "err", err)
	}

	go pprof.Do(ctx, c.labels(taskChanPump), func(ctx context.Context) {
		c.channelPump(ctx, m)
	})

	c.app.metrics.joinsTotal.Inc()
	c.log.Debug("joined", "channel", name)
	return ""
}

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

	// Told directly: closing the subscription discards what it has not read.
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

func (c *conn) leave(m *membership) {
	m.ch.remove(c)
	m.sub.Close() // unblocks the pump, which is parked in Recv
	<-m.done
}

func (c *conn) leaveAll(ctx context.Context) {
	// Swapped, not copied: under the lock, that leaves us the only holder.
	c.membersMu.Lock()
	memberships := c.memberships
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

func (c *conn) joined(name string) (*membership, bool) {
	c.membersMu.RLock()
	defer c.membersMu.RUnlock()
	m, ok := c.memberships[name]
	return m, ok
}

func (c *conn) channelPump(ctx context.Context, m *membership) {
	defer close(m.done)

	batch := make([][]byte, c.app.cfg.WriteBatch)
	for {
		n, err := m.sub.Recv(batch)

		if !c.writeBatch(ctx, batch[:n]) {
			return
		}

		switch {
		case err == nil:
			continue
		case errors.Is(err, broadcast.ErrLagged):
			// Said in the close frame: blind, a client cannot tell this from the network.
			c.app.metrics.laggedTotal.Inc()
			c.log.Info("dropped: too slow", "channel", m.ch.name)
			c.close(websocket.StatusPolicyViolation, "too slow")
			return
		default:
			return
		}
	}
}
