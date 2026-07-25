package main

import (
	"context"
	"sort"
	"strings"
	"sync"
	"unicode"

	"github.com/sztanpet/ws-chat/internal/broadcast"
	"github.com/sztanpet/ws-chat/internal/proto"
	"github.com/sztanpet/ws-chat/internal/ratelimit"
)

// A channel is a fan-out, a rate limit and a member list.
//
// The fan-out is one broadcaster per codec, for the same reason it always
// was: a ring holds encoded bytes, so clients that negotiated different
// wire formats cannot share one. A channel therefore costs len(codecs)
// rings, which is why an empty one is reclaimed rather than kept.
//
// The member list is here rather than derived from the connection directory
// because presence is a per-channel question — who is in this room — and
// walking every connection to answer it would make NAMES O(server) instead
// of O(channel).
type channel struct {
	name  string
	bcs   map[string]broadcast.Broadcaster
	limit *ratelimit.Bucket

	mu      sync.RWMutex
	members map[string]*conn // by nick
}

// validChannel reports whether a name is one the server will create.
//
// Bounded and boring on purpose. The name is a map key, a metric label
// would be tempting, and it comes from whoever is connected; a client that
// can name a channel anything can make the server hold anything.
func validChannel(name string) bool {
	if name == "" || len(name) > maxChannelName {
		return false
	}
	for _, r := range name {
		if unicode.IsSpace(r) || !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}

const (
	// maxChannelName bounds a name a client can invent.
	maxChannelName = 64

	// maxNames caps a NAMES response. A channel with ten thousand people in
	// it would otherwise be a hundred-kilobyte frame; the total is reported
	// separately so a client knows it got a sample.
	maxNames = 1000
)

// channelFor returns a channel, creating it if nobody has been in it yet.
//
// Creation is on demand because the alternative is a list of channels in
// the config, which is a deployment's business and is what the Channels
// hook is for. The cap is what stops a client inventing channels until the
// server runs out of memory; a deployment that cares about which names
// exist refuses the rest in CanJoin.
func (a *app) channelFor(ctx context.Context, name string) (*channel, bool) {
	a.channelsMu.RLock()
	ch, ok := a.channels[name]
	a.channelsMu.RUnlock()
	if ok {
		return ch, true
	}

	a.channelsMu.Lock()
	defer a.channelsMu.Unlock()

	if ch, ok = a.channels[name]; ok {
		return ch, true
	}
	if len(a.channels) >= a.cfg.MaxChannels {
		return nil, false
	}

	ch = &channel{
		name:    name,
		bcs:     make(map[string]broadcast.Broadcaster, len(proto.Codecs())),
		limit:   a.channelLimiter(ctx, name),
		members: make(map[string]*conn),
	}
	for _, codec := range proto.Codecs() {
		ch.bcs[codec.Name()] = broadcast.NewRing(a.cfg.Capacity)
	}
	a.channels[name] = ch
	a.metrics.channelsTotal.Inc()

	return ch, true
}

// lookupChannel finds a channel without creating one.
func (a *app) lookupChannel(name string) (*channel, bool) {
	a.channelsMu.RLock()
	defer a.channelsMu.RUnlock()
	ch, ok := a.channels[name]
	return ch, ok
}

func (ch *channel) add(c *conn) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.members[c.nick()] = c
}

func (ch *channel) remove(c *conn) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	// Only if it is still ours, so a reconnect under the same name is not
	// removed by the old connection's teardown.
	if cur, ok := ch.members[c.nick()]; ok && cur == c {
		delete(ch.members, c.nick())
	}
}

func (ch *channel) len() int {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	return len(ch.members)
}

// names returns up to maxNames members, sorted, and the true total.
func (ch *channel) names() ([]string, int) {
	ch.mu.RLock()
	nicks := make([]string, 0, len(ch.members))
	for nick := range ch.members {
		nicks = append(nicks, nick)
	}
	total := len(ch.members)
	ch.mu.RUnlock()

	sort.Strings(nicks)
	if len(nicks) > maxNames {
		nicks = nicks[:maxNames]
	}
	return nicks, total
}

// sweepChannels reclaims channels nobody is in.
//
// Free to do, which is the only reason it is done: an empty channel's rings
// hold frames for subscribers that no longer exist, and the backlog a
// joining client is shown comes from the History hook, not from here. There
// is nothing in an empty channel anybody can observe.
func (a *app) sweepChannels() int {
	a.channelsMu.Lock()
	defer a.channelsMu.Unlock()

	removed := 0
	for name, ch := range a.channels {
		if ch.len() > 0 {
			continue
		}
		for _, bc := range ch.bcs {
			bc.Close()
		}
		delete(a.channels, name)
		removed++
	}
	return removed
}

// closeChannels ends every channel's fan-out, which unblocks the write
// pumps parked in them.
func (a *app) closeChannels() {
	a.channelsMu.RLock()
	defer a.channelsMu.RUnlock()
	for _, ch := range a.channels {
		for _, bc := range ch.bcs {
			bc.Close()
		}
	}
}

// broadcastTo fans a frame out to one channel, encoded once per codec.
//
// The id is assigned and the rings are written under one lock, as before,
// so clients on different codecs cannot disagree about what happened first.
// The lock is per channel now: two channels do not serialize against each
// other.
func (ch *channel) broadcastTo(a *app, build func(id uint64) proto.Outbound, after func(uint64)) (uint64, error) {
	frames := make(map[string][]byte, len(ch.bcs))

	a.sendMu.Lock()
	defer a.sendMu.Unlock()

	id := a.seq.Add(1)
	payload := build(id)

	// Encode everything before delivering anything: a codec that fails must
	// not leave half the room having seen the message.
	for _, codec := range proto.Codecs() {
		frame, err := codec.Encode(payload)
		if err != nil {
			return 0, err
		}
		frames[codec.Name()] = frame
	}

	if after != nil {
		after(id)
	}
	for name, frame := range frames {
		ch.bcs[name].Broadcast(frame)
	}
	return id, nil
}

// channelNames lists the channels that exist, for logging and tests.
func (a *app) channelNames() []string {
	a.channelsMu.RLock()
	defer a.channelsMu.RUnlock()

	names := make([]string, 0, len(a.channels))
	for name := range a.channels {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// normalise trims a channel name and applies the default when empty.
//
// An empty channel on a MSG means the default, so a client that only ever
// uses one room does not have to name it on every message.
func (a *app) normalise(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return a.cfg.DefaultChannel
	}
	return name
}
