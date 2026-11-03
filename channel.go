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

// A ring holds encoded bytes, hence one per codec.
type channel struct {
	name  string
	bcs   map[string]broadcast.Broadcaster
	limit *ratelimit.Bucket

	// Same order in every codec's ring; channels need none between them.
	sendMu sync.Mutex

	mu      sync.RWMutex
	members map[string]*conn // by nick
}

// Bounded and boring: the name is client-chosen, and becomes a map key.
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
	maxChannelName = 64

	maxNames = 1000
)

// c joins under channelsMu, or the sweep could reclaim the channel mid-join.
func (a *app) channelFor(ctx context.Context, name string, c *conn) (*channel, bool) {
	a.channelsMu.RLock()
	ch, ok := a.channels[name]
	if ok {
		ch.add(c)
	}
	a.channelsMu.RUnlock()
	if ok {
		return ch, true
	}

	a.channelsMu.Lock()
	defer a.channelsMu.Unlock()

	if ch, ok = a.channels[name]; ok {
		ch.add(c)
		return ch, true
	}
	if len(a.channels) >= a.cfg.MaxChannels {
		return nil, false
	}

	ch = &channel{
		name:    name,
		bcs:     make(map[string]broadcast.Broadcaster, len(proto.Codecs())),
		limit:   a.channelLimiter(ctx, name),
		members: map[string]*conn{c.nick(): c},
	}
	for _, codec := range proto.Codecs() {
		ch.bcs[codec.Name()] = broadcast.NewSeqRing(a.cfg.Capacity)
	}
	a.channels[name] = ch
	a.metrics.channelsTotal.Inc()

	return ch, true
}

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
	// Only if still ours: the old connection's teardown must spare a reconnect.
	if cur, ok := ch.members[c.nick()]; ok && cur == c {
		delete(ch.members, c.nick())
	}
}

func (ch *channel) len() int {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	return len(ch.members)
}

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

// Nothing in an empty channel is observable: the backlog lives in History.
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

func (a *app) closeChannels() {
	a.channelsMu.RLock()
	defer a.channelsMu.RUnlock()
	for _, ch := range a.channels {
		for _, bc := range ch.bcs {
			bc.Close()
		}
	}
}

func (ch *channel) broadcastTo(a *app, build func(id uint64) proto.Outbound, after func(uint64)) (uint64, error) {
	frames := make(map[string][]byte, len(ch.bcs))

	ch.sendMu.Lock()
	defer ch.sendMu.Unlock()

	id := a.seq.Add(1)
	payload := build(id)

	// All encoded before any is sent, or a failing codec leaves half the room told.
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

func (a *app) normalise(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return a.cfg.DefaultChannel
	}
	return name
}
