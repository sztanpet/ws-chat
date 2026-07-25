package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sztanpet/ws-chat/internal/config"
	"github.com/sztanpet/ws-chat/internal/filter"
	"github.com/sztanpet/ws-chat/internal/history"
	"github.com/sztanpet/ws-chat/internal/hook"
	"github.com/sztanpet/ws-chat/internal/metrics"
	"github.com/sztanpet/ws-chat/internal/moderation"
	"github.com/sztanpet/ws-chat/internal/proto"
	"github.com/sztanpet/ws-chat/internal/ratelimit"
)

// app is the composition root. Everything the handlers need hangs off it,
// and nothing reaches for a package-level global.
type app struct {
	cfg config.Config
	log *slog.Logger

	// hooks are the layers outside the chat core: authentication, chatter
	// data, moderation, persistence. All optional; see internal/hook.
	hooks hook.Hooks

	// records is the persistence queue. Bounded and lossy on purpose — a
	// slow store must not become backpressure on the chat.
	records     chan func(context.Context) error
	recordsDone chan struct{}
	dropped     atomic.Uint64

	// channels is every channel that currently exists, created on demand
	// and reclaimed when empty. Each owns its own fan-out (one broadcaster
	// per codec), its own rate limit and its own member list.
	channelsMu sync.RWMutex
	channels   map[string]*channel

	// filters is the chain run in front of every message: the built-in
	// text-hygiene filters, then whatever the deployment installed. Built
	// once, since none of it changes at runtime.
	filters hook.Filter

	// mod is who is muted and who is banned. One store for now; per
	// channel once channels exist.
	mod *moderation.Store

	// hist is the replay window a connecting client is shown. The default
	// is history.Memory; a deployment can install anything.
	hist hook.History

	// limiters are the rate limit buckets shared between the connections of
	// one account, keyed by whatever the Limiter hook decided an account
	// is. Connections with no key are not in here at all.
	limitersMu sync.Mutex
	limiters   map[string]*limiterEntry

	// sendMu is held while a message is assigned its id and written to
	// every codec's ring. Without it two senders could reach the rings in
	// different orders and clients on different codecs would disagree about
	// what happened first. The critical section is a couple of ring writes,
	// tens of nanoseconds; consistent ordering is worth that.
	sendMu sync.Mutex

	mux       *http.ServeMux
	srv       *http.Server
	debug     *http.Server
	debugAddr string // the address it actually bound, for tests and logs

	// registry and metrics are the server describing itself. Not a hook:
	// how many connections are held is not policy, and anything that wants
	// the numbers elsewhere can scrape them.
	registry *metrics.Registry
	metrics  *appMetrics

	// ctx is the lifetime of every connection. Cancelling it is what stops
	// the read pumps: net/http's Shutdown does not wait for — or even know
	// about — hijacked connections.
	ctx      context.Context
	stopConn context.CancelFunc

	// conns is the directory private messages are addressed through. A user
	// has one connection and (once channels exist) many memberships, so
	// this is a lookup by name, separate from any channel's member set.
	connsMu sync.RWMutex
	conns   map[string]*conn

	// seq numbers messages. Per-channel once channels exist.
	seq atomic.Uint64
	// anon names unauthenticated connections until there is a login to do
	// it properly.
	anon atomic.Uint64
}

func newApp(configPath string) (*app, error) {
	cfg, err := config.Load(configPath)
	if err != nil {
		return nil, err
	}
	// No hooks are installed here yet. This is the seam where a real
	// deployment wires in its auth, directory and storage layers; the
	// server runs without them.
	return newAppWithConfig(cfg, hook.Hooks{})
}

func newAppWithConfig(cfg config.Config, hooks hook.Hooks) (*app, error) {
	level, err := parseLevel(cfg.LogLevel)
	if err != nil {
		return nil, err
	}

	a := &app{
		cfg:      cfg,
		hooks:    hooks,
		log:      slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})),
		mux:      http.NewServeMux(),
		conns:    make(map[string]*conn),
		channels: make(map[string]*channel),
		limiters: make(map[string]*limiterEntry),

		records:     make(chan func(context.Context) error, recordQueue),
		recordsDone: make(chan struct{}),
	}
	a.mod = moderation.New()

	// The default window is what the server did before the hook existed:
	// the last cfg.Backlog messages, in memory.
	a.hist = hooks.History
	if a.hist == nil {
		a.hist = history.NewMemory(cfg.Backlog)
	}

	// Text hygiene first, then policy. A message that is not valid text
	// should never reach a filter that was written assuming it was.
	a.filters = filter.Chain(
		filter.UTF8{},
		filter.Zalgo{Max: cfg.MaxDiacritics},
		hooks.Filter,
	)

	a.registry = metrics.New("wschat_")
	a.metrics = newMetrics(a.registry)
	a.registerStateMetrics(a.registry)

	a.ctx, a.stopConn = context.WithCancel(context.Background())
	go a.janitor(a.ctx)
	if hooks.Recorder != nil {
		go a.recordWorker(a.ctx)
	} else {
		close(a.recordsDone)
	}
	a.routes()
	a.srv = &http.Server{Addr: cfg.Addr, Handler: a.mux}
	return a, nil
}

// limiterEntry is a shared rate limit bucket and the number of connections
// currently holding it.
type limiterEntry struct {
	bucket *ratelimit.Bucket
	refs   int
}

// janitorInterval is how often expired moderation entries and unreferenced
// rate limiters are reclaimed. Nothing waits on it, so it is deliberately
// unhurried.
const janitorInterval = time.Minute

func (a *app) routes() {
	a.mux.HandleFunc("GET /ws", a.handleWS)
	a.mux.HandleFunc("GET /health", a.handleHealth)
}

func parseLevel(s string) (slog.Level, error) {
	var l slog.Level
	if err := l.UnmarshalText([]byte(s)); err != nil {
		return l, fmt.Errorf("bad LogLevel %q: %w", s, err)
	}
	return l, nil
}

// register adds a connection to the directory. Nicks are assigned by the
// server and unique, so this cannot collide today; when logins arrive it
// will, and this is where that is decided.
func (a *app) register(c *conn) {
	a.connsMu.Lock()
	defer a.connsMu.Unlock()
	a.conns[c.nick()] = c
}

func (a *app) unregister(c *conn) {
	a.connsMu.Lock()
	defer a.connsMu.Unlock()
	// Only if it is still ours: a reconnect under the same name must not
	// have its entry removed by the old connection's teardown.
	if cur, ok := a.conns[c.nick()]; ok && cur == c {
		delete(a.conns, c.nick())
	}
}

func (a *app) lookup(nick string) (*conn, bool) {
	a.connsMu.RLock()
	defer a.connsMu.RUnlock()
	c, ok := a.conns[nick]
	return c, ok
}

// broadcastMsg fans out a chat message and adds it to the replay window.
//
// Everything a client sees is derived from the identity and the text, so
// the caller passes those rather than assembling a frame: the wire form and
// the history form are then two views of the same thing and cannot drift.
//
// The window is appended under the same lock that orders the fan-out, so
// what a joining client is shown cannot disagree with the order the room
// saw.
func (a *app) broadcastMsg(ch *channel, from hook.Identity, data string, at time.Time) (uint64, error) {
	return ch.broadcastTo(a,
		func(id uint64) proto.Outbound {
			return wireMsg(ch.name, hook.Message{ID: id, From: from, Data: data, At: at})
		},
		func(id uint64) {
			a.hist.Append(context.Background(), ch.name, hook.Message{
				ID: id, From: from, Data: data, At: at,
			})
		},
	)
}

// wireMsg is the one place a recorded message becomes a frame, so the
// backlog and live traffic cannot describe the same message differently.
func wireMsg(channel string, m hook.Message) proto.Msg {
	return proto.NewMsg(proto.Msg{
		Channel:   channel,
		ID:        m.ID,
		Nick:      m.From.Nick,
		Data:      m.Data,
		Timestamp: m.At.UnixMilli(),
		Roles:     m.From.Roles,
		Attrs:     m.From.Attrs,
	})
}

// backlog is what a connecting client is replayed.
//
// Deliberately not under sendMu: a History implementation may be backed by
// something slow, and Recent is allowed to be — it runs once per
// connection. Holding the broadcast lock across it would let a slow store
// stall the whole channel.
func (a *app) backlog(ctx context.Context, channel string) []proto.Msg {
	if a.cfg.Backlog < 1 {
		return nil
	}

	recent, err := a.hist.Recent(ctx, channel, a.cfg.Backlog)
	if err != nil {
		// Failing to show history is not a reason to refuse somebody a
		// connection. Log it; they get an empty window.
		a.log.Error("cannot read history", "channel", channel, "err", err)
		return nil
	}

	msgs := make([]proto.Msg, len(recent))
	for i, m := range recent {
		msgs[i] = wireMsg(channel, m)
	}
	return msgs
}

func (a *app) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintln(w, "ok")
}
