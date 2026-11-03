package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"runtime/pprof"
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

type app struct {
	cfg config.Config
	log *slog.Logger

	hooks hook.Hooks

	// Bounded and lossy: a slow store must not become backpressure on the chat.
	records     chan func(context.Context) error
	recordsDone chan struct{}
	dropped     atomic.Uint64

	channelsMu sync.RWMutex
	channels   map[string]*channel

	filters hook.Filter

	mod *moderation.Store

	hist hook.History

	limitersMu sync.Mutex
	limiters   map[string]*limiterEntry

	mux       *http.ServeMux
	srv       *http.Server
	debug     *http.Server
	debugAddr string // the address it actually bound, for tests and logs

	registry *metrics.Registry
	metrics  *appMetrics

	// Cancelled on shutdown: net/http's Shutdown ignores hijacked connections.
	ctx      context.Context
	stopConn context.CancelFunc

	connsMu sync.RWMutex
	conns   map[string]*conn

	// Server-wide, so an id names one message whichever channel it was in.
	seq  atomic.Uint64
	anon atomic.Uint64
}

func newApp(configPath string) (*app, error) {
	cfg, err := config.Load(configPath)
	if err != nil {
		return nil, err
	}
	// The seam for a deployment's hooks; the server runs without any.
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

	a.hist = hooks.History
	if a.hist == nil {
		a.hist = history.NewMemory(cfg.Backlog)
	}

	// Hygiene before policy: no filter should see text that is not valid.
	a.filters = filter.Chain(
		filter.UTF8{},
		filter.Zalgo{Max: cfg.MaxDiacritics},
		hooks.Filter,
	)

	a.registry = metrics.New("wschat_")
	a.metrics = newMetrics(a.registry)
	a.registerStateMetrics(a.registry)

	// Before the listener: no connection is answered while bans are still loading.
	if err := a.loadSanctions(context.Background()); err != nil {
		return nil, err
	}

	a.ctx, a.stopConn = context.WithCancel(context.Background())
	go pprof.Do(a.ctx, pprof.Labels(labelTask, taskJanitor), a.janitor)
	if hooks.Recorder != nil || hooks.Sanctions != nil {
		go pprof.Do(a.ctx, pprof.Labels(labelTask, taskRecord), a.recordWorker)
	} else {
		close(a.recordsDone)
	}
	a.routes()
	a.srv = &http.Server{Addr: cfg.Addr, Handler: a.mux}
	return a, nil
}

type limiterEntry struct {
	bucket *ratelimit.Bucket
	refs   int
}

// Unhurried: nothing waits on it.
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

func (a *app) register(c *conn) {
	a.connsMu.Lock()
	defer a.connsMu.Unlock()
	a.conns[c.nick()] = c
}

func (a *app) unregister(c *conn) {
	a.connsMu.Lock()
	defer a.connsMu.Unlock()
	// Only if still ours: the old connection's teardown must spare a reconnect.
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

// Appended under the fan-out lock, so the backlog keeps the room's order.
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

// The one place a message becomes a frame, so backlog and live cannot differ.
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

// Not under sendMu: Recent may be slow, and the channel must not wait on it.
func (a *app) backlog(ctx context.Context, channel string) []proto.Msg {
	if a.cfg.Backlog < 1 {
		return nil
	}

	recent, err := a.hist.Recent(ctx, channel, a.cfg.Backlog)
	if err != nil {
		// Not a reason to refuse a connection: they get an empty window.
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
