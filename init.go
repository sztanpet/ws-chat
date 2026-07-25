package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"sync/atomic"

	"github.com/sztanpet/ws-chat/internal/broadcast"
	"github.com/sztanpet/ws-chat/internal/config"
	"github.com/sztanpet/ws-chat/internal/hook"
	"github.com/sztanpet/ws-chat/internal/proto"
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

	// bcs is the fan-out: one broadcaster per wire format, keyed by codec
	// name. A ring holds encoded bytes and every subscriber gets the same
	// ones, so clients that negotiated different codecs cannot share one —
	// a message is encoded once per codec instead of once per subscriber,
	// which keeps the cost O(codecs) rather than O(members).
	//
	// There is one set of these for now, a single implicit channel every
	// connection joins. Channels replace it with a lookup from channel name
	// to its own set.
	bcs map[string]broadcast.Broadcaster

	// sendMu is held while a message is assigned its id and written to
	// every codec's ring. Without it two senders could reach the rings in
	// different orders and clients on different codecs would disagree about
	// what happened first. The critical section is two ring writes, about
	// forty nanoseconds; consistent ordering is worth that.
	sendMu sync.Mutex

	mux *http.ServeMux
	srv *http.Server

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
		cfg:   cfg,
		hooks: hooks,
		log:   slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})),
		bcs:   make(map[string]broadcast.Broadcaster, len(proto.Codecs())),
		mux:   http.NewServeMux(),
		conns: make(map[string]*conn),

		records:     make(chan func(context.Context) error, recordQueue),
		recordsDone: make(chan struct{}),
	}
	for _, codec := range proto.Codecs() {
		a.bcs[codec.Name()] = broadcast.NewRing(cfg.Capacity)
	}

	a.ctx, a.stopConn = context.WithCancel(context.Background())
	if hooks.Recorder != nil {
		go a.recordWorker(a.ctx)
	} else {
		close(a.recordsDone)
	}
	a.routes()
	a.srv = &http.Server{Addr: cfg.Addr, Handler: a.mux}
	return a, nil
}

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

// broadcast encodes the payload once per codec and hands each ring its own
// copy, under the lock that keeps every client's view of the order the
// same.
func (a *app) broadcast(verb string, build func(id uint64) any) (uint64, error) {
	frames := make(map[string][]byte, len(a.bcs))

	a.sendMu.Lock()
	defer a.sendMu.Unlock()

	id := a.seq.Add(1)
	payload := build(id)

	// Encode everything before delivering anything: a codec that fails must
	// not leave half the room having seen the message.
	for _, codec := range proto.Codecs() {
		frame, err := codec.Encode(verb, payload)
		if err != nil {
			return 0, err
		}
		frames[codec.Name()] = frame
	}
	for name, frame := range frames {
		a.bcs[name].Broadcast(frame)
	}
	return id, nil
}

func (a *app) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintln(w, "ok")
}
