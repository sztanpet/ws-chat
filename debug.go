package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	httppprof "net/http/pprof"
	"runtime/pprof"
	"time"
)

// Profiling and metrics live on their own listener, not on the mux that
// serves chat.
//
// pprof is remote code execution's polite cousin: /debug/pprof/heap hands
// out a dump of everything in memory, and anyone who can reach
// /debug/pprof/profile can pin a core for thirty seconds as often as they
// like. It has no authentication and is not going to grow any. So the
// default binds to localhost and the documentation says what changing that
// means.
//
// Metrics are on the same listener for the same reason at lower stakes: a
// scrape tells you how many people are connected and how often things are
// being refused, which is operational detail that does not belong on a
// public endpoint. Point the scraper at this address, and bind it to an
// interface the scraper can reach if that is not the loopback.

// Every goroutine the server starts is labelled with pprof.Do, so a
// profile says which of them the time went to rather than which function.
// That is the distinction that matters here: conn.channelPump is 96% of
// CPU under load and there is one of it per membership, so "where is the
// time" is answered by what a goroutine is *for* — feeding a room, writing
// a private message, draining the record queue — and the call graph alone
// cannot separate a pump from the connection that started it.
//
// Labels are inherited by whatever a labelled goroutine starts, which is
// most of the arrangement: net/http's handler goroutines come out of the
// listener already labelled, and a connection's pumps come out of the
// connection. Each then labels itself over the top, since the same key
// wins the closest binding.
//
// One key, and values from a closed set, for the same reason metric labels
// are. Filter a profile with `-tagfocus=task=channel-pump`.
const labelTask = "task"

const (
	taskListener = "listener"
	taskDebug    = "debug-listener"
	taskConn     = "conn"
	taskPrivPump = "priv-pump"
	taskChanPump = "channel-pump"
	taskJanitor  = "janitor"
	taskRecord   = "record-worker"
)

// debugShutdownGrace bounds how long the debug server gets to finish
// in-flight requests. A CPU profile takes thirty seconds and nobody should
// wait for one during a deploy.
const debugShutdownGrace = 2 * time.Second

// serveDebug starts the profiling and metrics listener. An empty DebugAddr
// disables it entirely and nothing is bound.
func (a *app) serveDebug(ctx context.Context) error {
	if a.cfg.DebugAddr == "" {
		a.log.Info("debug listener disabled")
		return nil
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", a.handleMetrics)

	// The stdlib registers these on DefaultServeMux as a side effect of
	// being imported, which is exactly the accident this avoids: they are
	// mounted here, deliberately, on a mux that is not the public one.
	//
	// Aliased because runtime/pprof is the one the rest of this package
	// means by "pprof": every goroutine is labelled with it, and only these
	// five lines want the HTTP end.
	mux.HandleFunc("GET /debug/pprof/", httppprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", httppprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", httppprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", httppprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", httppprof.Trace)

	listener, err := net.Listen("tcp", a.cfg.DebugAddr)
	if err != nil {
		return err
	}

	a.debugAddr = listener.Addr().String()
	a.debug = &http.Server{
		Handler: mux,
		// No read or write timeout: a CPU profile is a thirty-second
		// response by design and a trace can be longer. This is the one
		// listener where a slow handler is the point.
		ReadHeaderTimeout: 5 * time.Second,
	}
	a.log.Info("debug listener", "addr", a.debugAddr)

	// Labelled, and so are the handler goroutines it spawns: a CPU profile
	// includes the work of serving that profile, and it should be possible
	// to see that is what it is.
	go pprof.Do(ctx, pprof.Labels(labelTask, taskDebug), func(context.Context) {
		err := a.debug.Serve(listener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.log.Error("debug listener stopped", "err", err)
		}
	})
	return nil
}

// handleMetrics writes the current values in the Prometheus text format.
func (a *app) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if _, err := a.registry.WriteTo(w); err != nil {
		a.log.Debug("metrics scrape failed", "err", err)
	}
}

// closeDebug stops the debug listener.
func (a *app) closeDebug() {
	if a.debug == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), debugShutdownGrace)
	defer cancel()
	_ = a.debug.Shutdown(ctx)
}
