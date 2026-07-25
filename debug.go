package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/pprof"
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
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)

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

	go func() {
		err := a.debug.Serve(listener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.log.Error("debug listener stopped", "err", err)
		}
	}()
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
