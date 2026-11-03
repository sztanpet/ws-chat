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

// Short: nobody should wait out a thirty-second CPU profile during a deploy.
const debugShutdownGrace = 2 * time.Second

func (a *app) serveDebug(ctx context.Context) error {
	if a.cfg.DebugAddr == "" {
		a.log.Info("debug listener disabled")
		return nil
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", a.handleMetrics)

	// Mounted here, not left on DefaultServeMux where the import puts them.
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
		// No timeouts: a CPU profile is a thirty-second response by design.
		ReadHeaderTimeout: 5 * time.Second,
	}
	a.log.Info("debug listener", "addr", a.debugAddr)

	// Labelled, so a profile can tell its own serving apart.
	go pprof.Do(ctx, pprof.Labels(labelTask, taskDebug), func(context.Context) {
		err := a.debug.Serve(listener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.log.Error("debug listener stopped", "err", err)
		}
	})
	return nil
}

func (a *app) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if _, err := a.registry.WriteTo(w); err != nil {
		a.log.Debug("metrics scrape failed", "err", err)
	}
}

func (a *app) closeDebug() {
	if a.debug == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), debugShutdownGrace)
	defer cancel()
	_ = a.debug.Shutdown(ctx)
}
