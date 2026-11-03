package main

import (
	"context"
	"errors"
	"net/http"
	"os/signal"
	"runtime/pprof"
	"syscall"
	"time"

	"github.com/coder/websocket"
)

// For HTTP requests only: hijacked WebSockets are close's business.
const shutdownGrace = 10 * time.Second

func (a *app) run(ctx context.Context) error {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := a.serveDebug(ctx); err != nil {
		return err
	}

	// Labels every handler goroutine net/http starts from here.
	serving := make(chan error, 1)
	go pprof.Do(ctx, pprof.Labels(labelTask, taskListener), func(context.Context) {
		a.log.Info("listening", "addr", a.cfg.Addr)
		err := a.srv.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		serving <- err
	})

	select {
	case err := <-serving:
		return err
	case <-ctx.Done():
		a.log.Info("shutting down")
	}

	sctx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	err := a.srv.Shutdown(sctx)
	a.close()
	return err
}

// Goodbye first: a cancelled read drops the socket without a close frame.
func (a *app) close() {
	a.connsMu.RLock()
	conns := make([]*conn, 0, len(a.conns))
	for _, c := range a.conns {
		conns = append(conns, c)
	}
	a.connsMu.RUnlock()

	for _, c := range conns {
		c.close(websocket.StatusGoingAway, "server shutting down")
	}

	a.stopConn()
	a.closeChannels()

	<-a.recordsDone

	a.closeDebug()
}
