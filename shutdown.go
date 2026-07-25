package main

import (
	"context"
	"errors"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/coder/websocket"
)

// shutdownGrace is how long in-flight HTTP requests get to finish. WebSocket
// connections are not covered by it — see close.
const shutdownGrace = 10 * time.Second

// run serves until the process is asked to stop, then shuts down.
func (a *app) run(ctx context.Context) error {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serving := make(chan error, 1)
	go func() {
		a.log.Info("listening", "addr", a.cfg.Addr)
		err := a.srv.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		serving <- err
	}()

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

// close ends every connection.
//
// net/http's Shutdown deliberately ignores hijacked connections, and every
// WebSocket here is one, so waiting on it would return promptly while every
// client stayed connected. Everything below is therefore our own doing.
//
// The order matters. Cancelling a read context does not politely end a
// WebSocket — the library has to drop the socket, because a cancelled read
// leaves the stream at an unknown offset — so the goodbye has to go out
// first. A client that is dropped without a close frame cannot tell a
// deliberate shutdown from a network failure, and will treat one as the
// other.
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

	// Then unblock both pumps: the read pumps by cancelling their context,
	// the write pumps by ending the subscriptions they are parked in.
	a.stopConn()
	for _, bc := range a.bcs {
		bc.Close()
	}

	// The persistence worker gets the same cancellation and makes a best
	// effort to finish what is already queued.
	<-a.recordsDone
}
