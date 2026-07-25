// Command ws-chat is a WebSocket chat server.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
)

func main() {
	path := flag.String("config", "config.hujson", "path to the config file")
	flag.Parse()

	a, err := newApp(*path)
	if err != nil {
		slog.Error("startup failed", "err", err)
		os.Exit(1)
	}

	if err := a.run(context.Background()); err != nil {
		a.log.Error("server stopped", "err", err)
		os.Exit(1)
	}
}
