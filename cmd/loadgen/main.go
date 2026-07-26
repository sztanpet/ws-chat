// Command loadgen points a crowd at a ws-chat server and says what came
// back.
//
//	loadgen -url ws://127.0.0.1:8080/ws -conns 5000 -speak 10 -rate 1 -for 60s
//
// It runs in its own process on purpose. A benchmark living inside the
// server shares its scheduler, its allocator and its GC, and then reports
// on them as if they were the server's — and it cannot be pointed at a
// machine that is not this one.
//
// Note the file descriptor limit: one connection is one socket at each end,
// so anything above about a thousand needs `ulimit -n` raising on both
// sides.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sztanpet/ws-chat/internal/loadgen"
	"github.com/sztanpet/ws-chat/internal/proto"
)

func main() {
	cfg := loadgen.Config{Out: os.Stderr}

	flag.StringVar(&cfg.URL, "url", "ws://127.0.0.1:8080/ws", "the server's WebSocket endpoint")
	flag.IntVar(&cfg.Conns, "conns", 100, "connections to hold open")
	flag.Float64Var(&cfg.SpeakPercent, "speak", 10, "percentage of connections that talk")
	flag.Float64Var(&cfg.Rate, "rate", 1, "messages per second per talking connection")
	flag.IntVar(&cfg.MessageSize, "size", 64, "message body size in bytes")
	flag.IntVar(&cfg.Channels, "channels", 1, "channels to spread the connections over")
	flag.DurationVar(&cfg.Duration, "for", 30*time.Second, "how long to talk for, once everybody is connected; 0 runs until interrupted")
	flag.DurationVar(&cfg.Ramp, "ramp", 5*time.Second, "spread the dialling over this long")
	flag.DurationVar(&cfg.PingEvery, "ping", 30*time.Second, "how often every connection pings, idle or not")
	flag.StringVar(&cfg.Codec, "codec", proto.NameJSON, "wire format: "+proto.NameJSON+" or "+proto.NameMsgPack)
	flag.DurationVar(&cfg.Progress, "progress", time.Second, "how often to print a progress line; 0 for none")
	flag.Parse()

	// Ctrl-C stops the run and still prints the report. A load test you
	// cannot interrupt without losing its numbers is one nobody interrupts,
	// and so one that always runs for the wrong length of time.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	result, err := loadgen.Run(ctx, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
	fmt.Print(result)
}
