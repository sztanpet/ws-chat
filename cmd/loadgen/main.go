// Command loadgen points a crowd at a ws-chat server and reports what came back.
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

	// Ctrl-C still gets the report, or nobody would ever cut a run short.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	result, err := loadgen.Run(ctx, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
	fmt.Print(result)
}
