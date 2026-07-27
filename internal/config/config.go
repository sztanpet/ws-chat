// Package config loads the server's HuJSON configuration.
//
// config_default.hujson is committed, fully commented out, and documents
// every field with its built-in default. The real config.hujson sits next
// to the binary and is not committed. An absent or empty config file is
// valid: it means "all defaults".
package config

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/tailscale/hujson"
)

// Duration is a time.Duration that reads from a Go duration string, so the
// config says "90s" rather than 90000000000.
type Duration time.Duration

func (d Duration) Duration() time.Duration { return time.Duration(d) }

func (d Duration) String() string { return time.Duration(d).String() }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf(`duration must be a string like "90s": %w`, err)
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(parsed)
	return nil
}

// Config is the whole of the server's configuration.
type Config struct {
	// Addr is the listen address for the HTTP server.
	Addr string

	// AllowedOrigins are the origins permitted to open a WebSocket. Empty
	// means same-origin only; "*" disables the check entirely.
	AllowedOrigins []string

	// Capacity is how many messages the shared fan-out buffer holds. It
	// doubles as the lag threshold: a client that falls this far behind is
	// disconnected rather than waited on.
	//
	// It has to be sized for the lag SPREAD, not the average. In a busy
	// room the mean client is a handful of messages behind while one
	// straggler is thousands, and it is the straggler that decides whether
	// anything gets dropped. Measured at ten thousand subscribers with a
	// tenth of them talking, 256 collapses to almost no delivery at all and
	// 4096 delivers 99.8% — see state/broadcast.md.
	//
	// Read it as a time window rather than a size: slots divided by the
	// channel's message rate. The window gets LONGER as a room grows rather
	// than shorter, because a big room is bound by delivery bandwidth —
	// members times message rate — and so cannot carry many messages a
	// second to begin with. At ~350k deliveries/s on four cores, a
	// hundred-thousand-member channel sustains three or four messages a
	// second, where this default is over an hour of history. A room that
	// size sheds clients because the server cannot deliver, and no capacity
	// fixes that: ChannelLimits on the Limiter hook is the lever.
	//
	// Memory is per channel and per codec, not per server, and the slot
	// array is allocated when the channel is created — 8 bytes a slot, so
	// 16384 slots is 128KB a ring and 256KB for a channel carrying both
	// codecs. MaxChannels bounds the worst case: 1024 channels on both
	// codecs is 256MB of slot arrays, which is why raising this much
	// further wants MaxChannels lowered or channel creation gated in
	// CanJoin. A ring also pins the last Capacity encoded frames from
	// collection, and for a busy channel that is the bigger number — 16384
	// frames of 150 bytes is ~2.5MB.
	Capacity int

	// WriteBatch is how many frames a connection's write pump takes per
	// wakeup. Larger means fewer wakeups when a client is behind.
	WriteBatch int

	// MaxFrameSize is the largest frame the server will read from a client.
	MaxFrameSize int64

	// MaxMessage is the longest chat message body accepted, in bytes.
	MaxMessage int

	// DefaultChannel is the channel an empty channel name means, and the
	// one a connection joins when nothing says otherwise.
	DefaultChannel string

	// MaxChannels bounds how many channels can exist at once. Channels are
	// created on demand by whoever joins one, so without a cap a client can
	// invent them until the server runs out of memory.
	MaxChannels int

	// MaxChannelsPerConn bounds how many one connection may be in.
	MaxChannelsPerConn int

	// Backlog is how many recent messages a connecting client is sent, so
	// it arrives with context instead of an empty window. Zero disables it.
	Backlog int

	// MaxDiacritics is how many combining marks may be stacked on one
	// character before a message is refused as zalgo. Non-positive disables
	// the filter.
	MaxDiacritics int

	// PrivBuffer is how many private messages may be queued for one client
	// before further ones are refused. It is deliberately small: private
	// messages are rare, and a client that is not draining them is gone.
	PrivBuffer int

	// WriteTimeout bounds a socket write — or one wakeup's worth of them,
	// since a write pump drains its batch under a single deadline. Without
	// it a client that stops reading wedges the goroutine writing to it,
	// which is how backpressure escapes into the rest of the server.
	WriteTimeout Duration

	// IdleTimeout closes a connection that has sent nothing at all — not
	// even a PING — for this long. Clients are expected to ping.
	IdleTimeout Duration

	// DebugAddr is where profiling and metrics are served. Empty disables
	// them and binds nothing.
	//
	// It defaults to the loopback because pprof has no authentication and
	// is not going to grow any: /debug/pprof/heap hands out a dump of
	// everything in memory, and anyone who can reach /debug/pprof/profile
	// can pin a core for thirty seconds as often as they like. Move it to
	// an interface a scraper can reach only if that interface is not
	// reachable by anybody else.
	DebugAddr string

	// LogLevel is one of debug, info, warn, error.
	LogLevel string
}

// Default returns the configuration used when nothing overrides it.
func Default() Config {
	return Config{
		Addr:               ":8080",
		Capacity:           16384,
		WriteBatch:         16,
		MaxFrameSize:       32 << 10,
		MaxMessage:         512,
		DefaultChannel:     "main",
		MaxChannels:        1024,
		MaxChannelsPerConn: 32,
		Backlog:            50,
		MaxDiacritics:      5,
		PrivBuffer:         32,
		WriteTimeout:       Duration(10 * time.Second),
		IdleTimeout:        Duration(90 * time.Second),
		DebugAddr:          "127.0.0.1:6060",
		LogLevel:           "info",
	}
}

// Load reads path over the defaults. A missing file is not an error.
func Load(path string) (Config, error) {
	cfg := Default()

	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return cfg, nil
	}
	if err != nil {
		return cfg, err
	}

	// HuJSON is JSON with comments and trailing commas; Standardize strips
	// them back to something encoding/json accepts.
	std, err := hujson.Standardize(raw)
	if err != nil {
		return cfg, fmt.Errorf("%s: %w", path, err)
	}
	// Case-insensitive names are asked for explicitly because
	// encoding/json/v2 dropped them, and a config file is the one place
	// that must not get stricter quietly: a deployment that wrote "addr"
	// would have its listen address silently replaced by the default.
	if err := json.Unmarshal(std, &cfg, json.MatchCaseInsensitiveNames(true)); err != nil {
		return cfg, fmt.Errorf("%s: %w", path, err)
	}
	if err := cfg.check(); err != nil {
		return cfg, fmt.Errorf("%s: %w", path, err)
	}
	return cfg, nil
}

// check rejects values that would misbehave later in ways that are hard to
// trace back to the config.
func (c Config) check() error {
	switch {
	case c.Addr == "":
		return errors.New("field Addr must not be empty")
	case c.Capacity < 1:
		return errors.New("field Capacity must be at least 1")
	case c.WriteBatch < 1:
		return errors.New("field WriteBatch must be at least 1")
	case c.MaxFrameSize < 1:
		return errors.New("field MaxFrameSize must be at least 1")
	case c.MaxMessage < 1:
		return errors.New("field MaxMessage must be at least 1")
	case c.Backlog < 0:
		return errors.New("field Backlog must not be negative")
	case c.DefaultChannel == "":
		return errors.New("field DefaultChannel must not be empty")
	case c.MaxChannels < 1:
		return errors.New("field MaxChannels must be at least 1")
	case c.MaxChannelsPerConn < 1:
		return errors.New("field MaxChannelsPerConn must be at least 1")
	case c.PrivBuffer < 1:
		return errors.New("field PrivBuffer must be at least 1")
	case c.WriteTimeout <= 0:
		return errors.New("field WriteTimeout must be positive")
	case c.IdleTimeout <= 0:
		return errors.New("field IdleTimeout must be positive")
	}
	return nil
}
