// Package config loads the server's HuJSON configuration.
//
// config_default.hujson is committed, fully commented out, and documents
// every field with its built-in default. The real config.hujson sits next
// to the binary and is not committed. An absent or empty config file is
// valid: it means "all defaults".
package config

import (
	"encoding/json"
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
	Capacity int

	// WriteBatch is how many frames a connection's write pump takes per
	// wakeup. Larger means fewer wakeups when a client is behind.
	WriteBatch int

	// MaxFrameSize is the largest frame the server will read from a client.
	MaxFrameSize int64

	// MaxMessage is the longest chat message body accepted, in bytes.
	MaxMessage int

	// PrivBuffer is how many private messages may be queued for one client
	// before further ones are refused. It is deliberately small: private
	// messages are rare, and a client that is not draining them is gone.
	PrivBuffer int

	// WriteTimeout bounds a single socket write. Without it a client that
	// stops reading wedges the goroutine writing to it, which is how
	// backpressure escapes into the rest of the server.
	WriteTimeout Duration

	// IdleTimeout closes a connection that has sent nothing at all — not
	// even a PING — for this long. Clients are expected to ping.
	IdleTimeout Duration

	// LogLevel is one of debug, info, warn, error.
	LogLevel string
}

// Default returns the configuration used when nothing overrides it.
func Default() Config {
	return Config{
		Addr:         ":8080",
		Capacity:     256,
		WriteBatch:   16,
		MaxFrameSize: 32 << 10,
		MaxMessage:   512,
		PrivBuffer:   32,
		WriteTimeout: Duration(10 * time.Second),
		IdleTimeout:  Duration(90 * time.Second),
		LogLevel:     "info",
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
	if err := json.Unmarshal(std, &cfg); err != nil {
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
		return errors.New("Addr must not be empty")
	case c.Capacity < 1:
		return errors.New("Capacity must be at least 1")
	case c.WriteBatch < 1:
		return errors.New("WriteBatch must be at least 1")
	case c.MaxFrameSize < 1:
		return errors.New("MaxFrameSize must be at least 1")
	case c.MaxMessage < 1:
		return errors.New("MaxMessage must be at least 1")
	case c.PrivBuffer < 1:
		return errors.New("PrivBuffer must be at least 1")
	case c.WriteTimeout <= 0:
		return errors.New("WriteTimeout must be positive")
	case c.IdleTimeout <= 0:
		return errors.New("IdleTimeout must be positive")
	}
	return nil
}
