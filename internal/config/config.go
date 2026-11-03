// Package config is HuJSON over defaults; config_default.hujson documents each.
package config

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/tailscale/hujson"
)

// Duration is spelled "90s" in the config, not 90000000000.
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

type Config struct {
	Addr               string
	AllowedOrigins     []string
	Capacity           int
	WriteBatch         int
	MaxFrameSize       int64
	MaxMessage         int
	DefaultChannel     string
	MaxChannels        int
	MaxChannelsPerConn int
	Backlog            int
	MaxDiacritics      int
	PrivBuffer         int
	WriteTimeout       Duration
	IdleTimeout        Duration
	DebugAddr          string
	LogLevel           string
}

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
		PrivBuffer:         32, // small: a client not draining them is gone
		WriteTimeout:       Duration(10 * time.Second),
		IdleTimeout:        Duration(90 * time.Second),
		DebugAddr:          "127.0.0.1:6060",
		LogLevel:           "info",
	}
}

// Load treats a missing file as all defaults.
func Load(path string) (Config, error) {
	cfg := Default()

	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return cfg, nil
	}
	if err != nil {
		return cfg, err
	}

	std, err := hujson.Standardize(raw)
	if err != nil {
		return cfg, fmt.Errorf("%s: %w", path, err)
	}
	// Case-insensitive, or a config saying "addr" would silently get the default.
	if err := json.Unmarshal(std, &cfg, json.MatchCaseInsensitiveNames(true)); err != nil {
		return cfg, fmt.Errorf("%s: %w", path, err)
	}
	if err := cfg.check(); err != nil {
		return cfg, fmt.Errorf("%s: %w", path, err)
	}
	return cfg, nil
}

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
