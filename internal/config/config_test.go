package config

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func write(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.hujson")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writing the test config: %v", err)
	}
	return path
}

// An absent config is a valid config. A fresh checkout has to run.
func TestLoadMissingFile(t *testing.T) {
	cfg, err := Load(filepath.Join(t.TempDir(), "nope.hujson"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !reflect.DeepEqual(cfg, Default()) {
		t.Fatal("a missing file did not produce the defaults")
	}
}

// The committed default file is the documentation, so it had better parse
// and mean what the defaults mean.
func TestCommittedDefaultFileMatchesDefaults(t *testing.T) {
	cfg, err := Load("../../config_default.hujson")
	if err != nil {
		t.Fatalf("config_default.hujson does not load: %v", err)
	}
	if !reflect.DeepEqual(cfg, Default()) {
		t.Fatalf("config_default.hujson does not match Default():\n got %+v\nwant %+v", cfg, Default())
	}
}

// Comments and trailing commas are the reason for HuJSON in the first
// place.
func TestLoadHuJSON(t *testing.T) {
	path := write(t, `{
		// The address to listen on.
		"Addr": ":9999",
		/* durations are written the way Go writes them */
		"IdleTimeout": "30s",
		"Capacity": 1024,
	}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Addr != ":9999" {
		t.Errorf("Addr = %q, want %q", cfg.Addr, ":9999")
	}
	if cfg.IdleTimeout.Duration() != 30*time.Second {
		t.Errorf("IdleTimeout = %v, want 30s", cfg.IdleTimeout)
	}
	if cfg.Capacity != 1024 {
		t.Errorf("Capacity = %d, want 1024", cfg.Capacity)
	}
	// Everything unmentioned keeps its default.
	if cfg.WriteBatch != Default().WriteBatch {
		t.Errorf("WriteBatch = %d, want the default %d", cfg.WriteBatch, Default().WriteBatch)
	}
}

func TestLoadRejectsBadValues(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{"empty addr", `{"Addr": ""}`},
		{"zero capacity", `{"Capacity": 0}`},
		{"negative batch", `{"WriteBatch": -1}`},
		{"zero idle timeout", `{"IdleTimeout": "0s"}`},
		{"duration as a number", `{"IdleTimeout": 90}`},
		{"nonsense duration", `{"IdleTimeout": "ninety"}`},
		{"not json at all", `this is not a config`},
		{"wrong type", `{"Capacity": "lots"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Load(write(t, tt.content)); err == nil {
				t.Fatalf("Load accepted %s", tt.content)
			}
		})
	}
}

func TestDurationRoundTrip(t *testing.T) {
	path := write(t, `{"IdleTimeout": "2m30s", "WriteTimeout": "1s"}`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.IdleTimeout.Duration(); got != 150*time.Second {
		t.Errorf("IdleTimeout = %v, want 2m30s", got)
	}
	if got := cfg.WriteTimeout.Duration(); got != time.Second {
		t.Errorf("WriteTimeout = %v, want 1s", got)
	}
}
