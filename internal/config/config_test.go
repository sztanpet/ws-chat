package config

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"
)

// documented matches a commented-out setting: // "Key": value,
var documented = regexp.MustCompile(`^// "[A-Za-z][A-Za-z0-9]*":`)

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

// The committed default file is the documentation, so it had better parse.
func TestCommittedDefaultFileLoads(t *testing.T) {
	cfg, err := Load("../../config_default.hujson")
	if err != nil {
		t.Fatalf("config_default.hujson does not load: %v", err)
	}
	if !reflect.DeepEqual(cfg, Default()) {
		t.Fatalf("config_default.hujson changes something:\n got %+v\nwant %+v", cfg, Default())
	}
}

// And it had better say what the defaults ARE.
//
// The test above cannot check that on its own: every value in the file is
// commented out, so it loads as the defaults whatever the comments claim,
// and comparing that to Default() compares Default() to itself. It passed
// happily while the file documented a Capacity of 4096 and the code used
// 256 — twice, in two different fields.
//
// So this one uncomments the file and loads THAT. Every documented value
// has to be the real one.
func TestCommittedDefaultFileDocumentsTheRealDefaults(t *testing.T) {
	raw, err := os.ReadFile("../../config_default.hujson")
	if err != nil {
		t.Fatalf("reading config_default.hujson: %v", err)
	}

	var uncommented strings.Builder
	uncommented.WriteString("{\n")
	settings := 0
	for line := range strings.Lines(string(raw)) {
		trimmed := strings.TrimSpace(line)
		// A documented setting looks like: // "Key": value,
		// Prose in the comments can start with a quote too, so the key has
		// to look like a field name and be followed by a colon.
		if !documented.MatchString(trimmed) {
			continue
		}
		uncommented.WriteString(strings.TrimPrefix(trimmed, "// "))
		uncommented.WriteString("\n")
		settings++
	}
	uncommented.WriteString("}\n")

	if settings == 0 {
		t.Fatal("no documented settings found — has the file's shape changed?")
	}

	path := write(t, uncommented.String())
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("the documented values do not load: %v\n%s", err, uncommented.String())
	}
	// An empty JSON array decodes to an empty slice where the zero value is
	// nil. They mean the same thing here and DeepEqual disagrees.
	if len(cfg.AllowedOrigins) == 0 {
		cfg.AllowedOrigins = nil
	}
	if !reflect.DeepEqual(cfg, Default()) {
		t.Fatalf("config_default.hujson documents values that are not the defaults:\n got %+v\nwant %+v", cfg, Default())
	}

	// Every field should be documented, or the file is not the reference it
	// claims to be.
	if want := reflect.TypeFor[Config]().NumField(); settings != want {
		t.Errorf("the file documents %d settings, but Config has %d fields", settings, want)
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
