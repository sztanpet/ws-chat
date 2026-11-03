package main

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/sztanpet/ws-chat/internal/config"
	"github.com/sztanpet/ws-chat/internal/hook"
	"github.com/sztanpet/ws-chat/internal/proto"
)

func (ta *testApp) scrape(t *testing.T) string {
	t.Helper()
	var out strings.Builder
	if _, err := ta.app.registry.WriteTo(&out); err != nil {
		t.Fatalf("scrape: %v", err)
	}
	return out.String()
}

func requireMetric(t *testing.T, scrape, want string) {
	t.Helper()
	if !strings.Contains(scrape, want) {
		t.Errorf("metrics are missing %q:\n%s", want, scrape)
	}
}

func TestMetricsCountConnectionsAndTraffic(t *testing.T) {
	ta := newTestApp(t)

	requireMetric(t, ta.scrape(t), "wschat_connections 0")

	alice, bob := ta.dial(t), ta.dial(t)
	alice.send(proto.Command{Verb: proto.VerbMsg, Data: "hello"})
	expectAll([]*client{alice, bob}, "", "hello")

	alice.send(proto.Command{Verb: proto.VerbPriv, Nick: bob.nick, Data: "psst"})
	bob.expectPriv("psst")
	alice.expectPriv("psst")

	out := ta.scrape(t)
	requireMetric(t, out, "wschat_connections 2")
	requireMetric(t, out, "wschat_connections_total 2")
	requireMetric(t, out, "wschat_messages_total 1")
	requireMetric(t, out, "wschat_private_messages_total 1")
	requireMetric(t, out, `wschat_commands_total{verb="MSG"} 1`)
	requireMetric(t, out, `wschat_commands_total{verb="PRIVMSG"} 1`)
	requireMetric(t, out, `wschat_codec_negotiated_total{codec="chat.json"} 2`)

	requireMetric(t, out, "wschat_channels 1")
	requireMetric(t, out, "wschat_memberships 2")
	requireMetric(t, out, "wschat_joins_total 2")
}

func TestMetricsCountRefusalsByCode(t *testing.T) {
	ta := newTestApp(t, func(c *config.Config) { c.MaxMessage = 4 })
	c := ta.dial(t)

	c.send(proto.Command{Verb: proto.VerbMsg, Data: ""})
	c.expectErr(proto.ErrEmpty)
	c.send(proto.Command{Verb: proto.VerbMsg, Data: "far too long"})
	c.expectErr(proto.ErrTooLong)
	c.send(proto.Command{Verb: "FLARP"})
	c.expectErr(proto.ErrUnknown)

	out := ta.scrape(t)
	requireMetric(t, out, `wschat_refusals_total{code="empty"} 1`)
	requireMetric(t, out, `wschat_refusals_total{code="toolong"} 1`)
	requireMetric(t, out, `wschat_refusals_total{code="unknown"} 1`)
}

func TestMetricsCountHookRefusals(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{
		Auth:   optionalAuth{"a": {ID: "u1", Nick: "alice"}},
		Filter: registeredOnly{},
	})

	alice, err := ta.dialWith(t, "?token=a")
	if err != nil {
		t.Fatalf("dial alice: %v", err)
	}
	watcher, err := ta.dialWith(t, "")
	if err != nil {
		t.Fatalf("dial the watcher: %v", err)
	}

	watcher.send(proto.Command{Verb: proto.VerbMsg, Data: "let me in"})
	watcher.expectErr(proto.ErrNeedLogin)
	watcher.send(proto.Command{Verb: proto.VerbPriv, Nick: alice.nick, Data: "let me in"})
	watcher.expectErr(proto.ErrNeedLogin)

	out := ta.scrape(t)
	requireMetric(t, out, `wschat_refusals_total{code="needlogin"} 2`)

	requireMetric(t, out, `wschat_commands_total{verb="PRIVMSG"} 1`)
	requireMetric(t, out, "wschat_private_messages_total 0")
}

func TestMetricsCountRefusedConnections(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{
		Auth: fakeAuth{byToken: map[string]hook.Identity{"good": {ID: "u1"}}},
	})

	if _, err := ta.dialWith(t, "?token=nope"); err == nil {
		t.Fatal("dial succeeded without a token")
	}
	requireMetric(t, ta.scrape(t), `wschat_connections_refused_total{reason="unauthorized"} 1`)
}

func TestMetricsCountModeration(t *testing.T) {
	ta, mod, user := modApp(t, hook.Hooks{})

	mod.send(proto.Command{Verb: proto.VerbMute, Nick: "auser", Channel: "main"})
	mod.expectMod(proto.ActionMute, "auser")
	user.expectMod(proto.ActionMute, "auser")

	out := ta.scrape(t)
	requireMetric(t, out, `wschat_moderation_total{action="mute"} 1`)
	requireMetric(t, out, "wschat_moderation_mutes 1")
}

func TestMetricsGaugesFollowState(t *testing.T) {
	ta := newTestApp(t)

	c := ta.dial(t)
	requireMetric(t, ta.scrape(t), "wschat_connections 1")

	_ = c.ws.CloseNow()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(ta.scrape(t), "wschat_connections 0") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("the connection gauge did not come back down:\n%s", ta.scrape(t))
}

func TestDebugListener(t *testing.T) {
	ta := newTestApp(t, func(c *config.Config) { c.DebugAddr = "127.0.0.1:0" })

	if err := ta.app.serveDebug(t.Context()); err != nil {
		t.Fatalf("serveDebug: %v", err)
	}
	t.Cleanup(ta.app.closeDebug)

	base := "http://" + ta.app.debugAddr

	t.Run("metrics", func(t *testing.T) {
		body, ctype := get(t, base+"/metrics")
		if !strings.HasPrefix(ctype, "text/plain") {
			t.Errorf("content type = %q, want text/plain", ctype)
		}
		if !strings.Contains(body, "wschat_connections") {
			t.Errorf("the scrape has no metrics in it:\n%s", body)
		}
	})

	t.Run("pprof index", func(t *testing.T) {
		if body, _ := get(t, base+"/debug/pprof/"); !strings.Contains(body, "goroutine") {
			t.Errorf("the pprof index looks wrong:\n%s", body)
		}
	})

	t.Run("heap profile", func(t *testing.T) {
		if body, _ := get(t, base+"/debug/pprof/heap?debug=1"); !strings.Contains(body, "heap profile") {
			t.Errorf("the heap profile looks wrong:\n%.200s", body)
		}
	})
}

// Not taskListener: httptest's listener goroutine is not ours to label.
func TestGoroutinesAreLabelled(t *testing.T) {
	ta := newTestApp(t, func(c *config.Config) { c.DebugAddr = "127.0.0.1:0" })
	if err := ta.app.serveDebug(t.Context()); err != nil {
		t.Fatalf("serveDebug: %v", err)
	}
	t.Cleanup(ta.app.closeDebug)

	c := ta.dial(t)
	c.send(proto.Command{Verb: proto.VerbMsg, Data: "hello"})
	c.expectMsg("", "hello")

	profile := goroutineProfile(t)
	for _, task := range []string{taskConn, taskPrivPump, taskChanPump, taskJanitor, taskDebug} {
		if !bytes.Contains(profile, stringEntry(task)) {
			t.Errorf("no goroutine is labelled %s=%q", labelTask, task)
		}
	}
	if !bytes.Contains(profile, stringEntry(labelTask)) {
		t.Errorf("the label key %q is not in the profile", labelTask)
	}
}

// Protobuf, the only form with labels: ?debug=1 drops them.
func goroutineProfile(t *testing.T) []byte {
	t.Helper()

	var gz bytes.Buffer
	if err := pprof.Lookup("goroutine").WriteTo(&gz, 0); err != nil {
		t.Fatalf("goroutine profile: %v", err)
	}
	zr, err := gzip.NewReader(&gz)
	if err != nil {
		t.Fatalf("the profile is not gzipped: %v", err)
	}
	raw, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("reading the profile: %v", err)
	}
	return raw
}

// The framing, not the bare text: (*conn).serve would match "conn" regardless.
func stringEntry(s string) []byte {
	return append([]byte{6<<3 | 2, byte(len(s))}, s...)
}

func TestDebugListenerDisabled(t *testing.T) {
	ta := newTestApp(t, func(c *config.Config) { c.DebugAddr = "" })

	if err := ta.app.serveDebug(t.Context()); err != nil {
		t.Fatalf("serveDebug: %v", err)
	}
	if ta.app.debug != nil {
		t.Fatal("an empty DebugAddr still started a listener")
	}
}

func get(t *testing.T, url string) (body, contentType string) {
	t.Helper()

	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d", url, resp.StatusCode)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading %s: %v", url, err)
	}
	return string(raw), resp.Header.Get("Content-Type")
}
