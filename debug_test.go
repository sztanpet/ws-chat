package main

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/sztanpet/ws-chat/internal/config"
	"github.com/sztanpet/ws-chat/internal/hook"
	"github.com/sztanpet/ws-chat/internal/proto"
)

// scrape reads the metrics straight out of the registry, which is what the
// endpoint does.
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

	// Nothing has happened yet, but the metrics exist to say so.
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

	// One channel, both connections in it.
	requireMetric(t, out, "wschat_channels 1")
	requireMetric(t, out, "wschat_memberships 2")
	requireMetric(t, out, "wschat_joins_total 2")
}

// Every refusal is counted by the code the client was told, because they
// are counted in the same place they are sent.
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

	mod.send(proto.Command{Verb: proto.VerbMute, Nick: "auser"})
	mod.expectMod(proto.ActionMute, "auser")
	user.expectMod(proto.ActionMute, "auser")

	out := ta.scrape(t)
	requireMetric(t, out, `wschat_moderation_total{action="mute"} 1`)
	requireMetric(t, out, "wschat_moderation_mutes 1")
}

// The gauges read the live state rather than a mirror of it, so they go
// down again.
func TestMetricsGaugesFollowState(t *testing.T) {
	ta := newTestApp(t)

	c := ta.dial(t)
	requireMetric(t, ta.scrape(t), "wschat_connections 1")

	c.ws.CloseNow()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(ta.scrape(t), "wschat_connections 0") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("the connection gauge did not come back down:\n%s", ta.scrape(t))
}

// The debug listener serves both, and on its own address.
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
		// A real profile, fetched the way pprof fetches it.
		if body, _ := get(t, base+"/debug/pprof/heap?debug=1"); !strings.Contains(body, "heap profile") {
			t.Errorf("the heap profile looks wrong:\n%.200s", body)
		}
	})
}

// An empty DebugAddr binds nothing at all.
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
