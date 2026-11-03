package main

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/sztanpet/ws-chat/internal/config"
	"github.com/sztanpet/ws-chat/internal/hook"
	"github.com/sztanpet/ws-chat/internal/proto"
)

func (c *client) expectJoined(channel string) proto.Backlog {
	c.t.Helper()

	var backlog proto.Backlog
	for {
		verb, payload := c.recv()
		if verb == proto.VerbBacklog {
			c.decode(payload, &backlog)
			if backlog.Channel != channel {
				c.t.Fatalf("BACKLOG channel = %q, want %q", backlog.Channel, channel)
			}
			break
		}
		if verb != proto.VerbJoin && verb != proto.VerbPart {
			c.t.Fatalf("got %s %s, want %s first", verb, payload, proto.VerbBacklog)
		}
	}

	c.expectJoin(channel, c.nick)
	return backlog
}

// Steps over other presence: a third client connecting must not fail the test.
func (c *client) expectJoin(channel, nick string) proto.Join {
	c.t.Helper()
	for {
		verb, payload := c.recv()
		if verb == proto.VerbJoin {
			var join proto.Join
			c.decode(payload, &join)
			if join.Channel == channel && (nick == "" || join.Nick == nick) {
				return join
			}
			continue
		}
		if verb != proto.VerbPart {
			c.t.Fatalf("got %s %s, want a JOIN for %s/%s", verb, payload, channel, nick)
		}
	}
}

func (c *client) expectPart(channel, nick string) {
	c.t.Helper()
	for {
		verb, payload := c.recv()
		if verb == proto.VerbPart {
			var part proto.Part
			c.decode(payload, &part)
			if part.Channel == channel && part.Nick == nick {
				return
			}
			continue
		}
		if verb != proto.VerbJoin {
			c.t.Fatalf("got %s %s, want a PART for %s/%s", verb, payload, channel, nick)
		}
	}
}

func (c *client) expectNames(channel string) proto.Names {
	c.t.Helper()
	verb, payload := c.recv()
	if verb != proto.VerbNames {
		c.t.Fatalf("got %s %s, want %s", verb, payload, proto.VerbNames)
	}
	var names proto.Names
	c.decode(payload, &names)
	if names.Channel != channel {
		c.t.Fatalf("NAMES channel = %q, want %q", names.Channel, channel)
	}
	return names
}

func TestAutojoinsTheDefaultChannel(t *testing.T) {
	ta := newTestApp(t)
	c := ta.dial(t)

	if !slices.Equal(c.joined, []string{"main"}) {
		t.Fatalf("joined %v, want [main]", c.joined)
	}
	if got := ta.app.channelNames(); !slices.Equal(got, []string{"main"}) {
		t.Fatalf("channels %v, want [main]", got)
	}
}

func TestMessagesStayInTheirChannel(t *testing.T) {
	ta := newTestApp(t)
	alice, bob := ta.dial(t), ta.dial(t)
	alice.expectJoin("main", bob.nick)

	alice.send(proto.Command{Verb: proto.VerbJoin, Channel: "other"})
	alice.expectJoined("other")

	alice.send(proto.Command{Verb: proto.VerbMsg, Channel: "other", Data: "in other"})
	if got := alice.expectMsg(alice.nick, "in other"); got.Channel != "other" {
		t.Fatalf("channel = %q, want other", got.Channel)
	}

	alice.send(proto.Command{Verb: proto.VerbMsg, Data: "in main"})
	if got := bob.expectMsg(alice.nick, "in main"); got.Channel != "main" {
		t.Fatalf("channel = %q, want main", got.Channel)
	}
}

func TestMessageToAChannelYouAreNotIn(t *testing.T) {
	ta := newTestApp(t)
	c := ta.dial(t)

	c.send(proto.Command{Verb: proto.VerbMsg, Channel: "elsewhere", Data: "hello?"})
	c.expectErr(proto.ErrNotJoined)
}

func TestJoinAndPart(t *testing.T) {
	ta := newTestApp(t)
	alice, bob := ta.dial(t), ta.dial(t)
	alice.expectJoin("main", bob.nick)

	alice.send(proto.Command{Verb: proto.VerbJoin, Channel: "second"})
	alice.expectJoined("second")

	bob.send(proto.Command{Verb: proto.VerbJoin, Channel: "second"})
	bob.expectJoined("second")
	alice.expectJoin("second", bob.nick)

	bob.send(proto.Command{Verb: proto.VerbPart, Channel: "second"})
	bob.expectPart("second", bob.nick)
	alice.expectPart("second", bob.nick)

	bob.send(proto.Command{Verb: proto.VerbMsg, Channel: "second", Data: "still here?"})
	bob.expectErr(proto.ErrNotJoined)
}

func TestJoinTwice(t *testing.T) {
	ta := newTestApp(t)
	c := ta.dial(t)

	c.send(proto.Command{Verb: proto.VerbJoin, Channel: "main"})
	c.expectErr(proto.ErrAlreadyJoined)
}

func TestPartSomewhereYouAreNot(t *testing.T) {
	ta := newTestApp(t)
	c := ta.dial(t)

	c.send(proto.Command{Verb: proto.VerbPart, Channel: "nowhere"})
	c.expectErr(proto.ErrNotJoined)
}

func TestBadChannelNames(t *testing.T) {
	ta := newTestApp(t)
	c := ta.dial(t)

	for _, name := range []string{"has space", strings.Repeat("x", 65), "tab\there", "new\nline"} {
		c.send(proto.Command{Verb: proto.VerbJoin, Channel: name})
		c.expectErr(proto.ErrNoChannel)
	}
}

func TestDisconnectPartsEverything(t *testing.T) {
	ta := newTestApp(t)
	alice, bob := ta.dial(t), ta.dial(t)
	alice.expectJoin("main", bob.nick)

	bob.send(proto.Command{Verb: proto.VerbJoin, Channel: "second"})
	bob.expectJoined("second")
	alice.send(proto.Command{Verb: proto.VerbJoin, Channel: "second"})
	alice.expectJoined("second")
	bob.expectJoin("second", alice.nick)

	_ = bob.ws.CloseNow()

	// In whichever order the teardown got to them.
	var parted []string
	for range 2 {
		verb, payload := alice.recv()
		if verb != proto.VerbPart {
			t.Fatalf("got %s %s, want PART", verb, payload)
		}
		var part proto.Part
		alice.decode(payload, &part)
		parted = append(parted, part.Channel)
	}
	slices.Sort(parted)
	if !slices.Equal(parted, []string{"main", "second"}) {
		t.Fatalf("parted %v, want both channels", parted)
	}
}

func TestNames(t *testing.T) {
	ta := newTestApp(t)
	alice, bob := ta.dial(t), ta.dial(t)
	alice.expectJoin("main", bob.nick)

	alice.send(proto.Command{Verb: proto.VerbNames, Channel: "main"})
	names := alice.expectNames("main")

	if names.Total != 2 {
		t.Fatalf("total = %d, want 2", names.Total)
	}
	want := []string{alice.nick, bob.nick}
	slices.Sort(want)
	if !slices.Equal(names.Nicks, want) {
		t.Fatalf("nicks = %v, want %v", names.Nicks, want)
	}
}

func TestNamesRequiresMembership(t *testing.T) {
	ta := newTestApp(t)
	alice := ta.dial(t)

	alice.send(proto.Command{Verb: proto.VerbJoin, Channel: "private"})
	alice.expectJoined("private")

	bob := ta.dial(t)
	bob.send(proto.Command{Verb: proto.VerbNames, Channel: "private"})
	bob.expectErr(proto.ErrNotJoined)
}

func TestBacklogIsPerChannel(t *testing.T) {
	ta := newTestApp(t)
	alice := ta.dial(t)

	alice.send(proto.Command{Verb: proto.VerbMsg, Data: "in main"})
	alice.expectMsg("", "in main")

	alice.send(proto.Command{Verb: proto.VerbJoin, Channel: "second"})
	alice.expectJoined("second")
	alice.send(proto.Command{Verb: proto.VerbMsg, Channel: "second", Data: "in second"})
	alice.expectMsg("", "in second")

	bob := ta.dial(t)
	if len(bob.backlog) != 1 {
		t.Fatalf("backlog has %d messages, want 1", len(bob.backlog))
	}
	if bob.backlog[0].Data != "in main" {
		t.Fatalf("backlog = %q, want the main-channel message", bob.backlog[0].Data)
	}
	if bob.backlog[0].Channel != "main" {
		t.Fatalf("backlog channel = %q, want main", bob.backlog[0].Channel)
	}

	bob.send(proto.Command{Verb: proto.VerbJoin, Channel: "second"})
	backlog := bob.expectJoined("second")
	if len(backlog.Messages) != 1 || backlog.Messages[0].Data != "in second" {
		t.Fatalf("second's backlog = %v", backlog.Messages)
	}
}

func TestChannelRateLimitIsPerChannel(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{
		Limiter: &fakeLimiter{channel: hook.Limits{Burst: 1, Interval: time.Hour}},
	})
	c := ta.dial(t)

	c.send(proto.Command{Verb: proto.VerbMsg, Data: "one"})
	c.expectMsg("", "one")
	c.send(proto.Command{Verb: proto.VerbMsg, Data: "two"})
	c.expectErr(proto.ErrChanThrottled)

	c.send(proto.Command{Verb: proto.VerbJoin, Channel: "second"})
	c.expectJoined("second")
	c.send(proto.Command{Verb: proto.VerbMsg, Channel: "second", Data: "elsewhere"})
	c.expectMsg("", "elsewhere")
}

func TestTooManyChannelsPerConnection(t *testing.T) {
	ta := newTestApp(t, func(c *config.Config) { c.MaxChannelsPerConn = 2 })
	c := ta.dial(t) // already in main

	c.send(proto.Command{Verb: proto.VerbJoin, Channel: "second"})
	c.expectJoined("second")

	c.send(proto.Command{Verb: proto.VerbJoin, Channel: "third"})
	c.expectErr(proto.ErrTooManyChans)
}

func TestTooManyChannelsOnTheServer(t *testing.T) {
	ta := newTestApp(t, func(c *config.Config) { c.MaxChannels = 1 })
	c := ta.dial(t) // main is the one channel there is room for

	c.send(proto.Command{Verb: proto.VerbJoin, Channel: "second"})
	c.expectErr(proto.ErrTooManyChans)
}

func TestEmptyChannelsAreReclaimed(t *testing.T) {
	ta := newTestApp(t)
	c := ta.dial(t)

	c.send(proto.Command{Verb: proto.VerbJoin, Channel: "temporary"})
	c.expectJoined("temporary")
	c.send(proto.Command{Verb: proto.VerbMsg, Channel: "temporary", Data: "remember me"})
	c.expectMsg("", "remember me")

	if got := ta.app.channelNames(); !slices.Contains(got, "temporary") {
		t.Fatalf("channels %v, want temporary among them", got)
	}
	if n := ta.app.sweepChannels(); n != 0 {
		t.Fatalf("swept %d channels that still have members", n)
	}

	c.send(proto.Command{Verb: proto.VerbPart, Channel: "temporary"})
	c.expectPart("temporary", c.nick)

	// The PART arrives before the teardown, so the member set may not be empty yet.
	c.sync()

	if n := ta.app.sweepChannels(); n != 1 {
		t.Fatalf("swept %d channels, want the 1 that emptied", n)
	}
	if got := ta.app.channelNames(); slices.Contains(got, "temporary") {
		t.Fatalf("channels %v, want temporary gone", got)
	}

	c.send(proto.Command{Verb: proto.VerbJoin, Channel: "temporary"})
	backlog := c.expectJoined("temporary")
	if len(backlog.Messages) != 1 || backlog.Messages[0].Data != "remember me" {
		t.Fatalf("the reclaimed channel lost its history: %v", backlog.Messages)
	}
}

type fakeChannels struct {
	autojoin []string
	refuse   map[string]string // channel -> reason
}

func (f fakeChannels) Autojoin(ctx context.Context, id hook.Identity) []string {
	return f.autojoin
}

func (f fakeChannels) CanJoin(ctx context.Context, id hook.Identity, channel string) (bool, string) {
	if reason, ok := f.refuse[channel]; ok {
		return false, reason
	}
	return true, ""
}

func TestAutojoinHook(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{
		Channels: fakeChannels{autojoin: []string{"lobby", "announcements"}},
	})
	c := ta.dial(t)

	slices.Sort(c.joined)
	if !slices.Equal(c.joined, []string{"announcements", "lobby"}) {
		t.Fatalf("joined %v, want both channels the hook named", c.joined)
	}

	c.send(proto.Command{Verb: proto.VerbMsg, Channel: "lobby", Data: "hello lobby"})
	if got := c.expectMsg("", "hello lobby"); got.Channel != "lobby" {
		t.Fatalf("channel = %q, want lobby", got.Channel)
	}
}

func TestAutojoinNothing(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{
		Channels: fakeChannels{autojoin: []string{}},
	})
	c := ta.dial(t)

	if len(c.joined) != 0 {
		t.Fatalf("joined %v, want nothing", c.joined)
	}
	c.send(proto.Command{Verb: proto.VerbMsg, Data: "anybody there"})
	c.expectErr(proto.ErrNotJoined)

	c.send(proto.Command{Verb: proto.VerbJoin, Channel: "main"})
	c.expectJoined("main")
}

func TestCanJoinRefuses(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{
		Channels: fakeChannels{refuse: map[string]string{"staff": "inviteonly"}},
	})
	c := ta.dial(t)

	c.send(proto.Command{Verb: proto.VerbJoin, Channel: "staff"})
	c.expectErr("inviteonly")

	c.send(proto.Command{Verb: proto.VerbJoin, Channel: "public"})
	c.expectJoined("public")

	if got := ta.app.channelNames(); slices.Contains(got, "staff") {
		t.Fatalf("channels %v, want no staff channel", got)
	}
}

func TestAutojoinSkipsCanJoin(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{
		Channels: fakeChannels{
			autojoin: []string{"staff"},
			refuse:   map[string]string{"staff": "inviteonly"},
		},
	})
	c := ta.dial(t)

	if !slices.Equal(c.joined, []string{"staff"}) {
		t.Fatalf("joined %v, want [staff] despite CanJoin refusing it", c.joined)
	}
}

func TestChannelsAreIsolatedUnderLoad(t *testing.T) {
	ta := newTestApp(t)
	alice, bob := ta.dial(t), ta.dial(t)
	alice.expectJoin("main", bob.nick)

	alice.send(proto.Command{Verb: proto.VerbJoin, Channel: "quiet"})
	alice.expectJoined("quiet")

	const n = 10
	for i := range n {
		bob.send(proto.Command{Verb: proto.VerbMsg, Data: fmt.Sprintf("main %d", i)})
	}
	alice.send(proto.Command{Verb: proto.VerbMsg, Channel: "quiet", Data: "quiet one"})

	seen := map[string]int{}
	for range n + 1 {
		msg := alice.expectMsg("", "")
		seen[msg.Channel]++
	}
	if seen["main"] != n || seen["quiet"] != 1 {
		t.Fatalf("saw %v, want %d in main and 1 in quiet", seen, n)
	}
}

// The window is narrow, hence a sweep spinning flat out and 1000 joins.
func TestJoinDoesNotRaceTheSweep(t *testing.T) {
	ta := newTestApp(t)
	c := ta.dial(t)

	stop := make(chan struct{})
	swept := make(chan struct{})
	go func() {
		defer close(swept)
		for {
			select {
			case <-stop:
				return
			default:
				ta.app.sweepChannels()
			}
		}
	}()
	defer func() { close(stop); <-swept }()

	for range 1000 {
		c.send(proto.Command{Verb: proto.VerbJoin, Channel: "revolving"})
		c.expectJoined("revolving")
		c.send(proto.Command{Verb: proto.VerbPart, Channel: "revolving"})
		c.expectPart("revolving", c.nick)
		c.sync()
	}
}
