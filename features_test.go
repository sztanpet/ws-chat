package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/sztanpet/ws-chat/internal/config"
	"github.com/sztanpet/ws-chat/internal/filter"
	"github.com/sztanpet/ws-chat/internal/hook"
	"github.com/sztanpet/ws-chat/internal/proto"
)

// --- backlog ---------------------------------------------------------

func TestBacklogReplay(t *testing.T) {
	ta := newTestApp(t)
	alice := ta.dial(t)

	if len(alice.backlog) != 0 {
		t.Fatalf("the first client got %d backlog messages, want none", len(alice.backlog))
	}

	for i := range 3 {
		alice.send(proto.Command{Verb: proto.VerbMsg, Data: fmt.Sprintf("message %d", i)})
		alice.expectMsg("", fmt.Sprintf("message %d", i))
	}

	// Somebody arriving now sees what was said, in order, before anything
	// live.
	bob := ta.dial(t)
	if len(bob.backlog) != 3 {
		t.Fatalf("backlog has %d messages, want 3", len(bob.backlog))
	}
	for i, msg := range bob.backlog {
		if want := fmt.Sprintf("message %d", i); msg.Data != want {
			t.Errorf("backlog[%d] = %q, want %q", i, msg.Data, want)
		}
		if msg.Nick != alice.nick {
			t.Errorf("backlog[%d] nick = %q, want %q", i, msg.Nick, alice.nick)
		}
		if msg.ID == 0 || msg.Timestamp == 0 {
			t.Errorf("backlog[%d] lost its id or timestamp", i)
		}
	}

	// And then live traffic follows it.
	alice.send(proto.Command{Verb: proto.VerbMsg, Data: "live"})
	bob.expectMsg(alice.nick, "live")
}

func TestBacklogIsCapped(t *testing.T) {
	ta := newTestApp(t, func(c *config.Config) { c.Backlog = 3 })
	alice := ta.dial(t)

	for i := range 6 {
		alice.send(proto.Command{Verb: proto.VerbMsg, Data: fmt.Sprintf("m%d", i)})
		alice.expectMsg("", fmt.Sprintf("m%d", i))
	}

	bob := ta.dial(t)
	if len(bob.backlog) != 3 {
		t.Fatalf("backlog has %d messages, want the cap of 3", len(bob.backlog))
	}
	// The LAST three, not the first three.
	for i, msg := range bob.backlog {
		if want := fmt.Sprintf("m%d", i+3); msg.Data != want {
			t.Errorf("backlog[%d] = %q, want %q", i, msg.Data, want)
		}
	}
}

func TestBacklogDisabled(t *testing.T) {
	ta := newTestApp(t, func(c *config.Config) { c.Backlog = 0 })
	alice := ta.dial(t)

	alice.send(proto.Command{Verb: proto.VerbMsg, Data: "said before you arrived"})
	alice.expectMsg("", "said before you arrived")

	// dial asserts the frame sequence, so getting here at all means no
	// BACKLOG frame was sent. The next frame bob sees is live.
	bob := ta.dial(t)
	if len(bob.backlog) != 0 {
		t.Fatalf("backlog disabled but %d messages arrived", len(bob.backlog))
	}
	alice.send(proto.Command{Verb: proto.VerbMsg, Data: "live"})
	bob.expectMsg(alice.nick, "live")
}

// Private messages are not channel history and must not turn up in it.
func TestBacklogExcludesPrivateMessages(t *testing.T) {
	ta := newTestApp(t)
	alice, bob := ta.dial(t), ta.dial(t)

	alice.send(proto.Command{Verb: proto.VerbPriv, Nick: bob.nick, Data: "for bob only"})
	bob.expectPriv("for bob only")
	alice.expectPriv("for bob only")

	carol := ta.dial(t)
	for _, msg := range carol.backlog {
		if strings.Contains(msg.Data, "for bob only") {
			t.Fatal("a private message turned up in the channel backlog")
		}
	}
}

// The backlog is written under the lock that orders the fan-out, so what a
// joining client is shown cannot disagree with what the room saw.
func TestBacklogOrderMatchesDelivery(t *testing.T) {
	ta := newTestApp(t)
	alice, bob := ta.dial(t), ta.dial(t)

	const n = 20
	for i := range n {
		sender := alice
		if i%2 == 1 {
			sender = bob
		}
		sender.send(proto.Command{Verb: proto.VerbMsg, Data: fmt.Sprintf("m%d", i)})
	}

	var delivered []uint64
	for range n {
		delivered = append(delivered, alice.expectMsg("", "").ID)
	}

	carol := ta.dial(t)
	if len(carol.backlog) != n {
		t.Fatalf("backlog has %d, want %d", len(carol.backlog), n)
	}
	for i, msg := range carol.backlog {
		if msg.ID != delivered[i] {
			t.Fatalf("backlog[%d] id = %d, delivered was %d", i, msg.ID, delivered[i])
		}
	}
}

// --- data attached to a user's messages -------------------------------

func TestMessagesCarryRolesAndAttrs(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{
		Auth: fakeAuth{byToken: map[string]hook.Identity{
			"a": {ID: "u1", Nick: "alice"},
			"b": {ID: "u2", Nick: "bob"},
		}},
		Directory: fakeDirectory{
			"u1": {Roles: []string{"mod", "sub"}, Attrs: map[string]string{"colour": "red"}},
		},
	})

	alice, err := ta.dialWith(t, "?token=a")
	if err != nil {
		t.Fatalf("dial alice: %v", err)
	}
	bob, err := ta.dialWith(t, "?token=b")
	if err != nil {
		t.Fatalf("dial bob: %v", err)
	}

	alice.send(proto.Command{Verb: proto.VerbMsg, Data: "hello"})
	for _, c := range []*client{alice, bob} {
		msg := c.expectMsg("alice", "hello")
		if len(msg.Roles) != 2 || msg.Roles[0] != "mod" {
			t.Errorf("roles = %v, want [mod sub]", msg.Roles)
		}
		if msg.Attrs["colour"] != "red" {
			t.Errorf("attrs = %v, want colour=red", msg.Attrs)
		}
	}

	// Somebody with nothing attached carries nothing, rather than empty
	// collections on every message.
	bob.send(proto.Command{Verb: proto.VerbMsg, Data: "plain"})
	msg := alice.expectMsg("bob", "plain")
	if len(msg.Roles) != 0 || len(msg.Attrs) != 0 {
		t.Errorf("an identity with no data carried roles=%v attrs=%v", msg.Roles, msg.Attrs)
	}
}

func TestPrivateMessagesCarryRolesAndAttrs(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{
		Auth: fakeAuth{byToken: map[string]hook.Identity{
			"a": {ID: "u1", Nick: "alice"},
			"b": {ID: "u2", Nick: "bob"},
		}},
		Directory: fakeDirectory{
			"u1": {Roles: []string{"mod"}},
			"u2": {Roles: []string{"sub"}},
		},
	})

	alice, _ := ta.dialWith(t, "?token=a")
	bob, _ := ta.dialWith(t, "?token=b")

	alice.send(proto.Command{Verb: proto.VerbPriv, Nick: "bob", Data: "hi"})

	// The recipient's copy describes the sender.
	if got := bob.expectPriv("hi"); len(got.Roles) != 1 || got.Roles[0] != "mod" {
		t.Errorf("recipient sees roles %v, want the sender's [mod]", got.Roles)
	}
	// The sender's echo describes the recipient.
	if echo := alice.expectPriv("hi"); len(echo.Roles) != 1 || echo.Roles[0] != "sub" {
		t.Errorf("sender's echo has roles %v, want the recipient's [sub]", echo.Roles)
	}
}

func TestBacklogCarriesRolesAndAttrs(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{
		Auth: fakeAuth{byToken: map[string]hook.Identity{
			"a": {ID: "u1", Nick: "alice"},
			"b": {ID: "u2", Nick: "bob"},
		}},
		Directory: fakeDirectory{"u1": {Roles: []string{"mod"}}},
	})

	alice, _ := ta.dialWith(t, "?token=a")
	alice.send(proto.Command{Verb: proto.VerbMsg, Data: "for the record"})
	alice.expectMsg("alice", "for the record")

	bob, err := ta.dialWith(t, "?token=b")
	if err != nil {
		t.Fatalf("dial bob: %v", err)
	}
	if len(bob.backlog) != 1 {
		t.Fatalf("backlog has %d messages, want 1", len(bob.backlog))
	}
	if roles := bob.backlog[0].Roles; len(roles) != 1 || roles[0] != "mod" {
		t.Errorf("backlog message has roles %v, want [mod]", roles)
	}
}

// --- filters ----------------------------------------------------------

// A MessagePack client sends binary frames, which nothing validates, so
// this is the path invalid UTF-8 can actually arrive by.
func TestInvalidUTF8Refused(t *testing.T) {
	ta := newTestApp(t)
	c := ta.dialCodec(t, proto.MsgPack{})

	c.send(proto.Command{Verb: proto.VerbMsg, Data: "well \xff hello"})
	c.expectErr(filter.ReasonInvalidUTF8)

	// And the connection survives it.
	c.send(proto.Command{Verb: proto.VerbMsg, Data: "fine now"})
	c.expectMsg("", "fine now")
}

func TestZalgoRefused(t *testing.T) {
	ta := newTestApp(t)
	c := ta.dial(t)

	const combining = "́"
	c.send(proto.Command{Verb: proto.VerbMsg, Data: "h" + strings.Repeat(combining, 20)})
	c.expectErr(filter.ReasonZalgo)

	// Five is allowed; the default threshold is generous on purpose.
	c.send(proto.Command{Verb: proto.VerbMsg, Data: "h" + strings.Repeat(combining, 5)})
	c.expectMsg("", "h"+strings.Repeat(combining, 5))

	// Normal text that uses marks is unaffected.
	c.send(proto.Command{Verb: proto.VerbMsg, Data: "Tiếng Việt"})
	c.expectMsg("", "Tiếng Việt")
}

func TestZalgoThresholdIsConfigurable(t *testing.T) {
	ta := newTestApp(t, func(c *config.Config) { c.MaxDiacritics = 2 })
	c := ta.dial(t)

	c.send(proto.Command{Verb: proto.VerbMsg, Data: "h́́́"})
	c.expectErr(filter.ReasonZalgo)

	c.send(proto.Command{Verb: proto.VerbMsg, Data: "h́́"})
	c.expectMsg("", "h́́")
}

func TestFiltersCanBeDisabled(t *testing.T) {
	ta := newTestApp(t, func(c *config.Config) { c.MaxDiacritics = 0 })
	c := ta.dial(t)

	data := "h" + strings.Repeat("́", 50)
	c.send(proto.Command{Verb: proto.VerbMsg, Data: data})
	c.expectMsg("", data)
}

// The built-in filters run in front of a hook filter, and the hook still
// gets its say on what the built-ins allow.
func TestBuiltInFiltersRunBeforeTheHook(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{
		Filter: fakeFilter{deny: "badword", reason: "muted"},
	})
	c := ta.dial(t)

	// Zalgo is caught by a built-in even though the hook would allow it.
	c.send(proto.Command{Verb: proto.VerbMsg, Data: "h" + strings.Repeat("́", 20)})
	c.expectErr(filter.ReasonZalgo)

	// The hook still refuses what it refuses.
	c.send(proto.Command{Verb: proto.VerbMsg, Data: "badword"})
	c.expectErr("muted")

	c.send(proto.Command{Verb: proto.VerbMsg, Data: "fine"})
	c.expectMsg("", "fine")
}

func TestFiltersApplyToPrivateMessages(t *testing.T) {
	ta := newTestApp(t)
	alice, bob := ta.dial(t), ta.dial(t)

	alice.send(proto.Command{Verb: proto.VerbPriv, Nick: bob.nick, Data: "h" + strings.Repeat("́", 20)})
	alice.expectErr(filter.ReasonZalgo)
}

// --- moderation -------------------------------------------------------

// fakeAuthz answers per scope: the "mod" role runs a channel, and only
// "admin" may act server-wide. Keeping those separate is the point of the
// scope being passed in.
type fakeAuthz struct{}

func (fakeAuthz) CanModerate(ctx context.Context, id hook.Identity, channel string) bool {
	if channel == "" {
		return id.Has("admin")
	}
	return id.Has("mod") || id.Has("admin")
}

// modApp builds a server with a moderator, a normal user, and moderation
// authorization wired up.
func modApp(t *testing.T, hooks hook.Hooks, tweak ...func(*config.Config)) (*testApp, *client, *client) {
	t.Helper()

	hooks.Auth = fakeAuth{byToken: map[string]hook.Identity{
		"mod":   {ID: "u1", Nick: "themod", Roles: []string{"mod"}},
		"user":  {ID: "u2", Nick: "auser"},
		"later": {ID: "u3", Nick: "alatecomer"},
		"admin": {ID: "u4", Nick: "anadmin", Roles: []string{"admin"}},
	}}
	hooks.Authz = fakeAuthz{}

	ta := newTestAppWith(t, hooks, tweak...)
	mod, err := ta.dialWith(t, "?token=mod")
	if err != nil {
		t.Fatalf("dial the moderator: %v", err)
	}
	user, err := ta.dialWith(t, "?token=user")
	if err != nil {
		t.Fatalf("dial the user: %v", err)
	}
	return ta, mod, user
}

// expectMod reads one frame and requires it to be a moderation action.
func (c *client) expectMod(action, nick string) proto.Mod {
	c.t.Helper()
	verb, payload := c.nextInteresting()
	if verb != proto.VerbMod {
		c.t.Fatalf("got %s %s, want %s", verb, payload, proto.VerbMod)
	}
	var m proto.Mod
	c.decode(payload, &m)
	if m.Action != action {
		c.t.Fatalf("action = %q, want %q", m.Action, action)
	}
	if m.Nick != nick {
		c.t.Fatalf("target = %q, want %q", m.Nick, nick)
	}
	return m
}

// Default deny: a server that has not been told who its moderators are has
// none.
func TestModerationDeniedByDefault(t *testing.T) {
	ta := newTestApp(t)
	alice, bob := ta.dial(t), ta.dial(t)

	alice.send(proto.Command{Verb: proto.VerbMute, Nick: bob.nick, Channel: "main"})
	alice.expectErr(proto.ErrForbidden)

	// And nothing happened to bob.
	bob.send(proto.Command{Verb: proto.VerbMsg, Data: "still talking"})
	bob.expectMsg("", "still talking")
}

func TestNonModeratorRefused(t *testing.T) {
	_, mod, user := modApp(t, hook.Hooks{})

	user.send(proto.Command{Verb: proto.VerbMute, Nick: "themod"})
	user.expectErr(proto.ErrForbidden)

	// The moderator saw nothing, because nothing happened.
	mod.send(proto.Command{Verb: proto.VerbMsg, Data: "still here"})
	mod.expectMsg("themod", "still here")
}

func TestMuteSilences(t *testing.T) {
	_, mod, user := modApp(t, hook.Hooks{})

	mod.send(proto.Command{Verb: proto.VerbMute, Nick: "auser", Channel: "main", Reason: "spam"})

	// Everybody is told, including the person it is about.
	for _, c := range []*client{mod, user} {
		m := c.expectMod(proto.ActionMute, "auser")
		if m.By != "themod" {
			t.Errorf("by = %q, want %q", m.By, "themod")
		}
		if m.Reason != "spam" {
			t.Errorf("reason = %q, want %q", m.Reason, "spam")
		}
		if m.Until != 0 {
			t.Errorf("until = %d, want 0 for a permanent mute", m.Until)
		}
	}

	user.send(proto.Command{Verb: proto.VerbMsg, Data: "can i talk"})
	user.expectErr(proto.ErrMuted)

	// A mute is not a disconnect: the user still receives.
	mod.send(proto.Command{Verb: proto.VerbMsg, Data: "you cannot"})
	user.expectMsg("themod", "you cannot")
}

// A channel mute is about that channel. Somebody silenced in one room may
// still talk to people directly, because a channel mute is not a statement
// that they may not talk to anybody at all.
func TestChannelMuteDoesNotBlockPrivateMessages(t *testing.T) {
	_, mod, user := modApp(t, hook.Hooks{})

	mod.send(proto.Command{Verb: proto.VerbMute, Nick: "auser", Channel: "main"})
	mod.expectMod(proto.ActionMute, "auser")
	user.expectMod(proto.ActionMute, "auser")

	user.send(proto.Command{Verb: proto.VerbPriv, Nick: "themod", Data: "let me out"})
	mod.expectPriv("let me out")
	user.expectPriv("let me out")
}

// A server-wide mute does block them.
func TestGlobalMuteBlocksPrivateMessages(t *testing.T) {
	ta, _, user := modApp(t, hook.Hooks{})

	admin, err := ta.dialWith(t, "?token=admin")
	if err != nil {
		t.Fatalf("dial the admin: %v", err)
	}

	admin.send(proto.Command{Verb: proto.VerbMute, Nick: "auser"})
	user.expectMod(proto.ActionMute, "auser")

	user.send(proto.Command{Verb: proto.VerbPriv, Nick: "themod", Data: "let me out"})
	user.expectErr(proto.ErrMuted)
}

func TestUnmute(t *testing.T) {
	_, mod, user := modApp(t, hook.Hooks{})

	mod.send(proto.Command{Verb: proto.VerbMute, Nick: "auser", Channel: "main"})
	mod.expectMod(proto.ActionMute, "auser")
	user.expectMod(proto.ActionMute, "auser")

	user.send(proto.Command{Verb: proto.VerbMsg, Data: "blocked"})
	user.expectErr(proto.ErrMuted)

	mod.send(proto.Command{Verb: proto.VerbUnmute, Nick: "auser", Channel: "main"})
	mod.expectMod(proto.ActionUnmute, "auser")
	user.expectMod(proto.ActionUnmute, "auser")

	user.send(proto.Command{Verb: proto.VerbMsg, Data: "free"})
	user.expectMsg("auser", "free")
	mod.expectMsg("auser", "free")
}

func TestTimedMuteReportsItsDeadline(t *testing.T) {
	_, mod, user := modApp(t, hook.Hooks{})

	before := time.Now()
	mod.send(proto.Command{Verb: proto.VerbMute, Nick: "auser", Channel: "main", Duration: "10m"})
	m := mod.expectMod(proto.ActionMute, "auser")
	user.expectMod(proto.ActionMute, "auser")

	if m.Until == 0 {
		t.Fatal("a timed mute reported no deadline")
	}
	until := time.UnixMilli(m.Until)
	if until.Before(before.Add(9*time.Minute)) || until.After(before.Add(11*time.Minute)) {
		t.Fatalf("until = %v, want about ten minutes from now", until)
	}
}

func TestBadDuration(t *testing.T) {
	_, mod, _ := modApp(t, hook.Hooks{})

	for _, d := range []string{"ten minutes", "-5m", "0s", "10"} {
		mod.send(proto.Command{Verb: proto.VerbMute, Nick: "auser", Channel: "main", Duration: d})
		mod.expectErr(proto.ErrBadDuration)
	}
}

func TestModerationOfUnknownNick(t *testing.T) {
	_, mod, _ := modApp(t, hook.Hooks{})

	mod.send(proto.Command{Verb: proto.VerbMute, Nick: "nobodyhere", Channel: "main"})
	mod.expectErr(proto.ErrNoSuch)
}

func TestServerWideBanDisconnectsAndRefusesReconnection(t *testing.T) {
	ta, mod, user := modApp(t, hook.Hooks{})

	admin, err := ta.dialWith(t, "?token=admin")
	if err != nil {
		t.Fatalf("dial the admin: %v", err)
	}

	// No channel means server-wide, which only an admin may do here.
	admin.send(proto.Command{Verb: proto.VerbBan, Nick: "auser", Reason: "enough"})

	// The room is told.
	mod.expectMod(proto.ActionBan, "auser")

	// The banned client is disconnected with a reason. It may or may not
	// see the MOD frame first — its write pump is racing its own socket
	// being closed — so expectClosed drains whatever arrived and asserts
	// on the close itself.
	if code := user.expectClosed(); code != websocket.StatusPolicyViolation {
		t.Fatalf("closed with %v, want %v", code, websocket.StatusPolicyViolation)
	}

	// And coming back is refused before the upgrade.
	if _, err = ta.dialWith(t, "?token=user"); err == nil {
		t.Fatal("a banned user reconnected")
	} else if !strings.Contains(err.Error(), "403") {
		t.Fatalf("dial error = %v, want a 403", err)
	}
}

func TestUnbanNamesSomebodyWhoIsGone(t *testing.T) {
	ta, _, user := modApp(t, hook.Hooks{})

	admin, err := ta.dialWith(t, "?token=admin")
	if err != nil {
		t.Fatalf("dial the admin: %v", err)
	}

	admin.send(proto.Command{Verb: proto.VerbBan, Nick: "auser"})
	admin.expectMod(proto.ActionBan, "auser")
	user.expectClosed()

	// Unbanning names somebody who is no longer connected, which the
	// server cannot resolve — the limitation is real and worth pinning
	// down in a test rather than discovering later.
	admin.send(proto.Command{Verb: proto.VerbUnban, Nick: "auser"})
	admin.expectErr(proto.ErrNoSuch)
}

func TestModerationIsRecorded(t *testing.T) {
	rec := newFakeSanctions()
	_, mod, user := modApp(t, hook.Hooks{Sanctions: rec})

	mod.send(proto.Command{Verb: proto.VerbMute, Nick: "auser", Channel: "main", Duration: "5m", Reason: "spam"})
	mod.expectMod(proto.ActionMute, "auser")
	user.expectMod(proto.ActionMute, "auser")

	select {
	case m := <-rec.recorded:
		if m.Action != proto.ActionMute {
			t.Errorf("action = %q, want %q", m.Action, proto.ActionMute)
		}
		if m.By.ID != "u1" {
			t.Errorf("by = %q, want the moderator's stable id u1", m.By.ID)
		}
		if m.Target != "auser" {
			t.Errorf("target = %q, want auser", m.Target)
		}
		if m.Key != "id:u2" {
			t.Errorf("key = %q, want id:u2", m.Key)
		}
		if m.Scope != "main" {
			t.Errorf("scope = %q, want main", m.Scope)
		}
		if m.Reason != "spam" {
			t.Errorf("reason = %q, want spam", m.Reason)
		}
		if m.Until.IsZero() {
			t.Error("a timed mute was recorded with no deadline")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the moderation action was never recorded")
	}
}

// Moderation actions are announced to the channel but are not chat, so
// they do not join the replay history.
func TestModerationIsNotInTheBacklog(t *testing.T) {
	ta, mod, user := modApp(t, hook.Hooks{})

	mod.send(proto.Command{Verb: proto.VerbMute, Nick: "auser", Channel: "main"})
	mod.expectMod(proto.ActionMute, "auser")
	user.expectMod(proto.ActionMute, "auser")

	later, err := ta.dialWith(t, "?token=later")
	if err != nil {
		t.Fatalf("dial the latecomer: %v", err)
	}
	if len(later.backlog) != 0 {
		t.Fatalf("backlog has %d entries, want none — a MOD frame got in", len(later.backlog))
	}
}

// --- the history hook -------------------------------------------------

// fakeHistory serves a canned window and records what it was handed.
type fakeHistory struct {
	mu       sync.Mutex
	appended []hook.Message
	canned   []hook.Message
	fail     error
	askedFor int
}

func (f *fakeHistory) Append(ctx context.Context, channel string, m hook.Message) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.appended = append(f.appended, m)
}

func (f *fakeHistory) Recent(ctx context.Context, channel string, n int) ([]hook.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.askedFor = n
	if f.fail != nil {
		return nil, f.fail
	}
	return f.canned, nil
}

func TestHistoryHookReplacesTheDefault(t *testing.T) {
	hist := &fakeHistory{canned: []hook.Message{
		{ID: 41, From: hook.Identity{Nick: "ghost", Roles: []string{"mod"}}, Data: "from the archive", At: time.UnixMilli(1700000000000)},
		{ID: 42, From: hook.Identity{Nick: "ghost"}, Data: "and another", At: time.UnixMilli(1700000001000)},
	}}
	ta := newTestAppWith(t, hook.Hooks{History: hist})

	c := ta.dial(t)
	if len(c.backlog) != 2 {
		t.Fatalf("backlog has %d messages, want the hook's 2", len(c.backlog))
	}
	if c.backlog[0].Data != "from the archive" || c.backlog[1].Data != "and another" {
		t.Fatalf("backlog = %v, want the hook's messages in order", c.backlog)
	}
	if c.backlog[0].Nick != "ghost" {
		t.Errorf("nick = %q, want %q", c.backlog[0].Nick, "ghost")
	}
	if c.backlog[0].ID != 41 {
		t.Errorf("id = %d, want the hook's 41", c.backlog[0].ID)
	}
	if c.backlog[0].Timestamp != 1700000000000 {
		t.Errorf("timestamp = %d, want the hook's", c.backlog[0].Timestamp)
	}
	// Whatever the hook attached to the sender rides along, exactly as it
	// does on a live message.
	if roles := c.backlog[0].Roles; len(roles) != 1 || roles[0] != "mod" {
		t.Errorf("roles = %v, want [mod]", roles)
	}

	hist.mu.Lock()
	defer hist.mu.Unlock()
	if hist.askedFor != config.Default().Backlog {
		t.Errorf("the hook was asked for %d, want the configured %d", hist.askedFor, config.Default().Backlog)
	}
}

func TestHistoryHookSeesDeliveredMessages(t *testing.T) {
	hist := &fakeHistory{}
	ta := newTestAppWith(t, hook.Hooks{
		Auth:    fakeAuth{byToken: map[string]hook.Identity{"a": {ID: "u1", Nick: "alice"}}},
		History: hist,
	})

	alice, err := ta.dialWith(t, "?token=a")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	alice.send(proto.Command{Verb: proto.VerbMsg, Data: "remember this"})
	alice.expectMsg("alice", "remember this")

	hist.mu.Lock()
	defer hist.mu.Unlock()
	if len(hist.appended) != 1 {
		t.Fatalf("the hook was handed %d messages, want 1", len(hist.appended))
	}
	got := hist.appended[0]
	if got.Data != "remember this" {
		t.Errorf("data = %q", got.Data)
	}
	// The identity, not just the display name: a store wants the stable id.
	if got.From.ID != "u1" {
		t.Errorf("from = %q, want the stable id u1", got.From.ID)
	}
	if got.ID == 0 || got.At.IsZero() {
		t.Error("the recorded message has no id or no timestamp")
	}
}

// Private messages are not channel history.
func TestHistoryHookDoesNotSeePrivateMessages(t *testing.T) {
	hist := &fakeHistory{}
	ta := newTestAppWith(t, hook.Hooks{History: hist})
	alice, bob := ta.dial(t), ta.dial(t)

	alice.send(proto.Command{Verb: proto.VerbPriv, Nick: bob.nick, Data: "private"})
	bob.expectPriv("private")
	alice.expectPriv("private")

	hist.mu.Lock()
	defer hist.mu.Unlock()
	if len(hist.appended) != 0 {
		t.Fatalf("the history hook was handed %d private messages", len(hist.appended))
	}
}

// A history that is having a bad day must not cost anybody a connection.
func TestHistoryFailureIsNotFatal(t *testing.T) {
	hist := &fakeHistory{fail: errors.New("archive on fire")}
	ta := newTestAppWith(t, hook.Hooks{History: hist})

	c := ta.dial(t) // fails the test if the connection is refused
	if len(c.backlog) != 0 {
		t.Fatalf("a failing history produced %d messages", len(c.backlog))
	}

	// And the connection works.
	c.send(proto.Command{Verb: proto.VerbMsg, Data: "still fine"})
	c.expectMsg("", "still fine")
}

// Backlog=0 switches replay off whoever provides it, so a hook is not
// consulted at all.
func TestHistoryHookNotConsultedWhenDisabled(t *testing.T) {
	hist := &fakeHistory{canned: []hook.Message{{ID: 1, Data: "would be replayed"}}}
	ta := newTestAppWith(t, hook.Hooks{History: hist}, func(c *config.Config) { c.Backlog = 0 })

	c := ta.dial(t)
	if len(c.backlog) != 0 {
		t.Fatalf("backlog disabled but %d messages arrived", len(c.backlog))
	}

	hist.mu.Lock()
	defer hist.mu.Unlock()
	if hist.askedFor != 0 {
		t.Errorf("the hook was consulted (asked for %d) with the backlog off", hist.askedFor)
	}
}

// --- moderation scope -------------------------------------------------

// A mute in one channel is about that channel.
func TestChannelMuteIsLocalToItsChannel(t *testing.T) {
	_, mod, user := modApp(t, hook.Hooks{})

	// Both move into a second room as well. Only the one already there
	// sees the other arrive — presence is what happens while you are in
	// the room, not a history of it.
	for _, c := range []*client{mod, user} {
		c.send(proto.Command{Verb: proto.VerbJoin, Channel: "second"})
		c.expectJoined("second")
	}
	mod.expectJoin("second", user.nick)

	mod.send(proto.Command{Verb: proto.VerbMute, Nick: "auser", Channel: "main"})
	mod.expectMod(proto.ActionMute, "auser")
	user.expectMod(proto.ActionMute, "auser")

	// Silenced in main...
	user.send(proto.Command{Verb: proto.VerbMsg, Channel: "main", Data: "in main"})
	user.expectErr(proto.ErrMuted)

	// ...and not in second.
	user.send(proto.Command{Verb: proto.VerbMsg, Channel: "second", Data: "in second"})
	if got := user.expectMsg("auser", "in second"); got.Channel != "second" {
		t.Fatalf("channel = %q, want second", got.Channel)
	}
}

// A server-wide mute silences somebody in every channel, without having
// been written into any of them.
func TestGlobalMuteSilencesEverywhere(t *testing.T) {
	ta, _, user := modApp(t, hook.Hooks{})

	admin, err := ta.dialWith(t, "?token=admin")
	if err != nil {
		t.Fatalf("dial the admin: %v", err)
	}

	user.send(proto.Command{Verb: proto.VerbJoin, Channel: "second"})
	user.expectJoined("second")

	// Announced in every channel the target is in, so there are two.
	admin.send(proto.Command{Verb: proto.VerbMute, Nick: "auser"})
	user.expectMod(proto.ActionMute, "auser")
	user.expectMod(proto.ActionMute, "auser")

	for _, channel := range []string{"main", "second"} {
		user.send(proto.Command{Verb: proto.VerbMsg, Channel: channel, Data: "hello?"})
		user.expectErr(proto.ErrMuted)
	}
}

// Running one room is not permission to act across the server.
func TestChannelModeratorCannotActGlobally(t *testing.T) {
	_, mod, user := modApp(t, hook.Hooks{})

	mod.send(proto.Command{Verb: proto.VerbMute, Nick: "auser"})
	mod.expectErr(proto.ErrForbidden)

	// And nothing happened.
	user.send(proto.Command{Verb: proto.VerbMsg, Data: "still talking"})
	user.expectMsg("auser", "still talking")
}

// A channel ban takes somebody out of that room and keeps them out,
// without touching their connection or anywhere else they are.
func TestChannelBanPartsAndBars(t *testing.T) {
	_, mod, user := modApp(t, hook.Hooks{})

	mod.send(proto.Command{Verb: proto.VerbBan, Nick: "auser", Channel: "main", Reason: "enough"})
	mod.expectMod(proto.ActionBan, "auser")

	// The banned client is removed first and told why after, so the order
	// is PART then MOD and there is exactly one of each.
	user.expectPart("main", "auser")
	user.expectMod(proto.ActionBan, "auser")

	// Still connected — it can say so somewhere else.
	user.send(proto.Command{Verb: proto.VerbJoin, Channel: "elsewhere"})
	user.expectJoined("elsewhere")

	// But it cannot come back.
	user.send(proto.Command{Verb: proto.VerbJoin, Channel: "main"})
	user.expectErr(proto.ErrBanned)
}

// A channel ban outranks an autojoin: a layer putting somebody somewhere
// does not overrule a moderator who threw them out of it.
func TestChannelBanBeatsAutojoin(t *testing.T) {
	ta, mod, user := modApp(t, hook.Hooks{
		Channels: fakeChannels{autojoin: []string{"main"}},
	})

	mod.send(proto.Command{Verb: proto.VerbBan, Nick: "auser", Channel: "main"})
	mod.expectMod(proto.ActionBan, "auser")
	user.expectPart("main", "auser")
	user.expectMod(proto.ActionBan, "auser")

	// The ban is filed against the identity, so reconnecting does not shake
	// it off — the same account comes back and lands nowhere.
	back, err := ta.dialWith(t, "?token=user")
	if err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	if len(back.joined) != 0 {
		t.Fatalf("a banned user was autojoined into %v", back.joined)
	}
}

// The scope travels with the announcement, so a client can tell "muted
// here" from "muted everywhere".
func TestModFrameCarriesItsScope(t *testing.T) {
	ta, mod, user := modApp(t, hook.Hooks{})

	mod.send(proto.Command{Verb: proto.VerbMute, Nick: "auser", Channel: "main"})
	local := mod.expectMod(proto.ActionMute, "auser")
	user.expectMod(proto.ActionMute, "auser")
	if local.Scope != "main" {
		t.Errorf("scope = %q, want main", local.Scope)
	}
	if local.Channel != "main" {
		t.Errorf("channel = %q, want main", local.Channel)
	}

	admin, err := ta.dialWith(t, "?token=admin")
	if err != nil {
		t.Fatalf("dial the admin: %v", err)
	}
	admin.send(proto.Command{Verb: proto.VerbMute, Nick: "auser"})
	global := user.expectMod(proto.ActionMute, "auser")
	if global.Scope != "" {
		t.Errorf("scope = %q, want empty for a server-wide action", global.Scope)
	}
	if global.Channel != "main" {
		t.Errorf("channel = %q, want the channel it was announced in", global.Channel)
	}
}

// --- sanctions surviving a restart ------------------------------------

// fakeSanctions is a store that keeps what it is told, so a second server
// can be handed the first one's state.
type fakeSanctions struct {
	mu       sync.Mutex
	active   map[string]hook.Moderation // by scope+key+action
	recorded chan hook.Moderation
	failLoad error
	failSave error
}

func newFakeSanctions() *fakeSanctions {
	return &fakeSanctions{
		active:   make(map[string]hook.Moderation),
		recorded: make(chan hook.Moderation, 16),
	}
}

func (f *fakeSanctions) Record(ctx context.Context, m hook.Moderation) error {
	if f.failSave != nil {
		return f.failSave
	}

	f.mu.Lock()
	switch m.Action {
	case proto.ActionMute, proto.ActionBan:
		f.active[m.Scope+"\x00"+m.Key+"\x00"+m.Action] = m
	case proto.ActionUnmute:
		delete(f.active, m.Scope+"\x00"+m.Key+"\x00"+proto.ActionMute)
	case proto.ActionUnban:
		delete(f.active, m.Scope+"\x00"+m.Key+"\x00"+proto.ActionBan)
	}
	f.mu.Unlock()

	f.recorded <- m
	return nil
}

func (f *fakeSanctions) Active(ctx context.Context) ([]hook.Moderation, error) {
	if f.failLoad != nil {
		return nil, f.failLoad
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]hook.Moderation, 0, len(f.active))
	for _, m := range f.active {
		out = append(out, m)
	}
	return out, nil
}

// waitFor blocks until the background worker has persisted n actions.
func (f *fakeSanctions) waitFor(t *testing.T, n int) {
	t.Helper()
	for range n {
		select {
		case <-f.recorded:
		case <-time.After(5 * time.Second):
			t.Fatal("the action was never persisted")
		}
	}
}

// The point of the whole hook: a mute issued by one server is still in
// force on the next one.
func TestMutesSurviveARestart(t *testing.T) {
	store := newFakeSanctions()

	// First server: mute somebody, and let it reach the store.
	func() {
		_, mod, user := modApp(t, hook.Hooks{Sanctions: store})
		mod.send(proto.Command{Verb: proto.VerbMute, Nick: "auser", Channel: "main"})
		mod.expectMod(proto.ActionMute, "auser")
		user.expectMod(proto.ActionMute, "auser")
		store.waitFor(t, 1)
	}()

	// Second server, same store. The user comes back to find it still
	// applies, without anybody having to re-issue it.
	_, _, user := modApp(t, hook.Hooks{Sanctions: store})
	user.send(proto.Command{Verb: proto.VerbMsg, Data: "am i free"})
	user.expectErr(proto.ErrMuted)
}

// And its scope survives with it: a mute in one channel does not come back
// as a mute everywhere.
func TestScopeSurvivesARestart(t *testing.T) {
	store := newFakeSanctions()

	func() {
		_, mod, user := modApp(t, hook.Hooks{Sanctions: store})
		mod.send(proto.Command{Verb: proto.VerbMute, Nick: "auser", Channel: "main"})
		mod.expectMod(proto.ActionMute, "auser")
		user.expectMod(proto.ActionMute, "auser")
		store.waitFor(t, 1)
	}()

	_, _, user := modApp(t, hook.Hooks{Sanctions: store})
	user.send(proto.Command{Verb: proto.VerbMsg, Channel: "main", Data: "in main"})
	user.expectErr(proto.ErrMuted)

	user.send(proto.Command{Verb: proto.VerbJoin, Channel: "second"})
	user.expectJoined("second")
	user.send(proto.Command{Verb: proto.VerbMsg, Channel: "second", Data: "in second"})
	user.expectMsg("auser", "in second")
}

// A server-wide ban survives too, and is enforced where it always was:
// before the upgrade.
func TestBansSurviveARestart(t *testing.T) {
	store := newFakeSanctions()

	func() {
		ta, _, user := modApp(t, hook.Hooks{Sanctions: store})
		admin, err := ta.dialWith(t, "?token=admin")
		if err != nil {
			t.Fatalf("dial the admin: %v", err)
		}
		admin.send(proto.Command{Verb: proto.VerbBan, Nick: "auser"})
		admin.expectMod(proto.ActionBan, "auser")
		user.expectClosed()
		store.waitFor(t, 1)
	}()

	ta := newTestAppWith(t, hook.Hooks{
		Sanctions: store,
		Auth:      fakeAuth{byToken: map[string]hook.Identity{"user": {ID: "u2", Nick: "auser"}}},
	})
	if _, err := ta.dialWith(t, "?token=user"); err == nil {
		t.Fatal("a banned user connected to the restarted server")
	} else if !strings.Contains(err.Error(), "403") {
		t.Fatalf("dial error = %v, want a 403", err)
	}
}

// Lifting a sanction has to be persisted too, or it comes back.
func TestUnmuteSurvivesARestart(t *testing.T) {
	store := newFakeSanctions()

	func() {
		_, mod, user := modApp(t, hook.Hooks{Sanctions: store})
		mod.send(proto.Command{Verb: proto.VerbMute, Nick: "auser", Channel: "main"})
		mod.expectMod(proto.ActionMute, "auser")
		user.expectMod(proto.ActionMute, "auser")

		mod.send(proto.Command{Verb: proto.VerbUnmute, Nick: "auser", Channel: "main"})
		mod.expectMod(proto.ActionUnmute, "auser")
		user.expectMod(proto.ActionUnmute, "auser")
		store.waitFor(t, 2)
	}()

	_, _, user := modApp(t, hook.Hooks{Sanctions: store})
	user.send(proto.Command{Verb: proto.VerbMsg, Data: "free again"})
	user.expectMsg("auser", "free again")
}

// An expired sanction handed back is ignored rather than re-applied.
func TestExpiredSanctionsAreIgnoredOnLoad(t *testing.T) {
	store := newFakeSanctions()
	store.active["expired"] = hook.Moderation{
		Action: proto.ActionMute,
		Scope:  "main",
		Key:    "id:u2",
		Until:  time.Now().Add(-time.Hour),
	}

	_, _, user := modApp(t, hook.Hooks{Sanctions: store})
	user.send(proto.Command{Verb: proto.VerbMsg, Data: "long over"})
	user.expectMsg("auser", "long over")
}

// A store that cannot say who is banned stops the server, rather than
// starting one that lets everybody in.
func TestStartupFailsIfSanctionsCannotBeLoaded(t *testing.T) {
	store := newFakeSanctions()
	store.failLoad = errors.New("the database is on fire")

	cfg := config.Default()
	cfg.LogLevel = "error"
	if _, err := newAppWithConfig(cfg, hook.Hooks{Sanctions: store}); err == nil {
		t.Fatal("the server started without knowing who is banned")
	}
}

// No Sanctions installed is still a working server; it just forgets.
func TestNoSanctionsHookStillWorks(t *testing.T) {
	_, mod, user := modApp(t, hook.Hooks{})

	mod.send(proto.Command{Verb: proto.VerbMute, Nick: "auser", Channel: "main"})
	mod.expectMod(proto.ActionMute, "auser")
	user.expectMod(proto.ActionMute, "auser")

	user.send(proto.Command{Verb: proto.VerbMsg, Data: "muted"})
	user.expectErr(proto.ErrMuted)
}
