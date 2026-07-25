package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/sztanpet/ws-chat/internal/hook"
	"github.com/sztanpet/ws-chat/internal/proto"
)

// The layers are exercised through the same public surface as everything
// else: a real client connecting to a real server that happens to have
// fakes installed. Nothing here reaches into the server to check that a
// hook "was called" — it checks that installing one changed what a client
// sees, which is the only reason to have them.

type fakeAuth struct {
	byToken map[string]hook.Identity
}

func (f fakeAuth) Authenticate(ctx context.Context, r hook.Request) (hook.Identity, error) {
	id, ok := f.byToken[r.Query("token")]
	if !ok {
		return hook.Identity{}, hook.ErrUnauthorized
	}
	return id, nil
}

type fakeDirectory map[string]hook.Chatter

func (f fakeDirectory) Chatter(ctx context.Context, id string) (hook.Chatter, error) {
	c, ok := f[id]
	if !ok {
		return hook.Chatter{}, hook.ErrNoChatter
	}
	return c, nil
}

type fakeFilter struct {
	deny   string // messages containing this are refused
	reason string
}

func (f fakeFilter) Allow(ctx context.Context, from hook.Identity, data string) (bool, string) {
	if f.deny != "" && strings.Contains(data, f.deny) {
		return false, f.reason
	}
	return true, ""
}

type fakeRecorder struct {
	msgs  chan hook.Message
	privs chan hook.Private
	fail  error
}

func newFakeRecorder() *fakeRecorder {
	return &fakeRecorder{
		msgs:  make(chan hook.Message, 16),
		privs: make(chan hook.Private, 16),
	}
}

func (f *fakeRecorder) Message(ctx context.Context, m hook.Message) error {
	f.msgs <- m
	return f.fail
}

func (f *fakeRecorder) Private(ctx context.Context, p hook.Private) error {
	f.privs <- p
	return f.fail
}

// dialWith connects with extra query parameters, for the auth fake.
func (ta *testApp) dialWith(t *testing.T, query string) (*client, error) {
	t.Helper()

	ctx, cancel := contextWithTimeout()
	defer cancel()

	url := "ws" + strings.TrimPrefix(ta.srv.URL, "http") + "/ws" + query
	ws, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { ws.CloseNow() })

	c := &client{t: t, ws: ws}
	verb, payload := c.recv()
	if verb != proto.VerbReady {
		t.Fatalf("first frame was %s, want %s", verb, proto.VerbReady)
	}
	var ready proto.Ready
	mustUnmarshal(t, payload, &ready)
	c.nick = ready.Nick
	return c, nil
}

func TestAuthNamesTheConnection(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{
		Auth: fakeAuth{byToken: map[string]hook.Identity{
			"good": {ID: "u1", Nick: "alice"},
		}},
	})

	c, err := ta.dialWith(t, "?token=good")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if c.nick != "alice" {
		t.Fatalf("READY nick = %q, want %q", c.nick, "alice")
	}

	c.send(`MSG {"data":"hello"}`)
	if msg := c.expectMsg("alice", "hello"); msg.Nick != "alice" {
		t.Fatalf("message nick = %q, want %q", msg.Nick, "alice")
	}
}

// A refusal happens before the upgrade, so the client gets an HTTP status
// it can actually read rather than a socket that opens and shuts.
func TestAuthRefusalIsAnHTTPError(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{
		Auth: fakeAuth{byToken: map[string]hook.Identity{"good": {ID: "u1"}}},
	})

	if _, err := ta.dialWith(t, "?token=nope"); err == nil {
		t.Fatal("dial succeeded without a valid token")
	} else if !strings.Contains(err.Error(), "401") {
		t.Fatalf("dial error = %v, want a 401", err)
	}
}

func TestDirectoryFillsInChatterData(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{
		Auth: fakeAuth{byToken: map[string]hook.Identity{
			"good": {ID: "u1", Nick: "placeholder"},
		}},
		Directory: fakeDirectory{
			"u1": {Nick: "Alice", Roles: []string{"mod"}},
		},
	})

	c, err := ta.dialWith(t, "?token=good")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if c.nick != "Alice" {
		t.Fatalf("nick = %q, want the directory's %q", c.nick, "Alice")
	}
}

// A directory that has nothing on file is not a failure.
func TestDirectoryMissIsNotFatal(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{
		Auth: fakeAuth{byToken: map[string]hook.Identity{
			"good": {ID: "unknown", Nick: "asauthed"},
		}},
		Directory: fakeDirectory{},
	})

	c, err := ta.dialWith(t, "?token=good")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if c.nick != "asauthed" {
		t.Fatalf("nick = %q, want the authenticated %q", c.nick, "asauthed")
	}
}

func TestFilterRefusesMessages(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{
		Filter: fakeFilter{deny: "badword", reason: "muted"},
	})
	c := ta.dial(t)

	c.send(`MSG {"data":"this has badword in it"}`)
	c.expectErr("muted")

	// And the refusal is per message, not per connection.
	c.send(`MSG {"data":"this one is fine"}`)
	c.expectMsg("", "this one is fine")
}

func TestFilterAppliesToPrivateMessages(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{
		Filter: fakeFilter{deny: "badword", reason: "muted"},
	})
	alice, bob := ta.dial(t), ta.dial(t)

	alice.send(`PRIVMSG {"nick":"` + bob.nick + `","data":"badword"}`)
	alice.expectErr("muted")
}

func TestRecorderSeesMessages(t *testing.T) {
	rec := newFakeRecorder()
	ta := newTestAppWith(t, hook.Hooks{
		Auth: fakeAuth{byToken: map[string]hook.Identity{
			"a": {ID: "u1", Nick: "alice"},
			"b": {ID: "u2", Nick: "bob"},
		}},
		Recorder: rec,
	})

	alice, err := ta.dialWith(t, "?token=a")
	if err != nil {
		t.Fatalf("dial alice: %v", err)
	}
	bob, err := ta.dialWith(t, "?token=b")
	if err != nil {
		t.Fatalf("dial bob: %v", err)
	}

	alice.send(`MSG {"data":"public"}`)
	alice.expectMsg("alice", "public")
	bob.expectMsg("alice", "public")

	select {
	case m := <-rec.msgs:
		if m.Data != "public" {
			t.Errorf("recorded data = %q, want %q", m.Data, "public")
		}
		if m.From.ID != "u1" {
			t.Errorf("recorded sender = %q, want the stable id %q", m.From.ID, "u1")
		}
		if m.ID == 0 || m.At.IsZero() {
			t.Error("recorded message has no id or no timestamp")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the public message was never recorded")
	}

	alice.send(`PRIVMSG {"nick":"bob","data":"secret"}`)
	bob.expectPriv("secret")
	alice.expectPriv("secret")

	select {
	case p := <-rec.privs:
		if p.Data != "secret" {
			t.Errorf("recorded data = %q, want %q", p.Data, "secret")
		}
		if p.From.ID != "u1" || p.To.ID != "u2" {
			t.Errorf("recorded %q -> %q, want u1 -> u2", p.From.ID, p.To.ID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the private message was never recorded")
	}
}

// Persistence is behind delivery, not in front of it: a store that is
// failing must not cost anybody a message.
func TestRecorderFailureDoesNotAffectDelivery(t *testing.T) {
	rec := newFakeRecorder()
	rec.fail = errors.New("database on fire")

	ta := newTestAppWith(t, hook.Hooks{Recorder: rec})
	alice, bob := ta.dial(t), ta.dial(t)

	alice.send(`MSG {"data":"still delivered"}`)
	alice.expectMsg("", "still delivered")
	bob.expectMsg("", "still delivered")
}

// The zero Hooks is a working server. This is the configuration every other
// test in the package runs under, but it is worth saying out loud.
func TestNoHooksInstalled(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{})
	c := ta.dial(t)

	if !strings.HasPrefix(c.nick, "anon") {
		t.Fatalf("nick = %q, want an assigned anonymous name", c.nick)
	}
	c.send(`MSG {"data":"no layers here"}`)
	c.expectMsg("", "no layers here")
}

func TestIdentityHelpers(t *testing.T) {
	var anon hook.Identity
	if !anon.Anonymous() {
		t.Error("the zero identity is not anonymous")
	}

	id := hook.Identity{ID: "u1", Roles: []string{"mod"}}
	if id.Anonymous() {
		t.Error("an identity with an id is anonymous")
	}
	if !id.Has("mod") || id.Has("admin") {
		t.Error("Has does not report roles correctly")
	}

	// Apply overlays only what the chatter has an opinion about.
	got := hook.Identity{ID: "u1", Nick: "old", Roles: []string{"user"}}.
		Apply(hook.Chatter{Roles: []string{"mod"}, Attrs: map[string]string{"colour": "red"}})
	if got.Nick != "old" {
		t.Errorf("nick = %q, want it left alone", got.Nick)
	}
	if !got.Has("mod") {
		t.Error("roles were not applied")
	}
	if got.Attrs["colour"] != "red" {
		t.Error("attrs were not applied")
	}
}
