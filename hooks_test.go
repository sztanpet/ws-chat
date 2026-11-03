package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/sztanpet/ws-chat/internal/hook"
	"github.com/sztanpet/ws-chat/internal/proto"
)

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

func (f fakeDirectory) Resolve(ctx context.Context, nick string) (hook.Identity, error) {
	for id, c := range f {
		if c.Nick == nick {
			return hook.Identity{ID: id, Nick: c.Nick, Roles: c.Roles, Attrs: c.Attrs}, nil
		}
	}
	return hook.Identity{}, hook.ErrNoChatter
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

func (ta *testApp) dialWith(t *testing.T, query string) (*client, error) {
	t.Helper()

	ctx, cancel := contextWithTimeout()
	defer cancel()

	url := "ws" + strings.TrimPrefix(ta.srv.URL, "http") + "/ws" + query
	ws, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = ws.CloseNow() })

	c := &client{t: t, ws: ws, codec: proto.Default()}
	verb, payload := c.recv()
	if verb != proto.VerbReady {
		t.Fatalf("first frame was %s, want %s", verb, proto.VerbReady)
	}
	var ready proto.Ready
	mustUnmarshal(t, c, payload, &ready)
	c.nick = ready.Nick
	c.settle(ta)
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

	c.send(proto.Command{Verb: proto.VerbMsg, Data: "hello"})
	if msg := c.expectMsg("alice", "hello"); msg.Nick != "alice" {
		t.Fatalf("message nick = %q, want %q", msg.Nick, "alice")
	}
}

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

	c.send(proto.Command{Verb: proto.VerbMsg, Data: "this has badword in it"})
	c.expectErr("muted")

	c.send(proto.Command{Verb: proto.VerbMsg, Data: "this one is fine"})
	c.expectMsg("", "this one is fine")
}

func TestFilterAppliesToPrivateMessages(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{
		Filter: fakeFilter{deny: "badword", reason: "muted"},
	})
	alice, bob := ta.dial(t), ta.dial(t)

	alice.send(proto.Command{Verb: proto.VerbPriv, Nick: bob.nick, Data: "badword"})
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

	alice.send(proto.Command{Verb: proto.VerbMsg, Data: "public"})
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

	alice.send(proto.Command{Verb: proto.VerbPriv, Nick: "bob", Data: "secret"})
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

func TestRecorderFailureDoesNotAffectDelivery(t *testing.T) {
	rec := newFakeRecorder()
	rec.fail = errors.New("database on fire")

	ta := newTestAppWith(t, hook.Hooks{Recorder: rec})
	alice, bob := ta.dial(t), ta.dial(t)

	alice.send(proto.Command{Verb: proto.VerbMsg, Data: "still delivered"})
	alice.expectMsg("", "still delivered")
	bob.expectMsg("", "still delivered")
}

func TestNoHooksInstalled(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{})
	c := ta.dial(t)

	if !strings.HasPrefix(c.nick, "anon") {
		t.Fatalf("nick = %q, want an assigned anonymous name", c.nick)
	}
	c.send(proto.Command{Verb: proto.VerbMsg, Data: "no layers here"})
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

type fakeLimiter struct {
	client  hook.Limits
	channel hook.Limits

	mu    sync.Mutex
	asked []string
}

func (f *fakeLimiter) ClientLimits(ctx context.Context, id hook.Identity) hook.Limits {
	f.mu.Lock()
	f.asked = append(f.asked, id.Nick)
	f.mu.Unlock()
	return f.client
}

func (f *fakeLimiter) ChannelLimits(ctx context.Context, channel string) hook.Limits {
	return f.channel
}

func TestNoLimiterIsUnlimited(t *testing.T) {
	ta := newTestApp(t)
	c := ta.dial(t)

	for i := range 50 {
		c.send(proto.Command{Verb: proto.VerbMsg, Data: fmt.Sprintf("message %d", i)})
		c.expectMsg("", fmt.Sprintf("message %d", i))
	}
}

func TestZeroLimitsAreUnlimited(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{Limiter: &fakeLimiter{}})
	c := ta.dial(t)

	for i := range 50 {
		c.send(proto.Command{Verb: proto.VerbMsg, Data: fmt.Sprintf("message %d", i)})
		c.expectMsg("", fmt.Sprintf("message %d", i))
	}
}

func TestClientRateLimit(t *testing.T) {
	// One an hour: no refill during the test, so nothing depends on timing.
	ta := newTestAppWith(t, hook.Hooks{
		Limiter: &fakeLimiter{client: hook.Limits{Burst: 3, Interval: time.Hour}},
	})
	alice, bob := ta.dial(t), ta.dial(t)

	for i := range 3 {
		want := fmt.Sprintf("burst %d", i)
		alice.send(proto.Command{Verb: proto.VerbMsg, Data: want})
		alice.expectMsg(alice.nick, want)
		bob.expectMsg(alice.nick, want)
	}

	alice.send(proto.Command{Verb: proto.VerbMsg, Data: "one too many"})
	alice.expectErr(proto.ErrThrottled)

	bob.send(proto.Command{Verb: proto.VerbMsg, Data: "bob is fine"})
	bob.expectMsg(bob.nick, "bob is fine")

	alice.expectMsg(bob.nick, "bob is fine")
}

func TestThrottledMessageIsNotDelivered(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{
		Limiter: &fakeLimiter{client: hook.Limits{Burst: 1, Interval: time.Hour}},
	})
	alice, bob := ta.dial(t), ta.dial(t)

	alice.send(proto.Command{Verb: proto.VerbMsg, Data: "allowed"})
	alice.expectMsg("", "allowed")
	bob.expectMsg("", "allowed")

	alice.send(proto.Command{Verb: proto.VerbMsg, Data: "refused"})
	alice.expectErr(proto.ErrThrottled)

	bob.send(proto.Command{Verb: proto.VerbMsg, Data: "next"})
	bob.expectMsg(bob.nick, "next")
}

func TestChannelRateLimit(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{
		Limiter: &fakeLimiter{channel: hook.Limits{Burst: 2, Interval: time.Hour}},
	})
	alice, bob := ta.dial(t), ta.dial(t)

	alice.send(proto.Command{Verb: proto.VerbMsg, Data: "one"})
	alice.expectMsg("", "one")
	bob.expectMsg("", "one")

	bob.send(proto.Command{Verb: proto.VerbMsg, Data: "two"})
	alice.expectMsg("", "two")
	bob.expectMsg("", "two")

	alice.send(proto.Command{Verb: proto.VerbMsg, Data: "three"})
	alice.expectErr(proto.ErrChanThrottled)

	bob.send(proto.Command{Verb: proto.VerbMsg, Data: "four"})
	bob.expectErr(proto.ErrChanThrottled)
}

func TestClientLimitIsReportedBeforeChannelLimit(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{
		Limiter: &fakeLimiter{
			client:  hook.Limits{Burst: 1, Interval: time.Hour},
			channel: hook.Limits{Burst: 1, Interval: time.Hour},
		},
	})
	c := ta.dial(t)

	c.send(proto.Command{Verb: proto.VerbMsg, Data: "first"})
	c.expectMsg("", "first")

	c.send(proto.Command{Verb: proto.VerbMsg, Data: "second"})
	c.expectErr(proto.ErrThrottled)
}

func TestPrivateMessagesUseTheClientLimitOnly(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{
		Limiter: &fakeLimiter{
			client:  hook.Limits{Burst: 2, Interval: time.Hour},
			channel: hook.Limits{Burst: 1, Interval: time.Hour},
		},
	})
	alice, bob := ta.dial(t), ta.dial(t)

	bob.send(proto.Command{Verb: proto.VerbMsg, Data: "fills the channel"})
	alice.expectMsg(bob.nick, "fills the channel")
	bob.expectMsg(bob.nick, "fills the channel")

	alice.send(proto.Command{Verb: proto.VerbPriv, Nick: bob.nick, Data: "one"})
	bob.expectPriv("one")
	alice.expectPriv("one")

	alice.send(proto.Command{Verb: proto.VerbPriv, Nick: bob.nick, Data: "two"})
	bob.expectPriv("two")
	alice.expectPriv("two")

	alice.send(proto.Command{Verb: proto.VerbPriv, Nick: bob.nick, Data: "three"})
	alice.expectErr(proto.ErrThrottled)
}

func TestLimiterIsAskedPerConnectionWithIdentity(t *testing.T) {
	lim := &fakeLimiter{}
	ta := newTestAppWith(t, hook.Hooks{
		Auth: fakeAuth{byToken: map[string]hook.Identity{
			"a": {ID: "u1", Nick: "alice"},
			"b": {ID: "u2", Nick: "bob"},
		}},
		Limiter: lim,
	})

	if _, err := ta.dialWith(t, "?token=a"); err != nil {
		t.Fatalf("dial alice: %v", err)
	}
	if _, err := ta.dialWith(t, "?token=b"); err != nil {
		t.Fatalf("dial bob: %v", err)
	}

	lim.mu.Lock()
	defer lim.mu.Unlock()
	if !slices.Contains(lim.asked, "alice") || !slices.Contains(lim.asked, "bob") {
		t.Fatalf("the limiter was asked about %v, want both alice and bob", lim.asked)
	}
}

func TestThrottlingDoesNotCloseTheConnection(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{
		Limiter: &fakeLimiter{client: hook.Limits{Burst: 1, Interval: 50 * time.Millisecond}},
	})
	alice, bob := ta.dial(t), ta.dial(t)

	alice.send(proto.Command{Verb: proto.VerbMsg, Data: "first"})
	alice.expectMsg("", "first")
	bob.expectMsg("", "first")

	alice.send(proto.Command{Verb: proto.VerbMsg, Data: "too fast"})
	alice.expectErr(proto.ErrThrottled)

	bob.send(proto.Command{Verb: proto.VerbMsg, Data: "still here"})
	alice.expectMsg(bob.nick, "still here")
	bob.expectMsg(bob.nick, "still here")

	// The one real interval in these tests, so it is generous.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("the bucket never refilled")
		}
		alice.send(proto.Command{Verb: proto.VerbMsg, Data: "recovered"})
		verb, _ := alice.recv()
		if verb == proto.VerbMsg {
			bob.expectMsg(alice.nick, "recovered")
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

type keyedLimiter struct {
	client  hook.Limits
	channel hook.Limits
	attr    string // the Attrs entry to key on; empty means Identity.ID
}

func (k keyedLimiter) ClientLimits(ctx context.Context, id hook.Identity) hook.Limits {
	limits := k.client
	if k.attr != "" {
		limits.Key = id.Attrs[k.attr]
	} else {
		limits.Key = id.ID
	}
	return limits
}

func (k keyedLimiter) ChannelLimits(ctx context.Context, channel string) hook.Limits {
	return k.channel
}

func TestAccountRateLimitIsSharedAcrossConnections(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{
		Auth: fakeAuth{byToken: map[string]hook.Identity{
			"a1": {ID: "u1", Nick: "alice"},
			"a2": {ID: "u1", Nick: "alice2"}, // same account, second socket
			"b":  {ID: "u2", Nick: "bob"},
		}},
		Limiter: keyedLimiter{client: hook.Limits{Burst: 3, Interval: time.Hour}},
	})

	first, err := ta.dialWith(t, "?token=a1")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	second, err := ta.dialWith(t, "?token=a2")
	if err != nil {
		t.Fatalf("dial the second socket: %v", err)
	}
	bob, err := ta.dialWith(t, "?token=b")
	if err != nil {
		t.Fatalf("dial bob: %v", err)
	}

	room := []*client{first, second, bob}

	first.send(proto.Command{Verb: proto.VerbMsg, Data: "one"})
	expectAll(room, "alice", "one")
	first.send(proto.Command{Verb: proto.VerbMsg, Data: "two"})
	expectAll(room, "alice", "two")
	second.send(proto.Command{Verb: proto.VerbMsg, Data: "three"})
	expectAll(room, "alice2", "three")

	second.send(proto.Command{Verb: proto.VerbMsg, Data: "four"})
	second.expectErr(proto.ErrThrottled)
	first.send(proto.Command{Verb: proto.VerbMsg, Data: "also four"})
	first.expectErr(proto.ErrThrottled)

	bob.send(proto.Command{Verb: proto.VerbMsg, Data: "bob is fine"})
	expectAll(room, "bob", "bob is fine")
}

func TestAccountKeyFromAuthAttributes(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{
		Auth: fakeAuth{byToken: map[string]hook.Identity{
			"a": {ID: "u1", Nick: "alice", Attrs: map[string]string{"org": "acme"}},
			"b": {ID: "u2", Nick: "bob", Attrs: map[string]string{"org": "acme"}},
			"c": {ID: "u3", Nick: "carol", Attrs: map[string]string{"org": "other"}},
		}},
		Limiter: keyedLimiter{
			client: hook.Limits{Burst: 2, Interval: time.Hour},
			attr:   "org",
		},
	})

	alice, _ := ta.dialWith(t, "?token=a")
	bob, _ := ta.dialWith(t, "?token=b")
	carol, _ := ta.dialWith(t, "?token=c")

	room := []*client{alice, bob, carol}

	alice.send(proto.Command{Verb: proto.VerbMsg, Data: "one"})
	expectAll(room, "alice", "one")
	bob.send(proto.Command{Verb: proto.VerbMsg, Data: "two"})
	expectAll(room, "bob", "two")

	bob.send(proto.Command{Verb: proto.VerbMsg, Data: "three"})
	bob.expectErr(proto.ErrThrottled)

	carol.send(proto.Command{Verb: proto.VerbMsg, Data: "different org"})
	expectAll(room, "carol", "different org")
}

func TestAccountLimitSurvivesReconnection(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{
		Auth: fakeAuth{byToken: map[string]hook.Identity{
			"a": {ID: "u1", Nick: "alice"},
			"w": {ID: "u2", Nick: "watcher"},
		}},
		Limiter: keyedLimiter{client: hook.Limits{Burst: 1, Interval: time.Hour}},
	})

	first, err := ta.dialWith(t, "?token=a")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	// Someone stays to see the PART: teardown runs after the socket drops, so an
	// instant reconnect could be handed its own departure.
	watcher, err := ta.dialWith(t, "?token=w")
	if err != nil {
		t.Fatalf("dial watcher: %v", err)
	}

	first.send(proto.Command{Verb: proto.VerbMsg, Data: "spent it"})
	first.expectMsg("alice", "spent it")
	watcher.expectMsg("alice", "spent it")
	first.send(proto.Command{Verb: proto.VerbMsg, Data: "refused"})
	first.expectErr(proto.ErrThrottled)

	_ = first.ws.CloseNow()
	watcher.expectPart("main", "alice")

	second, err := ta.dialWith(t, "?token=a")
	if err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	second.send(proto.Command{Verb: proto.VerbMsg, Data: "try again"})
	second.expectErr(proto.ErrThrottled)
}

func TestPerConnectionIsTheDefault(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{
		Auth: fakeAuth{byToken: map[string]hook.Identity{
			"a1": {ID: "u1", Nick: "alice"},
			"a2": {ID: "u1", Nick: "alice2"},
		}},
		Limiter: &fakeLimiter{client: hook.Limits{Burst: 1, Interval: time.Hour}},
	})

	first, _ := ta.dialWith(t, "?token=a1")
	second, _ := ta.dialWith(t, "?token=a2")

	room := []*client{first, second}

	first.send(proto.Command{Verb: proto.VerbMsg, Data: "one"})
	expectAll(room, "alice", "one")
	first.send(proto.Command{Verb: proto.VerbMsg, Data: "refused"})
	first.expectErr(proto.ErrThrottled)

	second.send(proto.Command{Verb: proto.VerbMsg, Data: "still allowed"})
	expectAll(room, "alice2", "still allowed")
}

func TestSharedLimiterReclamation(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{
		Auth:    fakeAuth{byToken: map[string]hook.Identity{"a": {ID: "u1", Nick: "alice"}}},
		Limiter: keyedLimiter{client: hook.Limits{Burst: 2, Interval: time.Hour}},
	})

	c, err := ta.dialWith(t, "?token=a")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c.send(proto.Command{Verb: proto.VerbMsg, Data: "spend one"})
	c.expectMsg("alice", "spend one")

	if n := ta.app.sweepLimiters(time.Now().Add(24 * time.Hour)); n != 0 {
		t.Fatalf("swept %d buckets that are still held", n)
	}

	ta.app.releaseLimiter("u1")
	if n := ta.app.sweepLimiters(time.Now()); n != 0 {
		t.Fatalf("swept %d buckets that still had spent tokens", n)
	}

	if n := ta.app.sweepLimiters(time.Now().Add(24 * time.Hour)); n != 1 {
		t.Fatalf("swept %d buckets, want the 1 that had refilled", n)
	}
}
