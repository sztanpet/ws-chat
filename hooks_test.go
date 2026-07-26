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

// fakeDirectory knows a set of people by their stable id, and can find
// them by name the way a real one would — the same table read two ways.
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

	c.send(proto.Command{Verb: proto.VerbMsg, Data: "this has badword in it"})
	c.expectErr("muted")

	// And the refusal is per message, not per connection.
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

// Persistence is behind delivery, not in front of it: a store that is
// failing must not cost anybody a message.
func TestRecorderFailureDoesNotAffectDelivery(t *testing.T) {
	rec := newFakeRecorder()
	rec.fail = errors.New("database on fire")

	ta := newTestAppWith(t, hook.Hooks{Recorder: rec})
	alice, bob := ta.dial(t), ta.dial(t)

	alice.send(proto.Command{Verb: proto.VerbMsg, Data: "still delivered"})
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

// fakeLimiter hands out fixed limits, and records which identities it was
// asked about so a test can prove it was consulted per connection.
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

// No Limiter installed is unlimited, which is the default every other test
// in this package runs under.
func TestNoLimiterIsUnlimited(t *testing.T) {
	ta := newTestApp(t)
	c := ta.dial(t)

	for i := range 50 {
		c.send(proto.Command{Verb: proto.VerbMsg, Data: fmt.Sprintf("message %d", i)})
		c.expectMsg("", fmt.Sprintf("message %d", i))
	}
}

// A Limiter with no opinion is also unlimited: the zero Limits must not
// throttle somebody to nothing.
func TestZeroLimitsAreUnlimited(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{Limiter: &fakeLimiter{}})
	c := ta.dial(t)

	for i := range 50 {
		c.send(proto.Command{Verb: proto.VerbMsg, Data: fmt.Sprintf("message %d", i)})
		c.expectMsg("", fmt.Sprintf("message %d", i))
	}
}

func TestClientRateLimit(t *testing.T) {
	// Three messages, then one an hour: the refill never happens during
	// the test, so there is nothing timing-dependent about it.
	ta := newTestAppWith(t, hook.Hooks{
		Limiter: &fakeLimiter{client: hook.Limits{Burst: 3, Interval: time.Hour}},
	})
	alice, bob := ta.dial(t), ta.dial(t)

	for i := range 3 {
		want := fmt.Sprintf("burst %d", i)
		alice.send(proto.Command{Verb: proto.VerbMsg, Data: want})
		alice.expectMsg(alice.nick, want)
		bob.expectMsg(alice.nick, want) // bob is in the room the whole time
	}

	alice.send(proto.Command{Verb: proto.VerbMsg, Data: "one too many"})
	alice.expectErr(proto.ErrThrottled)

	// The limit is the sender's alone: bob still has his whole budget.
	bob.send(proto.Command{Verb: proto.VerbMsg, Data: "bob is fine"})
	bob.expectMsg(bob.nick, "bob is fine")

	// And alice's next frame is bob's message, not her own refused one.
	alice.expectMsg(bob.nick, "bob is fine")
}

// A throttled message must not reach anybody.
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

	// bob's next frame is the following message, not the refused one.
	bob.send(proto.Command{Verb: proto.VerbMsg, Data: "next"})
	bob.expectMsg(bob.nick, "next")
}

// The channel's bucket is shared: one person can exhaust it for everybody,
// which is the point of having it.
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

	// The channel's two are gone, so the next message from anybody is
	// refused — and refused as the channel's fault, not the sender's.
	alice.send(proto.Command{Verb: proto.VerbMsg, Data: "three"})
	alice.expectErr(proto.ErrChanThrottled)

	bob.send(proto.Command{Verb: proto.VerbMsg, Data: "four"})
	bob.expectErr(proto.ErrChanThrottled)
}

// The client's limit is checked first, so a client over its own budget is
// told it is the one at fault.
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

// Private messages spend the sender's budget but not the channel's: a busy
// room must not stop two people talking, and two people talking must not
// use up the room.
func TestPrivateMessagesUseTheClientLimitOnly(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{
		Limiter: &fakeLimiter{
			client:  hook.Limits{Burst: 2, Interval: time.Hour},
			channel: hook.Limits{Burst: 1, Interval: time.Hour},
		},
	})
	alice, bob := ta.dial(t), ta.dial(t)

	// Exhaust the channel with somebody else's message.
	bob.send(proto.Command{Verb: proto.VerbMsg, Data: "fills the channel"})
	alice.expectMsg(bob.nick, "fills the channel")
	bob.expectMsg(bob.nick, "fills the channel")

	// alice can still send privately, twice, on her own budget.
	alice.send(proto.Command{Verb: proto.VerbPriv, Nick: bob.nick, Data: "one"})
	bob.expectPriv("one")
	alice.expectPriv("one")

	alice.send(proto.Command{Verb: proto.VerbPriv, Nick: bob.nick, Data: "two"})
	bob.expectPriv("two")
	alice.expectPriv("two")

	// And then her own limit stops her.
	alice.send(proto.Command{Verb: proto.VerbPriv, Nick: bob.nick, Data: "three"})
	alice.expectErr(proto.ErrThrottled)
}

// The limit is asked for once per connection, with the identity, so a
// layer can give different people different limits.
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

// A refusal is per message, not per connection: a throttled client is
// still connected and still receiving.
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

	// Still connected: alice receives what bob says.
	bob.send(proto.Command{Verb: proto.VerbMsg, Data: "still here"})
	alice.expectMsg(bob.nick, "still here")
	bob.expectMsg(bob.nick, "still here")

	// And once the bucket refills, alice can talk again. This is the one
	// place a real interval is used, so it is generous.
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

// --- account-wide rate limits ------------------------------------------

// keyedLimiter builds the bucket key out of whatever the auth layer
// attached to the identity, which is the point of the key being a string
// the hook chooses rather than a flag the core interprets.
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

// Two connections of one account share a budget.
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

	// Two from one socket, one from the other: that is the account's three.
	first.send(proto.Command{Verb: proto.VerbMsg, Data: "one"})
	expectAll(room, "alice", "one")
	first.send(proto.Command{Verb: proto.VerbMsg, Data: "two"})
	expectAll(room, "alice", "two")
	second.send(proto.Command{Verb: proto.VerbMsg, Data: "three"})
	expectAll(room, "alice2", "three")

	// A fourth is refused on EITHER socket — opening a second one bought
	// nothing.
	second.send(proto.Command{Verb: proto.VerbMsg, Data: "four"})
	second.expectErr(proto.ErrThrottled)
	first.send(proto.Command{Verb: proto.VerbMsg, Data: "also four"})
	first.expectErr(proto.ErrThrottled)

	// And it is that account's budget, not everybody's.
	bob.send(proto.Command{Verb: proto.VerbMsg, Data: "bob is fine"})
	expectAll(room, "bob", "bob is fine")
}

// The key can come from anything the auth layer attached, not just the id.
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

	// Two different people, one organisation, one budget.
	alice.send(proto.Command{Verb: proto.VerbMsg, Data: "one"})
	expectAll(room, "alice", "one")
	bob.send(proto.Command{Verb: proto.VerbMsg, Data: "two"})
	expectAll(room, "bob", "two")

	bob.send(proto.Command{Verb: proto.VerbMsg, Data: "three"})
	bob.expectErr(proto.ErrThrottled)

	// A different organisation is untouched.
	carol.send(proto.Command{Verb: proto.VerbMsg, Data: "different org"})
	expectAll(room, "carol", "different org")
}

// Reconnecting must not hand back a full bucket, which is the first thing
// somebody being throttled would try.
func TestAccountLimitSurvivesReconnection(t *testing.T) {
	ta := newTestAppWith(t, hook.Hooks{
		Auth:    fakeAuth{byToken: map[string]hook.Identity{"a": {ID: "u1", Nick: "alice"}}},
		Limiter: keyedLimiter{client: hook.Limits{Burst: 1, Interval: time.Hour}},
	})

	first, err := ta.dialWith(t, "?token=a")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	first.send(proto.Command{Verb: proto.VerbMsg, Data: "spent it"})
	first.expectMsg("alice", "spent it")
	first.send(proto.Command{Verb: proto.VerbMsg, Data: "refused"})
	first.expectErr(proto.ErrThrottled)

	// Go away and come back.
	first.ws.CloseNow()

	second, err := ta.dialWith(t, "?token=a")
	if err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	second.send(proto.Command{Verb: proto.VerbMsg, Data: "try again"})
	second.expectErr(proto.ErrThrottled)
}

// Without a key it is one bucket per connection, which is the default and
// the weaker guarantee.
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

	// The same account's other socket has its own budget, because no key
	// was given.
	second.send(proto.Command{Verb: proto.VerbMsg, Data: "still allowed"})
	expectAll(room, "alice2", "still allowed")
}

// A shared bucket is reclaimed once nobody holds it AND it has refilled,
// never before.
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

	// Held by a live connection: not reclaimable however long we wait.
	if n := ta.app.sweepLimiters(time.Now().Add(24 * time.Hour)); n != 0 {
		t.Fatalf("swept %d buckets that are still held", n)
	}

	// Released but still spent: not reclaimable either, which is what
	// stops a reconnect refilling it.
	ta.app.releaseLimiter("u1")
	if n := ta.app.sweepLimiters(time.Now()); n != 0 {
		t.Fatalf("swept %d buckets that still had spent tokens", n)
	}

	// Released and refilled: now it holds nothing a new bucket would not.
	if n := ta.app.sweepLimiters(time.Now().Add(24 * time.Hour)); n != 1 {
		t.Fatalf("swept %d buckets, want the 1 that had refilled", n)
	}
}
