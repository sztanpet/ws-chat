// Package hook defines the server's extension points.
//
// The chat core knows how to move messages between sockets and nothing
// else. Who a connection belongs to, what is known about that person,
// whether they are allowed to say a thing, and where any of it is written
// down are all decided by layers that live outside this package and are
// handed to the server at startup.
//
// The direction of the dependency is the whole point: the core imports this
// package, an implementation imports this package, and the core never
// imports an implementation. A layer can therefore be a database, an HTTP
// call to something else, or a hard-coded map in a test, and the core
// cannot tell.
//
// Every hook is optional. A Hooks with nothing set is a working server:
// anonymous connections, everything allowed, nothing persisted.
//
// Rules an implementation has to live by:
//
//   - Authenticate, Chatter and Autojoin run once per connection, before it
//     is serving. They may block; they delay one connection's setup and
//     nothing else.
//   - CanJoin runs on the read pump in front of a JOIN, which is rare
//     enough not to be a hot path but frequent enough not to be a round
//     trip.
//   - Allow runs on the sender's read pump, in front of every message. It
//     is on the hot path, so it has to be fast — a lookup, not a round
//     trip. It is the wrong place for anything that talks to a network.
//   - ClientLimits and ChannelLimits are asked once each, when a connection
//     or a channel is set up. They decide policy; the enforcing is the
//     server's, on a token bucket, and costs a nil check when unlimited.
//   - Message, Private and Moderation run on a background worker AFTER the
//     thing has happened, so a slow or broken store delays persistence and
//     never delivery. Their queue is bounded: when it is full, records are
//     dropped and counted rather than allowed to become backpressure on
//     the chat.
//   - History.Append runs on the sender's path, under the lock that orders
//     the fan-out, and must not block. It is for maintaining a replay
//     window, not for durability — durability is Recorder, which is
//     asynchronous precisely so it can be slow. History.Recent runs once
//     per connection and may take its time.
//   - All of them may be called concurrently.
package hook

import (
	"context"
	"errors"
	"time"
)

// ErrUnauthorized tells the server to refuse the connection. Any other
// error from Authenticate is treated as the auth layer being broken, which
// is a server fault rather than the client's.
var ErrUnauthorized = errors.New("hook: unauthorized")

// ErrNoChatter means the directory has nothing on file. It is not a
// failure: the identity from Authenticate is used as-is.
var ErrNoChatter = errors.New("hook: no such chatter")

// Identity is who a connection belongs to. The zero value is an anonymous
// connection, which the server names itself.
type Identity struct {
	// ID is stable and unique, and empty for anonymous connections. It is
	// what a persistence layer should key on — nicks can change, ids
	// cannot.
	ID string

	// Nick is the display name. Empty means the server assigns one.
	Nick string

	// Roles are opaque to the core. It carries them and hands them back;
	// only the layers that set them know what they mean.
	Roles []string

	// Attrs is anything else a layer wants to keep attached to the
	// connection. Also opaque to the core.
	Attrs map[string]string
}

// Anonymous reports whether nobody has claimed this connection.
func (i Identity) Anonymous() bool { return i.ID == "" }

// Key is what moderation state is filed under: the stable id when there is
// one, and the display name when there is not.
//
// The anonymous case is weak on purpose rather than by accident. Keying an
// anonymous user by name means a reconnect under a new name escapes a mute,
// and there is nothing better available — an address is shared by everyone
// behind a NAT and changed by anyone with a phone. Moderation of anonymous
// users is a speed bump; moderation of logged-in users is not, and that is
// an argument for requiring a login to speak, which is a deployment's
// decision to make through Authenticator.
func (i Identity) Key() string {
	if i.ID != "" {
		return "id:" + i.ID
	}
	return "nick:" + i.Nick
}

// Has reports whether the identity carries a role.
func (i Identity) Has(role string) bool {
	for _, r := range i.Roles {
		if r == role {
			return true
		}
	}
	return false
}

// Limits is a rate limit: burst messages allowed at once, then one more
// every Interval.
//
// The zero value is unlimited, and so is any value with a non-positive
// field. That is deliberate — a server that installs no Limiter, or a
// Limiter that has no opinion about somebody, must not accidentally
// throttle them to nothing.
type Limits struct {
	// Burst is how many messages may be sent back to back.
	Burst int

	// Interval is how long one message costs, sustained. Burst 5 with an
	// Interval of 2s is "five at once, then one every two seconds".
	Interval time.Duration

	// Key shares one bucket between every connection that reports the same
	// key. Empty — the default — gives each connection its own, which is
	// the weaker guarantee: somebody who opens four sockets gets four
	// budgets.
	//
	// What the key MEANS is entirely the implementation's business, which
	// is why it is a string and not a flag. An account id is the obvious
	// one, but it can equally be a payment tier, an organisation from
	// Attrs, or an address — whatever the auth layer attached to the
	// identity is available here to build it from. The core only compares
	// keys for equality.
	//
	// Two things follow. The bucket is created by the first connection to
	// name a key and its limits stand for as long as anybody holds it, so
	// returning different limits for the same key does not change a bucket
	// already in use. And a keyed bucket outlives the connections holding
	// it, on purpose: a bucket dropped the moment its last connection left
	// would hand a full one back to anyone who reconnected, which is
	// exactly what somebody being throttled would try.
	Key string
}

// Unlimited reports whether these limits enforce anything.
func (l Limits) Unlimited() bool { return l.Burst < 1 || l.Interval <= 0 }

// Limiter supplies rate limits. Both of its answers default to unlimited,
// so installing no Limiter changes nothing.
//
// It decides policy only. A limit is enforced by the server on a token
// bucket, so an implementation is asked once and never again — it must not
// expect to see, or be able to veto, individual messages. Filter is the
// hook for that.
type Limiter interface {
	// ClientLimits is asked once per connection, at connect time, for how
	// fast this person may talk. It is the seam for "mods are not
	// throttled" and "new accounts are throttled harder".
	//
	// Whether the limit covers one connection or every connection of an
	// account is decided by Limits.Key: empty is per connection, which is
	// the default because an anonymous connection has no account to share a
	// budget with.
	ClientLimits(ctx context.Context, id Identity) Limits

	// ChannelLimits is asked once, when a channel is created, for how fast
	// the channel as a whole may move — every member's messages against
	// one bucket. It is what stops a room being unreadable during a raid,
	// however many people are doing it.
	//
	// It does not apply to private messages, which are not in a channel.
	ChannelLimits(ctx context.Context, channel string) Limits
}

// Chatter is what the directory knows about somebody. Empty fields mean
// "no opinion" and leave the identity's own values alone.
type Chatter struct {
	Nick  string
	Roles []string
	Attrs map[string]string
}

// Message is a public message, as recorded.
type Message struct {
	ID   uint64
	From Identity
	Data string
	At   time.Time
}

// Private is a message between two people, as recorded.
type Private struct {
	ID   uint64
	From Identity
	To   Identity
	Data string
	At   time.Time
}

// Authenticator turns a connection request into an identity. The request is
// the raw HTTP upgrade, so a layer can read whatever it puts there — a
// cookie, a bearer token, a query parameter.
//
// It runs once, at connect time. Nothing re-checks per frame.
type Authenticator interface {
	Authenticate(ctx context.Context, r Request) (Identity, error)
}

// Request is the part of the HTTP upgrade an auth layer is allowed to see.
// It is an interface rather than *http.Request so that an implementation
// cannot reach into the connection, hijack it, or start writing a response.
type Request interface {
	Header(name string) string
	Cookie(name string) (string, bool)
	Query(name string) string
	RemoteAddr() string
}

// Directory supplies what is known about a person: display name, roles,
// whatever else. Called once per connection, after Authenticate.
type Directory interface {
	Chatter(ctx context.Context, id string) (Chatter, error)
}

// Filter decides whether a message may be sent. It is the seam for
// moderation, rate limiting, and word filters.
//
// A refusal's reason becomes the ERR code the client is sent, so it should
// be a short machine-readable token ("muted", "slowdown"), not a sentence.
type Filter interface {
	Allow(ctx context.Context, from Identity, data string) (allowed bool, reason string)
}

// Moderation is a moderation action, as recorded.
type Moderation struct {
	ID     uint64
	Action string // one of proto's Action constants
	Scope  string // the channel it applies to, or empty for server-wide
	By     Identity
	Target string // the nick the action names
	Key    string // the Identity.Key the action is filed under
	Reason string
	Until  time.Time // zero when the action does not expire
	At     time.Time
}

// History is the recent messages a connecting client is replayed, so it
// arrives with context instead of an empty window.
//
// It is deliberately separate from Recorder. Recorder is durability and
// runs behind delivery on a worker; History is a window the server reads
// back on every connect and has to be fast. An implementation backed by a
// database should keep its own memory cache, feed it from Append, and
// serve Recent from that — or from the database, which is allowed to be
// slow because Recent runs once per connection.
//
// The default implementation is history.Memory, which keeps the last N per
// channel in memory. A server that installs nothing gets that.
type History interface {
	// Append records a delivered message. It runs on the sender's path,
	// under the lock that orders the fan-out, so it must not block: this is
	// bookkeeping, not persistence.
	Append(ctx context.Context, channel string, m Message)

	// Recent returns up to n messages, oldest first. It runs once per
	// connection and may block. An error is not fatal — the client is sent
	// an empty backlog and told nothing, because failing to show history is
	// not a reason to refuse somebody a connection.
	Recent(ctx context.Context, channel string, n int) ([]Message, error)
}

// Channels decides which channels a connection starts in and which it may
// enter. It is policy only: the server owns membership, fan-out and
// presence, and asks this what is allowed.
//
// With no Channels installed a connection joins the configured default
// channel and may join anything else it asks for, which is what the server
// did before channels existed.
type Channels interface {
	// Autojoin returns the channels a connection is put into on connect,
	// before it has said anything. It runs once per connection and may
	// block.
	//
	// Returning nil means the configured default. Returning an empty
	// non-nil slice means the connection starts in nothing at all, which is
	// a deployment where clients are expected to JOIN explicitly.
	//
	// The channels it names are joined without consulting CanJoin: a layer
	// that put somebody somewhere has already decided they may be there.
	Autojoin(ctx context.Context, id Identity) []string

	// CanJoin decides whether an identity may enter a channel it asked for.
	// It runs on the connection's read pump, in front of a JOIN, so it
	// should be a lookup rather than a round trip.
	//
	// The refusal reason becomes the ERR code the client is sent, so it
	// should be a short machine-readable token ("inviteonly", "banned"),
	// not a sentence.
	CanJoin(ctx context.Context, id Identity, channel string) (allowed bool, reason string)
}

// Recorder writes things down. Every method runs on a background worker,
// after the thing has already happened.
type Recorder interface {
	Message(ctx context.Context, m Message) error
	Private(ctx context.Context, p Private) error
	Moderation(ctx context.Context, m Moderation) error
}

// Authorizer decides who may use the moderation commands, and where.
//
// Unlike every other hook, its default is DENY. The rest of them default to
// permissive because the cost of being wrong is a server that does less
// than somebody wanted; here the cost of being wrong is anybody in the room
// being able to ban anybody else. A server with no Authorizer installed has
// no moderators, which is the correct behaviour for a server that has not
// been told who they are.
//
// Roles are opaque to the core, so this is the only place that can say
// whether "moderator" in an Identity means anything.
//
// The channel is the SCOPE of the action being attempted: a channel name,
// or empty for one that would apply server-wide. They are separate
// questions and an implementation should treat them as such — somebody
// running one room is not somebody who can silence a person everywhere,
// and answering only the first question is the common case.
type Authorizer interface {
	CanModerate(ctx context.Context, id Identity, channel string) bool
}

// Hooks is every extension point, bundled. Nil fields are simply not
// called.
type Hooks struct {
	Auth      Authenticator
	Directory Directory
	Filter    Filter
	Limiter   Limiter
	Channels  Channels
	History   History
	Recorder  Recorder
	Authz     Authorizer
}

// Apply overlays what the directory knows onto an identity. Empty fields in
// the Chatter leave the identity alone, so a layer can fill in only the
// parts it has an opinion about.
func (i Identity) Apply(c Chatter) Identity {
	if c.Nick != "" {
		i.Nick = c.Nick
	}
	if len(c.Roles) > 0 {
		i.Roles = c.Roles
	}
	for k, v := range c.Attrs {
		if i.Attrs == nil {
			i.Attrs = make(map[string]string, len(c.Attrs))
		}
		i.Attrs[k] = v
	}
	return i
}
