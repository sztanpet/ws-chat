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
//   - Authenticate and Chatter run once per connection, before it is
//     serving. They may block; they delay one connection's setup and
//     nothing else.
//   - Allow runs on the sender's read pump, in front of every message. It
//     is on the hot path, so it has to be fast — a lookup, not a round
//     trip. It is the wrong place for anything that talks to a network.
//   - ClientLimits and ChannelLimits are asked once each, when a connection
//     or a channel is set up. They decide policy; the enforcing is the
//     server's, on a token bucket, and costs a nil check when unlimited.
//   - Message and Private run on a background worker AFTER delivery, so a
//     slow or broken store delays persistence and never delivery. Their
//     queue is bounded: when it is full, records are dropped and counted
//     rather than allowed to become backpressure on the chat.
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
	// The limit is per CONNECTION, not per identity: an anonymous
	// connection has no stable id to key on, so somebody who opens two
	// sockets gets two buckets. Tightening that needs logins and a keyed
	// registry, and is noted in state/server.md.
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

// Recorder writes messages down. Both methods run on a background worker,
// after the message has already been delivered.
type Recorder interface {
	Message(ctx context.Context, m Message) error
	Private(ctx context.Context, p Private) error
}

// Hooks is every extension point, bundled. Nil fields are simply not
// called.
type Hooks struct {
	Auth      Authenticator
	Directory Directory
	Filter    Filter
	Limiter   Limiter
	Recorder  Recorder
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
