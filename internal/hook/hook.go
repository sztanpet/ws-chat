// Package hook is every extension point; the core imports no implementation.
package hook

import (
	"context"
	"errors"
	"slices"
	"time"
)

// ErrUnauthorized refuses the client; any other error is the auth layer's fault.
var ErrUnauthorized = errors.New("hook: unauthorized")

// ErrNoChatter is not a failure: the identity is used as Authenticate gave it.
var ErrNoChatter = errors.New("hook: no such chatter")

// Identity is anonymous when zero, and the server names it.
type Identity struct {
	// What persistence keys on: a nick can change, an ID cannot.
	ID string

	// Empty: the server assigns one.
	Nick string

	// Opaque to the core: only the layers that set them know what they mean.
	Roles []string

	Attrs map[string]string
}

func (i Identity) Anonymous() bool { return i.ID == "" }

// Key falls back to the nick: weak, but an address is shared behind every NAT.
func (i Identity) Key() string {
	if i.ID != "" {
		return "id:" + i.ID
	}
	return "nick:" + i.Nick
}

func (i Identity) Has(role string) bool {
	return slices.Contains(i.Roles, role)
}

// Limits with a non-positive field is unlimited: no opinion must not mean zero.
type Limits struct {
	Burst    int
	Interval time.Duration

	// Connections with the same Key share a bucket; the first one's limits stand.
	Key string
}

func (l Limits) Unlimited() bool { return l.Burst < 1 || l.Interval <= 0 }

// Limiter is asked once per connection or channel, never per message.
type Limiter interface {
	ClientLimits(ctx context.Context, id Identity) Limits

	// Not applied to private messages, which are in no channel.
	ChannelLimits(ctx context.Context, channel string) Limits
}

// Chatter fields left empty leave the identity's own values alone.
type Chatter struct {
	Nick  string
	Roles []string
	Attrs map[string]string
}

type Message struct {
	ID   uint64
	From Identity
	Data string
	At   time.Time
}

type Private struct {
	ID   uint64
	From Identity
	To   Identity
	Data string
	At   time.Time
}

// Authenticator runs once, before the upgrade, and may block; nothing re-checks.
type Authenticator interface {
	Authenticate(ctx context.Context, r Request) (Identity, error)
}

// Request is not an *http.Request, so auth cannot hijack the connection.
type Request interface {
	Header(name string) string
	Cookie(name string) (string, bool)
	Query(name string) string
	RemoteAddr() string
}

type Directory interface {
	// Once per connection, after Authenticate; it may block.
	Chatter(ctx context.Context, id string) (Chatter, error)

	// Lets moderation reach the absent; it runs on the moderator's read pump.
	Resolve(ctx context.Context, nick string) (Identity, error)
}

// Filter runs before every message, so no round trips; reason is the ERR code.
type Filter interface {
	Allow(ctx context.Context, from Identity, data string) (allowed bool, reason string)
}

type Moderation struct {
	ID     uint64
	Action string // one of proto's Action constants
	Scope  string // empty is server-wide
	By     Identity
	Target string
	Key    string // the Identity.Key it is filed under
	Reason string
	Until  time.Time // zero never expires
	At     time.Time
}

// History is a replay window, not durability, which is Recorder's.
type History interface {
	// Called under the channel's fan-out lock, so it must not block. Appends to
	// one channel arrive in id order; different channels' may run at once.
	Append(ctx context.Context, channel string, m Message)

	// Oldest first; once per connection, so it may block. An error means no backlog.
	Recent(ctx context.Context, channel string, n int) ([]Message, error)
}

type Channels interface {
	// nil means the default channel and empty means none; CanJoin is not asked.
	Autojoin(ctx context.Context, id Identity) []string

	// On the read pump: a lookup, not a round trip. reason is the ERR code sent.
	CanJoin(ctx context.Context, id Identity, channel string) (allowed bool, reason string)
}

// Recorder runs on a worker, after the fact; a full queue drops records.
type Recorder interface {
	Message(ctx context.Context, m Message) error
	Private(ctx context.Context, p Private) error
}

type Sanctions interface {
	// Unmute and unban too: remove what they lift, or Active hands it back.
	Record(ctx context.Context, m Moderation) error

	// Once, at startup; an error stops the server. Expired entries are ignored.
	Active(ctx context.Context) ([]Moderation, error)
}

// Authorizer is asked per scope ("" is server-wide); with none, nobody moderates.
type Authorizer interface {
	CanModerate(ctx context.Context, id Identity, channel string) bool
}

// Hooks may have nil fields; whatever is set may be called concurrently.
type Hooks struct {
	Auth      Authenticator
	Directory Directory
	Filter    Filter
	Limiter   Limiter
	Channels  Channels
	History   History
	Sanctions Sanctions
	Recorder  Recorder
	Authz     Authorizer
}

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
