// Package proto is the wire format. The verb is inside, so a frame is one decode.
package proto

import "errors"

const (
	VerbMsg  = "MSG"
	VerbPriv = "PRIVMSG"
	VerbPing = "PING"

	VerbJoin  = "JOIN"
	VerbPart  = "PART"
	VerbNames = "NAMES"

	VerbMute   = "MUTE"
	VerbUnmute = "UNMUTE"
	VerbBan    = "BAN"
	VerbUnban  = "UNBAN"
)

const (
	VerbReady   = "READY"
	VerbBacklog = "BACKLOG"
	VerbPong    = "PONG"
	VerbErr     = "ERR"
	VerbMod     = "MOD"
)

const (
	ActionMute   = "mute"
	ActionUnmute = "unmute"
	ActionBan    = "ban"
	ActionUnban  = "unban"
)

// Clients switch on these, so they are stable codes, never prose.
const (
	ErrProtocol = "protocol"
	ErrUnknown  = "unknown"
	ErrEmpty    = "empty"
	ErrTooLong  = "toolong"
	ErrFraming  = "framing"

	ErrNoSuch  = "nosuchnick"
	ErrBacklog = "recipientbusy"
	ErrSelf    = "self"

	// Two codes: only the first is something the client itself can fix.
	ErrThrottled     = "throttled"
	ErrChanThrottled = "channelthrottled"

	ErrMuted       = "muted"
	ErrBanned      = "banned"
	ErrForbidden   = "forbidden"
	ErrBadDuration = "badduration"

	// Only a deployment's login Filter sends it; named here so they all agree.
	ErrNeedLogin = "needlogin"

	ErrNoChannel     = "nosuchchannel"
	ErrNotJoined     = "notjoined"
	ErrAlreadyJoined = "alreadyjoined"
	ErrTooManyChans  = "toomanychannels"
)

var ErrMalformed = errors.New("proto: malformed frame")

// Command is one struct for every verb, so a frame costs one Unmarshal.
type Command struct {
	Verb     string `json:"verb" msgpack:"verb"`
	Data     string `json:"data,omitempty" msgpack:"data,omitempty"`
	Channel  string `json:"channel,omitempty" msgpack:"channel,omitempty"`
	Nick     string `json:"nick,omitempty" msgpack:"nick,omitempty"`
	Duration string `json:"duration,omitempty" msgpack:"duration,omitempty"`
	Reason   string `json:"reason,omitempty" msgpack:"reason,omitempty"`
}

// Outbound exists so that no frame can be encoded without a verb.
type Outbound interface{ frameVerb() string }

type Msg struct {
	Verb      string `json:"verb" msgpack:"verb"`
	Channel   string `json:"channel" msgpack:"channel"`
	ID        uint64 `json:"id" msgpack:"id"`
	Nick      string `json:"nick" msgpack:"nick"`
	Data      string `json:"data" msgpack:"data"`
	Timestamp int64  `json:"timestamp" msgpack:"timestamp"` // unix milliseconds

	// Repeated on every message, so a client can render any one without state.
	Roles []string          `json:"roles,omitempty" msgpack:"roles,omitempty"`
	Attrs map[string]string `json:"attrs,omitempty" msgpack:"attrs,omitempty"`
}

func (m Msg) frameVerb() string { return m.Verb }

func NewMsg(m Msg) Msg { m.Verb = VerbMsg; return m }

// Priv names the other party, so the sender's echo (Sent) names the recipient.
type Priv struct {
	Verb      string `json:"verb" msgpack:"verb"`
	ID        uint64 `json:"id" msgpack:"id"`
	Nick      string `json:"nick" msgpack:"nick"`
	Data      string `json:"data" msgpack:"data"`
	Timestamp int64  `json:"timestamp" msgpack:"timestamp"`
	// omitzero: under json/v2, omitempty still writes false.
	Sent bool `json:"sent,omitzero" msgpack:"sent,omitempty"`

	Roles []string          `json:"roles,omitempty" msgpack:"roles,omitempty"`
	Attrs map[string]string `json:"attrs,omitempty" msgpack:"attrs,omitempty"`
}

func (p Priv) frameVerb() string { return p.Verb }

func NewPriv(p Priv) Priv { p.Verb = VerbPriv; return p }

// Ready exists because the handshake completes before the client is subscribed.
type Ready struct {
	Verb string `json:"verb" msgpack:"verb"`
	Nick string `json:"nick" msgpack:"nick"`
}

func (r Ready) frameVerb() string { return r.Verb }

func NewReady(nick string) Ready { return Ready{Verb: VerbReady, Nick: nick} }

// Backlog is one frame so a client can tell what it missed from what is live.
type Backlog struct {
	Verb     string `json:"verb" msgpack:"verb"`
	Channel  string `json:"channel" msgpack:"channel"`
	Messages []Msg  `json:"messages" msgpack:"messages"`
}

func (b Backlog) frameVerb() string { return b.Verb }

func NewBacklog(channel string, messages []Msg) Backlog {
	return Backlog{Verb: VerbBacklog, Channel: channel, Messages: messages}
}

// Join is also the asker's acknowledgement; there is no other.
type Join struct {
	Verb    string `json:"verb" msgpack:"verb"`
	Channel string `json:"channel" msgpack:"channel"`
	Nick    string `json:"nick" msgpack:"nick"`

	Roles []string          `json:"roles,omitempty" msgpack:"roles,omitempty"`
	Attrs map[string]string `json:"attrs,omitempty" msgpack:"attrs,omitempty"`
}

func (j Join) frameVerb() string { return j.Verb }

func NewJoin(j Join) Join { j.Verb = VerbJoin; return j }

type Part struct {
	Verb    string `json:"verb" msgpack:"verb"`
	Channel string `json:"channel" msgpack:"channel"`
	Nick    string `json:"nick" msgpack:"nick"`
}

func (p Part) frameVerb() string { return p.Verb }

func NewPart(channel, nick string) Part {
	return Part{Verb: VerbPart, Channel: channel, Nick: nick}
}

// Names is capped, Total the real count: a big room would be one huge frame.
type Names struct {
	Verb    string   `json:"verb" msgpack:"verb"`
	Channel string   `json:"channel" msgpack:"channel"`
	Nicks   []string `json:"nicks" msgpack:"nicks"`
	Total   int      `json:"total" msgpack:"total"`
}

func (n Names) frameVerb() string { return n.Verb }

func NewNames(channel string, nicks []string, total int) Names {
	return Names{Verb: VerbNames, Channel: channel, Nicks: nicks, Total: total}
}

// Mod goes to its target too: invisible moderation gets argued over.
type Mod struct {
	Verb string `json:"verb" msgpack:"verb"`

	// Scope is what it covers; a server-wide action is announced in every Channel.
	Channel string `json:"channel" msgpack:"channel"`
	Scope   string `json:"scope,omitempty" msgpack:"scope,omitempty"`

	ID        uint64 `json:"id" msgpack:"id"`
	Action    string `json:"action" msgpack:"action"`
	Nick      string `json:"nick" msgpack:"nick"`
	By        string `json:"by" msgpack:"by"`
	Timestamp int64  `json:"timestamp" msgpack:"timestamp"`

	// Unix milliseconds; zero never expires.
	Until  int64  `json:"until,omitzero" msgpack:"until,omitempty"`
	Reason string `json:"reason,omitempty" msgpack:"reason,omitempty"`
}

func (m Mod) frameVerb() string { return m.Verb }

func NewMod(m Mod) Mod { m.Verb = VerbMod; return m }

type Pong struct {
	Verb string `json:"verb" msgpack:"verb"`
}

func (p Pong) frameVerb() string { return p.Verb }

func NewPong() Pong { return Pong{Verb: VerbPong} }

type Err struct {
	Verb        string `json:"verb" msgpack:"verb"`
	Description string `json:"description" msgpack:"description"`
}

func (e Err) frameVerb() string { return e.Verb }

func NewErr(description string) Err {
	return Err{Verb: VerbErr, Description: description}
}
