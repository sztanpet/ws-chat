// Package proto is the wire format: one command per frame, and the frame is
// a single encoded document with the verb inside it.
//
//	{"verb":"MSG","data":"hello"}
//
// The verb being a field rather than a prefix is what keeps an inbound
// frame to ONE decode. A "VERB {payload}" framing has to split the string,
// read the verb, decide what shape the rest is, and then parse the rest —
// two passes over the same bytes on every message a client sends. Here the
// verb and its arguments arrive together and come out of one Unmarshal.
//
// That is also why Command is one flat struct covering every inbound verb
// rather than one struct per verb: a union of a handful of short string
// fields costs nothing to decode into, and the alternative reintroduces
// exactly the second parse this design exists to remove.
//
// It removes the framing code entirely as a side benefit. Both codecs are
// now their encoder's Marshal and Unmarshal and nothing else — no
// splitting, no length prefix, no bare-verb special case.
package proto

import "errors"

// Verbs a client sends.
const (
	VerbMsg  = "MSG"
	VerbPriv = "PRIVMSG"
	VerbPing = "PING"

	// Channels. JOIN and PART travel in both directions: a client asks
	// with one, and the channel is told somebody arrived or left with the
	// same verb.
	VerbJoin  = "JOIN"
	VerbPart  = "PART"
	VerbNames = "NAMES"

	// Moderation. All four are answered with one MOD frame to the whole
	// channel, so a client needs one handler rather than four.
	VerbMute   = "MUTE"
	VerbUnmute = "UNMUTE"
	VerbBan    = "BAN"
	VerbUnban  = "UNBAN"
)

// Verbs the server sends. MSG and PRIVMSG travel in both directions.
const (
	VerbReady   = "READY"   // once, on connect
	VerbBacklog = "BACKLOG" // once, after READY
	VerbPong    = "PONG"
	VerbErr     = "ERR"
	VerbMod     = "MOD"
)

// Moderation actions, as they appear in a MOD frame.
const (
	ActionMute   = "mute"
	ActionUnmute = "unmute"
	ActionBan    = "ban"
	ActionUnban  = "unban"
)

// Error descriptions. These are stable machine-readable codes, not prose:
// a client should be able to switch on them without parsing English.
const (
	ErrProtocol = "protocol" // unparseable frame
	ErrUnknown  = "unknown"  // verb the server does not implement
	ErrEmpty    = "empty"    // message with no content
	ErrTooLong  = "toolong"  // message body over the configured limit
	ErrFraming  = "framing"  // wrong WebSocket message type for the codec

	ErrNoSuch  = "nosuchnick"    // addressed to nobody
	ErrBacklog = "recipientbusy" // recipient is not draining its queue
	ErrSelf    = "self"          // private message to yourself

	// Rate limits. Two codes, not one: a client needs to know whether it is
	// being told to slow down or whether the whole room is busy, because
	// only one of those is something it can do anything about.
	ErrThrottled     = "throttled"
	ErrChanThrottled = "channelthrottled"

	// Moderation.
	ErrMuted       = "muted"
	ErrBanned      = "banned"
	ErrForbidden   = "forbidden"
	ErrBadDuration = "badduration"

	// Channels.
	ErrNoChannel     = "nosuchchannel" // not a usable channel name
	ErrNotJoined     = "notjoined"     // you are not in that channel
	ErrAlreadyJoined = "alreadyjoined" // you already are
	ErrTooManyChans  = "toomanychannels"
)

// ErrMalformed is what a codec returns for a frame it cannot decode.
var ErrMalformed = errors.New("proto: malformed frame")

// Command is every field a client may send, in one struct.
//
// One struct rather than one per verb, deliberately: it is what lets a
// frame cost a single Unmarshal. The fields overlap heavily across the
// verbs anyway — a nick and a body is most of the protocol — and the ones
// that do not apply are simply absent from the encoded frame.
type Command struct {
	Verb string `json:"verb" msgpack:"verb"`

	// Data is the message body, for MSG and PRIVMSG.
	Data string `json:"data,omitempty" msgpack:"data,omitempty"`

	// Channel is which channel the command is about. Empty on a MSG means
	// the default channel, so a client that only ever uses one does not
	// have to name it.
	Channel string `json:"channel,omitempty" msgpack:"channel,omitempty"`

	// Nick is who the command is about: the recipient of a PRIVMSG, the
	// target of a moderation command.
	Nick string `json:"nick,omitempty" msgpack:"nick,omitempty"`

	// Duration is a Go duration string ("10m") on a moderation command.
	// Empty means the action does not expire.
	Duration string `json:"duration,omitempty" msgpack:"duration,omitempty"`

	// Reason accompanies a moderation command.
	Reason string `json:"reason,omitempty" msgpack:"reason,omitempty"`
}

// Outbound is implemented by every frame the server sends.
//
// It exists so a payload cannot be encoded without a verb. A frame with an
// empty verb is one no client can dispatch, and forgetting to set the field
// would otherwise be a silent bug that shows up only as a client quietly
// ignoring messages. The New* constructors are how it is set.
type Outbound interface{ frameVerb() string }

// Msg is a chat message.
type Msg struct {
	Verb      string `json:"verb" msgpack:"verb"`
	Channel   string `json:"channel" msgpack:"channel"`
	ID        uint64 `json:"id" msgpack:"id"`
	Nick      string `json:"nick" msgpack:"nick"`
	Data      string `json:"data" msgpack:"data"`
	Timestamp int64  `json:"timestamp" msgpack:"timestamp"` // unix milliseconds

	// Roles and Attrs are whatever the directory layer attached to the
	// sender, carried on every message they send.
	//
	// Repeating them per message rather than sending them once and having
	// clients keep a table is a deliberate trade: a client can render any
	// message on its own, with no state and no ordering problem when
	// somebody's roles change mid-conversation. It costs bytes on every
	// message, which is part of why MessagePack is the preferred codec.
	// Both are omitted entirely when empty, so a server with no directory
	// installed pays nothing.
	Roles []string          `json:"roles,omitempty" msgpack:"roles,omitempty"`
	Attrs map[string]string `json:"attrs,omitempty" msgpack:"attrs,omitempty"`
}

func (m Msg) frameVerb() string { return m.Verb }

// NewMsg is m with its verb set.
func NewMsg(m Msg) Msg { m.Verb = VerbMsg; return m }

// Priv is a private message. Both parties get one: the recipient's copy
// names the sender, and the sender's own echo names the recipient and sets
// Sent, so a client can render its own outgoing messages from the same
// frame it renders incoming ones.
type Priv struct {
	Verb      string `json:"verb" msgpack:"verb"`
	ID        uint64 `json:"id" msgpack:"id"`
	Nick      string `json:"nick" msgpack:"nick"`
	Data      string `json:"data" msgpack:"data"`
	Timestamp int64  `json:"timestamp" msgpack:"timestamp"`
	Sent      bool   `json:"sent,omitempty" msgpack:"sent,omitempty"`

	// Roles and Attrs describe the other party, the same way Msg carries
	// them for the sender.
	Roles []string          `json:"roles,omitempty" msgpack:"roles,omitempty"`
	Attrs map[string]string `json:"attrs,omitempty" msgpack:"attrs,omitempty"`
}

func (p Priv) frameVerb() string { return p.Verb }

// NewPriv is p with its verb set.
func NewPriv(p Priv) Priv { p.Verb = VerbPriv; return p }

// Ready is the first frame the server sends. It tells the client the name
// it was given, and — more importantly — that it is subscribed: a client
// that starts talking before this arrives can miss the replies, because the
// WebSocket handshake completes before the server has finished wiring the
// connection up.
type Ready struct {
	Verb string `json:"verb" msgpack:"verb"`
	Nick string `json:"nick" msgpack:"nick"`
}

func (r Ready) frameVerb() string { return r.Verb }

// NewReady is a READY frame.
func NewReady(nick string) Ready { return Ready{Verb: VerbReady, Nick: nick} }

// Backlog is the recent history of the channel, sent once on connect so a
// client arrives with context instead of an empty window.
//
// It is one frame with an array rather than a burst of MSG frames. A client
// can then tell "here is what you missed" from "this just happened" without
// a per-message flag, and render the block in one pass.
//
// A client MUST ignore anything whose id it has already seen. It is a
// general rule, not one about the backlog, though the backlog is where it
// bites first: the server subscribes a connection to the channel before it
// reads the history, so a message sent in between arrives twice, once in
// the backlog and once live. Doing that the other way round would lose the
// message instead, and a duplicate a client can drop by id is strictly
// better than a gap it cannot detect at all.

type Backlog struct {
	Verb     string `json:"verb" msgpack:"verb"`
	Channel  string `json:"channel" msgpack:"channel"`
	Messages []Msg  `json:"messages" msgpack:"messages"`
}

func (b Backlog) frameVerb() string { return b.Verb }

// NewBacklog is a BACKLOG frame for one channel.
func NewBacklog(channel string, messages []Msg) Backlog {
	return Backlog{Verb: VerbBacklog, Channel: channel, Messages: messages}
}

// Join announces that somebody entered a channel. The client that asked
// gets one too, which is how it learns the join succeeded.
type Join struct {
	Verb    string `json:"verb" msgpack:"verb"`
	Channel string `json:"channel" msgpack:"channel"`
	Nick    string `json:"nick" msgpack:"nick"`

	// Roles and Attrs describe the person who joined, so a client can put
	// them in its member list without waiting for them to speak.
	Roles []string          `json:"roles,omitempty" msgpack:"roles,omitempty"`
	Attrs map[string]string `json:"attrs,omitempty" msgpack:"attrs,omitempty"`
}

func (j Join) frameVerb() string { return j.Verb }

// NewJoin is j with its verb set.
func NewJoin(j Join) Join { j.Verb = VerbJoin; return j }

// Part announces that somebody left a channel, whether they asked to or
// simply disconnected.
type Part struct {
	Verb    string `json:"verb" msgpack:"verb"`
	Channel string `json:"channel" msgpack:"channel"`
	Nick    string `json:"nick" msgpack:"nick"`
}

func (p Part) frameVerb() string { return p.Verb }

// NewPart is a PART frame.
func NewPart(channel, nick string) Part {
	return Part{Verb: VerbPart, Channel: channel, Nick: nick}
}

// Names is who is in a channel.
//
// Nicks is capped by the server, and Total says how many there really are,
// because the alternative is a hundred-kilobyte frame for a channel with
// ten thousand people in it. A client that wants all of them in a room
// that big wants paging, which does not exist yet.
type Names struct {
	Verb    string   `json:"verb" msgpack:"verb"`
	Channel string   `json:"channel" msgpack:"channel"`
	Nicks   []string `json:"nicks" msgpack:"nicks"`
	Total   int      `json:"total" msgpack:"total"`
}

func (n Names) frameVerb() string { return n.Verb }

// NewNames is a NAMES frame.
func NewNames(channel string, nicks []string, total int) Names {
	return Names{Verb: VerbNames, Channel: channel, Nicks: nicks, Total: total}
}

// Mod is a moderation action as the server announces it to the channel.
//
// It goes to everybody, including the person it is about. Moderation that
// happens invisibly gets re-litigated in the channel by people guessing at
// what happened.
type Mod struct {
	Verb string `json:"verb" msgpack:"verb"`

	// Channel is where this announcement was made. Scope is what the action
	// covers: the same channel for a channel action, empty for one that
	// applies server-wide.
	//
	// Two fields rather than one because they answer different questions. A
	// server-wide mute is announced in every channel the person is in, so
	// Channel differs per copy while Scope stays empty — and a client
	// rendering "muted here" against "muted everywhere" needs to be able to
	// tell them apart.
	Channel string `json:"channel" msgpack:"channel"`
	Scope   string `json:"scope,omitempty" msgpack:"scope,omitempty"`

	ID        uint64 `json:"id" msgpack:"id"`
	Action    string `json:"action" msgpack:"action"`
	Nick      string `json:"nick" msgpack:"nick"`
	By        string `json:"by" msgpack:"by"`
	Timestamp int64  `json:"timestamp" msgpack:"timestamp"`

	// Until is when the action expires, in unix milliseconds. Zero means it
	// does not.
	Until  int64  `json:"until,omitempty" msgpack:"until,omitempty"`
	Reason string `json:"reason,omitempty" msgpack:"reason,omitempty"`
}

func (m Mod) frameVerb() string { return m.Verb }

// NewMod is m with its verb set.
func NewMod(m Mod) Mod { m.Verb = VerbMod; return m }

// Pong answers a PING.
type Pong struct {
	Verb string `json:"verb" msgpack:"verb"`
}

func (p Pong) frameVerb() string { return p.Verb }

// NewPong is a PONG frame.
func NewPong() Pong { return Pong{Verb: VerbPong} }

// Err is a refusal. Description is one of the codes above.
type Err struct {
	Verb        string `json:"verb" msgpack:"verb"`
	Description string `json:"description" msgpack:"description"`
}

func (e Err) frameVerb() string { return e.Verb }

// NewErr is an ERR frame carrying one of the codes above.
func NewErr(description string) Err {
	return Err{Verb: VerbErr, Description: description}
}
