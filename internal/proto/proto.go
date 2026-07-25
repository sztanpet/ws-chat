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
// A client MUST ignore a live message whose id it already has from the
// backlog. The server subscribes a connection to the channel before it
// reads the history, so a message sent in between arrives twice: once in
// the backlog and once live. Doing it the other way round would lose that
// message instead, and a duplicate a client can drop by id is strictly
// better than a gap it cannot detect at all. Ids are monotonic, so the
// check is a comparison against the last id in the backlog.
type Backlog struct {
	Verb     string `json:"verb" msgpack:"verb"`
	Messages []Msg  `json:"messages" msgpack:"messages"`
}

func (b Backlog) frameVerb() string { return b.Verb }

// NewBacklog is a BACKLOG frame.
func NewBacklog(messages []Msg) Backlog {
	return Backlog{Verb: VerbBacklog, Messages: messages}
}

// Mod is a moderation action as the server announces it to the channel.
//
// It goes to everybody, including the person it is about. Moderation that
// happens invisibly gets re-litigated in the channel by people guessing at
// what happened.
type Mod struct {
	Verb      string `json:"verb" msgpack:"verb"`
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
