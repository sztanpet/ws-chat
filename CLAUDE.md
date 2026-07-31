# ws-chat

Go backend for a WebSocket chat service with channel support: users
connect over a single WebSocket, join one or more channels, and exchange
messages, presence and moderation events. The server owns authentication,
channel membership, message fan-out, scrollback and moderation state.
Clients (web, bots) speak the line protocol described below.

> **Status: walking skeleton.** The fan-out, channels and memberships,
> private messages, moderation, rate limiting, both wire codecs, the
> extension points, metrics and the load generator exist and are tested.
> Persistence does not: the replay window is `history.Memory`, so
> scrollback is what is in RAM and dies with the process. Sections below
> marked *(not built yet)* are intent; everything else describes code that
> is there. When reality and this file disagree, reality wins and this file
> gets updated in the same commit.
>
> [`README.md`](README.md) is the how-to — building, running,
> configuring, the flags. This file is the why.

## Architecture

The Go toolchain pin in `go.mod` is a dependency like any other — `make
update-deps` bumps it, because govulncheck reports standard library CVEs
against it.

The `app` struct in `main.go` is the composition root: `init.go` wires
config, db, the hub, auth and the HTTP mux. Static assets, if any are
ever needed, are `go:embed`ded — the binary is self-contained except for
`config.hujson` and the `data/` directory next to the executable.

### The connection model — the one unusual convention

`coder/websocket` (`github.com/coder/websocket`, the former
`nhooyr.io/websocket`) does the upgrade on a stdlib `http.ServeMux`
route. After the upgrade a connection runs **three goroutines**
(`conn.go`):

- The **read pump** loops on `conn.Read` and parses one frame at a time.
  It writes to the socket only for replies belonging to this client alone
  (`PONG`, `ERR`).
- The **write pump** feeds the client the broadcast stream. It blocks in
  `Sub.Recv`, which hands back everything that piled up since the last
  call, so a client that has fallen behind costs one wakeup rather than
  one per message.
- The **private pump** delivers messages addressed to this client alone,
  off a small bounded queue.

All three may write, which is safe because coder/websocket serializes
writes internally ("all methods may be called concurrently except for
Reader and Read"). That is a deliberate deviation from a single-writer
rule: the alternative is one merge channel in front of the socket, which
would put a channel hop back in front of every broadcast message and undo
the work in `internal/broadcast`.

**Backpressure stops at the connection.** A subscriber that cannot keep
up is dropped (`ErrLagged`) rather than waited on, a private message to
somebody whose queue is full is refused (`ERR recipientbusy`) rather than
blocked on, and every socket write is bounded by `WriteTimeout` — one
deadline per wakeup, covering the whole batch a pump drains, because
bounding a wakeup is stricter than bounding a frame: sixteen frames would
otherwise get sixteen times the timeout to drain. A
private message arrives on *somebody else's* read pump, so this is not
theoretical: without it, one person who stopped reading would stall
everyone who messages them.

The deadline is set with `Conn.SetWriteDeadline` and the write itself gets
`context.Background()`, because a context per frame cost a `WithTimeout`
here and a `context.AfterFunc` in the library — ~700 bytes of garbage per
delivered message, *per member* of the room, and 99% of everything the
server allocated. An instant costs one atomic store.
**`SetWriteDeadline` is not in a released `coder/websocket`**: `go.mod`
pins a fork of it pending the PR in `code-websocket-pr/`, and the revert
instructions live beside that `replace` directive. The code is vendored, so
only `make vendor` needs the fork checked out beside this repo.

Fan-out is `internal/broadcast`. The shape is a shared buffer with each
subscriber reading at its own position, so a broadcast is O(1) in the
member count and never walks the subscribers. `SeqRing` is the
implementation to use: it is that design with the lock taken off the read
path, publishing a pointer per slot and validating a whole batch with a
seqlock, so a `Recv` is atomic loads and no read-modify-write. `Ring` is
the same design guarded by an `RWMutex` — simpler, one allocation per
broadcast instead of two, and kept as the reference the alternatives are
measured against.

The win is proportional to how much time readers spend contending, so it
shows up in busy rooms and disappears in quiet ones, and it is largest on
join/part under load because `SeqRing.Subscribe` takes no lock at all.
See `state/broadcast.md` for the numbers and the regimes.

### Channels

A `channel` (`channel.go`) is a fan-out, a rate limit and a member list.
The fan-out is one `SeqRing` **per codec**, since a ring holds encoded
bytes.

A user has **one connection and many memberships**, not one connection
per channel. There is a **write pump per membership** — `Sub.Recv` blocks
and cannot be selected on, and the alternative (one merge channel in
front of the socket) puts a channel hop back in front of every broadcast
message, which is what `internal/broadcast` exists to avoid. So a
connection in five channels runs five pumps and one read pump and one
private pump; `coder/websocket` serializes their writes.

The `map[nick]*conn` directory in `init.go` is what `PRIVMSG` is
addressed through, separate from any channel's member set for that
reason.

Channels are **created on demand** and **reclaimed when empty**. Empty is
free to reclaim: the ring holds frames for subscribers that no longer
exist, and the backlog lives in the `History` hook, so there is nothing
in an empty channel anybody can observe. `MaxChannels` is what stops a
client inventing them until the server runs out of memory; a deployment
that cares which names exist refuses the rest in `CanJoin`.

**The order on joining is deterministic within a channel, and the code
depends on it.** Subscribe, send the backlog directly, broadcast the
`JOIN`, *then* start the pump. A client therefore sees that channel's
`BACKLOG`, its own `JOIN`, then its live traffic. Across channels there is
no ordering — a second channel's backlog is written directly while the
first's `JOIN` is still coming through a pump — so a client joining two at
once may see both backlogs before either join. Starting the pump any earlier makes the first two race, which it
did. Parting is the mirror: the parting client is told **directly**
before its subscription ends, because ending it discards what it had not
read — its own `PART` would be the most likely message to lose.

### Wire protocol

One command per frame, and **the frame is a single encoded document with
the verb inside it**:

```json
{"verb":"MSG","data":"hello"}
```

The verb being a field rather than a prefix is what keeps an inbound frame
to **one decode**. A `VERB {payload}` framing has to split the string, read
the verb, decide what shape the rest is, and then parse the rest — two
passes over the same bytes on every message anybody sends. Here the verb
and its arguments come out of one `Unmarshal`.

That is why `proto.Command` is one flat struct covering every inbound verb
rather than one struct per verb. A union of a handful of short string
fields costs nothing to decode into, and one-struct-per-verb would
reintroduce exactly the second parse this design removes. Unknown verbs get
an `ERR` back and do not close the connection.

**The format is negotiated, not configured.** `internal/proto` defines a
`Codec` interface — `Encode`, `Decode`, plus whether frames are binary —
and a client picks one with a WebSocket subprotocol:

- `chat.msgpack` — MessagePack. Offered first and the one to prefer: ~20%
  smaller frames on a typical message and no number-to-string round trip.
- `chat.json` — the same document as JSON. The default for a client that
  negotiates nothing, because a client that did not ask is one being
  written by hand against a console.

Because the verb lives in the document, a codec is now *just* its encoder —
`Marshal` one way, `Unmarshal` the other. There is no framing code left: no
splitting, no length prefix, no bare-verb special case.

Outbound payloads carry their own verb and are built with the `New*`
constructors. `Encode` takes a `proto.Outbound`, which exists so a frame
cannot be encoded without one — a frame with an empty verb is one no client
can dispatch, and it would otherwise be a silent bug.

Both codecs are held to the same test table in `codec_test.go`; if one
needs its own test to pass they are not interchangeable. Struct fields
carry both `json:` and `msgpack:` tags so the two agree on names.

**The JSON codec is `encoding/json/v2`, which is experimental**, so the
whole module builds with `GOEXPERIMENT=jsonv2` — the Makefile exports it,
and anything run by hand needs it or the build fails with "build
constraints exclude all Go files in .../encoding/json/v2". That is the
price; what it buys, measured in `internal/proto/bench_test.go` on a
message with roles and attrs:

| chat.json | v1, no experiment | v1 API on the v2 engine | v2 API |
|---|---|---|---|
| Encode | 660ns, 5 allocs | 950ns, 5 allocs | 978ns, 5 allocs |
| Decode | 795ns, 8 allocs | 422ns, 1 alloc | 354ns, 1 alloc |

Read the middle column first: **encoding is slower because of the engine,
not the API.** Turning the experiment on costs it whether the code calls
v1 or v2, so with it on, calling v2 directly is strictly the better of the
two. A message costs one decode and one encode per codec, neither of which
scales with room size, so the net is about 8% less CPU and half the
allocations per message — a small win, and the correctness is the larger
part of it: invalid UTF-8 and duplicate object members are refused rather
than repaired.

Optional **scalars are tagged `omitzero`, not `omitempty`**. Under v2 those
are different: `omitempty` is defined in JSON terms and only drops null,
"", `{}` and `[]`, so a false bool and a zero int would start appearing on
the wire. Strings, slices and maps mean the same under either and keep
`omitempty`.

The consequence to keep in mind: **a ring holds encoded bytes**, so there
is one broadcaster per codec and a message is encoded once per codec
rather than once per subscriber — O(codecs), not O(members). A message id
is assigned and written to every ring under one small lock, so clients on
different codecs cannot disagree about what happened first. Private
messages are encoded with the **recipient's** codec, since the two ends
negotiate separately.

Client → server: `MSG`, `PRIVMSG`, `PING`, `JOIN`, `PART`, `NAMES`,
`MUTE`, `UNMUTE`, `BAN`, `UNBAN`. Server → client: `READY`, `BACKLOG`,
`MSG`, `PRIVMSG`, `JOIN`, `PART`, `NAMES`, `MOD`, `PONG`, `ERR`.

`MSG` names its channel; an empty channel means the configured default,
so a client that only ever uses one room does not have to name it.
Speaking into a room you are not in is refused with `ERR notjoined`
rather than silently joining you — a client that thinks it is somewhere
it is not should find out.

A connection always begins `READY`, then `BACKLOG` (unless the backlog is
switched off), then live traffic. `READY` carries the assigned nick and
exists because the WebSocket handshake completes before the server has
subscribed the connection — a client that talks before `READY` can miss
its own message. `BACKLOG` is one frame containing an array of the recent
history, not a burst of `MSG` frames, so a client can tell "here is what
you missed" from "this just happened" without a per-message flag.

**A client must ignore a live message whose id it already has from the
backlog.** The server subscribes a connection before it reads the history,
so a message sent in between arrives twice. Doing it the other way round
would lose that message instead, and a duplicate a client can drop by id
is strictly better than a gap it cannot detect. Ids are monotonic, so the
check is one comparison.

Every server-originated message carries `nick`, `timestamp` (unix millis)
and a server-assigned monotonic `id` — and, once channels exist,
`channel`. `MSG` and `PRIVMSG` also carry the sender's `roles` and
`attrs`, whatever the `Directory` hook attached to them, repeated on
every message so a client can render any message on its own with no state
and no ordering problem when somebody's roles change mid-conversation.
Both are omitted when empty, so a server with no directory pays nothing.

The four moderation commands all produce one `MOD` frame to the whole
channel, so a client needs one handler rather than four. A banned client
is disconnected and will usually *not* see the `MOD` frame — its write
pump is racing its own socket closing — which is why the close frame
carries a reason of its own.

The id, the timestamp and the nick are the server's to assign. A client
that puts a `nick` in its payload is ignored.

`ERR` payloads use a stable machine-readable code
(`{"description":"needlogin"}`-style), never a prose string clients have
to parse.

A frame arriving as the wrong WebSocket message type for the negotiated
codec is refused with `ERR framing` rather than guessed at, and a
well-formed document with no `verb` is refused with `ERR protocol`.

### Extension points

Everything that is not "move messages between sockets" lives behind an
interface in `internal/hook`, and is handed to the server at startup as a
`hook.Hooks`. The core imports `hook`, an implementation imports `hook`,
and the core never imports an implementation. The zero `Hooks` is a
working server: anonymous connections, everything allowed, nothing
persisted.

- **`Authenticator`** turns the HTTP upgrade into an `Identity`. Runs
  once, at connect time, before the upgrade, so a refusal is a real HTTP
  401 rather than a socket that opens and shuts. It sees a narrow
  `hook.Request`, not the `*http.Request`, so it cannot hijack the
  connection.
- **`Directory`** answers questions about people, two ways round.
  `Chatter` supplies display name, roles and attrs for an authenticated id,
  once per connection; a miss (`ErrNoChatter`) or a failure is not fatal,
  because a broken directory must not cost somebody their login.
  `Resolve` turns a display name into the identity behind it, which is what
  lets moderation name somebody who is **not connected** — otherwise a ban
  cannot be lifted, since the person it barred is by definition not here.
  A deployment that would rather not answer returns `ErrNoChatter` and
  moderation goes back to being limited to who is online.
- **`Filter`** decides whether a message may be sent. On the hot path, in
  front of every message, so it must be a lookup and not a round trip.
  Its refusal reason becomes the `ERR` code verbatim. It runs *last*, in a
  chain behind the built-in text filters in `internal/filter` — see below.

  It is also where **who may talk at all** lives, which is not the same
  question as who may connect. A server where anonymous visitors watch and
  only logged-in users speak is an `Authenticator` that hands back a zero
  `Identity` instead of `ErrUnauthorized`, and a filter that refuses
  `from.Anonymous()` with `needlogin`. Both hooks, because they refuse
  different things: one refuses the connection, the other refuses the
  message, and a read-only window is exactly the gap between them. There is
  nothing to switch on in the core — a lurker is a connection like any
  other, with `MSG` and `PRIVMSG` the only two verbs a filter sees.
  `TestUnregisteredMayWatchButNotTalk` in `features_test.go` is the whole
  arrangement in one place.
- **`Limiter`** supplies rate limits — `ClientLimits` per connection,
  `ChannelLimits` per channel. **Policy only**: the enforcing is the
  server's, on a token bucket in `internal/ratelimit`, so an
  implementation is asked once and never sees an individual message.
  Defaults are unlimited, and so is any `Limits` with a non-positive
  field — "no limit" and "a limit of nothing" must not be one typo apart.

  `Limits.Key` decides the **scope**. Empty (the default) is one bucket per
  connection. Non-empty shares one bucket between every connection
  reporting the same key, which is how an account is limited as a whole
  rather than per socket. What the key means is the hook's business — an
  account id, a payment tier, an organisation out of `Attrs` — since only
  it knows what the auth layer attached. The core compares keys for
  equality and nothing else.
- **`Channels`** decides where a connection starts and where it may go.
  `Autojoin` runs once per connection; returning nil means the configured
  default, and an empty non-nil slice means it starts nowhere, which is a
  deployment where clients `JOIN` explicitly. `CanJoin` gates a `JOIN` a
  client asked for and its refusal reason becomes the `ERR` code.
  **Autojoin does not consult `CanJoin`**: a layer that put somebody
  somewhere has already decided they may be there.
- **`History`** is the replay window a connecting client is shown.
  Separate from `Recorder` on purpose: `Recorder` is durability and runs
  behind delivery on a worker, `History` is read back on every connect and
  has to be fast. `Append` runs under the lock that orders the fan-out and
  **must not block**; `Recent` runs once per connection and may. A
  `Recent` that fails is logged and the client gets an empty window —
  failing to show history is not a reason to refuse somebody a connection.
  The default is `history.Memory`, the last `Backlog` messages per channel
  in memory, which is what the server did before the hook existed.
- **`Recorder`** writes the chat down: public and private messages. It runs
  on a background worker **after** the thing has happened, off a bounded
  queue, and drops with a counter when that queue is full. A store having a
  bad day costs history, never delivery.
- **`Sanctions`** persists mutes and bans **and hands them back**, which is
  the difference between moderation that survives a restart and moderation
  that does not. `Record` is on the same background queue as the chat
  records — a store must not stall the moderator issuing a command — and
  `Active` runs **once at startup**. Moderation is not in `Recorder`
  because a mute is not a log line, it is state the server has to have back.

  **A failing `Active` stops the server.** Starting without knowing who is
  banned means letting them in, and a server that cannot answer that
  question should not be answering connections. It is the one hook failure
  that is fatal, and the only one where failing open would be a security
  decision rather than a degraded feature.

  `Record` is called for `unmute` and `unban` too. An implementation has to
  remove what they lift, or `Active` hands back something that was
  cancelled — there is a test for exactly that.
- **`Authorizer`** decides who may use the moderation commands, **and
  where**: it is passed the scope, empty for server-wide. **Its default is
  deny**, unlike every other hook. The rest default permissive
  because the cost of being wrong is a server that does less than somebody
  wanted; here it is anybody in the room being able to ban anybody else. A
  server with no `Authorizer` has no moderators, which is correct for a
  server that has not been told who they are. Roles are opaque to the
  core, so this is the only thing that can say whether `"moderator"` in an
  `Identity` means anything.

`hooks.go` is the only place any of them is called, so the rules about
which may block live in one file.

### Message filters

`internal/filter` holds the filters the server runs in front of every
message and the `Chain` that runs them. A filter is a `hook.Filter`, so
a built-in and a deployment's own are the same kind of thing; what makes
these built in is that the server installs them by default, because they
are protocol hygiene rather than policy.

The chain is text validity first, then policy — a message that is not
valid text must never reach a filter written assuming it was:

1. **`UTF8`** refuses invalid UTF-8. This is *not* redundant with the
   WebSocket layer, which is the tempting assumption. A JSON client sends
   TEXT frames, which coder/websocket validates, and `encoding/json` would
   anyway replace bad bytes with U+FFFD. But a **MessagePack client sends
   BINARY frames**, nothing validates them, and a msgpack string is just
   bytes — so invalid UTF-8 arrives intact and would be fanned out to
   every other client, including the JSON ones, whose frames would then be
   invalid on the wire.
2. **`Zalgo`** refuses more than `MaxDiacritics` (default 5) *consecutive*
   combining marks. Consecutive, because that is what stacking is —
   counting marks across the whole message would refuse a long sentence in
   any script that uses them. Five is well clear of Devanagari, Thai,
   Hebrew with niqqud and Vietnamese, which stack marks legitimately.
3. Then the `Filter` hook, if one is installed.

An empty chain is nil, so a server with everything switched off skips the
call rather than looping over nothing.

### Moderation

`internal/moderation` is a `Store` of who is muted and who is banned
**and where**, with lazy expiry. It is state only: it does not decide who
may moderate (that is `Authorizer`), does not persist (that is
`Recorder`), and does not know what a connection is.

**The command's channel is the scope.** Naming one acts there and nowhere
else; leaving it empty acts server-wide. It falls out of a field the
protocol already had:

```json
{"verb":"MUTE","nick":"someone","channel":"main"}   silenced in main
{"verb":"MUTE","nick":"someone"}                    silenced everywhere
```

A lookup checks the channel and then the global entry, so a server-wide
mute applies in every channel without being written into each, and
lifting a channel mute does not lift a global one — they were separate
decisions.

What each scope means where:

- **Channel mute**: refused in that channel only. It does **not** block
  private messages. Being silenced in one room is not a statement that
  somebody may not talk to anybody at all, and treating it as one makes a
  channel mute a bigger punishment than the moderator asked for.
- **Server-wide mute**: refused everywhere, private messages included.
- **Channel ban**: parted from that channel and refused on rejoin —
  including on autojoin, since a layer putting somebody somewhere does not
  overrule a moderator who threw them out. The connection is untouched.
- **Server-wide ban**: checked **before the upgrade**, so it is an HTTP
  403 rather than a socket that opens and shuts.

`Authorizer.CanModerate` is asked **per scope**, so running one room is
not the same permission as acting across the server.

**A command can name somebody who is not connected.** The connection
directory answers for anybody here — it has the identity the connection is
running as, and the connection itself for anything to be enforced on — and
`Directory.Resolve` answers for everybody else. An action is filed against
the resolved `Identity.Key()`, so it still applies when they come back. An
error from `Resolve` is a miss rather than a guess: a broken directory must
not turn into a moderator quietly acting on the wrong person.

A channel ban removes the target **before** announcing. Announcing first
would put the frame in the ring while the target's subscription is being
closed — which discards it — so it would have to be sent directly too, and
would then arrive twice for anybody whose pump had already drained it.
Removing first means one copy of everything: the room sees `PART` then
`MOD`, and so does the person it happened to.

State is filed under `Identity.Key()`: the stable id when there is one,
the display name when there is not. The anonymous case is weak on purpose
rather than by accident — a reconnect under a new name escapes a mute, and
there is nothing better available, since an address is shared by everyone
behind a NAT. Moderation of anonymous users is a speed bump; that is an
argument for requiring a login to speak, which is a deployment's decision
to make through `Authenticator`.

### Database *(not built yet)*

SQLite via `modernc.org/sqlite` (pure Go, no cgo), files under `data/`.
Two pools: `db.Write` (max 1 conn — SQLite is a single-writer) and
`db.Read` (max 100). Use `db.WriteInTransaction` for multi-statement
writes (BEGIN IMMEDIATE, auto-rollback on error/panic). WAL mode, busy
timeout set on every connection.

Queries are **hand-written Go** in `internal/model/`, one file per
domain (`user.go`, `channel.go`, `message.go`, `ban.go`): the SQL as a
`const`, and a `*Queries` method that runs it via generic scan helpers
in `scan.go` (`one[T]`/`many[T]`/`exec`/`execRows`). No code generation.
Column conventions: `uuid`/`*_uuid` → `crypto.ID` (UUIDv7 stored as
BLOB), `*_at`/`*_until` → `*dbtime.Time`, enum types defined by hand in
`internal/model/*.go`. Query methods return the bare `sql.ErrNoRows`
sentinel — callers compare with `==`, never wrap it.

Migrations: plain SQL in `data/migration/` (rubenv/sql-migrate format,
`-- +migrate Up`), run automatically at startup. Tables are `STRICT`.

**The write path is not on the hot path.** Persisting a message must not
sit between "message received" and "message fanned out". Fan out first,
persist asynchronously from a buffered writer goroutine; a database
stall must degrade scrollback, not delivery.

### Other load-bearing pieces

- **Config**: HuJSON (`tailscale/hujson`) in `internal/config`.
  `config_default.hujson` is committed and fully commented-out; real
  values go in an uncommitted `config.hujson` next to the binary. A
  config test loads the committed file and compares it against the
  defaults in code, so the two cannot drift.
- **Auth**: see the `Authenticator` hook above. Resolved once, at connect
  time; never re-checked per frame.
- **Presence**: `NAMES` is served from the channel's in-memory member
  set, never from the database.
- **Scrollback**: the `History` hook, replayed as one `BACKLOG` frame on
  join. The default `history.Memory` keeps the last `Backlog` messages per
  channel in memory, which means scrollback dies with the process;
  anything longer-lived is a `History` implementation, and the SQLite one
  is *(not built yet)*.
- **Rate limiting** (`internal/ratelimit`): two token buckets, both
  unlimited by default and both configured by the `Limiter` hook. The
  client's bucket is per **connection** — an anonymous connection has no
  stable id to key on, so two sockets get two buckets; tightening that
  needs logins. The channel's bucket is shared by every member, which is
  what stops a room being unreadable during a raid however many people
  are doing it.

  A nil `*Bucket` allows everything, so the unlimited default costs a nil
  comparison on the hot path rather than a mutex and a clock read.

  A keyed bucket is refcounted and **outlives the connections holding
  it**. Dropping it when the last one leaves would hand a full bucket back
  to anyone who reconnected, which is the first thing somebody being
  throttled would try. The janitor reclaims a bucket only once nobody
  holds it *and* it has refilled — at which point it is indistinguishable
  from one that never existed, so forgetting it loses nothing. The same
  janitor sweeps expired mutes and bans.

  The client's limit is checked first, so somebody over their own budget
  is told it is their fault (`ERR throttled`) rather than the room's
  (`ERR channelthrottled`) — two codes, because only one of them is
  something a client can do anything about. Both are checked before the
  `Filter`, and both are spent whether or not the message survives what
  comes after: a rate limit that only counts *successful* messages makes
  sending garbage free. Private messages spend the client's budget but
  not the channel's, since they are not in the channel.
- **Moderation**: mutes and bans are `internal/moderation` state, scoped
  by channel or server-wide, with lazy expiry. Surviving a restart is the
  `Sanctions` hook's job (`Record` + `Active`), not the core's — nothing
  here writes to a database. A server-wide ban is checked at upgrade time,
  before the WebSocket handshake completes, so it is an HTTP 403 rather
  than a socket that opens and shuts.
- **Shutdown** (`shutdown.go`): on SIGTERM stop accepting upgrades, send
  a close frame to every connection, then unblock both pumps and drain
  the persistence queue. `net/http`'s `Shutdown` deliberately ignores
  hijacked connections and every WebSocket is one, so all of this is the
  server's own doing. **Order matters**: cancelling a read context drops
  the socket without a close frame, because a cancelled read leaves the
  stream at an unknown offset, so the goodbye has to go out first.

### Profiling and metrics

Both live on their own listener (`debug.go`), configured by `DebugAddr`
and **bound to the loopback by default**. Empty disables it and binds
nothing.

`pprof` is mounted deliberately rather than by the import side effect that
puts it on `DefaultServeMux`. It has no authentication and is not going to
grow any: `/debug/pprof/heap` hands out a dump of everything in memory,
and anyone who can reach `/debug/pprof/profile` can pin a core for thirty
seconds as often as they like. Move the bind address only to an interface
that only a scraper can reach.

**Every goroutine is labelled** with `pprof.Do`, so a profile can be asked
what the time went to rather than only which function it was in. The
vocabulary is in `debug.go`: one key, `task`, and a closed set of values —
`listener`, `debug-listener`, `conn`, `priv-pump`, `channel-pump`,
`janitor`, `record-worker`. Connection goroutines carry a second label,
`codec`, because that is the one thing that varies between two connections
doing identical work. Filter with `-tagfocus=task=channel-pump`.

The nesting does most of the work: labels are inherited, so net/http's
handler goroutines come out of the listener already labelled and a
connection's pumps come out of the connection, each relabelling itself over
the top. Nothing here is on the message path — a `pprof.Do` is once per
connection and once per join — so the label set is built where it costs
nothing. Values must stay a closed set for the same reason metric labels
must: the profiler keeps a map of every label set it has seen.
`TestGoroutinesAreLabelled` reads the labels back out of a real goroutine
profile, since a label nobody can see is not worth the line.

`internal/loadgen` labels its own goroutines the same way — `barrier`,
`dialer`, `client`, `reader`, `progress` — which is how "some of the
latency is the measuring" stops being a guess. The binary has no profiling
endpoint of its own yet, so today they are read by profiling the package
under test.

`/metrics` is the Prometheus text format, written by `internal/metrics` —
a hand-rolled registry rather than the client library, because what the
server needs is a few atomic counters and a way to print them. The
exposition format is the part that matters, and it is a dozen lines to
emit.

Metrics are **deliberately not a hook**. The hooks are for policy the
server should have no opinion about; how many connections it is holding is
the server describing itself, and anything that wants those numbers
elsewhere can scrape them.

Two conventions worth keeping:

- **Anything already counted elsewhere is a `GaugeFunc` reading the real
  thing**, never a mirror maintained alongside it. Connections are the
  length of the directory, drops are what the broadcasters already count.
  A second copy is a second thing that can be wrong.
- **Refusals are counted where they are sent** (`conn.reply`), so
  `refusals_total{code=...}` cannot drift from what clients were actually
  told.

`CounterVec` labels must come from a closed set — ERR codes, verbs, codec
names. Never a nick or anything else a client controls: that turns a
metric into a memory leak with a network interface.

### Load generation

`cmd/loadgen` is a **separate process** that points a crowd at a running
server, so the numbers are the ones a client sees rather than counters
read out of the thing being measured. The why — and the four things it
has to get right or the numbers lie — is in
[`internal/loadgen/CLAUDE.md`](internal/loadgen/CLAUDE.md).

### Makefile

All tooling lives in the `Makefile` (`make help` lists targets). It
exports `GOEXPERIMENT=jsonv2`, which the module does not compile without —
`go vet`, staticcheck and golangci-lint included, since they are compilers
with opinions. Use `make`, or set it yourself:

```
GOEXPERIMENT=jsonv2 go test ./...
```


- `make init` — what a fresh clone runs once: installs the linters and
  the git pre-commit hook. `make lint` refuses to run without them and
  says so rather than skipping them quietly.
- `make soak` — the long-running connection soak tests, gated behind the
  `soak` build tag so they stay out of `make test`.
- `make vulncheck` — `govulncheck ./...` (kept out of pre-commit: it
  needs the network vuln db).
- `make update-deps` — `go get -u`, `go get toolchain@latest`, tidy,
  vendor, then test + vulncheck. The toolchain is updated with everything
  else because govulncheck reports standard library CVEs against the pin
  in `go.mod`, and Go's patch releases are mostly security fixes. It is
  safe there and nowhere else: the test suite and the scan run
  immediately after it.

### Continuous integration

`.woodpecker.yaml` runs on every push and pull request: `make test-race`,
`make lint`, then the production build, with `make vulncheck` on a cron.
Every step shells out to the Makefile rather than to `go`, so the
`GOEXPERIMENT=jsonv2` the module needs is stated in exactly one place.

CI does **not** run `make vendor`, the one thing `make pre-commit` does
that it skips: a CI run checks the tree it was handed instead of
rewriting it. A stale `vendor/` fails the build a step later anyway.

Every cache Go has lives on the agent's persistent `/cache` volume —
`GOCACHE`, `GOPATH` (the module cache *and* the tool binaries) and
`GOLANGCI_LINT_CACHE` — the same convention as the sibling
`kikapcsologo/backend` pipeline on the same agent. Without it every run
recompiles every vendored dependency and rebuilds staticcheck and
golangci-lint from source, which over there was half the pipeline. The
lint step therefore installs the linters only when they are missing
rather than calling `make tools` unconditionally.

Detail, the step table and the known rough edges are in
[`state/ci.md`](state/ci.md).

Tests are integration-first: a real `httptest.Server` and a real
WebSocket client dialing it, over real SQLite, via a `newTestApp`
harness (`app_test.go`). Unit tests only where the wire surface cannot
reach. A test that asserts on delivery must assert on what the *client*
received, not on hub internals. Concurrency bugs here are the whole
game — anything touching the hub or fan-out ships with a `-race` test
that runs multiple connections at once.

## Working process

When writing code, use the persona of Linus Torvalds, and avoid needless complexity.

### Work on main, and push it

**Never create a branch.** All work happens on `main`, committed directly
to it. No feature branches, no worktrees, no pull requests.

**Commit and push without being asked.** Every logical step is committed as
it is finished, in the style below, and pushed. Do not ask for permission
to commit, do not ask for permission to push, and do not leave finished
work sitting uncommitted waiting for a prompt. The commit is part of doing
the work, not a separate thing to check in about.

This is a single-maintainer repository and history is linear. What keeps it
safe is not a branch, it is the rule below that every commit compiles and
passes `make test` on its own — so anything that turns out to be wrong can
be found by bisect and reverted as one commit.

A feature is never one big commit. Break its development into logical steps,
and **every logical step gets its own git commit in the documented style
below** — not just the final milestone. A logical step is any self-contained,
bisectable unit that compiles and passes tests (a schema change, a new
helper, the binding that uses it, the test that covers it). Commit as you go;
do not batch several steps into a single squashed commit at the end.

Follow this sequence for every implementation milestone, and for every logical
step within it:

1. **Search before writing.** Use semble to find existing patterns, helpers, and conventions in the codebase before introducing new code.
2. **Implement the milestone.** Keep each commit logically complete — tests travel with the code they test, not in a separate commit.
3. **Update the working state.** Record the detail (done/pending, decisions, commit hashes) in the track's `state/<track>.md`, and refresh the single `## Latest` pointer in `AI.state`. See "AI working state" below.
4. **Commit.** Follow the git style below.

### Git commit style

Model after Linus Torvalds' kernel commits: each commit must be a self-contained, bisectable unit of work that compiles and passes `go test ./...`. Never bundle unrelated changes. Never leave a "fix typo in previous commit" in the history — amend or rebase before the work is considered done.

**Subject line** — imperative mood, ≤50 characters:
```
hub: drop clients that outrun their write pump
```

Use a `subsystem:` prefix that matches the primary package changed (`hub`,
`channel`, `proto`, `model`, `config`, etc.). For cross-cutting changes use
the broadest relevant prefix or omit it.

**Body** — wrap at 72 characters, explain *why* not *what*:
```
hub: drop clients that outrun their write pump

Broadcast used a blocking send into each member's outbound channel, so
one stalled TCP connection stopped fan-out for the whole channel. The
send is now non-blocking: a full buffer closes that connection with a
1008 policy-violation close frame and unregisters it.

256 frames of buffer is roughly two seconds of a busy channel, which is
long enough that a client hitting the limit is genuinely gone rather
than briefly descheduled.
```

**Rules:**
- Every logical step in a feature's development is its own commit, written in
  the style above. Do not defer committing until the feature is "done."
- Every commit must compile and pass all existing tests.
- Tests go in the same commit as the code they test.
- Refactors that are not part of a feature get their own commit.
- Rebase to fix up mistakes; never push a "fix previous commit" to main.
- Use `git rebase -i` to squash or reorder before a milestone is declared done.

---

## AI working state

Claude tracks work state in a **two-level** layout so a session reads only what
it needs, never one giant file of mostly-irrelevant history:

- **`AI.state`** holds ONLY globally-useful data: the single most-recent/
  in-progress thing (a one-paragraph "Latest" pointer to its full state file),
  an index of the per-spec state files, the release log, and the cross-cutting
  "Key decisions / Removed / User preferences" lists. It stays small.
- **`state/<track>.md`** holds the detailed, spec-scoped working state —
  one file per spec (e.g. `state/channels.md` for `channels-spec.md`). Each
  spec links to its own state file from a `> **Working state:** …` line in
  the spec's header.

**At the start of every session:** read `AI.state` first. Then, only if you are
touching a specific track, read that track's `state/<track>.md`. Do not read the
other tracks' state files.

**When updating after a completed milestone / release:**
1. Put the detail in the relevant `state/<track>.md` (decisions, commit hashes,
   gotchas, what's done/pending for that track).
2. In `AI.state`, keep the **`## Latest`** section to a SINGLE thing — replace
   it with whatever you just worked on, plus the pointer to its state file. Only
   ever one "latest" entry; older context lives in the state files, not here.
3. Add the version to the `AI.state` release log, and add any new cross-cutting
   decision to the "Key decisions" list. Update the state-file index if you
   created a new track.

When a brand-new spec's work begins, create its `state/<track>.md`, add the
`> **Working state:**` link to the spec header, and add the file to the AI.state
index.
