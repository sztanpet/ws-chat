# ws-chat

Go backend for a WebSocket chat service with channel support: users
connect over a single WebSocket, join one or more channels, and exchange
messages, presence and moderation events. The server owns authentication,
channel membership, message fan-out, scrollback and moderation state.
Clients (web, bots) speak the line protocol described below.

> **Status: walking skeleton.** The fan-out, the config loader, the frame
> codec, the extension points and a single-channel server exist and are
> tested. Channels themselves, persistence and scrollback do not. Sections
> below marked *(not built yet)* are intent; everything else describes code
> that is there. When reality and this file disagree, reality wins and this
> file gets updated in the same commit.

## Architecture

Single Go module `github.com/sztanpet/ws-chat` (Go 1.26, toolchain pinned
to 1.26.4; dependencies vendored in `vendor/`). The server is `package
main` at the repo root; helper packages live under `internal/`; auxiliary
binaries under `cmd/`.

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
blocked on, and every socket write is bounded by `WriteTimeout`. A
private message arrives on *somebody else's* read pump, so this is not
theoretical: without it, one person who stopped reading would stall
everyone who messages them.

Fan-out is `internal/broadcast`. `Ring` is the implementation to use: one
shared buffer, each subscriber reading at its own position, so a
broadcast is O(1) in the member count and never walks the subscribers.
See `state/broadcast.md` for the measurements behind that choice.

There is currently **one broadcaster**, a single implicit channel every
connection joins. *(Not built yet)* Channels replace it with a lookup
from channel name to its own `Ring`; nothing in the connection handling
should need to change.

A user has **one connection and many channel memberships**, not one
connection per channel. The `map[nick]*conn` directory in `init.go` is
what `PRIVMSG` is addressed through, and it is separate from any
channel's member set for that reason.

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

The consequence to keep in mind: **a ring holds encoded bytes**, so there
is one broadcaster per codec and a message is encoded once per codec
rather than once per subscriber — O(codecs), not O(members). A message id
is assigned and written to every ring under one small lock, so clients on
different codecs cannot disagree about what happened first. Private
messages are encoded with the **recipient's** codec, since the two ends
negotiate separately.

Implemented today — client → server: `MSG`, `PRIVMSG`, `PING`, `MUTE`,
`UNMUTE`, `BAN`, `UNBAN`. Server → client: `READY`, `BACKLOG`, `MSG`,
`PRIVMSG`, `MOD`, `PONG`, `ERR`.

*(Not built yet)*: `JOIN`, `PART`, `NAMES`.

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
- **`Directory`** supplies chatter data (display name, roles, attrs) for
  an authenticated id. Also once per connection. A miss (`ErrNoChatter`)
  or a failure is not fatal — a broken directory must not cost somebody
  their login.
- **`Filter`** decides whether a message may be sent. On the hot path, in
  front of every message, so it must be a lookup and not a round trip.
  Its refusal reason becomes the `ERR` code verbatim. It runs *last*, in a
  chain behind the built-in text filters in `internal/filter` — see below.
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
- **`History`** is the replay window a connecting client is shown.
  Separate from `Recorder` on purpose: `Recorder` is durability and runs
  behind delivery on a worker, `History` is read back on every connect and
  has to be fast. `Append` runs under the lock that orders the fan-out and
  **must not block**; `Recent` runs once per connection and may. A
  `Recent` that fails is logged and the client gets an empty window —
  failing to show history is not a reason to refuse somebody a connection.
  The default is `history.Memory`, the last `Backlog` messages per channel
  in memory, which is what the server did before the hook existed.
- **`Recorder`** writes things down: public messages, private messages and
  moderation actions. It runs on a background worker **after** the thing
  has happened, off a bounded queue, and drops with a counter when that
  queue is full. A store having a bad day costs history, never delivery.
- **`Authorizer`** decides who may use the moderation commands. **Its
  default is deny**, unlike every other hook. The rest default permissive
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

`internal/moderation` is a `Store` of who is muted and who is banned, with
lazy expiry. It is state only: it does not decide who may moderate (that
is `Authorizer`), does not persist (that is `Recorder`), and does not know
what a connection is.

Mutes are checked on both message paths — a mute is a mute, and somebody
silenced in the room does not get to carry on in private. Bans are checked
**before the upgrade**, so a banned client gets an HTTP 403 rather than a
socket that opens and shuts.

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
- **Scrollback**: the last N messages per channel are kept in a ring
  buffer on the channel and replayed on `JOIN`; the SQLite table is for
  history beyond that.
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
- **Moderation**: mutes and bans are in-memory state on the channel with
  a SQLite row behind them, so a restart does not clear them. An IP ban
  is checked at upgrade time, before the WebSocket handshake completes.
- **Shutdown** (`shutdown.go`): on SIGTERM stop accepting upgrades, send
  a close frame to every connection, then unblock both pumps and drain
  the persistence queue. `net/http`'s `Shutdown` deliberately ignores
  hijacked connections and every WebSocket is one, so all of this is the
  server's own doing. **Order matters**: cancelling a read context drops
  the socket without a close frame, because a cancelled read leaves the
  stream at an unknown offset, so the goodbye has to go out first.

### Makefile

All tooling lives in the `Makefile` (`make help` lists targets):

- `make run` — `go build -race` + run.
- `make build` — full production build (`make generate`, then
  `go build -ldflags="-buildid=" -trimpath -race`).
- `make test` — `go test ./...`.
- `make test-race` — the same with the race detector; this is the one
  that matters for a fan-out server, so it is in `pre-commit`.
- `make soak` — the long-running connection soak tests, gated behind the
  `soak` build tag so they stay out of `make test`.
- `make lint` / `make vendor` / `make pre-commit` — what the git
  pre-commit hook runs; `make hook` installs the hook.
- `make tools` — install staticcheck, golangci-lint, govulncheck.
- `make vulncheck` — `govulncheck ./...` (kept out of pre-commit: it
  needs the network vuln db).
- `make update-deps` — `go get -u`, tidy, vendor, then test + vulncheck.

Tests are integration-first: a real `httptest.Server` and a real
WebSocket client dialing it, over real SQLite, via a `newTestApp`
harness (`app_test.go`). Unit tests only where the wire surface cannot
reach. A test that asserts on delivery must assert on what the *client*
received, not on hub internals. Concurrency bugs here are the whole
game — anything touching the hub or fan-out ships with a `-race` test
that runs multiple connections at once.

## Working process

When writing code, use the persona of Linus Torvalds, and avoid needless complexity.

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

## Go package documentation (pkg.go.dev API)

Use the pkg.go.dev REST API to look up package docs, available versions, symbols, and vulnerabilities without leaving the terminal. The API is at `https://pkg.go.dev/v1beta/`.

```bash
# Package metadata (synopsis, version, redistributable, …)
curl -s "https://pkg.go.dev/v1beta/package/github.com/coder/websocket" | jq .

# Specific version
curl -s "https://pkg.go.dev/v1beta/package/modernc.org/sqlite?version=v1.29.0" | jq .

# All exported symbols (types, funcs, consts, vars)
curl -s "https://pkg.go.dev/v1beta/symbols/github.com/coder/websocket" | jq .

# Available versions for a module
curl -s "https://pkg.go.dev/v1beta/versions/github.com/coder/websocket" | jq .

# All packages inside a module
curl -s "https://pkg.go.dev/v1beta/packages/golang.org/x/pkgsite" | jq .

# Search
curl -s "https://pkg.go.dev/v1beta/search?q=websocket" | jq .

# Known vulnerabilities for a module
curl -s "https://pkg.go.dev/v1beta/vulns/github.com/coder/websocket" | jq .
```

Full OpenAPI spec: `https://pkg.go.dev/v1beta/openapi.yaml`

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
