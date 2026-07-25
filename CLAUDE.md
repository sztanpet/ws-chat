# ws-chat

Go backend for a WebSocket chat service with channel support: users
connect over a single WebSocket, join one or more channels, and exchange
messages, presence and moderation events. The server owns authentication,
channel membership, message fan-out, scrollback and moderation state.
Clients (web, bots) speak the line protocol described below.

> **Status: greenfield.** Nothing but tooling exists in this repo yet.
> Everything under "Architecture" is the intended design, not a
> description of code that is already there — it is what the first
> milestones should build toward. When reality and this file disagree,
> reality wins and this file gets updated in the same commit.

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
route. Everything after the upgrade follows one rule: **exactly two
goroutines per connection, and only the writer touches the socket.**

- The **read pump** loops on `conn.Read`, parses one frame, and hands
  the command to the hub or the channel. It never writes to the socket.
- The **write pump** owns `conn.Write` and drains a *bounded* outbound
  `chan []byte` (~256 frames). Nothing else in the process is allowed to
  write to a connection.
- A full outbound channel means the client is slower than the fan-out.
  **Drop the connection, never block the sender.** Backpressure that
  propagates into the hub is how a chat server dies: one dead TCP
  connection stalls every other member of the channel.

Fan-out is per-channel, not global. The hub holds
`map[channelName]*channel` behind an `RWMutex` and is only consulted on
join/part/lookup. Each `*channel` owns its own member set and does its
own fan-out: a broadcast iterates that channel's members and does a
non-blocking send into each member's outbound channel. So a busy channel
never serializes behind a quiet one, and the hot path takes no global
lock.

A user has **one connection and many channel memberships**, not one
connection per channel. `PRIVMSG` and presence therefore need a
`map[userID]*conn` lookup that lives on the hub, alongside the channel
map.

### Wire protocol

Text frames, one command per frame, in the form:

```
VERB {"json":"payload"}
```

Verb is uppercase, followed by a single space, followed by a JSON
object. No trailing newline, no batching — one frame is one command. A
frame with no payload is just the verb (`PING`). Unknown verbs get an
`ERR` back and do not close the connection.

Client → server: `MSG`, `PRIVMSG`, `JOIN`, `PART`, `NAMES`, `PING`, and
the moderation verbs `MUTE`, `UNMUTE`, `BAN`, `UNBAN`.

Server → client: `MSG`, `PRIVMSG`, `JOIN`, `QUIT`, `NAMES`, `PONG`,
`ERR`, `BROADCAST`, plus the moderation echoes. Every server-originated
message carries `channel`, `nick`, `timestamp` (unix millis) and a
server-assigned monotonic `id` for the channel.

`ERR` payloads use a stable machine-readable code
(`{"description":"needlogin"}`-style), never a prose string clients have
to parse.

### Database

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

- **Config**: HuJSON (`tailscale/hujson`). `config_default.hujson` is
  committed and fully commented-out; real values go in an uncommitted
  `config.hujson` next to the binary.
- **Auth**: the client presents a session token on the upgrade request
  (cookie or `Authorization`), resolved to a user once, at connect time.
  Never re-check auth per frame. Anonymous connections may read but not
  `MSG` — the check is a field on the connection, not a database hit.
- **Presence**: `NAMES` is served from the channel's in-memory member
  set, never from the database.
- **Scrollback**: the last N messages per channel are kept in a ring
  buffer on the channel and replayed on `JOIN`; the SQLite table is for
  history beyond that.
- **Rate limiting**: per-connection token bucket, checked in the read
  pump before the command reaches the hub. Exceeding it gets an `ERR`,
  and repeated abuse closes the connection.
- **Moderation**: mutes and bans are in-memory state on the channel with
  a SQLite row behind them, so a restart does not clear them. An IP ban
  is checked at upgrade time, before the WebSocket handshake completes.
- **Shutdown**: `signal.go`/`shutdown.go` — on SIGTERM stop accepting
  upgrades, send a close frame to every connection, drain the message
  writer, then exit. Systemd socket activation via `coreos/go-systemd`
  if the deployment ever needs zero-downtime restarts.

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
