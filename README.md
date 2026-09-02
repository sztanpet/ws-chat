# ws-chat

Go backend for a WebSocket chat service with channels: clients connect
over a single WebSocket, join one or more channels, and exchange
messages, presence and moderation events. The server owns nick
assignment, channel membership, message fan-out, the replay window and
moderation state.

Everything that is not "move messages between sockets" — authentication,
who may moderate, what is persisted, rate limit policy — is an interface
in `internal/hook`. The zero value of `hook.Hooks` is a working server:
anonymous connections, everything allowed, nothing persisted.

> **Status: walking skeleton.** Fan-out, channels, private messages,
> moderation, rate limiting, the two wire codecs, the hook surface,
> metrics and the load generator are built and tested. Persistence is
> not: the replay window is the in-memory `history.Memory` and there is
> no database yet. `CLAUDE.md` marks what is intent rather than code.

Architecture and the reasoning behind it are in [`CLAUDE.md`](CLAUDE.md);
this file is the how-to for building, running and hacking on it.

## Prerequisites

- **Go 1.27+** (the module pins toolchain `go1.27.0`; the `go` command
  fetches it automatically). Not optional: the JSON codec is
  `encoding/json/v2`, which shipped in 1.27, and the module's `go`
  directive is what unlocks it. On anything older the build fails with
  "json.Marshal requires go1.27 or later".

Dependencies are vendored, so builds never touch the network.

## First-time setup

```sh
make init   # linters, govulncheck, and the git pre-commit hook
make run    # go build -race, then serve on :8080
```

There is nothing else to bootstrap: no database, no assets, no config.
`config.hujson` is optional — a missing file is not an error, and the
defaults are a working server. To change something, copy the committed,
fully commented-out `config_default.hujson` to `config.hujson` next to
the binary and uncomment what you need. A config test compares that file
against the defaults in code, so the two cannot drift.

## Talking to it

| route | what |
|---|---|
| `GET /ws` | the WebSocket endpoint |
| `GET /health` | liveness |
| `GET /metrics`, `/debug/pprof/…` | on the debug listener, not this one |

The format is negotiated with a WebSocket subprotocol: `chat.msgpack`
(offered first, ~20% smaller frames) or `chat.json`. A client that
negotiates nothing gets JSON, on the assumption that it is being written
by hand against a console.

One command per frame, and the frame is a single encoded document with
the verb inside it:

```json
{"verb":"MSG","data":"hello"}
```

A connection always begins with `READY` (which carries the assigned
nick), then `BACKLOG` (one frame holding an array of recent messages),
then live traffic. **A client must ignore a live message whose id it
already has from the backlog** — the server subscribes a connection
before it reads the history, so a message sent in between arrives twice.

Client → server: `MSG`, `PRIVMSG`, `PING`, `JOIN`, `PART`, `NAMES`,
`MUTE`, `UNMUTE`, `BAN`, `UNBAN`.
Server → client: `READY`, `BACKLOG`, `MSG`, `PRIVMSG`, `JOIN`, `PART`,
`NAMES`, `MOD`, `PONG`, `ERR`.

`ERR` payloads carry a stable machine-readable code
(`{"description":"notjoined"}`), never prose. An unknown verb is refused
rather than fatal. The full protocol, including what is guaranteed about
ordering, is in [`CLAUDE.md`](CLAUDE.md#wire-protocol).

## Configuration

`config.hujson` is [HuJSON] — JSON with comments and trailing commas.
The keys that matter, with their defaults:

| key | default | what |
|---|---|---|
| `Addr` | `:8080` | listen address |
| `Capacity` | `16384` | fan-out ring slots per channel per codec, shared by that channel's subscribers |
| `WriteBatch` | `16` | frames a write pump drains per wakeup |
| `MaxFrameSize` | `32768` | largest inbound frame |
| `MaxMessage` | `512` | largest message body |
| `DefaultChannel` | `main` | what an empty channel name means |
| `MaxChannels` | `1024` | channels the server will create |
| `MaxChannelsPerConn` | `32` | channels one connection may join |
| `Backlog` | `50` | messages replayed per channel on join; 0 disables |
| `MaxDiacritics` | `5` | consecutive combining marks allowed |
| `PrivBuffer` | `32` | queued private messages before `ERR recipientbusy` |
| `WriteTimeout` | `10s` | bounds one write pump wakeup |
| `IdleTimeout` | `90s` | drops a connection that has not even pinged |
| `DebugAddr` | `127.0.0.1:6060` | pprof + `/metrics`; empty binds nothing |
| `LogLevel` | `info` | `debug`, `info`, `warn`, `error` |

`Capacity` is sized for the lag *spread*, not the average, and is best read
as a time window: slots over the channel's message rate. The window grows
with the room, because a big room is bound by delivery bandwidth and cannot
carry many messages a second in the first place — so a room that sheds
clients at that size needs a channel rate limit, not a bigger buffer.

Storage is shared by a channel's subscribers, but it is per channel and per
codec, and the slots are allocated when the channel is created: 8 bytes a
slot, so 16384 is 256KB for a channel carrying both codecs, and `MaxChannels`
bounds the worst case at 256MB. See
[`state/broadcast.md`](state/broadcast.md) for the measurements.

Rate limits are **not** config: they come from the `Limiter` hook, and
the default is unlimited.

## Extending it

`internal/hook` is the whole extension surface. Implement what you need,
hand it to the server at startup, and leave the rest zero:

| hook | decides | default |
|---|---|---|
| `Authenticator` | who a connection is, before the upgrade | anonymous |
| `Directory` | display name, roles and attrs; name → identity | none |
| `Filter` | whether a message may be sent | allow |
| `Limiter` | rate limit policy (the server enforces it) | unlimited |
| `Channels` | where a connection starts and may go | the default channel |
| `History` | the replay window | `history.Memory` |
| `Recorder` | writing the chat down, off the hot path | nothing |
| `Sanctions` | mutes and bans that survive a restart | nothing |
| `Authorizer` | who may moderate, and where | **deny** |

`Authorizer` is the one that defaults to deny: a server that has not been
told who its moderators are has none, which is the only safe reading.
`hooks.go` is the only place any hook is called, so the rules about which
may block are in one file.

### Letting people watch without letting them talk

A common arrangement — anybody may read the chat, only logged-in users may
post — is two small hooks and no server configuration. The `Authenticator`
lets the unrecognised connection in as nobody:

```go
func (a myAuth) Authenticate(ctx context.Context, r hook.Request) (hook.Identity, error) {
	id, ok := a.lookUp(r.Cookie("session"))
	if !ok {
		return hook.Identity{}, nil // anonymous, not refused
	}
	return id, nil
}
```

and the `Filter` refuses anything they try to say:

```go
func (registeredOnly) Allow(ctx context.Context, from hook.Identity, data string) (bool, string) {
	if from.Anonymous() {
		return false, proto.ErrNeedLogin
	}
	return true, ""
}
```

Returning `hook.ErrUnauthorized` from `Authenticate` instead would refuse
the connection with a 401, which is the other deployment. The filter runs
in front of both `MSG` and `PRIVMSG`, so there is no third place to
remember; everything else a client sends — `JOIN`, `PART`, `NAMES`, the
backlog it arrives with — keeps working, because none of it is speech. The
client is told `{"description":"needlogin"}` and stays connected.

## Testing

```sh
make test        # go test ./...
make test-race   # the same with -race; this is the one that matters
make soak        # long-running connection soak tests (build tag `soak`)
```

Tests are integration-first: a real `httptest.Server` and a real
WebSocket client dialing it, through the `newTestApp` harness in
`app_test.go`. A test that asserts on delivery asserts on what the
*client* received, not on hub internals. Concurrency bugs are the whole
game here, so anything touching the hub or the fan-out ships with a
`-race` test that runs several connections at once.

## Linting and the pre-commit hook

```sh
make init        # tools + hook; what a fresh clone runs
make lint        # go vet + golangci-lint
make pre-commit  # vendor + lint + test-race (what the hook runs)
```

`make lint` refuses to run when golangci-lint is missing and says to run
`make init`, rather than installing it in front of every commit.
staticcheck and `unused` run from inside golangci-lint, configured in
`.golangci.yml` to honnef's own check set rather than the redistributed
default.

## Dependencies and security

```sh
make update-deps  # go get -u, toolchain, tidy, vendor, then test + vulncheck
make vulncheck    # govulncheck ./...
make vendor       # go mod vendor + tidy
```

`update-deps` bumps the pinned toolchain too: govulncheck reports
standard library CVEs against it, and Go's patch releases are mostly
security fixes. `vulncheck` is deliberately not in the pre-commit hook —
it needs the network vulnerability database, and a hook that fails
offline is a hook that gets skipped.

## Load generation

`cmd/loadgen` is a separate process that points a crowd at a running
server and reports what came back:

```sh
make loadgen
./loadgen -url ws://127.0.0.1:8080/ws -conns 5000 -speak 10 -rate 1 -for 60s
```

It is a client and nothing else — it counts what lands on its own
sockets rather than reading the server's counters, so the numbers are
the ones a client would see. Latency is measured at the receiver, from a
timestamp in the message body, so every message received is a sample.

Both ends need file descriptors: one connection is a socket at each end,
so past ~1000 raise `ulimit -n` on both. A generator sharing a machine
with the server competes with it, so past a few hundred thousand
deliveries a second some of the latency being measured is the measuring.
Detail and numbers in [`state/loadgen.md`](state/loadgen.md).

## Metrics and profiling

Both live on `DebugAddr`, **bound to the loopback by default**: `/metrics`
in the Prometheus text format, and `pprof` under `/debug/pprof/`. pprof
has no authentication and is not going to grow any — `/debug/pprof/heap`
hands out a dump of everything in memory — so move that bind address only
to an interface a scraper alone can reach. Empty disables both.

## CI

[`.woodpecker.yaml`](.woodpecker.yaml) runs `make test-race`, `make lint`
and the production build on every push and pull request, and
`make vulncheck` on a cron. Every step shells out to the Makefile, so
the flags a step needs are stated in exactly one place. Go's caches live
on the agent's persistent `/cache` volume, which is what keeps the lint
step from rebuilding the linters from source every run.

## Layout

```
main.go, init.go, conn.go     the server: composition root and connections
channel.go, membership.go     channels, and one write pump per membership
hooks.go, moderation.go       the only callers of the hook interfaces
internal/broadcast            the fan-out primitive (SeqRing)
internal/proto                the wire: one Command in, Outbound out
internal/hook                 the extension surface, interfaces only
internal/filter               UTF-8 and zalgo, in front of every message
internal/moderation           who is muted and banned, and where
internal/ratelimit            token buckets, refcounted by key
internal/history              the replay window (in-memory default)
internal/metrics              a hand-rolled Prometheus registry
internal/config               HuJSON config over defaults in code
cmd/loadgen                   the load generator
state/                        working notes per track; see AI.state
```

[HuJSON]: https://github.com/tailscale/hujson
