# Working state: the server

The walking skeleton and everything bolted to it: config, wire codecs,
extension points, and a single-channel WebSocket chat server.

## Done

- `internal/config` — HuJSON loader. `config_default.hujson` is committed,
  fully commented out, and a test compares it against `Default()` so they
  cannot drift. Commit `fca8ba8`.
- `internal/proto` — verbs, payload structs, and the `Codec` interface with
  JSON and MessagePack implementations. Commits `42b526b`, `f673e75`.
- `internal/hook` — `Authenticator`, `Directory`, `Filter`, `Limiter`,
  `Recorder`, `Authorizer`. Commits `49b1a1c`, `18abbf3`.
- `internal/ratelimit` — token bucket. Commit `fecb04e`.
- `internal/filter` — the message filter chain, with UTF-8 and zalgo
  built in. Commit `cda8cf7`.
- `internal/moderation` — mute and ban state with lazy expiry. Commit
  `bdde971`.
- `main` — server, connection handling, private messages, hook wiring.
  Commit `f1ee0c2`.

## Decisions

- **Three goroutines per connection**, not two: read pump, write pump
  (broadcast stream), private pump (messages for this client alone). All
  three write to the socket, which coder/websocket serializes internally.
  The alternative — one merge channel in front of the socket — puts a
  channel hop back in front of every broadcast and undoes
  `internal/broadcast`.
- **`READY` is sent before anything else.** The WebSocket handshake
  completes before the server has subscribed the connection, so a client
  that talks immediately can miss its own message. This was a real race
  that failed about one run in three, not a hypothetical.
- **A private message never blocks its sender.** It arrives on somebody
  else's read pump; a recipient whose queue is full is refused (`ERR
  recipientbusy`) rather than waited on. Every socket write is bounded by
  `WriteTimeout` for the same reason.
- **One broadcaster per codec.** A ring holds encoded bytes, so clients on
  different wire formats cannot share one. A message is encoded once per
  codec — O(codecs), not O(members).
- **Message ids are assigned under the same lock that writes to every
  ring**, so clients on different codecs cannot disagree about ordering.
  ~40ns against ring writes of ~8ns.
- **Private messages are encoded with the recipient's codec.** The one
  place a frame crosses between connections.
- **Nick, id and timestamp are the server's to assign.** A client that puts
  a nick in its payload is ignored.
- **Hooks are nil-able rather than no-op implementations.** The zero
  `hook.Hooks` is a working server and costs a nil check, not a call.
- **`hooks.go` is the only place a hook is called**, so the rules about
  which may block live in one file.
- **Rate limiting is mechanism in the core, policy in the hook.** The
  `Limiter` hook is asked once per connection and once per channel and
  never sees a message; `internal/ratelimit` does the enforcing.
- **A nil `*ratelimit.Bucket` allows everything**, which is what makes the
  unlimited default free — a nil comparison rather than a mutex and a
  clock read.
- **Client limits are per connection, not per identity.** Anonymous
  connections have no stable id to key on. Per-identity needs logins and a
  keyed registry with a lifetime; noted under Pending.
- **Both buckets are spent even when the message is later refused.** A
  rate limit that only counts successful messages makes sending garbage
  free.
- **`ratelimit.Bucket` has `AllowAt(now)`.** Every test drives the clock
  explicitly; a rate limiter tested against real time fails on a loaded
  machine.

- **The backlog is written under the same lock that orders the fan-out**,
  so what a joining client is shown cannot disagree with what the room
  saw. There is a test for exactly that.
- **`BACKLOG` is always sent when enabled, even when empty.** A
  conditional frame would make "the room is new" and "the backlog is off"
  look the same to a client and turn every client's first-frame handling
  into a guess.
- **Sender roles/attrs ride on every message** rather than being sent once
  with clients keeping a table. Costs bytes; buys stateless rendering and
  no ordering problem when roles change mid-conversation.
- **Moderation is announced to everyone, including its target.** Invisible
  moderation gets re-litigated in the room by people guessing.
- **A banned client usually does not see its own MOD frame.** Its write
  pump is racing its socket being closed, so the close frame carries the
  reason. Waiting for the write pump would let a slow client delay its own
  ban.
- **`MOD` frames are not chat and do not enter the backlog.**
- **Mutes are enforced on both message paths.** Somebody silenced in the
  room does not get to carry on in private.
- **Bans are checked before the upgrade**, so a banned client gets an HTTP
  403 rather than a socket that opens and shuts.

## Gotchas

1. **`net/http`'s `Shutdown` ignores hijacked connections**, and every
   WebSocket is one. Ending them is entirely the server's own doing.
2. **Cancelling a read context cannot be how a connection is closed
   politely.** A cancelled read leaves the stream at an unknown offset, so
   the library drops the socket without a close frame, and a client cannot
   tell that from a network failure. The goodbye has to go out first —
   `app.close()` does them in that order.
3. **Both pumps need telling separately**: the read pumps by cancelling
   their context, the write pumps by ending the subscription they are
   parked in.
4. A **test that dials and immediately sends** is racing the server's
   setup. Wait for `READY`; the harness does.
5. **A test client that connects after any message now gets a `BACKLOG`
   frame**, which the harness consumes. Tests that dial into a server with
   history and then read a frame will see the backlog first if they do
   their own dialling.
6. **Moderation can only name somebody who is connected.** `lookup` is by
   nick over the connection directory, so unbanning a banned user fails
   with `nosuchnick` — they were disconnected by the ban. There is a test
   pinning that down as known behaviour rather than a surprise.

## Pending

- **Channels.** The single implicit channel becomes a lookup from channel
  name to its own set of per-codec rings. `JOIN`, `PART`, `NAMES`, and a
  `channel` field on every server-originated message. The backlog, the
  channel rate limiter and the moderation store all become per channel.
- **Resolving a nick to a key without a connection.** Moderation, and
  especially *un*-moderation, needs to name somebody who is not connected.
  That wants a `Directory` lookup by nick, which the interface does not
  have yet.
- **Moderation state is memory-only.** It is recorded through `Recorder`
  but never loaded back, so a restart forgets every mute and ban. Loading
  wants a hook method that reads them at startup.
- **Per-identity rate limits.** Today two sockets from one person get two
  buckets. Needs a registry keyed by `Identity.ID`, with an eviction
  policy, and it only means anything once logins exist.
- **Real nick collision handling.** Nicks are server-assigned and unique
  today, so `register` cannot collide. With logins it can, and `init.go`
  is where that gets decided.
- Scrollback replay on join (the ring already holds the last `Capacity`
  messages; nothing reads them yet).
- Moderation verbs, and a `Filter` implementation to back them.
- No `cmd/` binary wires real hooks yet — `newApp` passes an empty
  `hook.Hooks`. That is the seam a deployment fills in.
- `PrivBuffer` overflow is only tested at the unit level (`deliver`);
  whether a real recipient's queue fills depends on kernel buffer sizes,
  and a test that depends on those fails on somebody else's machine.
