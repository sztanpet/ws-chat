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
- **The verb is a field inside the frame, not a prefix.** One decode per
  inbound frame instead of a split plus a parse. `proto.Command` is a flat
  union of every inbound field for the same reason — one struct per verb
  would put the second parse back.
- **Outbound payloads carry their own verb**, set by the `New*`
  constructors, and `Encode` takes a `proto.Outbound` so a verbless frame
  cannot be built. It would otherwise be a silent bug: a client would
  simply ignore the frame.
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
- **Client limit scope is the hook's choice, via `Limits.Key`.** Empty is
  per connection, which stays the default because an anonymous connection
  has no account to share a budget with. Non-empty shares a refcounted
  bucket between every connection naming that key. The key is a string
  rather than a flag so the hook can build it from anything the auth layer
  attached — an account id, a tier, an org out of `Attrs`.
- **A keyed bucket outlives its connections on purpose.** Reclaiming it at
  refcount zero would refill somebody's throttle for them the moment they
  reconnected. The janitor only drops a bucket that nobody holds *and*
  that has refilled, which by definition holds nothing.
- **The limits of a bucket already in use are never changed.** A later
  connection reporting different numbers for the same key would otherwise
  rebuild — and so refill — a budget somebody is part way through.
- **Both buckets are spent even when the message is later refused.** A
  rate limit that only counts successful messages makes sending garbage
  free.
- **`ratelimit.Bucket` has `AllowAt(now)`.** Every test drives the clock
  explicitly; a rate limiter tested against real time fails on a loaded
  machine.

- **The backlog is behind the `History` hook**, with `history.Memory` as
  the default. Split from `Recorder` because they are different jobs:
  durability is asynchronous and allowed to be slow, a replay window is
  read on every connect and is not.
- **`History.Append` runs under the lock that orders the fan-out**, so what
  a joining client is shown cannot disagree with what the room saw. There
  is a test for exactly that. It is documented as must-not-block for the
  same reason.
- **`History.Recent` is deliberately NOT under that lock.** An
  implementation may be backed by something slow; holding the broadcast
  lock across it would let a slow store stall the channel.
- **`wireMsg` is the only place a recorded message becomes a frame**, so
  the backlog and live traffic cannot describe the same message
  differently.
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

- **Profiling and metrics are on their own listener**, loopback by
  default. pprof has no auth and never will; `/debug/pprof/heap` is a
  memory dump and `/debug/pprof/profile` is a thirty-second core pin
  available to anyone who can reach it.
- **Metrics are not a hook.** Policy is a hook; the server describing
  itself is not. Scrape it.
- **Anything already counted is a `GaugeFunc` over the real thing**, not a
  maintained mirror — one number that can be wrong beats two that can
  disagree.
- **Refusals are counted inside `conn.reply`**, the one place an ERR is
  sent, so the metric cannot drift from what clients were told.

- **A write pump per membership**, not one multiplexing over all of them.
  `Recv` blocks and cannot be selected on, and a merge channel in front of
  the socket would undo `internal/broadcast`.
- **Join order is deterministic and load-bearing**: subscribe, backlog
  directly, broadcast JOIN, *then* start the pump. A client sees BACKLOG,
  its own JOIN, then live traffic. Starting the pump earlier raced them.
- **The parting client is told directly**, before its subscription ends —
  ending it discards unread frames, so its own PART is exactly the one
  that would go missing.
- **Empty channels are reclaimed and it costs nothing**: the ring holds
  frames for subscribers that no longer exist, and the backlog is in the
  History hook.
- **Moderation is scoped by the command's channel**: named acts there,
  empty acts server-wide. A lookup checks the channel then the global
  entry. A channel action is announced in that channel; a server-wide one
  in every channel the target is in.
- **A channel mute does not block private messages**; only a server-wide
  one does. Silencing somebody in a room is not a statement that they may
  not talk to anybody.
- **`CanModerate` is asked per scope**, so running a room is not
  permission to act across the server.
- **A channel ban removes the target before announcing**, which is what
  keeps it to one copy of each frame. The other order needs a direct send
  as well and then duplicates for anybody whose pump had already drained
  the broadcast.
- **Moderation state persists through the `Sanctions` hook**, separate
  from `Recorder`: a mute is not a log line, it is state the server needs
  back. `Record` is async on the shared queue; `Active` runs once at
  startup and **a failure is fatal** — the alternative is a server that
  lets banned people in because it could not find out.
- **`record()` cannot check the hook for you.** It takes a closure and
  cannot see which hook is inside it, so the caller checks. Getting that
  wrong queues a job that dereferences nil on a worker goroutine, which is
  how it was found.
- **A refused autojoin sends the client an ERR**, rather than leaving it
  to wonder why the room it was put in is empty.

## Gotchas

1. **`net/http`'s `Shutdown` ignores hijacked connections**, and every
   WebSocket is one. Ending them is entirely the server's own doing.
2. **Only the ordering WITHIN a channel is guaranteed on join.** Its
   backlog is written directly while its JOIN comes through its pump, so
   with two channels the second backlog can overtake the first join. The
   test harness counts outcomes rather than assuming an order.
3. **Cancelling a read context cannot be how a connection is closed
   politely.** A cancelled read leaves the stream at an unknown offset, so
   the library drops the socket without a close frame, and a client cannot
   tell that from a network failure. The goodbye has to go out first —
   `app.close()` does them in that order.
4. **Both pumps need telling separately**: the read pumps by cancelling
   their context, the write pumps by ending the subscription they are
   parked in.
5. A **test that dials and immediately sends** is racing the server's
   setup. Wait for `READY`; the harness does.
6. **A test client that connects after any message now gets a `BACKLOG`
   frame**, which the harness consumes. Tests that dial into a server with
   history and then read a frame will see the backlog first if they do
   their own dialling.
7. **A message sent between subscribe and the backlog read arrives
   twice.** The server subscribes first on purpose — the other order loses
   the message instead, and a duplicate a client drops by id beats a gap it
   cannot see. The protocol documents the client-side rule.
8. **Moderation can only name somebody who is connected.** `lookup` is by
   nick over the connection directory, so unbanning a banned user fails
   with `nosuchnick` — they were disconnected by the ban. There is a test
   pinning that down as known behaviour rather than a surprise.

## Pending

- **Resolving a nick to a key without a connection.** Moderation, and
  especially *un*-moderation, needs to name somebody who is not connected.
  That wants a `Directory` lookup by nick, which the interface does not
  have yet.
- **Nothing sweeps on a timer except the janitor**, which runs once a
  minute. Expired mutes and unreferenced limiters therefore linger for up
  to that long; every read already treats them as absent, so this is
  memory, not correctness.
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
