# Working state: the server

The walking skeleton and everything bolted to it: config, wire codecs,
extension points, and a single-channel WebSocket chat server.

## Done

- `internal/config` — HuJSON loader. `config_default.hujson` is committed,
  fully commented out, and a test compares it against `Default()` so they
  cannot drift. Commit `fca8ba8`.
- `internal/proto` — verbs, payload structs, and the `Codec` interface with
  JSON and MessagePack implementations. Commits `42b526b`, `f673e75`.
- `internal/hook` — `Authenticator`, `Directory`, `Filter`, `Recorder`.
  Commit `49b1a1c`.
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

## Pending

- **Channels.** The single implicit channel becomes a lookup from channel
  name to its own set of per-codec rings. `JOIN`, `PART`, `NAMES`, and a
  `channel` field on every server-originated message.
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
