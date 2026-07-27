# Load generation — working state

Track for `internal/loadgen` and `cmd/loadgen`: an out-of-process crowd
pointed at a running server. Spec: none — this started from "benchmark the
server with a separate process, a configurable number of connections of
which a configurable percentage speak".

## Done

- **`2c06c36` metrics: count a private message before echoing it**, and
  **`bc6a7e5` test: wait for the server, not for a frame.** Both land
  first, because the generator's own tests are enough extra load to make
  two latent races in the existing suite fail regularly. See below.
- **`16ea90e` loadgen: add the latency histogram.** Log-linear, 8 buckets
  per octave of microseconds, 240 counters, ≤12.5% error, with count/sum/max
  kept exactly. One per connection, merged when it stops.
- **`e1717f5` loadgen: add the connection driver.** `Config`/`Run`/`Result`,
  the client, the client-side codecs, and the tests — including a stub chat
  server so the generator's arithmetic is tested without the real server's
  timing attached.
- **`b220fb1` loadgen: add the command.** Flags, `signal.NotifyContext` so
  Ctrl-C still prints the report, `make loadgen`, the `### Load generation`
  section in CLAUDE.md.
- **`5c6b14e`** the profiling notes below, **`f1be661`** the closed-set
  close reasons, **`85adfe5` conn: one write deadline per wakeup** — the
  fix the profiling asked for.

### What the generator's own tests found in the existing suite

Running them alongside everything else was enough load to expose two
things that had been passing on a quiet machine:

- **`metrics`**: the private-message counter was incremented *after* the
  sender's echo had been written to its socket. A client holding its echo
  could scrape `/metrics` and be told the message never happened. It is now
  counted where the recipient's copy is delivered.
- **Two tests asserted on server state straight after receiving a frame.**
  A frame arriving does not mean the handler that sent it has finished —
  worst for `PART`, where the leaving client is told *before* its
  membership is torn down, deliberately. `client.sync()` (a PING/PONG round
  trip on the same read pump) is the barrier now.

## Decisions

- **Separate process, not a Go benchmark.** A benchmark inside the server
  shares its scheduler, allocator and GC and reports on them as the
  server's, and cannot be pointed at another machine. Built without
  `-race`: the instrument must not be the bottleneck.
- **Client-side codec, duplicated on purpose.** `proto.Codec.Encode` takes
  a `proto.Outbound` — what the *server* sends — so a client cannot use it
  for commands. The generator carries two function values per format
  instead, and one flat `frame` struct decodes every inbound frame in one
  pass, the same trick `proto.Command` plays on the way in.
- **A barrier before anybody speaks, and the clock starts there.**
  Otherwise the delivery ratio measures the ramp, and `-ramp` eats into
  `-for`. Every client releases the barrier exactly once (`sync.OnceFunc`)
  whether it settled or failed, or one refused dial hangs the run.
- **Every connection pings.** `IdleTimeout` (90s) closes a connection that
  has sent nothing, and a listener sends nothing.
- **Speakers are chosen within a channel.** The obvious "every Nth
  connection" aliases against round-robin channel assignment — 10% of 100
  over 4 channels lands entirely in channels 1 and 3. Caught by
  `TestSpeakerSpread`, which is why that test asserts per-channel coverage
  rather than just the total.
- **Latency is sampled at every receiver**, from a timestamp in the message
  body, so it prices the whole path and every delivery is a sample. Same
  process, so one clock, no agreement needed between the ends.
- **`Expected` is a denominator, not a promise**: messages sent in a
  channel × that channel's members, assuming nobody left. Received trails
  it by whatever was in flight at the stop.
- **Settle has its own 30s timeout** and its failure is counted as
  `never joined`. Nothing else would count a server that answers the
  handshake and then does not finish the job, and the barrier would hang.
- **Loss and refusal reasons must come from a closed set.** The map is
  keyed by the reason, and a `net.OpError` prints the peer address, so the
  first 3000-connection run ended in a report with one `losses` line per
  dropped socket, port number and all. `reasonOf` unwraps to the error
  inside the `OpError` and folds EOF, `net.ErrClosed` and context errors
  onto fixed strings — the same rule the server applies to its metric
  labels, learned the same way.
- **A run never aborts on connection failures.** Dial errors, drops and
  refusals are counted and printed; a load test that gives up when three
  dials out of ten thousand fail never finishes.

## Measured (26 Jul 2026, one laptop, server and generator sharing it)

Server on defaults (Capacity 4096, JSON unless stated).

| run | result |
|---|---|
| 500 conns, 10% at 2/s, 1 channel | 47.4k deliveries/s, 99.81%, p50 4.1ms p99 20.5ms, 0 dropped |
| 400 conns, 25% at 5/s, 4 channels, msgpack | 48.7k/s, 99.97%, p50 1.02ms p99 3.84ms, 0 dropped |
| 1500 conns, 20% at 20/s, 1 channel | 302k/s, 11.5% delivered, 1231 of 1500 dropped `too slow`, p50 1.97s |
| 3000 conns, 90% at 1/s, 1 channel | 300–390k/s, ~5% delivered, most of the room dropped `too slow`, p50 >20s |

The last two are the overload shape and they are the useful ones: the
server's lag disconnect shows up by name in `losses`. Deliveries plateau
around 300–390k/s on this box either way, which is a four-core box with the
generator on it, not a server limit. Two chaotic overload runs are **not
comparable to each other** on throughput or drop count — the population
collapses differently every time. Profiles taken during them are, because
they are rates inside one window.

## Profiled (26 Jul 2026, 4 cores, generator on the same box)

Steady load: 500 connections, 10% speaking at 2/s, one channel — 49k
deliveries/s, 100.00% delivered, p50 4.1ms p99 36.9ms. 25s CPU profile off
`/debug/pprof/profile`, allocation delta over 20s off `/debug/pprof/allocs`.

CPU: 94.5% cumulative in `conn.channelPump`, 60% flat in `write(2)`. That
is the right shape — a fan-out server should be a socket-write server.

**The one real finding: a deadline context per delivered frame.**
`conn.write` (`conn.go:477`) does `context.WithTimeout(ctx,
cfg.WriteTimeout)` on every frame, inside `channelPump`'s batch loop.

- 9.2% of CPU: `context.WithTimeout` → `WithDeadlineCause`, split between
  `newobject`, `propagateCancel` and `time.AfterFunc`.
- **99% of everything allocated under load** is that context and the timer
  machinery it drags in — ~740 bytes of garbage per delivered message,
  none of it the message. `WithTimeout` 264MB, coder/websocket's own
  `setupWriteTimeout` → `context.AfterFunc` 376MB, `cancelCtx.Done` 101MB
  over 20 seconds.

### Fixed, and what it was worth

`conn.writeBatch` now gives a whole wakeup one deadline (`conn.go`,
`membership.go`). The library half cannot be hoisted away:
`setupWriteTimeout` skips everything when `ctx.Done() == nil`
(`vendor/.../conn.go:171`) but allocates an `AfterFunc` per `Write` for any
cancellable context, and coder/websocket has no `SetWriteDeadline`. One
`Write` per batch would fix that too, and the protocol does not allow it —
one chat message is one frame.

**At the steady load it changes nothing measurable**, and that is the point
worth remembering: at 100 messages a second into a room, a pump wakes up
with exactly one frame, so one deadline per batch *is* one per frame.

**At overload it is most of the garbage.** 3000 connections, 90% speaking,
one room, 20s windows on the same load before and after:

| | pre-fix | fixed |
|---|---|---|
| `context.WithTimeout` CPU | 11.0% | 1.7% |
| allocated in 20s | 9.95 GB | 1.24 GB |
| `write(2)` share of CPU | 56.5% | 77.5% |

Half a gigabyte of garbage a second, down to sixty megabytes, and the
remaining profile is the syscall it should be. This is the case where the
CPU actually matters: a server that is behind is exactly the server whose
pumps drain sixteen frames a wakeup.

No leaks: in-use heap 16MB, and after 2950 connections over the session
the process is back to 8 goroutines.

## Profiled again with SeqRing (27 Jul 2026, same box, `91c50de`)

Server built without `-race`, `DebugAddr` on the loopback, 20s CPU profile
and a 20s allocation delta bracketing it. Both regimes, to answer one
question: now that the fan-out is twice as fast, does the server notice?

**It does not, and the profiles say why.**

| | steady (500 conns, 10% at 2/s) | overload (3000 conns, 90% at 1/s) |
|---|---|---|
| delivered | 49.6k/s, 100.00%, p50 3.84ms p99 13.3ms | 199k/s, 7.57%, p50 10.5s, 2958 of 3000 dropped |
| `conn.channelPump` | 96.16% cum | 95.99% cum |
| `Syscall6` (write) | 63.01% flat | 65.43% flat |
| **fan-out total** | **~4.5% cum** | **<1% cum** |
| `context.WithDeadlineCause` | 7.19% cum | 1.32% cum |
| `setupWriteTimeout` | 54% of allocation | 86.7% of allocation |
| allocated in 20s | 689MB | 1.61GB |
| **fan-out allocation** | **none attributable** | **none attributable** |
| heap in use / goroutines | 11MB / 1508 | 71MB / 161 |

The fan-out is `seqSub.Recv` at 3.91% and `SeqRing.Broadcast` at 0.56%
cumulative when the room is healthy, and `broadcastTo` 0.43% plus
`Recv` 0.26% when it is collapsing. **SeqRing's 24-byte publication does
not appear in the allocation profile at either load** — at 100 broadcasts a
second it is tens of kilobytes against 689MB of context machinery.

That is the honest frame for yesterday's fan-out work: a 2x improvement in
a primitive that is 4.5% of the profile is worth ~2% of the server, which
is precisely why the wire measurements showed a wash in a quiet room and a
modest tail improvement in a busy one. **The fan-out was not the
bottleneck, and now it is even less of one.** The primitive is still worth
having — the churn win is structural and the paced numbers are real — but
nobody should expect the next fan-out rewrite to move the server.

`conn.writeBatch` is still holding: `WithDeadlineCause` is 1.32% of CPU
under overload where the pre-fix profile had it at 11.0%. At steady load it
is 7.19%, because a pump woken with one frame gets one deadline either way
— the same non-result recorded above, reproduced.

**The remaining target is not ours.** `coder/websocket`'s
`setupWriteTimeout` allocates a `context.AfterFunc` per `Write` and is
86.7% of all garbage under overload; the whole context/timer complex is
99.1% of it at steady load, about 700 bytes per delivered message and none
of it the message. That matches the ~740 bytes already recorded. The
library has no `SetWriteDeadline`, so bounding a write means a cancellable
context, so the allocation is unavoidable without changing or forking the
library. Anything spent on allocation here should be spent there.

**Caveat on reading the overload column.** The server used 1.74 of 4 cores
(34.89s of samples in 20s) and the steady run 0.72 — it is not CPU bound in
either. Under overload it is blocked in `write(2)` against clients that are
not draining, while the generator on the same box competes for the cores.
So "the server is behind" here means socket backpressure, not compute, and
the population collapse (2962 lag drops server-side,
`wschat_fanout_drops_total`, against loadgen's 2958) is the drop policy
working as designed rather than a CPU wall.

## Pending

- Two machines. Everything above shares a laptop with the server, so the
  numbers say more about the box than the code past ~100k deliveries/s.
  This is now the blocking limitation for fan-out work specifically: the
  primitive is under 5% of the profile, so anything further needs a box
  where the server is actually the bottleneck.
- Nothing exercises `PRIVMSG`, moderation or reconnect churn — the
  generator connects, joins, talks and listens, and that is all.
- No CSV/JSON output. The report is for reading.
