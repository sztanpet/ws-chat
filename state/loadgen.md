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

## The setupWriteTimeout patch (tried, measured, not adopted)

The profile above says `coder/websocket`'s `setupWriteTimeout` is 87% of
the garbage under overload. It arms a fresh `context.AfterFunc` on every
frame and disarms it when that frame completes, so `writeBatch` — one
context, up to `WriteBatch`=16 frames — arms the same deadline on the same
context sixteen times, each arming cancelling the last. Fifteen of sixteen
are waste.

Patched in a local fork (`replace` directive, ~50 lines in `conn.go`):
remember the context whose deadline is armed, skip re-arming for the same
one, and return false so `writeFrame`'s deferred disarm never runs. The
library's own suite passes under `-race`, and so does this one.

**It works, and it is not worth taking.** Allocation normalised by
`time.newTimer` bytes, which counts contexts created and so is stable
across runs that collapse differently:

| run | total/ctx | AfterFunc/ctx |
|---|---|---|
| baseline steady | 7.83 | 3.96 |
| patched steady | 7.45 | 3.57 |
| baseline overload | **28.00** | **21.15** |
| patched overload | **7.69** | **3.34** |

The amplification is gone: a patched server under overload allocates the
same per context as one at rest, where the baseline allocated 3.6x more.
That is 6.3x less `AfterFunc` garbage, and CPU follows —
`setupWriteTimeout` 7.62% → 2.63%, `mallocgc` 5.59% → 3.15%,
`gcBgMarkWorker` 2.15% → 0.98%.

Why it is not worth taking anyway:

- **Steady load is unchanged** (730MB against 689MB over 20s, noise, and
  the wrong direction). A pump woken with one frame arms once per batch
  either way. The same non-result `conn.writeBatch` itself has.
- **Overload behaviour is unchanged.** 2845 of 3000 connections dropped
  patched against 2958 baseline; both collapse. Five points of CPU in a
  regime where the server is blocked in `write(2)` at 1.74 of 4 cores
  changes no outcome, which is what the earlier profile predicted.
- **It is a behaviour change, not purely an optimisation.** The deadline
  stays live between frames and after the last one until the context is
  cancelled or superseded, so a caller holding a live cancellable context
  and not writing would lose its connection at the deadline. Fine for a
  pump that cancels via defer; not something upstream should take as-is.
- **Forking costs the vuln feed.** `govulncheck` tracks
  `github.com/coder/websocket` and the module is current (v1.8.15, the
  latest). A fork under another path drops out of that for single-digit
  CPU in a regime that is already lost. Bad trade for RFC 6455 parsing
  code.

The better proposal was `SetWriteDeadline` on `Conn`, and it was then
built and measured. See below — it is a different order of result.

## SetWriteDeadline on Conn (tried, measured, worth upstreaming)

Same fork, 88 lines. `Conn.SetWriteDeadline(t time.Time)` stores an
instant in an atomic; `writeFrame` arms a per-connection `time.Timer`
from it — the `netconn.go` pattern, one `time.AfterFunc` per connection at
construction and nothing but `Reset`/`Stop` thereafter, neither of which
allocates. The server (31 lines) drops its per-batch
`context.WithTimeout`, arms the deadline instead, and passes
`context.Background()` to `Write`, so `setupWriteTimeout` returns at its
first line and allocates nothing either.

**Garbage per delivered message**, the steady column being the rigorous one
(deterministic load, 100% delivered, 49.6k/s both times):

| | steady | overload |
|---|---|---|
| baseline | 729 B/msg | 424 B/msg |
| memoize patch | 773 B/msg | 353 B/msg |
| **SetWriteDeadline** | **4.9 B/msg** | **15.3 B/msg** |

689MB per 20s becomes 4.6MB, and what remains is not the write path at all
— it is `bufio` buffers for connections being set up, `slog` args and
`net/http` header parsing. The write path allocates **nothing**.

It is not only garbage. At steady load, where every other change in this
file was a wash:

| | baseline | SetWriteDeadline |
|---|---|---|
| CPU samples per 20s | 14.33s | 12.58s |
| `write(2)` share | 63.0% | 73.6% |
| `mallocgc` | 4.82% | 0.24% |
| `gcBgMarkWorker` | 0.84% | below threshold |
| mean latency | 4.20ms | 3.56ms |
| p50 | 3.84ms | 3.33ms |

**12% less CPU for identical delivered work, and 15% off mean latency.**
Both deadline runs came in at 3.56-3.63ms mean, below every run of every
other variant measured on this box (the previous best spread was
3.75-4.24). The profile is now very nearly the syscall and nothing else,
which is what this server should look like.

Behaviour is preserved where it matters: the overload run still drops slow
clients by the same mechanism and in the same numbers (2829 lag drops
against the baseline's 2962), so the deadline is really firing.

The one semantic difference to weigh before proposing it: a deadline on the
connection is shared by every goroutine writing to it, where a context is
per call. Arming inside `writeFrame` under `writeFrameMu` means it cannot
disturb a frame already in flight, and a batch written under one stored
instant still shares that instant rather than restarting the clock per
frame — but with several pumps per connection, the last one to set it
wins. For this server every writer sets the same `WriteTimeout`, so it
does not matter here; a general API should say so plainly.

**Not adopted, for the same reason as the memoize patch and no other: it
needs a fork, and forking drops the module out of `govulncheck`.** The
difference is that this one is worth taking upstream on the strength of the
numbers rather than filed as a curiosity.

**Both halves of it are in
[`code-websocket-pr/`](../code-websocket-pr/README.md)**, and the library
half is now a pushed branch — `feat/conn-set-write-deadline` at `9f84ce4`
in the fork, off upstream `master` at `9c8faad`. That directory holds the
patch, the caller-side patch, the PR description and the commands to
reproduce every number. Measured against the pushed commit: **zero sampled
allocations** (bytes and objects) over a 20s window at 49.6k deliveries/s,
mean latency 3.55ms where every baseline run of this load sat at
3.75-4.24ms, and 12.65s of CPU samples against 14.33s.

`AUTOBAHN=1 go test` deserves a note, because it looked like a regression
and was not. Its protocol cases pass, but `TestMain` asserts
`runtime.NumGoroutine() == 1` at process exit, and one branch run tripped it
while a `master` run and a second branch run were clean. **It is the check
racing teardown**: the failing run reported 2 goroutines and then dumped
only 1, so the second had already exited. The new code is inert under
autobahn anyway — nothing there calls `SetWriteDeadline`, so the timer stays
stopped from construction, and a stopped `time.AfterFunc` is in no timer
heap and runs nothing.

Two things found while making it submittable, both worth keeping:

- **`armWriteDeadline` cannot close the connection inline.** `close` takes
  `writeFrameMu` for a client connection via `msgWriter.close`, and
  `armWriteDeadline` runs with that lock held, so an already-expired
  deadline that closed inline **deadlocks**. Upstream's context path never
  hits this because its `close` runs on the `AfterFunc` goroutine. The fix
  is to refuse the frame with `os.ErrDeadlineExceeded` and leave the closing
  to the timer. This was caught by the new test, not by reading the code.
- **The API has to exist on `ws_js.go` too**, per the library's own
  AGENTS.md. Writes there never block, so the deadline is checked when a
  write is attempted rather than interrupting one.

One trap worth repeating if anyone rebuilds it: the first version had
`armWriteDeadline` return the `func()` that stops the timer, which is a
closure over the connection and therefore a heap allocation on every
frame. It cost 7.7MB per 20s and was the entire remaining allocation on
that path. Returning a bool and calling a method took it to zero.

## Confirmed after adoption, and two knobs that do nothing (27 Jul 2026)

Re-run against `b895888`, the committed deadline version, 500 connections at
2/s in one channel. The numbers hold, and 4x on either buffer changes
nothing:

| config | delivered | mean | p50 | p99 | alloc/20s | CPU/20s | `write(2)` |
|---|---|---|---|---|---|---|---|
| defaults (`Capacity` 4096, `WriteBatch` 16) | 99.98% | 3.38ms | 3.33ms | 9.22ms | 9.0MB | 12.36s | 72.3% |
| `Capacity` 16384 | 99.99% | 3.59ms | 3.33ms | 9.22ms | 0 | 12.60s | 70.3% |
| `WriteBatch` 64 | 99.99% | 3.54ms | 3.33ms | 10.24ms | 3.5MB | 12.63s | 70.6% |

Against the context-per-frame baseline of 4.20ms mean, 689MB and 14.33s,
all three sit in the same place. The three are indistinguishable from each
other, and both null results were predictable:

- **`Capacity` is inert here because nobody lags.** At 100 messages a second
  into 500 members every pump keeps up, so the ring never approaches its
  wrap limit and its size cannot matter. Capacity buys tolerance for the lag
  *spread*, which only bites under overload — that is where
  `state/broadcast.md` measured 256 collapsing at ten thousand subscribers
  while 4096 held. It is not free: 16384 slots is ~393KB of slice headers per
  ring per codec and pins up to 16384 message buffers from collection.
- **`WriteBatch` is inert here because a batch is one frame.** A pump woken
  at 100 messages a second has a single frame waiting, so a 64-slot batch
  behaves exactly like a 16-slot one. It only bites when a pump is behind —
  and note it is coupled to `WriteTimeout`, because `writeBatch` gives the
  whole batch one deadline: at 64 a lagging client gets a quarter the time
  per frame that it gets at 16. Raising one means thinking about the other.

**The residual allocation is not the write path.** Zero write-path entries
in two of three runs, and in the third the only two were the per-pump
`batch` slice (`membership.go`, once per membership, not per frame) and one
sampled `context.WithTimeout` from the **read** pump's `IdleTimeout`
(`conn.go`), which this work never touched. The rest is connection setup:
`bufio` buffers, the per-connection cancel context, HTTP request parsing.
The 0-to-9MB spread across runs is just which setup happened to land inside
the 20s window.

If anyone wants the next slice of this, it is the read pump's per-read
`context.WithTimeout`, not anything on the write side.

### Raising the Capacity default to 16384

A deployment expecting ~3k users, spiking to multiples of that and rarely to
100k, wanted more tolerance for the lag spread. The default moved 4096 →
16384. What the runs say about the cost, interleaved pairs at 500
connections and 2/s:

```
        cap 4096            cap 16384
r1      mean 3.71ms         mean 3.95ms
r2      mean 3.56ms         mean 3.62ms
singles mean 3.38, 3.55     mean 3.59, 3.89
```

Four pairs, all with 16384 the higher of the two, by 0.06-0.24ms — but p50
was identical in three of four, **no CPU or GC difference is measurable at
all** (`gcBgMarkWorker`, `gcDrain` and `scanobject` are all below the
profile's reporting threshold at both capacities, and total CPU samples
differ by 1.9%), and the histogram's buckets are ~9% wide at 3.5ms. So: a
consistently signed difference at or below the resolution of the instrument.
Do not treat it as an established 6% cost, and do not treat it as free
either.

Heap in use at the new default with one channel live is 6.08MB total, of
which `NewSeqRing` is ~579kB — consistent with two rings of 128KB (one per
codec) once heap-profile sampling error on 128KB objects is allowed for.

Worth knowing for anyone tempted to go further: the slot array is
`[]atomic.Pointer[[]byte]`, allocated eagerly per channel per codec at
channel creation, so `MaxChannels` sets the worst case — 1024 channels on
both codecs is 256MB at this capacity and 1GB at 65536. That, rather than
the latency, is what bounds the default.

## Pending

- Two machines. Everything above shares a laptop with the server, so the
  numbers say more about the box than the code past ~100k deliveries/s.
  This is now the blocking limitation for fan-out work specifically: the
  primitive is under 5% of the profile, so anything further needs a box
  where the server is actually the bottleneck.
- Nothing exercises `PRIVMSG`, moderation or reconnect churn — the
  generator connects, joins, talks and listens, and that is all.
- No CSV/JSON output. The report is for reading.
- **The generator's goroutines are labelled (`task=barrier|dialer|client|
  reader|progress`) but the binary cannot be profiled.** `cmd/loadgen` has
  no `-cpuprofile` flag and no debug listener, so the labels are only
  reachable by profiling `internal/loadgen` under test. They exist because
  "some of the latency being measured is the measuring" is a standing
  question here and the labels are what would answer it; the flag is two
  lines whenever somebody wants the answer.
