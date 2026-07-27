# Working state: internal/broadcast

The fan-out primitive: one message in, N frames out. Two implementations
behind one interface, and the harness that decides between them.

## Done

- `broadcast.go` — `Broadcaster` / `Sub` interfaces and `MapChan`, the
  reference implementation (`map[*mapSub]struct{}` behind an `RWMutex`, one
  buffered channel per subscriber, non-blocking sends). Commits `bb3bc4c`,
  `7a2e81d`.
- `ring.go` — `Ring`, a shared ring buffer. One copy of the history, each
  subscriber reading at its own position. Commit `5c26cf4`. The reference
  for the shared-storage design, and what the capacity findings below were
  measured on.
- `seqring.go` — `SeqRing`, `Ring` with the lock off the read path.
  **This is the one to use**; `channel.go` builds these (`04f0e8a`).
  Commits `aca8576`, `04f0e8a`.
- `ring_cond.go` — `CondRing`, Ring with a `sync.Cond` instead of the
  notify channel. Measured and rejected; see below.
- `broadcast_test.go` — contract tests, table-driven over the `impls` list,
  plus `TestConcurrentChurn` for `-race`. Both implementations pass the
  same suite.
- `bench_test.go` — the comparative harness. Commits `c6dc73a`, `5c26cf4`.

## Decisions

- **The interface exists from day one** because this is the component most
  likely to be rewritten. Adding an alternative means one line in the
  `impls` table in `broadcast_test.go`; every test and benchmark picks it up.
  This paid for itself immediately.
- **Receiving is batch-oriented** (`Recv(dst [][]byte) (int, error)`), not a
  channel of messages. A chan-per-message API forces one wakeup per message
  and makes shared storage impossible to express. A write pump wants to wake
  once, take everything pending, and coalesce it into one socket write.
- **`Broadcast` returns nothing; `Drops()` is a cumulative counter.** An
  implementation is free to notice a lagging subscriber lazily, on the
  reading side, rather than checking every subscriber on every send — which
  is exactly how `Ring` keeps its send path O(1).
- **A subscription ends with a reason**: `ErrLagged` or `ErrClosed`. The
  server wants that for the disconnect log.
- **`Broadcast` takes `[]byte`, not a generic `T`.** Frames are bytes.
- **The message is shared by reference across subscribers** and documented
  as immutable after the call. Copying per subscriber would dominate.
- **MapChan drops in a second pass under the write lock**, not inline under
  the read lock: closing a subscriber's channel while another goroutine may
  be mid-send into it panics. `Ring` sidesteps the problem entirely by never
  touching subscribers from the writer.

## Baseline (AMD Ryzen 5 5600G, 4 procs, go1.26.4)

Send path, no receivers running — what the sender pays:

```
subs      mapchan ns/op    ring ns/op
1              47.8            8.4
10            232              8.1
100          2233              8.3
1000        29250              8.0
10000      705919              7.9
```

Delivered cost with a paced sender, both at ~100% delivery — the number
that decides anything:

```
subs      mapchan        ring
1000      98.7 ns/msg    17.7 ns/msg
10000    337.9 ns/msg    24.9 ns/msg
```

Join/part: 1806-2204 ns/op and 6672 B/op for MapChan, 137-142 ns/op and
176 B/op for Ring. Under concurrent broadcasts (`ChurnUnderLoad`) the gap
goes to 200us against 336ns, because MapChan's `Subscribe` wants the write
lock that every broadcast holds at read.

**Ring wins on every axis except two:** the shared buffer means the slowest
reader decides how much history everyone gets (a per-subscriber queue
isolates them), and it allocates 112B once per broadcast whenever anyone is
asleep, for the replacement notify channel. MapChan allocates nothing.

## The sync.Cond variant (measured, rejected)

`Ring` wakes sleepers by closing a shared `notify chan struct{}` and
installing a fresh one, which allocates ~112B per broadcast whenever
anybody is asleep — and that is the *common* case, since a quiet room is
one where every write pump is parked in `Recv`. `sync.Cond` allocates
nothing, so it looked worth trying.

Medians over repeated runs (AMD Ryzen 5 5600G, 4 procs):

```
                         ring      condring
send path (no receivers)  8.3ns      9.6ns
paced delivery, 1000     20.8us     19.8us     (n=8 medians)
part, 1000 sleepers        169ns    2260ns     (n=5 medians)
part, 100 sleepers         147ns     495ns
part, no sleepers          146ns      80ns
alloc per broadcast       114B/1      0B/0
churn under load, 1 sender 405ns    4502ns
```

**Verdict: keep `Ring`.** The allocation saving is real and delivery is a
wash, but `sync.Cond` cannot wake one specific waiter — no select, and
`Signal` wakes an arbitrary one — so ending a subscription must
`Broadcast` and wake every sleeper in the room to re-check a predicate
and go back to sleep. That is 13x on a part, it grows linearly with room
size, and it is worst exactly where a chat server cares most. `Ring`
wakes exactly one, because a subscriber parks on a `select` over its own
`done` channel.

Below ~100 sleepers `CondRing` wins on churn (80ns against 146ns: no
`done` channel to allocate and nobody to wake). Not the case the server
is built for.

One thing `sync.Cond` is genuinely better at, and it is not performance:
`Wait` enqueues the waiter *before* releasing the lock, so lost wakeups
are impossible by construction. `Ring` needs the ordering argument
written out in its `Broadcast` comment — waiters register under `RLock`,
the decision is made under `Lock`, so the two cannot interleave. Correct,
but a paragraph of reasoning where `Cond` would be none.

Untested hypothesis for why the channel swap is not slower despite
allocating: waking generation N and parking for generation N+1 use two
*different* channel locks, where `sync.Cond` has one notify list that
both the waker and the re-parking sleeper contend on. Would need a
profile to confirm.

## SeqRing: the lock off the read path (measured, adopted)

`Ring`'s `RWMutex` protects very little — a reader wants the write cursor
and one slot per message — but an `RLock` is still a read-modify-write on
one shared word, so every `Recv` batch bounces that cache line and every
`Broadcast` takes the write half against all of them. `SeqRing` keeps
everything else about `Ring` and removes the lock:

- **A slot holds a pointer to the message header**, not the header. The
  writer replaces the pointer and never mutates what it points to, so a
  reader may hold what it loaded for as long as it likes and the GC is the
  grace period. That is the RCU shape: reads are plain loads and
  reclamation is free.
- **Publication is two counters bracketing the slot write** — `begin`
  before, `published` after. A reader loads `published`, copies pointers,
  then loads `begin` ONCE for the whole batch: if `begin` has not reached
  into what it copied, nothing it read was mid-replacement. A seqlock
  validated per batch rather than per message.
- **Writers serialize on a `Mutex` readers never touch.** Writer
  concurrency was never the contended axis — the server already assigns
  message ids under its own lock, so broadcasts to a channel are
  serialized upstream of this anyway. Reader concurrency is.

Medians of eight on `BenchmarkFanoutPaced`, the comparison that settles
things (AMD Ryzen 5 5600G, 4 procs):

```
subs      ring           seqring
1000      17.50 ns/msg    8.15 ns/msg
10000     15.12 ns/msg    8.22 ns/msg
```

Both at 0.999 delivered, `seqring` at **zero** drops/op against `ring`'s
0.04 at ten thousand, and steadier: eight runs spanned 8.1-9.3 ns/msg
where `ring` spanned 11.6-15.6.

Churn is the bigger win, medians of five on `ChurnUnderLoad` at a thousand
subscribers:

```
senders   ring      seqring
1          586ns     219ns
4         1476ns     236ns
```

**`SeqRing`'s churn is flat in the sender count** (+8%) where `Ring`'s
degrades 2.5x, because `Subscribe` takes no lock and cannot queue behind
the traffic. `Ring.Subscribe` wants the same write lock every `Broadcast`
holds. For a server where people join and leave busy channels constantly
that matters more than the fan-out number does.

**The cost is the send path: 28ns against 8ns, and one 24-byte allocation
per broadcast**, because publishing `&msg` makes the slice header escape.
It is once per broadcast, not per subscriber, against an encode measured
at 660ns and five allocations per message per codec — so any room of any
size pays it back. The zero-allocation version stores the header in the
slot and reads it while a writer may be replacing it, which is a torn read
`-race` refuses, and reassembling a possibly-torn pointer/length pair with
`unsafe.Slice` trips `checkptr`, which `-race` turns on. **The indirection
is what makes the design safe rather than merely fast** — do not try to
remove it without solving that first.

`TestDropThresholdIsExactlyTheSlack` (`4fc41dc`) pins every
implementation to the same boundary: a subscriber exactly `size` behind is
owed all of it, `size+1` behind is gone. That is what makes the capacity
numbers below transferable to `SeqRing` rather than needing to be
re-measured, and it is why a reader that loses the seqlock race **rewinds
and retries instead of dying** — dropping there would have cost a slot of
effective capacity and turned a near miss into a disconnection.

### At the wire, and the regime that decides it

`cmd/loadgen` against both binaries, one box, interleaved so drift
cancels. A busy room (700 conns, 10% at 4/s, 195k deliveries/s, one
channel), both rounds:

```
          p50      p90      p99      mean
ring      18.4ms   53.3ms   114.7ms  24.4ms
seqring   16.4ms   41.0ms    81.9ms  19.7ms
ring      20.5ms   53.3ms   114.7ms  25.3ms
seqring   20.5ms   49.2ms   106.5ms  24.4ms
```

A quiet room (500 conns at 2/s, 50k deliveries/s) is a **wash** over three
rounds each: ring means 3.75/3.79/4.24ms against seqring's
4.13/4.13/3.85, overlapping ranges with ring owning both the best and the
worst run.

**That is the expected shape, not a disappointment, and it is the thing to
remember about this implementation.** What `SeqRing` removes is readers
contending on the fan-out lock, so the win is proportional to how much
time readers spend holding it. A quiet room is one where every write pump
is parked in `Recv` between messages — no contention to remove, and
`SeqRing` only pays its extra publication. Busy rooms and churn are where
it earns its keep.

Throughput was identical in every wire run because none was throughput
bound: 195k/s is the offered rate and this box plateaus at 300-390k with
the generator on it. The overload runs that would push past that are
recorded in `state/loadgen.md` as **not comparable between runs**, so they
are not evidence and were not used as any.

## The chatty-room benchmark

`BenchmarkFanoutChatty` is the shape a real room has: everybody listening,
ten percent of them talking, each talker with a sender goroutine of its
own alongside its drainer — the server's read pump and write pump per
connection. Senders are paced collectively.

Medians of five runs, all at ~99% delivery:

```
subs   mapchan      ring        condring
100    34.9 ns/msg  35.2 ns/msg 31.6 ns/msg
1000   39.2 ns/msg  16.7 ns/msg 17.8 ns/msg
```

At a hundred subscribers the three are indistinguishable — ten concurrent
senders over ten thousand deliveries is not enough work for the data
structure to matter. At a thousand, `Ring` and `CondRing` are 2.3x ahead
of `MapChan` and level with each other; the allocation `Ring` pays per
broadcast starts to show against `CondRing` under a hundred concurrent
senders, but not enough to overturn the churn result above.

**Ten thousand subscribers with a thousand senders was first measured as a
wall, and it is not one. It is a buffer too small for the lag spread.**
Medians of five runs at ten thousand subscribers:

```
impl       cap      ns/op    ns/msg   delivered
mapchan    256     694 us      69.5      0.9993
ring       256    9248 us     158.2      0.0007   <- collapse
ring      4096      76 us       7.7      0.9982
ring     65536     104 us      10.5      0.9984
condring  4096     400 us      40.2      0.9968
```

Sixteen times the capacity turns total collapse into 99.8% delivery and
makes `Ring` **nine times faster than `MapChan`** at the same subscriber
count. The senders are paced on AGGREGATE delivery, which bounds the
average lag and says nothing about the spread: one straggler can be
thousands of messages behind while the mean sits at sixty-four, and a
256-slot buffer laps it. More slack absorbs the spread.

**The capacity that fixes it is affordable only because the storage is
shared.** 4096 slots is 64KB for the whole `Ring`, whatever the subscriber
count. The same slack per subscriber is 655MB for `MapChan` at ten
thousand, and 65536 slots would be ten gigabytes — which is why the sweep
cannot measure `MapChan` above 256 at all. Shared storage does not just
make fan-out cheaper; it makes generous lag tolerance free, and lag
tolerance is what stops the drop cascade.

**What survives from the original reading:** `Ring` does not throttle its
senders and `MapChan` accidentally does — an O(N) send path costing the
sender 700us of its own CPU limits a thousand senders whether they like it
or not. That is why `Ring` is the one that needs enough slack, and why
`internal/ratelimit` is load-bearing rather than a nicety. **What does
not:** the claim that ten thousand was beyond the implementation. It was
beyond a 256-slot buffer.

`CondRing` barely benefits (736us to 400us) and does not collapse at 256
in the first place, for the same reason: waking every sleeper on every
broadcast is work the sender does, which paces it. `Ring` skips the wakeup
when nobody is asleep, which is exactly why it is faster once it has room —
and why it has further to fall when it does not.

## Harness gotchas (hard-won, do not undo)

1. **Count deliveries, not Broadcast calls.** A broadcaster that drops on
   overrun looks fastest when it fails fastest — the drop path is cheaper
   than the send path. Run flat out, Ring appeared 164x faster than MapChan
   while delivering 2% of the messages. `delivered/sent` is reported
   everywhere and anything below ~1.0 invalidates the ns/msg beside it.
2. **An O(1) sender needs a paced benchmark.** MapChan's fan-out cost paces
   the sender against its own receivers by accident; Ring removes that, so
   the sender laps everyone. `BenchmarkFanoutPaced` holds the sender to a
   bounded lead over what was actually received. Compare implementations
   there, not in `FanoutLive`.
3. **Drops are permanent**, so benchmark drainers must reconnect or the
   member set bleeds to empty and the run times an empty map. This once
   produced a very convincing 8.65 ns/op for a 100-subscriber fan-out.
4. **A tight-loop sender always outruns a small set of receivers**, so the
   live family starts at 1000 subscribers *on this box*. On other hardware
   watch `delivered/sent` rather than trusting the constant.
5. **Concurrent senders cannot be drop-free with GOMAXPROCS=4** — every P
   busy sending leaves no core to receive on. That case is
   `FanoutSaturated` and is explicitly an overload benchmark.
6. **A ratio above 1.0 in delivered/sent is the harness measuring its own
   setup.** `warm` used to sleep a fixed 20ms, which is long enough only
   when the buffer is small enough that the leftovers get DROPPED. With a
   large buffer nothing is dropped, so a thousand warm-up broadcasts to ten
   thousand subscribers — ten million deliveries — were still queued when
   the timed region began and landed inside it, giving delivered/sent of
   1.7. `warm` now waits for the warm-up to be consumed, bounded by
   `warmPatience`.
7. **`BenchmarkFanoutPaced` is noisy — never read a single run.** It
   varies by up to 3x run to run, and the first run of a series is
   routinely a 10x outlier (goroutine setup, GC, `b.N` ramping). One run
   of it said `CondRing` was 2.2x slower on delivery; eight runs and a
   median put the two within 5%. Use `-count=8` and take the median for
   anything that decides something.
8. **A paced sender must be able to give up waiting.** A subscriber
   dropped for lagging stops receiving until it reconnects, so messages it
   missed are never delivered to anybody, and a sender waiting for the
   delivered count to catch up waits forever. The first chatty benchmark
   hung on exactly that. `pace` bails out after 100k yields with no
   progress at all, and the abandonment shows up as a low delivered/sent
   rather than as a hang.
9. **`BenchmarkFanout`'s untimed drain is O(b.N x subs)**, so an
   implementation with a much cheaper timed op gets a much larger `b.N` and
   the drain stops finishing. `needsDrain` skips it for shared-storage
   implementations, which genuinely do not need it. New implementations may
   need a line there.

10. **`BenchmarkFanoutChatty` at 100 subscribers is not usable on four
    cores.** Three runs of the identical configuration gave 259.8, 8624 and
    647.6 ns/msg — a 33x spread, where medians of five will not converge.
    `pace` spins on `runtime.Gosched()`, so ten sender goroutines starve
    the hundred drainers they are waiting for, and how badly is scheduling
    luck. The 1000-subscriber case is stable. Since the 100 case was
    already known to show every implementation as indistinguishable, it is
    the most expensive and least informative run in the package — do not
    reach for it to decide anything.
11. **`-bench` matches each `/`-separated element as an UNANCHORED
    regex.** `-bench='BenchmarkFanout/(ring|seqring)/'` runs all six
    `Fanout*` families — `Live`, `Paced`, `Saturated`, `Chatty`,
    `ChattyCapacity` — for `condring` as well, because `BenchmarkFanout`
    is a substring of the others and `ring` is a substring of `condring`.
    That is the 10,000-subscriber capacity sweep included by accident, and
    it ran for 29 minutes and 87 minutes of CPU before being killed. Anchor
    both elements: `-bench='^BenchmarkFanoutPaced$/^(ring|seqring)$/'`.
12. **Prefer a deterministic test to a statistical one where the property
    allows it.** Whether `SeqRing` had `Ring`'s lag tolerance looked like a
    question for the capacity sweep at ten thousand subscribers, tens of
    minutes a run and noisy. It is a boundary — survive at `size`, die at
    `size+1` — so it is a table test that runs in milliseconds and proves
    it outright. `TestDropThresholdIsExactlyTheSlack`.

## Pending

- **Settled: `SeqRing` is what `channel.go` builds** (`04f0e8a`). The
  shared-history fairness property never turned out to matter.
- **Read this before optimising the fan-out again.** Profiled at the wire
  (`state/loadgen.md`, 27 Jul 2026), the fan-out is ~4.5% of the server's
  CPU with a healthy room and under 1% with a collapsing one, and no
  allocation is attributable to it at either. The server is `write(2)` and
  the websocket library's per-`Write` deadline context. Another 2x here
  buys ~2% there; the numbers below are real and no longer where the time
  goes.
- The per-broadcast allocations are the remaining known cost: the
  replacement notify channel (both implementations, whenever anybody is
  asleep) and `SeqRing`'s 24-byte header publication. Worth attacking only
  if a profile says so — they are per broadcast, not per subscriber, and
  the notify channel is the one that would pay better, since it is the
  larger of the two and it is allocated in exactly the quiet-room regime
  where `SeqRing` currently wins nothing.
- Never measured for `SeqRing`, and the one gap in its numbers:
  `BenchmarkFanoutChattyCapacity` at ten thousand subscribers. The
  capacity findings transfer on the strength of an identical drop
  threshold (deterministic, see gotcha 12) rather than a re-measurement.
  If a large busy room ever behaves oddly, measure that before believing
  anything else.
- Untried alternatives, now probably not worth it: slice-based member set,
  sharded map, single owner goroutine.
- Capacity for a real channel is measured, not guessed: 256 is too small
  for a large busy room, and 4096 was the default until a deployment
  expecting 3k users with spikes to 100k moved it to **16384**. Read it as a
  time window (slots over the channel's message rate), which grows with the
  room since a big room is delivery-bandwidth-bound. The ceiling is
  `MaxChannels`: slots are 8 bytes and allocated per channel per codec at
  creation, so 16384 x 2 codecs x 1024 channels is 256MB.
