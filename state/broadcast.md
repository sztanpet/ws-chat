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
  notify channel. Measured and rejected; see below. Deleted afterwards,
  since nothing used it and the numbers below are what it was for; it is
  in `7cf97aa` for anyone who wants to re-measure.
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
  once and take everything pending. It gets one socket write out of it as
  well, though not from the library: coder/websocket flushes per message,
  so the server's `corkConn` holds those flushes until the batch is done.
  That only pays under load, since at steady load a batch is one frame
  (`state/loadgen.md`).
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
On go1.27.3 it is 19-23ns (Re-measured, below).
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

The delivery collapse in that table was the four-vCPU guest: the same
harness and toolchain on six vCPUs deliver 89% at 256 slots (Re-measured,
below). Too small still, but a leak rather than a wall.

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

## Re-measured (4 Nov 2026, go1.27.3, six vCPUs)

The guest has six vCPUs where July's had four, and the toolchain is
go1.27.3 where July's was go1.26.4. The implementations are token-identical
to July's apart from comments and `CondRing`'s deletion, and the harness
differs only in `pace` writing off what a drop lost (`808e806`, gotcha 8).
So anything that moved was moved by the toolchain or the guest, and to
tell which the same source ran under both toolchains on this guest,
interleaved, `-cpu 4`, three to six runs a side. Medians; ns/op for send,
join/part and churn, ns/msg for the rest:

| | July | go1.26.4 | go1.27.3 | moved by |
|---|---|---|---|---|
| send, 1000 subs: seqring | 28 | 25.2 | 19.2 | toolchain -24%, guest -10% |
| send: ring / mapchan | 8.0 / 29250 | 8.09 / 29364 | 8.17 / 29723 | nothing |
| join/part, 0/100/1000 existing: seqring | — | 91/91/110 | 79/80/101 | toolchain, -8 to -13% |
| join/part: ring | 146/147/169 | 98/99/116 | 82/82/104 | guest -32%, toolchain -10 to -17% |
| join/part: mapchan | 1806-2204 | 683/807/1235 | 624/811/1229 | guest, -40 to -65% |
| churn, 1/4 senders: seqring | 219/236 | 198/209 | 188/195 | guest -10%, toolchain -6% |
| churn: ring | 586/1476 | 554/1877 | 509/1332 | noise: 4 senders spans 1262-2012 |
| paced, 1000/10k: seqring | 8.15/8.22 | 7.74/8.19 | 5.88/8.23 | the guest's floor at 1000 (below) |
| paced: mapchan | 98.7/337.9 | 104/233 | 111/235 | toolchain +7%; guest -31% |
| chatty, 1000: seqring / ring | — / 16.7 | 11.9 / 15.4 | 10.9 / 15.4 | nothing |
| chatty: mapchan | 39.2 | 31.5 | 45.5 | bimodal (below) |

Paced `Ring` ran on go1.27.3 only, at 18.2 and 14.1 against July's 17.50
and 15.12.

**`SeqRing`'s send path is the compiler's.** 19-23ns from one subscriber
to ten thousand on go1.27.3 against 25.2 on go1.26.4 on this guest, so it
now costs 11ns more than `Ring`'s where it cost 20. Nothing else the
toolchain touched moved more than 17%, almost all of it down. The
exceptions are `MapChan` paced (+7%) and chatty (slower in its fast mode,
three runs a side without overlap): the reference, not what runs.

**Join/part is 30-65% cheaper, and that is the guest.** go1.26.4 on six
vCPUs already does most of it; go1.27.3 adds 8-17% for the rings and
nothing for `MapChan`.

**`SeqRing` against `Ring` today:** churn 2.7x at one sender and 6.8x at
four, still flat in the sender count; paced delivery 1.7x at ten thousand
and 1.8-3x at a thousand depending on the mode; the chatty room 1.4x.

**`SeqRing` paced at a thousand is bimodal on this guest.** Across twenty
runs on the fixed harness and both toolchains, every run under 1e-4
drops/op came in at 5.1-5.9 ns/msg, the runs near 5e-4 at 6-13, and the
two near 1e-3 at 51 and 60. The floor is the guest, against July's
8.1-9.3, and as far as the evidence goes so is the tail: it shows on both
toolchains, and at `-cpu 6`, where the guest has no vCPU to spare, every
run is drop-heavy and slow (9.2-31.5). It is not the write-off. The
slowest run dropped about two hundred subscribers, who resubscribe at once
having lost hundreds to a few thousand deliveries each; that crosses the
64k lead a handful of times at 10ms each, and the run took 12s where a
fast one takes 1.2.
The drops and the time have a common cause, most likely readers losing
their vCPU; not proven. The median of 6.00 in `808e806`'s message is the
first eight runs; the next eight gave 10.36.

**`MapChan` in the chatty room is bimodal on both toolchains**: 29.5-33.4
ns/msg on go1.26.4 and 38-47 on go1.27.3 in the fast mode, 98-237 in the
slow one, which took two runs in five on each and four in five on the
first go1.27.3 pass (median 195). July's 39.2 is the fast mode. Drop
counts do not separate the modes. Not investigated.

**The 256-slot collapse at ten thousand does not reproduce.** Fixed
harness, go1.27.3, medians of five, July in brackets; ns/msg is left out
below 0.99 delivered (gotcha 1):

| impl | cap | ns/msg | delivered | drops/op |
|---|---|---|---|---|
| mapchan | 256 | — | 0.936 (0.9993) | 1.0 |
| ring | 256 | — | 0.841 (0.0007) | 2.0 |
| ring | 4096 | 8.60 (7.7) | 0.9998 (0.9982) | 0 |
| ring | 65536 | 9.05 (10.5) | 0.9999 (0.9984) | 0 |
| seqring | 256 | — | 0.841 | 2.0 |
| seqring | 4096 | 2.66-2.79 | 0.9957 | 0.007-0.009 |
| seqring | 16384 | 2.04 | 0.9997 | 0 |
| seqring | 65536 | 2.10 | 0.9999 | 0 |

July's harness and toolchain on this guest deliver 89% at 256 slots too,
three runs out of three. What they do reproduce is the time, 2137-8990
ns/msg, which is the stall in gotcha 8. So the collapse was the four-vCPU
guest, and the harness made it look slow as well as lossy. **256 slots is
still too small**: two drops per message sent, which at ten thousand
messages a run is every subscriber twice, and one fixed-harness run in
eleven fell to 51%. A leak, not a wall; the capacity conclusion above
stands on that rather than on the collapse.

**`SeqRing` at ten thousand, the gap July left, costs a quarter to a
third of what `Ring` does once it has slack, and it wants more slack.**
Faster senders widen the lag spread: at 4096 slots about four subscribers
in a hundred are dropped once a run (0.0085 drops per message sent, ~48k
messages) where `Ring` drops none. 16384, the server's `Capacity`, is the
first size with no drops in any run, five out of five.

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
   anything that decides something. On six vCPUs `SeqRing` at a thousand
   is bimodal and eight is not always enough either: two sets of eight in
   a row gave medians of 6.00 and 10.36. Give the range and the drops
   beside it.
8. **A paced sender must write off what a drop lost, not just stop
   waiting for it.** A dropped subscriber resubscribes at the head, so
   what it missed is never delivered to anybody, and a sender waiting for
   the delivered count to catch up waits forever; the first chatty
   benchmark hung on exactly that. `pace` used to bail out after 100k
   yields with no progress, which bounded one wait and forgave nothing:
   once the accumulated loss passed the lead, every later send waited out
   the whole patience. On go1.27.3 the chatty room crosses that within a
   run, and `MapChan` reported 37,883-112,832 ns/msg at fifteen minutes a
   sample, `SeqRing` 1013. `pace` now writes the shortfall off once
   nothing has moved for 10ms (`808e806`). Wall time, because a yield
   lasts as long as every other spinner's turn: with the write-off but
   the yield count, chatty `SeqRing` still measured 38.6 ns/msg against
   11.1-11.8.
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

- **Reviewed 2026-11-03 and fit for purpose.** The seqlock validation
  matches the overwrite boundary exactly (a slot is replaced only once
  `begin` passes it plus capacity, and that is the check), the sleep
  protocol cannot lose a wakeup under Go's sequentially consistent
  atomics, and 30 `-race` runs of the suite were clean. One known
  imperfection, harmless: a reader whose validation fails spins without
  yielding until the writer that tripped it stores `published`, so a
  writer descheduled between its two stores is waited out on a core.
  The bugs that review found were all in the caller (`state/server.md`).
- **Settled: `SeqRing` is what `channel.go` builds** (`04f0e8a`). The
  shared-history fairness property never turned out to matter.
- **Read this before optimising the fan-out again.** Profiled at the wire
  (`state/loadgen.md`, 27 Jul 2026), the fan-out is ~4.5% of the server's
  CPU with a healthy room and under 1% with a collapsing one, and no
  allocation is attributable to it at either. The server is `write(2)` and
  the websocket library's per-`Write` deadline context. Another 2x here
  buys ~2% there; the numbers below are real and no longer where the time
  goes. Re-profiled 5 Nov with batching in, a busy room spends 8.8% here,
  6.2 points of it in the `select` a subscriber parks on between batches:
  the rest of the server got cheaper around it (`state/loadgen.md`).
- The per-broadcast allocations are the remaining known cost: the
  replacement notify channel (both implementations, whenever anybody is
  asleep) and `SeqRing`'s 24-byte header publication. Worth attacking only
  if a profile says so — they are per broadcast, not per subscriber, and
  the notify channel is the one that would pay better, since it is the
  larger of the two and it is allocated in exactly the quiet-room regime
  where `SeqRing` currently wins nothing.
- Untried alternatives, now probably not worth it: slice-based member set,
  sharded map, single owner goroutine.
- Capacity for a real channel is measured, not guessed: 256 is too small
  for a large busy room, and 4096 was the default until a deployment
  expecting 3k users with spikes to 100k moved it to **16384**. Read it as a
  time window (slots over the channel's message rate), which grows with the
  room since a big room is delivery-bandwidth-bound. The ceiling is
  `MaxChannels`: slots are 8 bytes and allocated per channel per codec at
  creation, so 16384 x 2 codecs x 1024 channels is 256MB. `SeqRing` at
  ten thousand subscribers and a thousand senders drops nobody at 16384
  and about 4% of the room once a run at 4096 (Re-measured, above), so
  going back to 4096 is not free.
- Open: why `SeqRing` paced at a thousand and `MapChan` in the chatty room
  are bimodal on six vCPUs (Re-measured, above). Neither moves a decision.
