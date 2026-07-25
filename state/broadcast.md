# Working state: internal/broadcast

The fan-out primitive: one message in, N frames out. Two implementations
behind one interface, and the harness that decides between them.

## Done

- `broadcast.go` — `Broadcaster` / `Sub` interfaces and `MapChan`, the
  reference implementation (`map[*mapSub]struct{}` behind an `RWMutex`, one
  buffered channel per subscriber, non-blocking sends). Commits `bb3bc4c`,
  `7a2e81d`.
- `ring.go` — `Ring`, a shared ring buffer. One copy of the history, each
  subscriber reading at its own position. **This is the one to use.**
  Commit `5c26cf4`.
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
6. **`BenchmarkFanoutPaced` is noisy — never read a single run.** It
   varies by up to 3x run to run, and the first run of a series is
   routinely a 10x outlier (goroutine setup, GC, `b.N` ramping). One run
   of it said `CondRing` was 2.2x slower on delivery; eight runs and a
   median put the two within 5%. Use `-count=8` and take the median for
   anything that decides something.
7. **`BenchmarkFanout`'s untimed drain is O(b.N x subs)**, so an
   implementation with a much cheaper timed op gets a much larger `b.N` and
   the drain stops finishing. `needsDrain` skips it for shared-storage
   implementations, which genuinely do not need it. New implementations may
   need a line there.

## Pending

- **Ring is the one to build the server on**, unless the shared-history
  fairness property turns out to matter. Nothing consumes either yet.
- Ring's per-broadcast allocation (the replacement notify channel) is its
  only loss to MapChan. Worth attacking only if profiles say so — one
  allocation per broadcast, not per subscriber.
- Untried alternatives, now probably not worth it given Ring's numbers:
  slice-based member set, sharded map, single owner goroutine.
- Not yet measured: memory per idle subscriber at 10k+, and what the shared
  capacity should be for a real channel (it bounds both scrollback and how
  far a client may lag).
