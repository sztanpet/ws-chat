# Working state: internal/broadcast

The fan-out primitive: one message in, N frames out. Reference
implementation plus the harness that alternatives will be measured against.

## Done

- `internal/broadcast/broadcast.go` — `Broadcaster` interface, `Sub`, and
  `MapChan`, the reference implementation (`map[*Sub]struct{}` behind an
  `RWMutex`, one buffered channel per subscriber, non-blocking sends).
  Commit `bb3bc4c`.
- `internal/broadcast/broadcast_test.go` — contract tests, table-driven over
  the `impls` list, plus `TestConcurrentChurn` for `-race`. Same commit.
- `internal/broadcast/bench_test.go` — the comparative harness. Commit
  `c6dc73a`.

## Decisions

- **The interface exists from day one** because this is the component most
  likely to be rewritten. Adding an alternative means one line in the
  `impls` table in `broadcast_test.go`; every test and benchmark picks it up.
- **`Broadcast` takes `[]byte`, not a generic `T`.** Frames are bytes. A
  type parameter buys nothing here and makes the benchmark output harder to
  compare.
- **The message is shared by reference across subscribers** and documented
  as immutable after the call. Copying per subscriber would dominate.
- **Dropping is a second pass under the write lock**, not inline under the
  read lock. Closing a subscriber's channel while another goroutine may be
  mid-send into it panics; taking the write lock is what proves no send is
  in flight. Any alternative implementation has to solve this same problem,
  and it is the first thing to look at in a lock-free design.
- **`Sub.Dropped()` distinguishes "fell behind" from "orderly close"** so
  the write pump can log a disconnect reason without a side channel.

## Baseline (AMD Ryzen 5 5600G, 4 procs, go1.26.4)

```
Fanout/subs=1            47.27 ns/op    47.27 ns/msg   0 allocs
Fanout/subs=10          252.3  ns/op    25.23 ns/msg   0 allocs
Fanout/subs=100        2193    ns/op    21.93 ns/msg   0 allocs
Fanout/subs=1000      29188    ns/op    29.19 ns/msg   0 allocs
Fanout/subs=10000    677798    ns/op    67.78 ns/msg   0 allocs
FanoutLive/subs=1000  85703    ns/op    85.70 ns/msg   0 drops
FanoutLive/subs=10000  2.59 ms/op      259.1  ns/msg   0 drops
SubscribeUnsubscribe            ~650 ns/op, 1928 B/op (the channel buffer)
ChurnUnderLoad/senders=1      174396 ns/op
ChurnUnderLoad/senders=4      608406 ns/op
```

**The number that matters:** the gap between Fanout and FanoutLive at the
same subscriber count — 29 vs 86, 68 vs 259. Two thirds of the cost of a
broadcast is waking the receiver goroutines, not walking the map. An
alternative that speeds up the member set alone is fighting for the small
third. The candidates that attack the big third are the ones that reduce
wakeups: a shared ring buffer the write pumps poll, or batching several
frames per wakeup.

Secondary: per-delivery cost is flat from 10 to 1000 subscribers and then
roughly triples by 10000 (21.9 → 67.8 ns/msg idle). That is cache
behaviour walking a large map, and it is the part a sharded or slice-based
member set could plausibly fix.

## Harness gotchas (hard-won, do not undo)

1. **Drops are permanent**, so drainers must reconnect or the member set
   bleeds to empty and the benchmark times an empty map. This produced a
   very convincing 8.65 ns/op for a 100-subscriber fan-out.
2. **A tight-loop sender always outruns a small set of receivers.** The
   live family therefore starts at 1000 subscribers *on this box*. Re-check
   on different hardware: the tell is a live ns/msg **below** the same
   count's idle Fanout figure, which is impossible unless the set was
   partly empty.
3. **Concurrent senders cannot be drop-free with GOMAXPROCS=4** — every P
   busy sending leaves no core to receive on. That case is
   `FanoutSaturated` and is explicitly an overload benchmark.
4. **Churn benchmarks use a 64-slot buffer**, not 256: `make(chan []byte,
   n)` costs the same in every implementation and a big buffer buries the
   membership bookkeeping being compared.

## Pending

- Alternative implementations to measure against `MapChan`:
  - slice-based member set (`[]*Sub` + copy-on-write) — better cache
    behaviour on the walk, `O(n)` removal.
  - sharded map — only worth it if `FanoutSaturated` shows the write lock
    is the problem.
  - single owner goroutine with a command channel — removes the mutex
    entirely, adds a hop per broadcast.
  - the one that targets the actual cost: fewer receiver wakeups (shared
    ring buffer, or batched frames per wakeup).
- Nothing consumes this package yet. The channel/hub layer that will is
  not written.
