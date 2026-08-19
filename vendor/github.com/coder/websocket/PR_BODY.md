# conn: add SetWriteDeadline to bound writes without allocating

## Why

Bounding a write on this library currently requires a context. For a server
that fans one message out to many connections, that cost is paid per
*recipient* rather than per *message*: a `context.WithTimeout` on the caller's
side, and a `context.AfterFunc` on ours for any cancelable context.

Profiling a chat server holding 500 connections in one room at ~49,600 message
deliveries a second attributed roughly 700 bytes of garbage per delivered
message to that pair, and 99% of everything the server allocated under that
load.

A deadline is an instant, and an instant needs no allocation to express.

## What

`Conn.SetWriteDeadline(t time.Time) error` stores the instant in an
`atomic.Int64`; `writeFrame` arms a `time.Timer` created once per connection,
using only `Reset` and `Stop`, neither of which allocates. This is the approach
`netconn.go` already takes for `net.Conn` deadlines, now exposed on `Conn` so
that a caller who can name its bound as a deadline does not have to buy a
context to carry it. Paired with a non-cancelable context on `Write` — which
#566 already made free — the write path allocates nothing.

`ws_js.go` gets the same method so the API does not differ by platform.

## Semantics

They follow `net.Conn`:

| situation | result |
| --- | --- |
| write started after the deadline passed | fails with `os.ErrDeadlineExceeded`, **connection stays open** |
| deadline passes while a frame is in flight | fails with `os.ErrDeadlineExceeded`, **connection closes** |
| no deadline set (the default) | one atomic load more than before, nothing else changes |
| zero `time.Time` | clears the deadline |

The second row is the one departure from `net.Conn`, and it is forced: a
half-written frame cannot be abandoned without corrupting the stream. It is the
same distinction `NetConn` already documents. Both rows report
`os.ErrDeadlineExceeded` rather than `net.ErrClosed`, so a caller can tell a
write deadline from a peer that hung up — the whole reason to set one.

A deadline belongs to the connection, not to a call, so where several
goroutines write to one connection the last to set it wins, and frames written
under one deadline share that instant rather than restarting the clock. Setting
or clearing a deadline never disturbs a frame already in flight: the deadline
that bounds a frame is the one in effect when the frame started. That is why
the timer is armed by `writeFrame` under `writeFrameMu` rather than by
`SetWriteDeadline`.

Deadlines are stored as unix nanoseconds, which wraps outside roughly
1677–2262, so `deadlineNanos` saturates at both ends. Without it a deadline in
the year 2300 would read back as one long expired and kill every write on the
connection.

## There is no SetReadDeadline

Deliberate, and worth deciding explicitly since this is public API. Reads are
bounded by the context passed to `Reader`, an idle connection waiting on its
peer is normal rather than an error, and the allocation this PR is about is
write side. A read deadline needs its own semantics discussion — what it bounds
(a frame? a message? the wait for the next one?) — and should not be smuggled
in behind a write-side change.

## Benchmark

`BenchmarkWriteDeadline`, five byte text frames over `wstest.Pipe`, added in
this PR so the numbers are reproducible:

```
BenchmarkWriteDeadline/none-4        2185 ns/op     512 B/op     1 allocs/op
BenchmarkWriteDeadline/deadline-4    2437 ns/op     512 B/op     1 allocs/op
BenchmarkWriteDeadline/context-4     3597 ns/op    1304 B/op    12 allocs/op
```

A deadline costs the same as no bound at all; a context costs 11 more
allocations and 792 more bytes per frame.

The one remaining allocation is not on the deadline path — it is present with
no deadline at all. It belongs to the peer draining the other end of the pipe,
which `testing` attributes to the benchmark because Go's allocation statistics
are process wide. That caveat is why the zero-allocation claim is not asserted
end to end (see below).

On the server the change came from, replacing the per-frame context with a
deadline moved allocation over a 20s window from 689MB to zero sampled, CPU
samples for identical delivered work from 14.33s to 12.50s, `write(2)` from
63.1% to 73.1% of the profile, `runtime.mallocgc` from 4.82% to below the
reporting threshold, and mean delivery latency measured at the client from
4.20ms to 3.53ms. Those numbers are not reproducible from this repo; the
benchmark above is.

## Tests

- `TestWriteDeadlineAllocs` (in-package) measures the deadline machinery
  directly and asserts **zero**. It deliberately does not go through `Write`
  over a pipe: `testing.AllocsPerRun` counts allocations process wide, so a
  peer goroutine reading the other end would have its allocations attributed to
  the write, both hiding regressions and reporting a number that is not the
  write path's. It also batches 32 frames per run, because `AllocsPerRun`
  divides by the run count in integer arithmetic and would otherwise round away
  anything that allocates on only some frames.
- `TestDeadlineNanos` covers the ends of the `UnixNano` range and the unix
  epoch, which is a deadline long past rather than an absent one even though it
  is zero in the stored representation.
- `TestSetWriteDeadline` asserts the error out of every way a deadline can be
  hit: a write started past it, a second write while it is still past (expiry
  is a standing state, not a one-off), a deadline that passes mid-write, a
  deadline at the unix epoch, a ping and a close frame past it, and a fragment
  of a multi-frame message past it — all `os.ErrDeadlineExceeded`. It also
  asserts the errors that must *not* be the deadline: after a mid-write
  deadline has closed the connection the next write is `net.ErrClosed`, not the
  deadline again. Plus recovery — re-arming or clearing puts the connection
  back to work — and the year 2300, which must not expire at all.
- A `Close` whose close frame the deadline refuses must still tear the
  connection down rather than leave it dangling; that is asserted too.
- The mid-write case bounds its `Read` with a context so that a connection
  which wrongly stayed alive fails the test instead of hanging it until the
  package timeout.
- `TestWasmSetWriteDeadline` covers the js implementation, which previously had
  no test at all.

## Review notes

- `armWriteDeadline` reports bools rather than returning a stop closure, for
  the reason #565 gives on the read path: a closure over the connection is an
  allocation per frame.
- `stopWriteDeadline` is called from inside `writeFrame`'s error-mapping defer
  rather than from a defer of its own, because its return value — did the frame
  beat the timer — is what distinguishes `os.ErrDeadlineExceeded` from
  `net.ErrClosed`, and defer ordering has to put the disarm first.
- `armWriteDeadline` must not close the connection itself: `close` takes
  `writeFrameMu` for a client connection via `msgWriter.close`, and it runs
  with that lock held.

Refs #565, #566
