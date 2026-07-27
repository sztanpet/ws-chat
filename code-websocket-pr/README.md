# A `SetWriteDeadline` PR for coder/websocket

Everything needed to open a pull request against
[`github.com/coder/websocket`](https://github.com/coder/websocket), prepared
against **v1.8.15** (the latest release at the time of writing).

Nothing here is applied to ws-chat. The server keeps using upstream
websocket, for the reason in
[`../state/loadgen.md`](../state/loadgen.md): forking would take the module
out of `govulncheck`'s view, and that is a bad trade for RFC 6455 parsing
code however good the numbers are. If the change lands upstream, the
caller-side patch below is what ws-chat would then adopt.

## Files

| file | what it is |
|---|---|
| `websocket-setwritedeadline.patch` | **the PR.** `conn.go`, `write.go`, `ws_js.go` and a new `writedeadline_test.go`. Apply with `patch -p1` inside a v1.8.15 checkout. |
| `ws-chat-caller.patch` | the downstream half, for evidence only — how ws-chat's `conn.go` uses the new API. Not part of the PR. |

Reproducing the measurements needs both, plus a `replace` directive
pointing the server at the patched library.

---

## PR message

### Title

`conn: add SetWriteDeadline, an allocation-free way to bound a write`

### Body

`Write` bounds a write with a context, and for a server fanning one message
out to many connections that is the dominant cost of sending it. Bounding a
write costs a `context.WithTimeout` in the caller and a
`context.AfterFunc` in `setupWriteTimeout`, per frame — and a broadcast pays
it once per recipient, not once per message.

Measured on a WebSocket chat server holding 500 connections in one room at
~49,600 message deliveries a second, profiling the server itself: **about
700 bytes of garbage per delivered message, none of which is the message.**
99% of everything the server allocated under that load was the context and
timer machinery for write deadlines — `context.AfterFunc` and
`propagateCancel` under `setupWriteTimeout`, plus the caller's own
`WithTimeout`.

A deadline is an instant, and an instant does not need an allocation to
express. This adds:

```go
func (c *Conn) SetWriteDeadline(t time.Time) error
```

It stores the instant in an `atomic.Int64`, and `writeFrame` arms a
`*time.Timer` created once per connection — `Reset` and `Stop` only, neither
of which allocates. That is the approach `netconn.go` already takes for
`net.Conn` deadlines; this exposes the same idea on `Conn` itself, so a
caller that can express its bound as a deadline does not have to pay for a
context to carry it. A caller passing a non-cancellable context to `Write`
and setting the deadline here allocates **nothing** on the write path,
because `setupWriteTimeout` returns at its first line.

Nothing changes for existing callers: the deadline defaults to unset, and
with no deadline set `writeFrame` does one atomic load more than before.

#### What it is worth

The same server, same load, before and after — the deadline replacing the
per-frame context, measured with an out-of-process load generator so the
numbers are what a client sees, and with pprof on the server for the rest:

| | context per frame | `SetWriteDeadline` |
|---|---|---|
| garbage per delivered message | ~700 B | **~0** |
| allocations in a 20s window | 689 MB | **0 sampled** |
| CPU samples per 20s | 14.33s | 12.50s |
| `write(2)` share of CPU | 63.0% | 73.1% |
| `runtime.mallocgc` | 4.82% | below threshold |
| mean delivery latency | 4.20 ms | **3.53 ms** |
| p50 | 3.84 ms | 3.33 ms |

**13% less CPU for identical delivered work and 16% off mean latency**, and
the profile becomes very nearly the write syscall and nothing else, which is
what a fan-out server should look like. The remaining cost of the mechanism
is `armWriteDeadline` at ~2% of CPU, which is the timer `Reset`/`Stop` pair.

Under a deliberate overload (3000 connections, 90% talking, one room) the
allocation goes from 1.61 GB per 20s to 21 MB, most of the remainder being
connection setup rather than the write path. Throughput and latency there
are not comparable run to run — the population of dropped clients collapses
differently every time — so the steady-load column above is the one to read.

#### Semantics, and the one tradeoff

- An expired deadline closes the connection, the same as an expired context
  passed to `Write`, and the write that hit it is refused with
  `os.ErrDeadlineExceeded`.
- A zero `t` clears the deadline.
- **The deadline belongs to the connection, not to a call.** Where several
  goroutines write to one connection, the last to set it wins. That is the
  real difference against a per-call context and it is worth stating
  plainly; for a caller where every writer uses the same write timeout, as
  in a chat server's per-room write pumps, it does not arise.
- Frames written under one deadline all wait for that same instant rather
  than restarting the clock per frame, which is usually what a caller
  batching frames into one wakeup actually wants.

Two implementation notes a reviewer will want:

**The timer is armed by `writeFrame`, not by `SetWriteDeadline`.** That puts
the arming under `writeFrameMu`, together with the frame it bounds, so
setting a deadline cannot disturb a frame already in flight.

**`armWriteDeadline` must not close the connection itself, and does not.**
`close` takes `writeFrameMu` for a client connection, via
`msgWriter.close`, and `armWriteDeadline` runs with that lock held —
closing inline deadlocks. This was found by the new test, not by reasoning.
An already-expired deadline therefore refuses the frame and leaves the timer
to do the closing from its own goroutine, which holds nothing. It is also
why `armWriteDeadline` returns two bools instead of the stop func it would
rather return: a closure over `c` is a heap allocation on every frame, which
would defeat the point.

#### WASM

`ws_js.go` gets the same method, since the API should not differ by
platform. Writes there are handed to the browser and never block, so there
is nothing to interrupt: the deadline is checked when a write is attempted,
and a write attempted after it has passed fails with
`os.ErrDeadlineExceeded` instead of sending.

#### Tests

`writedeadline_test.go`:

- a live deadline does not interfere, and the zero time clears it;
- an expired deadline refuses the write with `os.ErrDeadlineExceeded` and
  the connection goes away;
- `TestSetWriteDeadlineAllocs` asserts the claim directly — a write with a
  deadline set allocates no more than the same write without one, via
  `testing.AllocsPerRun`. It reports 1 either way.

The existing suite passes under `-race`, and both `GOOS=js` and native
build.

---

## Reproducing the measurements

```sh
# 1. patched library
git clone https://github.com/coder/websocket && cd websocket
git checkout v1.8.15
patch -p1 < /path/to/websocket-setwritedeadline.patch
go test -race ./...            # library suite
GOOS=js GOARCH=wasm go build ./...

# 2. server against it
cd /path/to/ws-chat
git apply /path/to/ws-chat-caller.patch
go mod edit -replace github.com/coder/websocket=/path/to/websocket
make vendor && make test-race

# 3. steady load, with the debug listener enabled in config.hujson
go build -o ws-chat-patched .          # no -race, it dominates the profile
./ws-chat-patched -config bench.hujson &
make loadgen
./loadgen -url ws://127.0.0.1:8080/ws -conns 500 -speak 10 -rate 2 -for 60s

# 4. during the run: a 20s CPU profile bracketed by two allocation profiles
curl -s localhost:6060/debug/pprof/allocs -o allocs0.pb.gz
curl -s 'localhost:6060/debug/pprof/profile?seconds=20' -o cpu.pb.gz
curl -s localhost:6060/debug/pprof/allocs -o allocs1.pb.gz
go tool pprof -top -sample_index=alloc_space -base allocs0.pb.gz ./ws-chat-patched allocs1.pb.gz
```

Build the server **without** `-race`; the detector's overhead swamps what is
being measured. The load generator and the server sharing one machine is
also why the absolute throughput ceiling here (~300-390k deliveries/s on
four cores) says more about the box than about either side.
