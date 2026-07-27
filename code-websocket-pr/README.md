# A `SetWriteDeadline` PR for coder/websocket

Everything for a pull request against
[`github.com/coder/websocket`](https://github.com/coder/websocket).

**The branch is pushed** to the fork at `../coder-websocket`
(`origin` = `git@github.com:sztanpet/websocket.git`):

| | |
|---|---|
| branch | `feat/conn-set-write-deadline` |
| commit | `9f84ce4`, off `master` at `9c8faad` |
| contents | 251 insertions, no deletions, across `conn.go`, `write.go`, `ws_js.go` and a new `writedeadline_test.go` |

Nothing here is applied to ws-chat, which still builds against upstream
v1.8.15. The reason is in [`../state/loadgen.md`](../state/loadgen.md):
running a fork takes the module out of `govulncheck`'s view, which is a bad
trade for RFC 6455 parsing code however good the numbers are. If this lands
upstream, `ws-chat-caller.patch` is what ws-chat adopts.

## Files

| file | what it is |
|---|---|
| `websocket-setwritedeadline.patch` | the library change as `git diff master..9f84ce4`, should you need to reapply it. An earlier hand-rolled version of the same change was verified to apply to a clean v1.8.15, whose `conn.go`, `write.go` and `ws_js.go` are byte-identical to `master` at `9c8faad`. |
| `ws-chat-caller.patch` | the downstream half, evidence only — how ws-chat's `conn.go` uses the API. Not part of the PR. |

## Verification run (the four passes in the library's AGENTS.md)

| pass | result |
|---|---|
| 1. `go test ./...` | pass |
| 2. `go vet ./...` | clean |
| 3. both platforms | native and `GOOS=js GOARCH=wasm` build |
| 4. `AUTOBAHN=1 go test` | protocol cases pass; one flake in the suite's goroutine check, see below |
| also | full suite under `-race`, `gofmt` clean |

### Pass 4 and the goroutine-count flake

Autobahn's protocol cases pass, which is expected since nothing here
touches framing, masking or compression. Worth recording is what happened
around them, because it looked like a regression and was not.

`main_test.go`'s `TestMain` asserts `runtime.NumGoroutine() == 1` at
process exit. Three runs of `AUTOBAHN=1 go test`, each ~5.5 minutes:

| run | result |
|---|---|
| branch, 1st | protocol `PASS`, then exit 1 — "goroutine leak detected, expected 1 but got 2" |
| `master`, unmodified | clean, `ok`, exit 0 |
| branch, 2nd | clean, `ok`, exit 0 |

**The failure is the check racing process teardown, not a leak.** It
reported 2 goroutines and then dumped only 1 — goroutine 1 itself — so the
second had already exited between `NumGoroutine()` and `runtime.Stack()`.
Two further facts agree: `go test ./...` passes that same check on the
branch while *running* the new tests, including the expired-deadline path
that arms a 1ns timer and lets it fire `c.close()` on a fresh goroutine; and
in the autobahn configuration the new code is inert, because autobahn never
calls `SetWriteDeadline`, so `writeDeadline` stays 0, `armWriteDeadline`
returns at its first branch, and the timer stays stopped from construction.
There is no path from an extra stopped `time.AfterFunc` to a live
goroutine — a stopped timer sits in no timer heap and runs nothing.

The residual uncertainty, stated plainly: `master` was run once and did not
flake, so it was not *established* that the check flakes independently of
this change. If a reviewer sees it in CI, this is the explanation to check
first, and the count-versus-dump discrepancy is the thing to look for in
the output.

---

## PR description

### Title

`feat(conn): add SetWriteDeadline to bound writes without allocating`

### Body

Bounding a write means passing a context, so a caller that wants a write
deadline allocates one per frame: a `context.WithTimeout` on its side and,
for any cancelable context, a `context.AfterFunc` on this side. A server
fanning one message out to many connections pays that **per recipient**
rather than per message, which makes it the dominant cost of sending.

Profiling a WebSocket chat server holding 500 connections in one room at
~49,600 message deliveries a second: about **700 bytes of garbage per
delivered message**, none of it the message, and 99% of everything the
server allocated under that load. `context.AfterFunc` and
`propagateCancel` beneath `setupWriteTimeout`, plus the caller's own
`WithTimeout` and its timer.

A deadline is an instant, and an instant needs no allocation to express:

```go
func (c *Conn) SetWriteDeadline(t time.Time) error
```

It stores the instant in an `atomic.Int64`; `writeFrame` arms a
`*time.Timer` created once per connection, using only `Reset` and `Stop`.
That is what `netconn.go` already does for `net.Conn` deadlines — this
exposes the same idea on `Conn`, so a caller that can name its bound as a
deadline need not buy a context to carry it. Paired with a non-cancelable
context on `Write`, which #566 already made free, the write path allocates
nothing at all.

Existing callers are unaffected. The deadline is unset by default, and with
none set `writeFrame` does one atomic load more than before.

#### Evidence

Same server, same load, per-frame context replaced by a deadline, measured
against this branch at `9f84ce4`. Latency is measured at the client by an
out-of-process load generator; the rest is pprof on the server. Server built
without `-race`, which otherwise swamps what is being measured.

| | context per frame | `SetWriteDeadline` |
|---|---|---|
| garbage per delivered message | ~700 B | **~0** |
| allocated in a 20s window | 689 MB | **0 sampled** |
| CPU samples per 20s | 14.33s | 12.65s |
| `write(2)` share of CPU | 63.1% | 70.9% |
| `runtime.mallocgc` | 4.82% | 0.32% |
| mean delivery latency | 4.20 ms | **3.55 ms** |
| p50 | 3.84 ms | 3.58 ms |

12% less CPU for identical delivered work, 15% off mean latency, and a
profile that becomes very nearly the write syscall and nothing else. What
remains of the mechanism is `armWriteDeadline` at ~1.7% of CPU, which is the
`Reset`/`Stop` pair.

Those figures are one run of the deadline build against one run of the
baseline, both at 100% delivery, so read them as bands rather than
constants: across four runs the deadline build's mean latency stayed within
3.53-3.55 ms and every baseline run of this load sat at 3.75-4.24 ms. The
bands do not overlap. Allocation is not a band — the delta is zero sampled
bytes and zero sampled objects over the window, repeatedly.

Under a deliberate overload — 3000 connections, 90% of them talking, one
room — allocation over 20s falls from 1.61 GB to 21 MB, and most of that
remainder is connection setup rather than the write path. Throughput and
latency there are not comparable between runs, because the population of
dropped clients collapses differently every time, so the steady-load table
above is the one to read.

`TestSetWriteDeadlineAllocs` asserts the claim in the test suite rather than
leaving it to a profile: a write with a deadline set allocates no more than
the same write without one, via `testing.AllocsPerRun`. It reports 1 either
way.

#### Semantics and tradeoffs

- An expired deadline closes the connection, as an expired context passed
  to `Write` does, and the write that hit it is refused with
  `os.ErrDeadlineExceeded`.
- A zero `t` clears it.
- **The deadline belongs to the connection, not to a call.** Where several
  goroutines write to one connection, the last to set it wins. This is the
  real difference from a per-call context. For a caller whose writers all
  use the same write timeout — a chat server's per-room write pumps, say —
  it does not arise, but it is a genuine narrowing and worth deciding on
  deliberately.
- Frames written under one deadline all wait for that same instant instead
  of restarting the clock per frame, which is usually what a caller
  batching frames into one wakeup wants.

Two implementation points a reviewer will ask about:

**The timer is armed by `writeFrame`, not by `SetWriteDeadline`.** That puts
the arming under `writeFrameMu` together with the frame it bounds, so
setting a deadline cannot disturb a frame already in flight.

**`armWriteDeadline` deliberately does not close the connection.** `close`
takes `writeFrameMu` for a client connection via `msgWriter.close`, and
`armWriteDeadline` runs with that lock held, so closing inline deadlocks —
found by the new test, not by reading the code. An already-expired deadline
therefore refuses the frame and leaves the closing to the timer's own
goroutine, which holds nothing. It reports bools instead of returning a
stop closure for the reason #565 gives on the read path: a closure over the
connection is an allocation per frame, which would defeat the purpose.

#### Alternatives considered

- **Memoizing the armed context** — skip re-arming when `Write` is called
  repeatedly with the same context, which is what a batching caller does.
  Built and measured: it removes the per-frame amplification (allocation per
  context created dropped from 28.0 to 7.69, the figure a server at rest
  shows) but nothing at all when each wakeup carries a single frame, which
  is the common case. It also changes behaviour, since the deadline must
  then stay armed after the last frame until the context is canceled. Worse
  on both counts than a deadline.
- **Reaching the underlying `net.Conn`** and setting an OS-level deadline
  from outside the library. No allocation either, but it aborts mid-frame
  and leaves the WebSocket stream at an unknown offset, where this closes
  the connection cleanly.

#### What this PR does not do

- No read deadline. `SetReadDeadline` would be the symmetric change and is
  deliberately left out: the read path's per-frame cost was already
  addressed by #565 and #566, and a read deadline wants its own thought
  about interaction with `CloseRead` and ping handling.
- Does not implement `net.Conn` on `Conn`, and is not a step toward it.
  `NetConn` remains the way to get that.
- Does not change any existing behaviour, signature or default. Nothing in
  the library calls `SetWriteDeadline` itself.
- Does not touch framing, masking or compression, so no wire bytes change.

Refs #565, #566

---

## Reproducing the measurements

```sh
# 1. the library, on the prepared branch
cd ../coder-websocket
git checkout feat/conn-set-write-deadline
go test ./... && go vet ./... && GOOS=js GOARCH=wasm go build ./...

# 2. the server against it
cd ../ws-chat
git apply code-websocket-pr/ws-chat-caller.patch
go mod edit -replace github.com/coder/websocket=../coder-websocket
make vendor && make test-race

# 3. steady load. Build WITHOUT -race, and enable DebugAddr in the config.
GOEXPERIMENT=jsonv2 go build -o ws-chat-patched .
./ws-chat-patched -config bench.hujson &
make loadgen
./loadgen -url ws://127.0.0.1:8080/ws -conns 500 -speak 10 -rate 2 -for 60s

# 4. during the run: a 20s CPU profile bracketed by two allocation profiles
curl -s localhost:6060/debug/pprof/allocs -o allocs0.pb.gz
curl -s 'localhost:6060/debug/pprof/profile?seconds=20' -o cpu.pb.gz
curl -s localhost:6060/debug/pprof/allocs -o allocs1.pb.gz
go tool pprof -top -sample_index=alloc_space -base allocs0.pb.gz ./ws-chat-patched allocs1.pb.gz
```

Afterwards, `git checkout -- conn.go go.mod go.sum vendor/` puts ws-chat
back on upstream.

The load generator sharing a machine with the server is why the absolute
ceiling here (~300-390k deliveries/s on four cores) describes the box
rather than either side. The comparisons above are all same-box,
same-session, and the steady load is deterministic enough to repeat.
