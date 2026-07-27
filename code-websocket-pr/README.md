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

A server writing to many connections needs every write bounded, so that one
client which has stopped reading cannot wedge the goroutine writing to it.
The only way to express that bound today is a context, and a context costs an
allocation per frame — which, for one message fanned out to a room, is an
allocation per recipient. This adds a way to say the same thing with a
deadline, which costs nothing.

Concretely: `Write` takes a context, so the bound is a `context.WithTimeout`
in the caller plus, for any cancelable context, a `context.AfterFunc` here.
Profiling a chat server at ~49,600 deliveries/s across 500 connections put
that at about **700 bytes of garbage per delivered message**, none of it the
message, and 99% of everything the server allocated.

A deadline is an instant, and an instant needs no allocation to express:

```go
func (c *Conn) SetWriteDeadline(t time.Time) error
```

It stores the instant in an `atomic.Int64`, and `writeFrame` arms a
`*time.Timer` created once per connection, using only `Reset` and `Stop` —
`netconn.go` already makes stopped timers this way for `net.Conn` deadlines.
Combined with a non-cancelable context on `Write`, which #566 made free, the
write path allocates nothing. The deadline is unset by default, so existing
callers are unaffected: with none set, `writeFrame` does one more atomic load
than before.

An expired deadline closes the connection, as an expired context does, and
refuses that frame with `os.ErrDeadlineExceeded`. A zero `t` clears it. The
timer is armed under `writeFrameMu` so that setting a deadline cannot disturb
a frame in flight; it is also why an expired deadline cannot close inline and
leaves that to the timer's goroutine, since `close` takes the same lock via
`msgWriter.close`.

#### Evidence

Same server and load, per-frame context replaced by a deadline, measured at
`9f84ce4`. Latency is measured at the client, the rest is pprof on the
server, built without `-race`.

| | context per frame | deadline |
|---|---|---|
| garbage per delivered message | ~700 B | **~0** |
| allocated in a 20s window | 689 MB | **0 sampled** |
| CPU samples per 20s | 14.33s | 12.65s |
| `write(2)` share of CPU | 63.1% | 70.9% |
| mean delivery latency | 4.20 ms | **3.55 ms** |

12% less CPU for identical delivered work and 15% off mean latency. Mean
latency held 3.53-3.55 ms across four deadline runs where every baseline run
of this load sat at 3.75-4.24 ms; the bands do not overlap.
`TestSetWriteDeadlineAllocs` asserts the allocation claim in the suite
itself, via `testing.AllocsPerRun`.

#### Tradeoff

The deadline belongs to the connection rather than to a call, so where
several goroutines write to one connection the last to set it wins. Frames
written under one deadline share that instant instead of restarting the
clock, which is usually what a caller batching frames into one wakeup wants.
It does not arise for callers whose writers share a single write timeout, but
it is a real narrowing against a per-call context.

#### Alternatives considered

- **Memoizing the armed context** across `Write` calls. Measured: it removes
  the per-frame cost when a caller batches, nothing when each wakeup carries
  one frame, and it must keep the deadline armed past the last frame.
- **An OS deadline on the underlying socket**, set from outside the library.
  Also free, but it aborts mid-frame and leaves the stream at an unknown
  offset where this closes cleanly.

#### Not in this PR

- No `SetReadDeadline`. The symmetric change wants its own thought about
  `CloseRead` and ping handling.
- No `net.Conn` on `Conn`. This is one method, not a step toward that;
  `NetConn` stays the way to get it.

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
