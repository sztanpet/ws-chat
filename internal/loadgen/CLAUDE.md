# Load generation

`cmd/loadgen` is a **separate process** that points a crowd at a running
server: N connections, a configurable percentage of which talk, and a
report of what came back.

```
make loadgen
./loadgen -url ws://127.0.0.1:8080/ws -conns 5000 -speak 10 -rate 1 -for 60s
```

Separate process on purpose. A benchmark inside the server shares its
scheduler, its allocator and its GC and then reports on them as if they
were the server's, and it cannot be pointed at another machine. The
mechanics are `internal/loadgen`; the binary is flags and a signal
handler. It is built **without** `-race`: the instrument must not be the
bottleneck.

It is a client and nothing else — it negotiates a subprotocol like anybody
else and counts what lands on its own sockets, so the numbers are the ones
a client would see rather than counters read out of the thing being
measured. That means a second, client-side wire implementation:
`proto.Codec` encodes what the *server* sends and its `Encode` will not
take a `proto.Command`. Inbound, one flat struct covers every frame the
generator looks at, the same single-decode trick `proto.Command` plays on
the server.

Four things it has to get right, or the numbers lie:

- **Nobody speaks until everybody has connected.** A speaker fanning out
  into a room that is still filling up makes the delivery ratio a
  measurement of the ramp. The run clock starts at that barrier too, so
  `-ramp` does not eat into `-for`.
- **Listeners ping** (`-ping`, 30s). The server closes a connection that
  has sent nothing at all for `IdleTimeout`, and a listener is exactly
  that; without it a run over 90s measures its own disconnections.
- **Speakers are picked within a channel, not across the run.** Every Nth
  connection speaking aliases against round-robin channel assignment: 10%
  of 100 connections over 4 channels is every tenth one, which is only
  ever channels 1 and 3.
- **Latency is measured at the receiver**, from a send timestamp inside
  the message body — so it prices the whole path, and every message
  received is a sample, not just the sender's own. Samples go into a
  per-connection log-linear histogram (`Histogram`, 8 buckets per octave)
  merged when the connection stops: keeping every sample is gigabytes, and
  sharing one histogram measures cache-line contention in the generator.

`Expected` in the report is a denominator, not a promise: messages sent in
a channel times that channel's members. Received trails it by whatever was
in flight when the clock stopped, so ~99.8% is a clean run.

The interesting numbers are the failure ones. A drop shows up as
`losses  StatusPolicyViolation: too slow=N` — the server's own lag
disconnect, named — and refusals are counted by `ERR` code, so
`refusals throttled=N` is the rate limiter doing its job rather than a
mystery.

Both ends need file descriptors: one connection is a socket at each end,
so past ~1000 raise `ulimit -n` on both. And a generator sharing a machine
with the server competes with it — at a few hundred thousand deliveries a
second on one box, some of the latency being measured is the measuring.

Detail and numbers are in [`state/loadgen.md`](../../state/loadgen.md).
