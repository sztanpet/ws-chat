# Load generation — working state

Track for `internal/loadgen` and `cmd/loadgen`: an out-of-process crowd
pointed at a running server. Spec: none — this started from "benchmark the
server with a separate process, a configurable number of connections of
which a configurable percentage speak".

## Done

- **`c811c13` loadgen: add the latency histogram.** Log-linear, 8 buckets
  per octave of microseconds, 240 counters, ≤12.5% error, with count/sum/max
  kept exactly. One per connection, merged when it stops.
- **`b7165e6` loadgen: add the connection driver.** `Config`/`Run`/`Result`,
  the client, the client-side codecs, and the tests — including a stub chat
  server so the generator's arithmetic is tested without the real server's
  timing attached.
- **`cmd/loadgen` + `make loadgen` + docs.** Flags, `signal.NotifyContext`
  so Ctrl-C still prints the report, `.gitignore` for the binary, the
  `### Load generation` section in CLAUDE.md.

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

The third is the overload shape and it is the useful one: the server's lag
disconnect shows up by name in `losses`. It is also where the generator is
competing with the server for the same cores, so treat those latencies as
an upper bound on this box rather than a server number.

## Pending

- Two machines. Everything above shares a laptop with the server, so the
  numbers say more about the box than the code past ~100k deliveries/s.
- Nothing exercises `PRIVMSG`, moderation or reconnect churn — the
  generator connects, joins, talks and listens, and that is all.
- No CSV/JSON output. The report is for reading.
