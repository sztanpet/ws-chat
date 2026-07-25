package broadcast

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// The comparative harness. Every benchmark runs over the impls table in
// broadcast_test.go, so an alternative implementation is measured against
// the reference the moment it is added to that table.
//
// There are two families, because no single setup measures this honestly:
//
//   - BenchmarkFanout has no receiver goroutines running. It isolates the
//     send path — the member-set walk plus the enqueue — and it cannot
//     drop, because the buffers are sized to the run and emptied outside
//     the timer. This is the number to compare data structures with.
//
//   - BenchmarkFanoutLive gives every subscriber a live drainer goroutine,
//     so the receiver wakeups are inside the measurement. That is the
//     server's real shape: every subscriber is a write pump that has to be
//     scheduled, and an implementation that optimises the map walk while
//     leaving N goroutine wakeups in place has optimised the wrong half.
//
// The live family deliberately starts at 1000 subscribers. Below that a
// sender in a tight loop simply outruns its receivers — it fills a 256-slot
// buffer in microseconds, the subscriber is dropped for falling behind, and
// the run degenerates into timing broadcasts to a nearly empty member set.
// That is an artifact of an unthrottled sender, not a property of the
// implementation, and the small-N send cost is what BenchmarkFanout is for.
//
// The tell that a live run has degenerated is a ns/msg BELOW the same
// subscriber count's BenchmarkFanout figure: delivering to live receivers
// cannot be cheaper than delivering to idle buffers, so a smaller number
// means it was not delivering to all of them. Where the threshold sits
// depends on the core count — 1000 is the floor on a 4-core box, and it is
// worth re-checking the two families against each other on new hardware.
//
// Reading the numbers:
//
//   - ns/op    cost of one Broadcast call to `subs` subscribers.
//   - ns/msg   the same divided by the subscriber count — the per-delivery
//              cost, which is what has to stay flat as a channel grows.
//   - drops/op subscribers dropped per broadcast, i.e. the rate at which
//              receivers fell behind and had to reconnect. Near zero is a
//              clean run. A large value means the run is dominated by the
//              drop path rather than the send path and the ns figures say
//              little about fan-out. It is also the cheat detector: an
//              implementation that posts a good time by quietly throwing
//              messages away cannot hide it here.
//   - B/op     the reference implementation allocates nothing on the
//              broadcast path when nobody is dropped. An alternative that
//              allocates per message is losing before it starts.

// benchSubCounts spans the range that matters: a private conversation, a
// small room, a busy channel, and a channel big enough that the fan-out
// itself is the cost.
var benchSubCounts = []int{1, 10, 100, 1_000, 10_000}

// benchLiveSubCounts is benchSubCounts minus the counts where a flat-out
// sender trivially outruns its receivers. See the package-level note above.
var benchLiveSubCounts = []int{1_000, 10_000}

// benchBuffer is the outbound buffer for live benchmark subscribers — the
// same one the server would use, so the drop threshold under test is the
// real one.
const benchBuffer = DefaultBuffer

// benchFanoutBudget caps the total buffered messages across all subscribers
// in BenchmarkFanout, which sizes its buffers to the subscriber count so
// that nothing can ever fill. 1<<20 slices is ~16MB of channel buffer.
const benchFanoutBudget = 1 << 20

// benchMsg is a plausible chat frame. Implementations share it by reference,
// so its size should not matter — if an implementation's numbers move with
// it, it is copying, which is worth knowing.
var benchMsg = []byte(`MSG {"nick":"someone","channel":"#general","data":"hello world"}`)

// benchBatch is the number of frames a drainer takes per wakeup. A real
// write pump would size this to what it can coalesce into one socket
// write.
const benchBatch = 16

// drained builds a broadcaster with n subscribers, each with a goroutine
// discarding its messages. The returned stop function closes the
// broadcaster and waits for every drainer to exit.
func drained(b *testing.B, mk func(int) Broadcaster, n, size int) (Broadcaster, func()) {
	b.Helper()
	bc := mk(size)

	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go drainer(bc, bc.Subscribe(), &wg)
	}

	return bc, func() {
		bc.Close()
		wg.Wait()
	}
}

// drainer receives and discards until the subscription ends for good. A
// subscriber dropped for falling behind immediately resubscribes, exactly
// like a real client reconnecting.
//
// That reconnect is load-bearing for the benchmark, not decoration. A drop
// is permanent, so without it a single scheduler hiccup removes a
// subscriber for the rest of the run and the member set bleeds down toward
// empty — at which point the benchmark is timing broadcasts to a map with
// nothing in it and reporting a wonderful number. An earlier version of
// this file did exactly that and claimed 8.65 ns/op for a 100-subscriber
// fan-out.
func drainer(bc Broadcaster, s Sub, wg *sync.WaitGroup) {
	defer wg.Done()
	dst := make([][]byte, benchBatch)
	for {
		if _, err := s.Recv(dst); err == nil {
			continue
		} else if !errors.Is(err, ErrLagged) {
			return // orderly Close: we are done
		}
		// Resubscribe. Once the broadcaster is closed this hands back an
		// already-finished Sub, so the next pass exits the loop.
		s = bc.Subscribe()
	}
}

// warm runs the fan-out untimed until the drainers are scheduled and their
// buffers are empty. A cold run's first few thousand broadcasts outrun
// receiver goroutines that have not been given a P yet, and the timed
// region should not start in the middle of that reconnect storm.
func warm(bc Broadcaster) {
	for range 4 * benchBuffer {
		bc.Broadcast(benchMsg)
	}
	time.Sleep(20 * time.Millisecond)
}

// report turns the raw timing into the per-delivery cost and the drop rate.
func report(b *testing.B, subs int, dropped int64) {
	b.Helper()
	deliveries := float64(b.N) * float64(subs)
	if deliveries > 0 {
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/deliveries, "ns/msg")
	}
	b.ReportMetric(float64(dropped)/float64(b.N), "drops/op")
}

// fanoutBuffer sizes a subscriber's buffer so that BenchmarkFanout can run
// many iterations between drains without any subscriber filling up.
func fanoutBuffer(subs int) int {
	return max(benchFanoutBudget/subs, benchBuffer)
}

// drainN takes exactly count messages off a subscription.
//
// The count has to be known: Recv blocks, and there is no non-blocking
// variant on purpose — a real write pump always wants to block. Stopping
// on a short batch instead would look like it works and then deadlock
// whenever the pending count happens to be an exact multiple of the batch
// size, which is what the first version of this did.
func drainN(s Sub, count int) {
	dst := make([][]byte, benchBatch)
	for got := 0; got < count; {
		n, err := s.Recv(dst)
		if err != nil {
			return
		}
		got += n
	}
}

// BenchmarkFanout measures the send path alone: no receiver goroutines, so
// no wakeups and no scheduler noise, just the member-set walk and the
// enqueue. Buffers are sized to the run and emptied outside the timer, so a
// drop here is impossible by construction and is treated as a harness bug.
func BenchmarkFanout(b *testing.B) {
	for _, impl := range impls {
		for _, subs := range benchSubCounts {
			b.Run(fmt.Sprintf("%s/subs=%d", impl.name, subs), func(b *testing.B) {
				size := fanoutBuffer(subs)
				bc := impl.new(size)
				defer bc.Close()

				list := make([]Sub, subs)
				for i := range list {
					list[i] = bc.Subscribe()
				}
				drainEvery := size / 2

				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					if i > 0 && i%drainEvery == 0 {
						// Untimed: the drain is harness bookkeeping, and
						// keeping it out of the measurement is the whole
						// reason the buffers are sized this way.
						b.StopTimer()
						for _, s := range list {
							drainN(s, drainEvery)
						}
						b.StartTimer()
					}
					bc.Broadcast(benchMsg)
				}
				b.StopTimer()

				if dropped := bc.Drops(); dropped > 0 {
					b.Fatalf("%d drops in a benchmark that cannot drop: sizing is wrong", dropped)
				}
				report(b, subs, 0)
			})
		}
	}
}

// BenchmarkFanoutLive is the production shape: one sender, N subscribers,
// every one of them a live goroutine on the receiving end, so the receiver
// wakeups are inside the measurement.
func BenchmarkFanoutLive(b *testing.B) {
	for _, impl := range impls {
		for _, subs := range benchLiveSubCounts {
			b.Run(fmt.Sprintf("%s/subs=%d", impl.name, subs), func(b *testing.B) {
				bc, stop := drained(b, impl.new, subs, benchBuffer)
				defer stop()
				warm(bc)

				before := bc.Drops()
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					bc.Broadcast(benchMsg)
				}
				b.StopTimer()
				report(b, subs, bc.Drops()-before)
			})
		}
	}
}

// BenchmarkFanoutSaturated is the overload case: concurrent senders, live
// receivers, and no attempt to keep the two in balance. With every P busy
// sending there is no core left to receive on, so buffers fill and
// subscribers get dropped and reconnect — deliberately. This does not
// measure clean fan-out and the ns/msg here should not be compared with
// the other two families.
//
// What it does measure is what the server does in a spam wave: how an
// implementation behaves when the drop path is hot and membership is
// changing under concurrent broadcasts. drops/op is the headline output,
// not a warning. In the reference implementation dropping takes the write
// lock, which serializes against every concurrent broadcast, so this is
// the benchmark that would justify a lock-free member set.
func BenchmarkFanoutSaturated(b *testing.B) {
	for _, impl := range impls {
		for _, subs := range []int{100, 1_000} {
			b.Run(fmt.Sprintf("%s/subs=%d", impl.name, subs), func(b *testing.B) {
				bc, stop := drained(b, impl.new, subs, benchBuffer)
				defer stop()
				warm(bc)

				before := bc.Drops()
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						bc.Broadcast(benchMsg)
					}
				})
				b.StopTimer()
				report(b, subs, bc.Drops()-before)
			})
		}
	}
}

// BenchmarkSubscribeUnsubscribe measures pure membership churn with nothing
// else going on — the join/part path, against member sets of three sizes.
//
// What a join costs is not only bookkeeping: an implementation that gives
// every subscriber its own buffer pays for that allocation here, and one
// with shared storage does not. That is a real difference between the two
// designs, so it is left in the measurement rather than tuned away.
func BenchmarkSubscribeUnsubscribe(b *testing.B) {
	for _, impl := range impls {
		for _, subs := range []int{0, 100, 1_000} {
			b.Run(fmt.Sprintf("%s/existing=%d", impl.name, subs), func(b *testing.B) {
				bc, stop := drained(b, impl.new, subs, benchBuffer)
				defer stop()

				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					bc.Subscribe().Close()
				}
			})
		}
	}
}

// BenchmarkChurnUnderLoad is the realistic mix: people joining and leaving
// while the channel is busy. Membership changes take the write lock and
// broadcasts take the read lock, so this is where an implementation that
// made Broadcast cheap by making Subscribe expensive gets caught.
func BenchmarkChurnUnderLoad(b *testing.B) {
	for _, impl := range impls {
		for _, senders := range []int{1, 4} {
			b.Run(fmt.Sprintf("%s/senders=%d", impl.name, senders), func(b *testing.B) {
				const subs = 1_000
				bc, stop := drained(b, impl.new, subs, benchBuffer)
				warm(bc)

				var wg sync.WaitGroup
				quit := make(chan struct{})
				for range senders {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for {
							select {
							case <-quit:
								return
							default:
							}
							bc.Broadcast(benchMsg)
						}
					}()
				}

				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					bc.Subscribe().Close()
				}
				b.StopTimer()

				close(quit)
				wg.Wait()
				stop()
			})
		}
	}
}
