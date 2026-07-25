package broadcast

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The comparative harness. Every benchmark runs over the impls table in
// broadcast_test.go, so an alternative implementation is measured against
// the reference the moment it is added to that table.
//
// There are two families, because no single setup measures this honestly:
//
//   - BenchmarkFanout has no receiver goroutines running. It isolates what
//     the SENDER pays — the member-set walk plus the enqueue — and it
//     cannot drop, because storage is sized to the run and emptied outside
//     the timer. Its ns/msg column is meaningless for an implementation
//     with shared storage: that work is not being divided, it is being
//     postponed to a reader this family never runs.
//
//   - BenchmarkFanoutLive gives every subscriber a live drainer goroutine
//     and counts what actually arrives. This is the one that decides
//     anything.
//
// Counting deliveries rather than Broadcast calls is not pedantry. A
// broadcaster that drops on overrun can always be made to look fast by
// running the sender past what the receivers can absorb — the drop path is
// cheaper than the send path, so the faster it fails the better it scores.
// The delivered/sent ratio is what stops that: it is the fraction of
// intended deliveries that a subscriber actually received, and any run
// where it is not close to 1.0 is a run that bought its ns/msg by throwing
// messages away.
//
// The live family deliberately starts at 1000 subscribers. Below that a
// sender in a tight loop simply outruns its receivers — it fills a 256-slot
// buffer in microseconds, the subscriber is dropped for falling behind, and
// the run degenerates into timing broadcasts to a nearly empty member set.
// That is an artifact of an unthrottled sender, not a property of the
// implementation. Where the threshold sits depends on the core count, so on
// new hardware watch delivered/sent rather than trusting the constant.
//
// Reading the numbers:
//
//   - ns/op          cost of one Broadcast call to `subs` subscribers.
//   - ns/msg         elapsed time per message actually DELIVERED. In
//                    BenchmarkFanout, where nothing receives, it is per
//                    message enqueued instead.
//   - delivered/sent deliveries that happened over deliveries intended.
//                    1.0 is a clean run. Anything less means the ns/msg
//                    beside it was bought by dropping subscribers.
//   - drops/op       subscribers dropped per broadcast, i.e. how often
//                    receivers fell behind and had to reconnect.
//   - B/op           both implementations allocate nothing per broadcast in
//                    the steady state. An alternative that allocates per
//                    message is losing before it starts.

// benchSubCounts spans the range that matters: a private conversation, a
// small room, a busy channel, and a channel big enough that the fan-out
// itself is the cost.
var benchSubCounts = []int{1, 10, 100, 1_000, 10_000}

// benchLiveSubCounts is benchSubCounts minus the counts where a flat-out
// sender trivially outruns its receivers. See the package-level note above.
var benchLiveSubCounts = []int{1_000, 10_000}

// benchBuffer is the slack a subscriber gets in the live benchmarks — the
// same the server would use, so the drop threshold under test is the real
// one.
const benchBuffer = DefaultBuffer

// benchFanoutBudget caps the total buffered messages across all subscribers
// in BenchmarkFanout, which sizes storage to the subscriber count so that
// nothing can ever fill. 1<<20 slices is ~16MB.
const benchFanoutBudget = 1 << 20

// benchMsg is a plausible chat frame. Implementations share it by reference,
// so its size should not matter — if an implementation's numbers move with
// it, it is copying, which is worth knowing.
var benchMsg = []byte(`MSG {"nick":"someone","channel":"#general","data":"hello world"}`)

// benchBatch is the number of frames a drainer takes per wakeup. A real
// write pump would size this to what it can coalesce into one socket write.
const benchBatch = 16

// counter is a per-drainer delivery count on its own cache line, so ten
// thousand drainers incrementing do not serialize on one.
type counter struct {
	n atomic.Int64
	_ [56]byte
}

// rig is a broadcaster with live drainers attached and a running count of
// what they have received.
type rig struct {
	bc       Broadcaster
	counters []*counter
	total    *atomic.Int64 // non-nil only when the sender needs to pace
	wg       sync.WaitGroup
}

func newRig(mk func(int) Broadcaster, subs, size int) *rig {
	return newRigPaced(mk, subs, size, false)
}

// newRigPaced optionally adds a shared delivery total. It is off by default
// because every drainer bumping one counter is contention that does not
// belong in a fan-out measurement; only the paced benchmark needs a number
// the sender can read cheaply.
func newRigPaced(mk func(int) Broadcaster, subs, size int, paced bool) *rig {
	r := &rig{bc: mk(size), counters: make([]*counter, subs)}
	if paced {
		r.total = &atomic.Int64{}
	}
	for i := range r.counters {
		c := &counter{}
		r.counters[i] = c
		r.wg.Add(1)
		go drainer(r.bc, r.bc.Subscribe(), c, r.total, &r.wg)
	}
	return r
}

// delivered totals what every drainer has received so far.
func (r *rig) delivered() int64 {
	var total int64
	for _, c := range r.counters {
		total += c.n.Load()
	}
	return total
}

func (r *rig) stop() {
	r.bc.Close()
	r.wg.Wait()
}

// drainer receives and discards until the subscription ends for good,
// counting what it got. A subscriber dropped for falling behind immediately
// resubscribes, exactly like a real client reconnecting.
//
// That reconnect is load-bearing for the benchmark, not decoration. A drop
// is permanent, so without it a single scheduler hiccup removes a
// subscriber for the rest of the run and the member set bleeds down toward
// empty — at which point the benchmark is timing broadcasts to a map with
// nothing in it and reporting a wonderful number. An earlier version of
// this file did exactly that and claimed 8.65 ns/op for a 100-subscriber
// fan-out.
func drainer(bc Broadcaster, s Sub, c *counter, total *atomic.Int64, wg *sync.WaitGroup) {
	defer wg.Done()
	dst := make([][]byte, benchBatch)
	for {
		n, err := s.Recv(dst)
		if n > 0 {
			c.n.Add(int64(n))
			if total != nil {
				total.Add(int64(n))
			}
		}
		if err == nil {
			continue
		}
		if !errors.Is(err, ErrLagged) {
			return // orderly Close: we are done
		}
		// Resubscribe. Once the broadcaster is closed this hands back an
		// already-finished Sub, so the next pass exits the loop.
		s = bc.Subscribe()
	}
}

// warm runs the fan-out untimed until the drainers are scheduled and their
// queues are empty. A cold run's first few thousand broadcasts outrun
// receiver goroutines that have not been given a P yet, and the timed
// region should not start in the middle of that reconnect storm.
func warm(bc Broadcaster) {
	for range 4 * benchBuffer {
		bc.Broadcast(benchMsg)
	}
	time.Sleep(20 * time.Millisecond)
}

// reportSend is for the family with no receivers: cost per message handed
// to the broadcaster.
func reportSend(b *testing.B, subs int) {
	b.Helper()
	if sent := float64(b.N) * float64(subs); sent > 0 {
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/sent, "ns/msg")
	}
}

// reportDelivery is for the families with live receivers: cost per message
// that actually arrived, and what fraction of the intended ones did.
func reportDelivery(b *testing.B, subs int, dropped, delivered int64) {
	b.Helper()
	if delivered > 0 {
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(delivered), "ns/msg")
	}
	if sent := float64(b.N) * float64(subs); sent > 0 {
		b.ReportMetric(float64(delivered)/sent, "delivered/sent")
	}
	b.ReportMetric(float64(dropped)/float64(b.N), "drops/op")
}

// fanoutBuffer sizes storage so that BenchmarkFanout can run many
// iterations between drains without any subscriber filling up.
func fanoutBuffer(subs int) int {
	return max(benchFanoutBudget/subs, benchBuffer)
}

// needsDrain reports whether an implementation's subscribers have to be
// emptied to stop them being dropped for lagging.
//
// Implementations that give every subscriber its own queue do: leave one
// unread and it fills. Ones with shared storage do not — an unread
// subscriber costs the sender nothing and is only noticed when it next
// reads, which in BenchmarkFanout is never.
//
// This is not a nicety. The untimed drain is O(b.N x subs), so an
// implementation whose timed op is a hundred times cheaper gets a b.N a
// hundred times larger and the drain stops finishing at all.
func needsDrain(bc Broadcaster) bool {
	_, shared := bc.(*Ring)
	return !shared
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
// enqueue.
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
				mustDrain := needsDrain(bc)

				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					if mustDrain && i > 0 && i%drainEvery == 0 {
						// Untimed: the drain is harness bookkeeping, and
						// keeping it out of the measurement is the whole
						// reason storage is sized this way.
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
				reportSend(b, subs)
			})
		}
	}
}

// BenchmarkFanoutLive is the production shape: one sender, N subscribers,
// every one of them a live goroutine on the receiving end, so the receiver
// wakeups are inside the measurement and only messages that reach a
// subscriber count.
func BenchmarkFanoutLive(b *testing.B) {
	for _, impl := range impls {
		for _, subs := range benchLiveSubCounts {
			b.Run(fmt.Sprintf("%s/subs=%d", impl.name, subs), func(b *testing.B) {
				r := newRig(impl.new, subs, benchBuffer)
				warm(r.bc)

				drops0, delivered0 := r.bc.Drops(), r.delivered()
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					r.bc.Broadcast(benchMsg)
				}
				b.StopTimer()

				drops, delivered := r.bc.Drops()-drops0, r.delivered()-delivered0
				r.stop()
				reportDelivery(b, subs, drops, delivered)
			})
		}
	}
}

// BenchmarkFanoutPaced is the comparison that actually settles anything.
//
// The other live benchmark runs the sender flat out, which is fine for an
// implementation whose Broadcast is expensive enough to pace itself against
// its own receivers, and useless for one whose Broadcast is O(1): the
// sender laps everyone and delivered/sent collapses, so the two are being
// measured in completely different regimes and their ns/msg cannot be
// compared.
//
// Here the sender is held to a bounded lead over what has actually been
// received, which is what a real chat server has anyway — the send rate is
// set by people typing, not by a loop. Both implementations then deliver
// essentially everything, and ns/msg is a like-for-like cost per delivered
// message.
func BenchmarkFanoutPaced(b *testing.B) {
	// How far ahead of the receivers the sender may get, per subscriber.
	// Well under benchBuffer so that pacing, not dropping, is what bounds
	// it.
	const lead = 64

	for _, impl := range impls {
		for _, subs := range benchLiveSubCounts {
			b.Run(fmt.Sprintf("%s/subs=%d", impl.name, subs), func(b *testing.B) {
				r := newRigPaced(impl.new, subs, benchBuffer, true)
				warm(r.bc)

				drops0, delivered0 := r.bc.Drops(), r.delivered()
				base := r.total.Load()
				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					r.bc.Broadcast(benchMsg)

					sent := int64(i+1) * int64(subs)
					for sent-(r.total.Load()-base) > int64(subs)*lead {
						runtime.Gosched()
					}
				}
				b.StopTimer()

				drops, delivered := r.bc.Drops()-drops0, r.delivered()-delivered0
				r.stop()
				reportDelivery(b, subs, drops, delivered)
			})
		}
	}
}

// BenchmarkFanoutSaturated is the overload case: concurrent senders, live
// receivers, and no attempt to keep the two in balance. With every P busy
// sending there is no core left to receive on, so storage fills and
// subscribers get dropped and reconnect — deliberately. delivered/sent is
// well below 1.0 here by construction, and the ns/msg should not be
// compared with the other two families.
//
// What it measures is what the server does in a spam wave: how an
// implementation behaves when the drop path is hot and membership is
// changing under concurrent broadcasts. How much still gets through, and at
// what cost, is the comparison.
func BenchmarkFanoutSaturated(b *testing.B) {
	for _, impl := range impls {
		for _, subs := range []int{100, 1_000} {
			b.Run(fmt.Sprintf("%s/subs=%d", impl.name, subs), func(b *testing.B) {
				r := newRig(impl.new, subs, benchBuffer)
				warm(r.bc)

				drops0, delivered0 := r.bc.Drops(), r.delivered()
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						r.bc.Broadcast(benchMsg)
					}
				})
				b.StopTimer()

				drops, delivered := r.bc.Drops()-drops0, r.delivered()-delivered0
				r.stop()
				reportDelivery(b, subs, drops, delivered)
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
				r := newRig(impl.new, subs, benchBuffer)
				defer r.stop()

				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					r.bc.Subscribe().Close()
				}
			})
		}
	}
}

// BenchmarkChurnUnderLoad is the realistic mix: people joining and leaving
// while the channel is busy. Membership changes and broadcasts contend, so
// this is where an implementation that made Broadcast cheap by making
// Subscribe expensive gets caught.
func BenchmarkChurnUnderLoad(b *testing.B) {
	for _, impl := range impls {
		for _, senders := range []int{1, 4} {
			b.Run(fmt.Sprintf("%s/senders=%d", impl.name, senders), func(b *testing.B) {
				const subs = 1_000
				r := newRig(impl.new, subs, benchBuffer)
				warm(r.bc)

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
							r.bc.Broadcast(benchMsg)
						}
					}()
				}

				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					r.bc.Subscribe().Close()
				}
				b.StopTimer()

				close(quit)
				wg.Wait()
				r.stop()
			})
		}
	}
}
