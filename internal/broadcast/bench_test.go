package broadcast

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var benchSubCounts = []int{1, 10, 100, 1_000, 10_000}

// Below a thousand a flat-out sender just outruns its receivers, on this box.
var benchLiveSubCounts = []int{1_000, 10_000}

const benchBuffer = DefaultBuffer

// 1<<20 slices is ~16MB across every subscriber's buffer.
const benchFanoutBudget = 1 << 20

// Shared by reference: if its size moves the numbers, something is copying.
var benchMsg = []byte(`MSG {"nick":"someone","channel":"#general","data":"hello world"}`)

// The server's default WriteBatch.
const benchBatch = 16

// Padded to its own cache line, or ten thousand drainers serialize on one.
type counter struct {
	n atomic.Int64
	_ [56]byte
}

type rig struct {
	bc       Broadcaster
	counters []*counter
	total    *atomic.Int64
	wg       sync.WaitGroup
}

func newRig(mk func(int) Broadcaster, subs, size int) *rig {
	return newRigPaced(mk, subs, size, false)
}

// Off by default: a counter every drainer bumps is contention of its own.
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

// Resubscribes on a drop, or the member set bleeds away and the numbers flatter.
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
			return
		}
		// Once closed, this is a finished Sub and the loop ends.
		s = bc.Subscribe()
	}
}

// With a small buffer some warm-up is dropped and never consumed.
const warmPatience = 5 * time.Second

// Waits rather than sleeps: unconsumed warm-up would land in the timed region.
func warm(r *rig) {
	const messages = 4 * benchBuffer

	for range messages {
		r.bc.Broadcast(benchMsg)
	}

	want := int64(messages) * int64(len(r.counters))
	deadline := time.Now().Add(warmPatience)
	for r.delivered() < want && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
}

const pacingPatience = 100_000

// It gives up eventually: what a dropped subscriber missed is never delivered.
func pace(r *rig, base, sent int64, subs, lead int) {
	limit := int64(subs) * int64(lead)
	last := r.total.Load()
	stalled := 0

	for sent*int64(subs)-(r.total.Load()-base) > limit {
		runtime.Gosched()

		if now := r.total.Load(); now != last {
			last, stalled = now, 0
			continue
		}
		if stalled++; stalled > pacingPatience {
			return
		}
	}
}

func reportSend(b *testing.B, subs int) {
	b.Helper()
	if sent := float64(b.N) * float64(subs); sent > 0 {
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/sent, "ns/msg")
	}
}

// Per delivery, not per send: a broadcaster that drops can always look fast.
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

func fanoutBuffer(subs int) int {
	return max(benchFanoutBudget/subs, benchBuffer)
}

// Only per-subscriber queues fill; draining the rest is O(b.N×subs) for nothing.
func needsDrain(bc Broadcaster) bool { return !sharedStorage(bc) }

func sharedStorage(bc Broadcaster) bool {
	switch bc.(type) {
	case *Ring, *SeqRing:
		return true
	}
	return false
}

// By count: Recv blocks, and an exact multiple of the batch never ends short.
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

// Send path only: shared storage defers its work to readers this never runs.
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

func BenchmarkFanoutLive(b *testing.B) {
	for _, impl := range impls {
		for _, subs := range benchLiveSubCounts {
			b.Run(fmt.Sprintf("%s/subs=%d", impl.name, subs), func(b *testing.B) {
				r := newRig(impl.new, subs, benchBuffer)
				warm(r)

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

// The fair comparison: an O(1) sender run flat out laps its receivers.
func BenchmarkFanoutPaced(b *testing.B) {
	// Well under benchBuffer, so pacing rather than dropping bounds the sender.
	const lead = 64

	for _, impl := range impls {
		for _, subs := range benchLiveSubCounts {
			b.Run(fmt.Sprintf("%s/subs=%d", impl.name, subs), func(b *testing.B) {
				r := newRigPaced(impl.new, subs, benchBuffer, true)
				warm(r)

				drops0, delivered0 := r.bc.Drops(), r.delivered()
				base := r.total.Load()
				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					r.bc.Broadcast(benchMsg)
					pace(r, base, int64(i+1), subs, lead)
				}
				b.StopTimer()

				drops, delivered := r.bc.Drops()-drops0, r.delivered()-delivered0
				r.stop()
				reportDelivery(b, subs, drops, delivered)
			})
		}
	}
}

// Overload by design: delivered/sent is low on purpose; compare only with itself.
func BenchmarkFanoutSaturated(b *testing.B) {
	for _, impl := range impls {
		for _, subs := range []int{100, 1_000} {
			b.Run(fmt.Sprintf("%s/subs=%d", impl.name, subs), func(b *testing.B) {
				r := newRig(impl.new, subs, benchBuffer)
				warm(r)

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

// Allocating a per-subscriber buffer is part of what a join costs: it stays in.
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

// Catches a Broadcast made cheap by making Subscribe expensive.
func BenchmarkChurnUnderLoad(b *testing.B) {
	for _, impl := range impls {
		for _, senders := range []int{1, 4} {
			b.Run(fmt.Sprintf("%s/senders=%d", impl.name, senders), func(b *testing.B) {
				const subs = 1_000
				r := newRig(impl.new, subs, benchBuffer)
				warm(r)

				var wg sync.WaitGroup
				quit := make(chan struct{})
				for range senders {
					wg.Go(func() {
						for {
							select {
							case <-quit:
								return
							default:
							}
							r.bc.Broadcast(benchMsg)
						}
					})
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

// Paced collectively, for the reason BenchmarkFanoutPaced is.
func BenchmarkFanoutChatty(b *testing.B) {
	for _, impl := range impls {
		// Ten thousand is BenchmarkFanoutChattyCapacity's: there capacity decides it.
		for _, subs := range []int{100, 1_000} {
			b.Run(fmt.Sprintf("%s/subs=%d", impl.name, subs), func(b *testing.B) {
				runChatty(b, impl.new, subs, benchBuffer)
			})
		}
	}
}

// Aggregate pacing bounds the mean lag, not the spread: can slack absorb it?
func BenchmarkFanoutChattyCapacity(b *testing.B) {
	const subs = 10_000

	for _, impl := range impls {
		sizes := []int{256, 4096, 65536}
		if !sharedStorage(impl.new(1)) {
			// Smallest size only: 65536 slots for each of ten thousand is 10GB.
			sizes = sizes[:1]
		}
		for _, size := range sizes {
			b.Run(fmt.Sprintf("%s/cap=%d", impl.name, size), func(b *testing.B) {
				runChatty(b, impl.new, subs, size)
			})
		}
	}
}

func runChatty(b *testing.B, mk func(int) Broadcaster, subs, size int) {
	// Well under the smallest capacity, so pacing rather than dropping bounds them.
	const lead = 64

	senders := max(1, subs/10)

	r := newRigPaced(mk, subs, size, true)
	warm(r)

	// Random, so no implementation that orders its members is flattered.
	chatty := rand.Perm(subs)[:senders]

	// A fixed share each, so no counter is contended mid-measurement.
	share := b.N / senders
	extra := b.N % senders

	drops0, delivered0 := r.bc.Drops(), r.delivered()
	base := r.total.Load()
	var sent atomic.Int64

	var wg sync.WaitGroup
	b.ReportAllocs()
	b.ResetTimer()

	for i := range chatty {
		mine := share
		if i < extra {
			mine++
		}
		wg.Add(1)
		go func(mine int) {
			defer wg.Done()
			for range mine {
				r.bc.Broadcast(benchMsg)

				// Shared: next to a microsecond broadcast, a contended add is noise.
				pace(r, base, sent.Add(1), subs, lead)
			}
		}(mine)
	}
	wg.Wait()
	b.StopTimer()

	drops, delivered := r.bc.Drops()-drops0, r.delivered()-delivered0
	r.stop()
	reportDelivery(b, subs, drops, delivered)
}
