package loadgen

import (
	"math"
	"math/bits"
	"time"
)

// Histogram is a log-linear latency histogram: eight sub-buckets per power
// of two microseconds, so a recorded value is within 12.5% of the truth and
// the whole thing is 240 counters.
//
// Keeping every sample is not an option here. Ten thousand receivers at a
// hundred messages a second each is a million samples a second, and sorting
// gigabytes at the end to find a median is not a load generator, it is a
// memory benchmark. Sharing one histogram is not an option either: ten
// thousand goroutines incrementing the same counters measures cache line
// contention in the load generator rather than latency in the server. So
// every connection owns one of these and they are merged when it stops.
//
// It is deliberately not safe for concurrent use, for that reason.
type Histogram struct {
	buckets [numBuckets]uint64
	count   uint64
	sum     uint64 // microseconds
	max     uint64
}

const (
	// subBits is the resolution: 2^subBits buckets per octave.
	subBits  = 3
	subCount = 1 << subBits

	// octaves covers up to 2^32 microseconds, about 71 minutes. Anything
	// slower than that is not a latency measurement, it is a hung socket.
	octaves    = 32
	numBuckets = (octaves - subBits + 1) * subCount
)

// Record adds one measurement. Sub-microsecond values land in bucket zero,
// which is honest: nothing here is measured that finely.
func (h *Histogram) Record(d time.Duration) {
	var us uint64
	if d > 0 {
		us = uint64(d / time.Microsecond)
	}

	h.buckets[bucketOf(us)]++
	h.count++
	h.sum += us
	if us > h.max {
		h.max = us
	}
}

// Merge folds another histogram into this one.
func (h *Histogram) Merge(o *Histogram) {
	for i, n := range o.buckets {
		h.buckets[i] += n
	}
	h.count += o.count
	h.sum += o.sum
	if o.max > h.max {
		h.max = o.max
	}
}

// Count is how many measurements went in.
func (h *Histogram) Count() uint64 { return h.count }

// Max is the largest measurement, exactly — it is kept outside the buckets
// because the worst case is the one number nobody wants rounded.
func (h *Histogram) Max() time.Duration {
	return time.Duration(h.max) * time.Microsecond
}

// Mean is exact too, for the same reason: it costs one counter.
func (h *Histogram) Mean() time.Duration {
	if h.count == 0 {
		return 0
	}
	return time.Duration(h.sum/h.count) * time.Microsecond
}

// Quantile is the value at q, as an upper bound: the true value is inside
// the bucket this names and no larger. It is capped at the recorded maximum,
// so a p99.9 can never come out above the worst sample.
func (h *Histogram) Quantile(q float64) time.Duration {
	if h.count == 0 {
		return 0
	}

	want := uint64(math.Ceil(q * float64(h.count)))
	if want == 0 {
		want = 1
	}

	var seen uint64
	for i, n := range h.buckets {
		seen += n
		if seen >= want {
			return min(time.Duration(bucketMax(i))*time.Microsecond, h.Max())
		}
	}
	return h.Max()
}

// bucketOf is the counter a value lands in. Below subCount every value has
// its own bucket; above it, the value's octave picks a block of subCount and
// the bits below the leading one pick a slot inside it.
func bucketOf(us uint64) int {
	if us < subCount {
		return int(us)
	}

	oct := bits.Len64(us) - 1
	if oct >= octaves {
		return numBuckets - 1
	}

	shift := oct - subBits
	sub := (us >> shift) & (subCount - 1)
	return (oct-subBits+1)*subCount + int(sub)
}

// bucketMax is the largest value a bucket holds, which is what a quantile
// reports: the answer is "no worse than this".
func bucketMax(i int) uint64 {
	if i < subCount {
		return uint64(i)
	}

	oct := i/subCount + subBits - 1
	shift := oct - subBits
	sub := uint64(i % subCount)
	return (subCount+sub)<<shift | (1<<shift - 1)
}
