package loadgen

import (
	"math"
	"math/bits"
	"time"
)

// Histogram is one per connection: a shared one would measure contention.
type Histogram struct {
	buckets [numBuckets]uint64
	count   uint64
	sum     uint64 // microseconds
	max     uint64
}

const (
	// 8 buckets an octave: a value is within 12.5%.
	subBits  = 3
	subCount = 1 << subBits

	// 2^32µs is 71 minutes; anything slower is a hung socket, not a latency.
	octaves    = 32
	numBuckets = (octaves - subBits + 1) * subCount
)

// Record puts sub-microsecond values in bucket zero: nothing is that precise.
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

func (h *Histogram) Count() uint64 { return h.count }

// Max is exact, outside the buckets: the worst case is not one to round.
func (h *Histogram) Max() time.Duration {
	return time.Duration(h.max) * time.Microsecond
}

func (h *Histogram) Mean() time.Duration {
	if h.count == 0 {
		return 0
	}
	return time.Duration(h.sum/h.count) * time.Microsecond
}

// Quantile is an upper bound, capped at Max so no p99.9 exceeds the worst sample.
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

// Octave by the leading bit, slot by the subBits after it; small values map 1:1.
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

func bucketMax(i int) uint64 {
	if i < subCount {
		return uint64(i)
	}

	oct := i/subCount + subBits - 1
	shift := oct - subBits
	sub := uint64(i % subCount)
	return (subCount+sub)<<shift | (1<<shift - 1)
}
