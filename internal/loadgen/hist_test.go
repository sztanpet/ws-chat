package loadgen

import (
	"math/rand/v2"
	"slices"
	"testing"
	"time"
)

// Checked across the whole range: everything else rests on this.
func TestHistogramResolution(t *testing.T) {
	for us := uint64(0); us < 1<<24; us = us + us/17 + 1 {
		i := bucketOf(us)
		hi := bucketMax(i)
		if us > hi {
			t.Fatalf("%dus landed in bucket %d, which tops out at %d", us, i, hi)
		}
		// 2^-subBits relative error, plus the one microsecond of truncation.
		if want := us + us>>subBits + 1; hi > want {
			t.Fatalf("%dus reported as %dus, worse than the promised %dus", us, hi, want)
		}
	}
}

func TestHistogramBucketsAreContiguous(t *testing.T) {
	prev := -1
	for us := range uint64(1 << 20) {
		i := bucketOf(us)
		if i != prev && i != prev+1 {
			t.Fatalf("%dus jumped from bucket %d to %d", us, prev, i)
		}
		prev = i
	}
}

func TestHistogramExactCounters(t *testing.T) {
	var h Histogram
	for _, d := range []time.Duration{time.Millisecond, 3 * time.Millisecond, 2 * time.Millisecond} {
		h.Record(d)
	}

	if h.Count() != 3 {
		t.Errorf("count = %d, want 3", h.Count())
	}
	if h.Max() != 3*time.Millisecond {
		t.Errorf("max = %v, want 3ms", h.Max())
	}
	if h.Mean() != 2*time.Millisecond {
		t.Errorf("mean = %v, want 2ms", h.Mean())
	}
}

func TestHistogramQuantiles(t *testing.T) {
	var h Histogram
	var sorted []time.Duration

	r := rand.New(rand.NewPCG(1, 2))
	for range 10000 {
		// Long tailed, as latency is: uniform input would prove little.
		d := time.Duration(r.ExpFloat64() * float64(time.Millisecond))
		h.Record(d)
		sorted = append(sorted, d.Truncate(time.Microsecond))
	}
	slices.Sort(sorted)

	for _, q := range []float64{0.5, 0.9, 0.99, 0.999} {
		want := sorted[int(q*float64(len(sorted)))-1]
		got := h.Quantile(q)
		if got < want {
			t.Errorf("p%v = %v, below the true %v", q*100, got, want)
		}
		if limit := want + want/subCount + time.Microsecond; got > limit {
			t.Errorf("p%v = %v, want no more than %v (true %v)", q*100, got, limit, want)
		}
	}

	if got := h.Quantile(1); got != h.Max() {
		t.Errorf("p100 = %v, want the max %v", got, h.Max())
	}
}

func TestHistogramEmpty(t *testing.T) {
	var h Histogram
	if h.Count() != 0 || h.Mean() != 0 || h.Max() != 0 || h.Quantile(0.99) != 0 {
		t.Error("an empty histogram reported something")
	}
}

func TestHistogramMerge(t *testing.T) {
	var a, b, both Histogram
	for i := range 100 {
		d := time.Duration(i) * time.Millisecond
		if i%2 == 0 {
			a.Record(d)
		} else {
			b.Record(d)
		}
		both.Record(d)
	}

	a.Merge(&b)
	if a != both {
		t.Errorf("merged histogram differs from one built in a single pass:\ncount %d/%d max %d/%d sum %d/%d",
			a.count, both.count, a.max, both.max, a.sum, both.sum)
	}
}

func TestHistogramSaturates(t *testing.T) {
	var h Histogram
	huge := time.Duration(1<<62 - 1)
	h.Record(huge)

	if h.Count() != 1 {
		t.Fatal("a huge sample was dropped")
	}
	if h.Max() != huge.Truncate(time.Microsecond) {
		t.Errorf("max = %v, want the sample %v", h.Max(), huge)
	}
	if got, want := h.Quantile(0.5), time.Duration(bucketMax(numBuckets-1))*time.Microsecond; got != want {
		t.Errorf("p50 = %v, want the top bucket %v", got, want)
	}
}
