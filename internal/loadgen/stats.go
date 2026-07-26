package loadgen

import (
	"fmt"
	"maps"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// stats is what a run counts while it runs.
//
// The plain counters are atomics because the progress line reads them while
// every connection writes them. The maps are behind one mutex, which is fine
// precisely because what goes in them is rare: a refusal, a lost connection,
// a dial that failed. If any of them were hot the run would be measuring
// something that had already gone wrong.
type stats struct {
	dialed     atomic.Uint64
	dialFailed atomic.Uint64
	lost       atomic.Uint64
	sent       atomic.Uint64
	sendFailed atomic.Uint64
	received   atomic.Uint64
	other      atomic.Uint64 // joins, parts, pongs, backlogs — load, but not delivery
	garbage    atomic.Uint64 // frames that would not decode

	// sentIn counts messages per channel, which is the only way to work out
	// how many deliveries a run should have produced: a message costs one
	// delivery per member of the channel it was said in, and the channels do
	// not have to be the same size.
	sentIn []atomic.Uint64

	mu       sync.Mutex
	refusals map[string]uint64
	losses   map[string]uint64
	dialErrs map[string]uint64
	latency  Histogram
}

func newStats(channels int) *stats {
	return &stats{
		sentIn:   make([]atomic.Uint64, channels),
		refusals: make(map[string]uint64),
		losses:   make(map[string]uint64),
		dialErrs: make(map[string]uint64),
	}
}

func (s *stats) refused(code string) { s.bump(s.refusals, code) }
func (s *stats) dialErr(reason string) {
	s.dialFailed.Add(1)
	s.bump(s.dialErrs, reason)
}

func (s *stats) closedEarly(reason string) {
	s.lost.Add(1)
	s.bump(s.losses, reason)
}

func (s *stats) bump(m map[string]uint64, key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m[key]++
}

// mergeLatency folds one connection's histogram in, once, when it stops.
func (s *stats) mergeLatency(h *Histogram) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.latency.Merge(h)
}

// Result is what a run measured. Everything in it is a client-side
// observation; nothing was read out of the server.
type Result struct {
	Conns     int // asked for
	Speakers  int
	Channels  int
	Rate      float64
	MsgSize   int
	Elapsed   time.Duration
	Dialed    uint64
	Failed    uint64
	Lost      uint64
	Sent      uint64
	SendFails uint64

	// Received is MSG frames that arrived. Expected is how many should have,
	// if every connection had been in place for the whole run — it is a
	// denominator, not a promise, and the two differ by whatever was still
	// in flight when the clock stopped.
	Received uint64
	Expected uint64
	Other    uint64
	Garbage  uint64

	Refusals map[string]uint64
	Losses   map[string]uint64
	DialErrs map[string]uint64
	Latency  Histogram
}

// snapshot freezes the counters into a Result.
func (s *stats) snapshot(cfg Config, elapsed time.Duration) *Result {
	s.mu.Lock()
	defer s.mu.Unlock()

	r := &Result{
		Conns:     cfg.Conns,
		Speakers:  cfg.speakers(),
		Channels:  cfg.Channels,
		Rate:      cfg.Rate,
		MsgSize:   cfg.MessageSize,
		Elapsed:   elapsed,
		Dialed:    s.dialed.Load(),
		Failed:    s.dialFailed.Load(),
		Lost:      s.lost.Load(),
		Sent:      s.sent.Load(),
		SendFails: s.sendFailed.Load(),
		Received:  s.received.Load(),
		Other:     s.other.Load(),
		Garbage:   s.garbage.Load(),
		Refusals:  maps.Clone(s.refusals),
		Losses:    maps.Clone(s.losses),
		DialErrs:  maps.Clone(s.dialErrs),
		Latency:   s.latency,
	}
	for i := range s.sentIn {
		r.Expected += s.sentIn[i].Load() * uint64(cfg.membersIn(i))
	}
	return r
}

// String is the report a run prints.
func (r *Result) String() string {
	var b strings.Builder
	secs := r.Elapsed.Seconds()

	fmt.Fprintf(&b, "elapsed      %s\n", r.Elapsed.Round(time.Millisecond))
	fmt.Fprintf(&b, "connections  %d up, %d failed to dial, %d lost\n", r.Dialed, r.Failed, r.Lost)
	fmt.Fprintf(&b, "speakers     %d of %d at %g msg/s each, %d byte messages\n",
		r.Speakers, r.Conns, r.Rate, r.MsgSize)
	fmt.Fprintf(&b, "channels     %d\n", r.Channels)
	fmt.Fprintf(&b, "sent         %d msgs (%s/s), %d writes failed\n",
		r.Sent, rate(r.Sent, secs), r.SendFails)
	fmt.Fprintf(&b, "received     %d msgs (%s/s) of %d expected%s\n",
		r.Received, rate(r.Received, secs), r.Expected, percent(r.Received, r.Expected))
	fmt.Fprintf(&b, "other frames %d (%s/s)\n", r.Other, rate(r.Other, secs))

	if h := &r.Latency; h.Count() > 0 {
		fmt.Fprintf(&b, "latency      p50 %s  p90 %s  p99 %s  p99.9 %s  max %s  mean %s  (%d samples)\n",
			dur(h.Quantile(0.5)), dur(h.Quantile(0.9)), dur(h.Quantile(0.99)),
			dur(h.Quantile(0.999)), dur(h.Max()), dur(h.Mean()), h.Count())
	}

	writeCounts(&b, "refusals", r.Refusals)
	writeCounts(&b, "losses", r.Losses)
	writeCounts(&b, "dial errs", r.DialErrs)
	if r.Garbage > 0 {
		fmt.Fprintf(&b, "garbage      %d frames the client could not decode\n", r.Garbage)
	}
	return b.String()
}

// writeCounts prints a map of counters, busiest first, and nothing at all
// when it is empty — a clean run should look clean.
func writeCounts(b *strings.Builder, label string, m map[string]uint64) {
	if len(m) == 0 {
		return
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if m[keys[i]] != m[keys[j]] {
			return m[keys[i]] > m[keys[j]]
		}
		return keys[i] < keys[j]
	})

	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s=%d", k, m[k])
	}
	fmt.Fprintf(b, "%-12s %s\n", label, strings.Join(parts, "  "))
}

func rate(n uint64, secs float64) string {
	if secs <= 0 {
		return "0"
	}
	return fmt.Sprintf("%.0f", float64(n)/secs)
}

func percent(got, want uint64) string {
	if want == 0 {
		return ""
	}
	return fmt.Sprintf(" — %.2f%%", 100*float64(got)/float64(want))
}

// dur keeps the report readable: three digits is as much as a latency
// measurement ever means.
func dur(d time.Duration) string {
	switch {
	case d >= time.Second:
		return d.Round(10 * time.Millisecond).String()
	case d >= time.Millisecond:
		return d.Round(10 * time.Microsecond).String()
	default:
		return d.Round(time.Microsecond).String()
	}
}
