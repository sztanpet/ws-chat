package websocket

import (
	"math"
	"time"
)

// A deadline is stored as unix nanoseconds in an atomic.Int64, so that setting
// one costs a single store and reading one costs a single load. Zero means no
// deadline, which is why deadlineNanos never returns it for a real time.
const noDeadline = 0

// maxDeadline is the latest instant time.Time.UnixNano can represent, roughly
// the year 2262. Beyond it UnixNano silently wraps to a negative number, which
// would read back as a deadline deep in the past.
var maxDeadline = time.Unix(0, math.MaxInt64)

// minDeadline is the earliest instant UnixNano can represent, roughly the year
// 1677, and wraps the same way in the other direction.
var minDeadline = time.Unix(0, math.MinInt64)

// deadlineNanos converts t to the representation stored for a deadline: unix
// nanoseconds, saturating rather than wrapping at the ends of the range
// time.Time.UnixNano can express, and never zero, which means no deadline.
//
// t must not be the zero time; callers handle that as clearing the deadline.
func deadlineNanos(t time.Time) int64 {
	switch {
	case t.After(maxDeadline):
		return math.MaxInt64
	case t.Before(minDeadline):
		// So far in the past that it is indistinguishable from any other
		// expired deadline. One nanosecond after the epoch will do.
		return 1
	}
	n := t.UnixNano()
	if n == noDeadline {
		// Exactly the unix epoch, which is a deadline long past rather than
		// an absent one.
		return 1
	}
	return n
}
