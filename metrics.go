package main

import (
	"github.com/sztanpet/ws-chat/internal/metrics"
)

// The server's metrics, declared in one place so the set can be read
// without grepping for increments.
//
// Every one of them earns its place by answering a question somebody asks
// at three in the morning: is anything connected, is anything being
// delivered, is the server refusing things, and is it dropping things. A
// metric nobody would page on is a metric that costs a line of code on the
// hot path for nothing.
//
// Counters here never reset — that is the point of a counter, and the
// scraper takes the difference. Anything that goes up and down is a gauge,
// and anything already counted elsewhere is a GaugeFunc reading the real
// thing rather than a mirror of it that can disagree.
type appMetrics struct {
	// Connections.
	connectionsTotal  *metrics.Counter
	connectionsFailed *metrics.CounterVec // by reason
	codecs            *metrics.CounterVec // by negotiated subprotocol

	// Traffic.
	commandsTotal *metrics.CounterVec // by verb, INCLUDING refused ones
	messagesTotal *metrics.Counter    // accepted and fanned out
	privateTotal  *metrics.Counter    // accepted and delivered
	refusalsTotal *metrics.CounterVec // by ERR code

	// Things going wrong.
	laggedTotal   *metrics.Counter // subscribers dropped for falling behind
	recordsFailed *metrics.Counter // persistence jobs that returned an error

	// Moderation.
	moderationTotal *metrics.CounterVec // by action
}

func newMetrics(r *metrics.Registry) *appMetrics {
	return &appMetrics{
		connectionsTotal: r.Counter("connections_total",
			"Connections accepted since start."),
		connectionsFailed: r.CounterVec("connections_refused_total",
			"Connections refused before the upgrade.", "reason"),
		codecs: r.CounterVec("codec_negotiated_total",
			"Connections by the wire format they negotiated.", "codec"),

		commandsTotal: r.CounterVec("commands_total",
			"Commands received, whether or not they were acted on.", "verb"),
		messagesTotal: r.Counter("messages_total",
			"Chat messages accepted and fanned out."),
		privateTotal: r.Counter("private_messages_total",
			"Private messages accepted and queued for their recipient."),
		refusalsTotal: r.CounterVec("refusals_total",
			"Commands refused, by the code the client was sent.", "code"),

		laggedTotal: r.Counter("subscribers_lagged_total",
			"Subscribers dropped for falling too far behind the fan-out."),
		recordsFailed: r.Counter("records_failed_total",
			"Persistence jobs that returned an error."),

		moderationTotal: r.CounterVec("moderation_total",
			"Moderation actions taken.", "action"),
	}
}

// registerStateMetrics wires the gauges that read the server's live state
// at scrape time rather than being maintained alongside it.
func (a *app) registerStateMetrics(r *metrics.Registry) {
	r.GaugeFunc("connections", "Connections currently held.", func() int64 {
		a.connsMu.RLock()
		defer a.connsMu.RUnlock()
		return int64(len(a.conns))
	})

	r.GaugeFunc("subscribers", "Live fan-out subscriptions across every codec.", func() int64 {
		var total int64
		for _, bc := range a.bcs {
			total += int64(bc.Len())
		}
		return total
	})

	r.GaugeFunc("moderation_mutes", "Mutes currently in force.", func() int64 {
		mutes, _ := a.mod.Counts()
		return int64(mutes)
	})
	r.GaugeFunc("moderation_bans", "Bans currently in force.", func() int64 {
		_, bans := a.mod.Counts()
		return int64(bans)
	})

	r.GaugeFunc("rate_limiters", "Shared rate limit buckets being held.", func() int64 {
		a.limitersMu.Lock()
		defer a.limitersMu.Unlock()
		return int64(len(a.limiters))
	})

	// The broadcasters count their own drops, and the persistence queue
	// counts its own. Reading them is better than maintaining a second
	// number that can disagree with the first.
	r.GaugeFunc("fanout_drops_total", "Subscribers dropped by the fan-out, as it counts them.", func() int64 {
		var total int64
		for _, bc := range a.bcs {
			total += bc.Drops()
		}
		return total
	})
	r.GaugeFunc("records_dropped_total", "Persistence jobs dropped because the queue was full.", func() int64 {
		return int64(a.dropped.Load())
	})
}

// refused counts a refusal alongside sending it, so the two cannot drift.
// Every ERR the server sends goes through conn.reply, which calls this.
func (a *app) refused(code string) {
	a.metrics.refusalsTotal.With(code).Inc()
}
