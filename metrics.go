package main

import (
	"github.com/sztanpet/ws-chat/internal/metrics"
)

// A metric earns its place by answering a question asked at three in the morning.
type appMetrics struct {
	connectionsTotal  *metrics.Counter
	connectionsFailed *metrics.CounterVec
	codecs            *metrics.CounterVec

	commandsTotal *metrics.CounterVec
	messagesTotal *metrics.Counter
	privateTotal  *metrics.Counter
	refusalsTotal *metrics.CounterVec

	laggedTotal   *metrics.Counter
	recordsFailed *metrics.Counter

	channelsTotal *metrics.Counter
	joinsTotal    *metrics.Counter
	partsTotal    *metrics.Counter

	moderationTotal *metrics.CounterVec
	sanctionsLoaded *metrics.Counter
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

		channelsTotal: r.Counter("channels_created_total",
			"Channels created on demand since start."),
		joinsTotal: r.Counter("joins_total",
			"Channel joins, including autojoins."),
		partsTotal: r.Counter("parts_total",
			"Channel parts, including those caused by a disconnect."),

		moderationTotal: r.CounterVec("moderation_total",
			"Moderation actions taken.", "action"),
		sanctionsLoaded: r.Counter("sanctions_loaded_total",
			"Mutes and bans restored from the Sanctions hook at startup."),
	}
}

func (a *app) registerStateMetrics(r *metrics.Registry) {
	r.GaugeFunc("connections", "Connections currently held.", func() int64 {
		a.connsMu.RLock()
		defer a.connsMu.RUnlock()
		return int64(len(a.conns))
	})

	r.GaugeFunc("channels", "Channels that currently exist.", func() int64 {
		a.channelsMu.RLock()
		defer a.channelsMu.RUnlock()
		return int64(len(a.channels))
	})

	r.GaugeFunc("memberships", "Channel memberships across every connection.", func() int64 {
		a.channelsMu.RLock()
		channels := make([]*channel, 0, len(a.channels))
		for _, ch := range a.channels {
			channels = append(channels, ch)
		}
		a.channelsMu.RUnlock()

		var total int64
		for _, ch := range channels {
			total += int64(ch.len())
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

	r.GaugeFunc("fanout_drops_total", "Subscribers dropped by the fan-out, as it counts them.", func() int64 {
		a.channelsMu.RLock()
		channels := make([]*channel, 0, len(a.channels))
		for _, ch := range a.channels {
			channels = append(channels, ch)
		}
		a.channelsMu.RUnlock()

		var total int64
		for _, ch := range channels {
			for _, bc := range ch.bcs {
				total += bc.Drops()
			}
		}
		return total
	})
	r.GaugeFunc("records_dropped_total", "Persistence jobs dropped because the queue was full.", func() int64 {
		return int64(a.dropped.Load())
	})
}

func (a *app) refused(code string) {
	a.metrics.refusalsTotal.With(code).Inc()
}
