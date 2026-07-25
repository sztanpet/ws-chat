package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/sztanpet/ws-chat/internal/hook"
	"github.com/sztanpet/ws-chat/internal/ratelimit"
)

// This file is where the core meets the layers outside it. Every call to a
// hook goes through here, so the rules about which of them may block, and
// what happens when one misbehaves, are enforced in one place instead of
// being scattered through the connection handling.

// recordQueue is how many pending writes the persistence worker will hold.
// Beyond it, records are dropped: the alternative is letting a slow store
// become backpressure on the chat, which is the one thing the whole design
// refuses to do.
const recordQueue = 1024

// httpRequest adapts an *http.Request to the narrow view an auth layer
// gets. It exists so an implementation cannot hijack the connection or
// start writing a response behind the server's back.
type httpRequest struct{ r *http.Request }

func (h httpRequest) Header(name string) string { return h.r.Header.Get(name) }

func (h httpRequest) Query(name string) string { return h.r.URL.Query().Get(name) }

func (h httpRequest) RemoteAddr() string { return h.r.RemoteAddr }

func (h httpRequest) Cookie(name string) (string, bool) {
	c, err := h.r.Cookie(name)
	if err != nil {
		return "", false
	}
	return c.Value, true
}

// identify resolves a connection request to an identity, applying whatever
// the directory knows on top. It is the only place either hook is called.
func (a *app) identify(ctx context.Context, r *http.Request) (hook.Identity, error) {
	var id hook.Identity

	if a.hooks.Auth != nil {
		var err error
		id, err = a.hooks.Auth.Authenticate(ctx, httpRequest{r})
		if err != nil {
			return id, err
		}
	}

	if a.hooks.Directory != nil && !id.Anonymous() {
		switch chatter, err := a.hooks.Directory.Chatter(ctx, id.ID); {
		case err == nil:
			id = id.Apply(chatter)
		case errors.Is(err, hook.ErrNoChatter):
			// Nothing on file. What Authenticate said stands.
		default:
			// A broken directory must not cost somebody their login. Log
			// it and carry on with what we have.
			a.log.Error("directory lookup failed", "id", id.ID, "err", err)
		}
	}

	// Anyone the layers did not name gets named here. Anonymous nicks are
	// distinguishable on purpose: nobody should be able to pass for a
	// logged-in user by picking their name.
	if id.Nick == "" {
		id.Nick = fmt.Sprintf("anon%d", a.anon.Add(1))
	}
	return id, nil
}

// clientLimiter builds the rate limiter for one connection, and returns the
// function that hands it back.
//
// No Limiter installed, or one with no opinion, means unlimited — which New
// returns as a nil *Bucket, so the check on the hot path costs a nil
// comparison. A Limits with no Key gets a bucket of its own and nothing to
// release; one with a Key shares a bucket with every other connection
// naming it.
func (a *app) clientLimiter(ctx context.Context, id hook.Identity) (*ratelimit.Bucket, func()) {
	if a.hooks.Limiter == nil {
		return nil, nil
	}

	limits := a.hooks.Limiter.ClientLimits(ctx, id)
	if limits.Key == "" {
		return ratelimit.New(limits.Burst, limits.Interval), nil
	}
	return a.acquireLimiter(limits)
}

// acquireLimiter hands out the bucket for a key, creating it if this is the
// first connection to ask.
//
// The limits of an existing bucket are left alone. A bucket in use is
// somebody's remaining budget, and rebuilding it because a later connection
// reported different numbers would refill it — which is a way to escape a
// throttle by opening a second socket.
func (a *app) acquireLimiter(limits hook.Limits) (*ratelimit.Bucket, func()) {
	a.limitersMu.Lock()
	defer a.limitersMu.Unlock()

	entry, ok := a.limiters[limits.Key]
	if !ok {
		entry = &limiterEntry{bucket: ratelimit.New(limits.Burst, limits.Interval)}
		a.limiters[limits.Key] = entry
	}
	entry.refs++

	key := limits.Key
	return entry.bucket, func() { a.releaseLimiter(key) }
}

// releaseLimiter drops one connection's hold on a shared bucket.
//
// It does NOT delete the entry at zero. A bucket that still has spent
// tokens is somebody's throttle, and handing back a fresh one the moment
// they disconnect is exactly what a person being throttled would try.
// Reclaiming is the janitor's job, and only once the bucket has refilled.
func (a *app) releaseLimiter(key string) {
	a.limitersMu.Lock()
	defer a.limitersMu.Unlock()
	if entry, ok := a.limiters[key]; ok && entry.refs > 0 {
		entry.refs--
	}
}

// sweepLimiters drops shared buckets that nobody holds and that have
// refilled, and reports how many it removed. A refilled bucket is
// indistinguishable from one that never existed, so forgetting it loses
// nothing.
func (a *app) sweepLimiters(now time.Time) int {
	a.limitersMu.Lock()
	defer a.limitersMu.Unlock()

	removed := 0
	for key, entry := range a.limiters {
		if entry.refs == 0 && entry.bucket.Idle(now) {
			delete(a.limiters, key)
			removed++
		}
	}
	return removed
}

// janitor reclaims what lazy expiry leaves behind. Nothing depends on it
// running — every read already treats an expired entry as absent — so it
// only exists to stop a long-lived server accumulating them.
func (a *app) janitor(ctx context.Context) {
	ticker := time.NewTicker(janitorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if n := a.sweepLimiters(now); n > 0 {
				a.log.Debug("reclaimed rate limiters", "count", n)
			}
			if n := a.mod.Sweep(); n > 0 {
				a.log.Debug("reclaimed expired moderation", "count", n)
			}
		}
	}
}

// channelLimiter builds the bucket every member of a channel shares.
func (a *app) channelLimiter(ctx context.Context, channel string) *ratelimit.Bucket {
	if a.hooks.Limiter == nil {
		return nil
	}
	limits := a.hooks.Limiter.ChannelLimits(ctx, channel)
	return ratelimit.New(limits.Burst, limits.Interval)
}

// allow runs the filter chain: the built-in text filters and then whatever
// the deployment installed. It runs on the sender's read pump, so a filter
// that blocks holds up one client — its own.
func (a *app) allow(ctx context.Context, from hook.Identity, data string) (bool, string) {
	if a.filters == nil {
		return true, ""
	}
	return a.filters.Allow(ctx, from, data)
}

// canModerate reports whether this identity may use the moderation
// commands. The default is DENY: a server that has not been told who its
// moderators are does not have any.
func (a *app) canModerate(ctx context.Context, id hook.Identity) bool {
	if a.hooks.Authz == nil {
		return false
	}
	return a.hooks.Authz.CanModerate(ctx, id)
}

// record queues a persistence job. It never blocks and never fails the
// message: by the time it is called, the message has already been
// delivered.
func (a *app) record(job func(context.Context) error) {
	if a.hooks.Recorder == nil {
		return
	}
	select {
	case a.records <- job:
	default:
		// Dropping is deliberate and counted. A store that cannot keep up
		// loses history; it does not get to stop the chat.
		a.dropped.Add(1)
		a.log.Warn("record queue full, dropping", "dropped", a.dropped.Load())
	}
}

func (a *app) recordMessage(m hook.Message) {
	a.record(func(ctx context.Context) error { return a.hooks.Recorder.Message(ctx, m) })
}

func (a *app) recordPrivate(p hook.Private) {
	a.record(func(ctx context.Context) error { return a.hooks.Recorder.Private(ctx, p) })
}

func (a *app) recordModeration(m hook.Moderation) {
	a.record(func(ctx context.Context) error { return a.hooks.Recorder.Moderation(ctx, m) })
}

// recordWorker drains the persistence queue until the server shuts down.
//
// One worker, on purpose: it keeps writes in the order they happened, which
// a store that reconstructs a conversation is going to want. If a real
// store turns out to need more throughput than one worker gives, that is a
// batching change here, not a change to anything on the hot path.
func (a *app) recordWorker(ctx context.Context) {
	defer close(a.recordsDone)

	for {
		select {
		case <-ctx.Done():
			a.drainRecords()
			return
		case job := <-a.records:
			a.runRecord(ctx, job)
		}
	}
}

// drainRecords makes a best effort to finish what is already queued when
// the server is going down, rather than silently losing it.
func (a *app) drainRecords() {
	ctx, cancel := context.WithTimeout(context.Background(), recordDrainGrace)
	defer cancel()

	for {
		select {
		case job := <-a.records:
			a.runRecord(ctx, job)
		default:
			return
		}
		if ctx.Err() != nil {
			return
		}
	}
}

const recordDrainGrace = 5 * time.Second

func (a *app) runRecord(ctx context.Context, job func(context.Context) error) {
	if err := job(ctx); err != nil {
		a.metrics.recordsFailed.Inc()
		// A failed write is the store's problem to shout about; the
		// message has already been delivered and there is nothing useful
		// to tell the sender at this point.
		a.log.Error("record failed", "err", err)
	}
}
