package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/sztanpet/ws-chat/internal/hook"
	"github.com/sztanpet/ws-chat/internal/proto"
	"github.com/sztanpet/ws-chat/internal/ratelimit"
)

const recordQueue = 1024

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
		default:
			// A broken directory must not cost somebody their login.
			a.log.Error("directory lookup failed", "id", id.ID, "err", err)
		}
	}

	// Anonymous nicks look anonymous, so nobody passes for a logged-in user.
	if id.Nick == "" {
		id.Nick = fmt.Sprintf("anon%d", a.anon.Add(1))
	}
	return id, nil
}

// The conn is nil for somebody who is not connected, and callers must cope.
func (a *app) resolve(ctx context.Context, nick string) (*conn, hook.Identity, bool) {
	if target, ok := a.lookup(nick); ok {
		return target, target.id, true
	}
	if a.hooks.Directory == nil {
		return nil, hook.Identity{}, false
	}

	id, err := a.hooks.Directory.Resolve(ctx, nick)
	switch {
	case errors.Is(err, hook.ErrNoChatter):
		return nil, hook.Identity{}, false
	case err != nil:
		// An error is a miss, not a guess: never act on the wrong person.
		a.log.Error("cannot resolve a nick", "nick", nick, "err", err)
		return nil, hook.Identity{}, false
	}

	// Nothing to file an action under is as good as not knowing the name.
	if id.Key() == "nick:" {
		return nil, hook.Identity{}, false
	}
	if id.Nick == "" {
		id.Nick = nick
	}
	return nil, id, true
}

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

// An existing bucket keeps its limits: rebuilding it would refill a throttle.
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

// Kept at zero holders: a fresh bucket on reconnect would escape the throttle.
func (a *app) releaseLimiter(key string) {
	a.limitersMu.Lock()
	defer a.limitersMu.Unlock()
	if entry, ok := a.limiters[key]; ok && entry.refs > 0 {
		entry.refs--
	}
}

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
			if n := a.sweepChannels(); n > 0 {
				a.log.Debug("reclaimed empty channels", "count", n)
			}
		}
	}
}

func (a *app) channelLimiter(ctx context.Context, channel string) *ratelimit.Bucket {
	if a.hooks.Limiter == nil {
		return nil
	}
	limits := a.hooks.Limiter.ChannelLimits(ctx, channel)
	return ratelimit.New(limits.Burst, limits.Interval)
}

func (a *app) allow(ctx context.Context, from hook.Identity, data string) (bool, string) {
	if a.filters == nil {
		return true, ""
	}
	return a.filters.Allow(ctx, from, data)
}

// Deny by default: a server not told who its moderators are has none.
func (a *app) canModerate(ctx context.Context, id hook.Identity, scope string) bool {
	if a.hooks.Authz == nil {
		return false
	}
	return a.hooks.Authz.CanModerate(ctx, id, scope)
}

func (a *app) autojoin(ctx context.Context, id hook.Identity) []string {
	if a.hooks.Channels == nil {
		return []string{a.cfg.DefaultChannel}
	}
	if channels := a.hooks.Channels.Autojoin(ctx, id); channels != nil {
		return channels
	}
	return []string{a.cfg.DefaultChannel}
}

func (a *app) canJoin(ctx context.Context, id hook.Identity, channel string) (bool, string) {
	if a.hooks.Channels == nil {
		return true, ""
	}
	return a.hooks.Channels.CanJoin(ctx, id, channel)
}

// Callers check the hook exists: the closure hides it, and nil would panic later.
func (a *app) record(job func(context.Context) error) {
	select {
	case a.records <- job:
	default:
		a.dropped.Add(1)
		a.log.Warn("record queue full, dropping", "dropped", a.dropped.Load())
	}
}

func (a *app) recordMessage(m hook.Message) {
	if a.hooks.Recorder == nil {
		return
	}
	a.record(func(ctx context.Context) error { return a.hooks.Recorder.Message(ctx, m) })
}

func (a *app) recordPrivate(p hook.Private) {
	if a.hooks.Recorder == nil {
		return
	}
	a.record(func(ctx context.Context) error { return a.hooks.Recorder.Private(ctx, p) })
}

func (a *app) recordSanction(m hook.Moderation) {
	if a.hooks.Sanctions == nil {
		return
	}
	a.record(func(ctx context.Context) error { return a.hooks.Sanctions.Record(ctx, m) })
}

// A failure stops the server: not knowing who is banned means letting them in.
func (a *app) loadSanctions(ctx context.Context) error {
	if a.hooks.Sanctions == nil {
		return nil
	}

	active, err := a.hooks.Sanctions.Active(ctx)
	if err != nil {
		return fmt.Errorf("loading sanctions: %w", err)
	}

	loaded := 0
	for _, m := range active {
		switch m.Action {
		case proto.ActionMute:
			a.mod.Mute(m.Scope, m.Key, m.Until)
		case proto.ActionBan:
			a.mod.Ban(m.Scope, m.Key, m.Until)
		default:
			// A lift is not a sanction: an Active returning one is broken, so say so.
			a.log.Warn("ignoring a lifted action in the active set",
				"action", m.Action, "key", m.Key)
			continue
		}
		loaded++
	}

	a.metrics.sanctionsLoaded.Add(int64(loaded))
	a.log.Info("loaded sanctions", "count", loaded)
	return nil
}

// One worker, so records are written in the order things happened.
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
		// Only logged: the message is delivered, and the sender can do nothing.
		a.log.Error("record failed", "err", err)
	}
}
