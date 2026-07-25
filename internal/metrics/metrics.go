// Package metrics is a small counter registry that exports in the
// Prometheus text format.
//
// Hand-written rather than pulled in, because what the server needs is a
// few atomic counters and a way to print them, and the client library is a
// large dependency to take on for that. The exposition format is the part
// that matters — it is what makes the numbers scrapeable by anything — and
// it is a dozen lines to emit.
//
// This is deliberately NOT a hook. The hooks are for policy the server
// should not have opinions about; the number of connections it is holding
// is not policy, it is the server describing itself, and a deployment that
// wants those numbers somewhere else can scrape them.
//
// Everything here is safe for concurrent use. Counters are atomic and the
// registry is only locked when a metric is registered or the whole set is
// written out, so the hot path is one atomic add.
package metrics

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Registry is a set of named metrics.
type Registry struct {
	prefix string

	mu      sync.RWMutex
	entries []entry // registration order, so output is stable
	names   map[string]bool
}

type entry struct {
	name string
	help string
	kind string // "counter" or "gauge"
	read func(w io.Writer, full string)
}

// New returns a registry whose metric names all start with prefix.
func New(prefix string) *Registry {
	return &Registry{prefix: prefix, names: make(map[string]bool)}
}

// Counter is a number that only goes up.
type Counter struct{ v atomic.Int64 }

func (c *Counter) Inc()         { c.v.Add(1) }
func (c *Counter) Add(n int64)  { c.v.Add(n) }
func (c *Counter) Value() int64 { return c.v.Load() }

// Gauge is a number that goes up and down.
type Gauge struct{ v atomic.Int64 }

func (g *Gauge) Set(n int64)  { g.v.Store(n) }
func (g *Gauge) Inc()         { g.v.Add(1) }
func (g *Gauge) Dec()         { g.v.Add(-1) }
func (g *Gauge) Value() int64 { return g.v.Load() }

// CounterVec is a counter split by one label — an error code, a verb, a
// codec name.
//
// One label rather than many, because everything this server wants to count
// splits one way. A general label set would need a key type, a hash of it
// on every increment, and a reason.
type CounterVec struct {
	label string

	mu sync.RWMutex
	m  map[string]*Counter
}

// With returns the counter for a label value, creating it on first use.
//
// The label values here are a closed set — the ERR codes, the verbs, the
// codec names — so this map cannot grow without bound. Do not hand it
// anything a client controls: a nick or a message would turn a metric into
// a memory leak with a network interface.
func (v *CounterVec) With(value string) *Counter {
	v.mu.RLock()
	c, ok := v.m[value]
	v.mu.RUnlock()
	if ok {
		return c
	}

	v.mu.Lock()
	defer v.mu.Unlock()
	if c, ok = v.m[value]; ok {
		return c
	}
	c = &Counter{}
	v.m[value] = c
	return c
}

// Counter registers a counter.
func (r *Registry) Counter(name, help string) *Counter {
	c := &Counter{}
	r.register(name, help, "counter", func(w io.Writer, full string) {
		fmt.Fprintf(w, "%s %d\n", full, c.Value())
	})
	return c
}

// Gauge registers a gauge.
func (r *Registry) Gauge(name, help string) *Gauge {
	g := &Gauge{}
	r.register(name, help, "gauge", func(w io.Writer, full string) {
		fmt.Fprintf(w, "%s %d\n", full, g.Value())
	})
	return g
}

// GaugeFunc registers a gauge read at scrape time.
//
// It is how anything already counted elsewhere gets exported without being
// counted twice: the number of connections is the length of a map the
// server already keeps, and mirroring it into a gauge would only create
// something that can disagree with it.
func (r *Registry) GaugeFunc(name, help string, read func() int64) {
	r.register(name, help, "gauge", func(w io.Writer, full string) {
		fmt.Fprintf(w, "%s %d\n", full, read())
	})
}

// CounterVec registers a counter split by one label.
func (r *Registry) CounterVec(name, help, label string) *CounterVec {
	v := &CounterVec{label: label, m: make(map[string]*Counter)}
	r.register(name, help, "counter", func(w io.Writer, full string) {
		v.mu.RLock()
		defer v.mu.RUnlock()

		values := make([]string, 0, len(v.m))
		for value := range v.m {
			values = append(values, value)
		}
		sort.Strings(values) // stable output, so a diff of two scrapes reads

		for _, value := range values {
			fmt.Fprintf(w, "%s{%s=\"%s\"} %d\n", full, v.label, escape(value), v.m[value].Value())
		}
	})
	return v
}

func (r *Registry) register(name, help, kind string, read func(io.Writer, string)) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.names[name] {
		// A duplicate name silently shadowing another metric is worse than
		// a panic at startup, which is when this can only happen.
		panic("metrics: duplicate metric " + name)
	}
	r.names[name] = true
	r.entries = append(r.entries, entry{name: name, help: help, kind: kind, read: read})
}

// WriteTo writes every metric in the Prometheus text exposition format.
func (r *Registry) WriteTo(w io.Writer) (int64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var buf strings.Builder
	for _, e := range r.entries {
		full := r.prefix + e.name
		fmt.Fprintf(&buf, "# HELP %s %s\n", full, e.help)
		fmt.Fprintf(&buf, "# TYPE %s %s\n", full, e.kind)
		e.read(&buf, full)
	}

	n, err := io.WriteString(w, buf.String())
	return int64(n), err
}

// escape quotes a label value the way the exposition format wants.
func escape(s string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s)
}
