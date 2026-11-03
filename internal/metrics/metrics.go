// Package metrics writes Prometheus text by hand: the client library is too big.
package metrics

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

type Registry struct {
	prefix string

	mu      sync.RWMutex
	entries []entry // registration order, so output is stable
	names   map[string]bool
}

type entry struct {
	name string
	help string
	kind string
	read func(w io.Writer, full string)
}

func New(prefix string) *Registry {
	return &Registry{prefix: prefix, names: make(map[string]bool)}
}

type Counter struct{ v atomic.Int64 }

func (c *Counter) Inc()         { c.v.Add(1) }
func (c *Counter) Add(n int64)  { c.v.Add(n) }
func (c *Counter) Value() int64 { return c.v.Load() }

type Gauge struct{ v atomic.Int64 }

func (g *Gauge) Set(n int64)  { g.v.Store(n) }
func (g *Gauge) Inc()         { g.v.Add(1) }
func (g *Gauge) Dec()         { g.v.Add(-1) }
func (g *Gauge) Value() int64 { return g.v.Load() }

// CounterVec has one label: everything splits one way, and more costs a hash.
type CounterVec struct {
	label string

	mu sync.RWMutex
	m  map[string]*Counter
}

// With must never see a client-controlled value, or the map grows unbounded.
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

func (r *Registry) Counter(name, help string) *Counter {
	c := &Counter{}
	r.register(name, help, "counter", func(w io.Writer, full string) {
		fmt.Fprintf(w, "%s %d\n", full, c.Value())
	})
	return c
}

func (r *Registry) Gauge(name, help string) *Gauge {
	g := &Gauge{}
	r.register(name, help, "gauge", func(w io.Writer, full string) {
		fmt.Fprintf(w, "%s %d\n", full, g.Value())
	})
	return g
}

// GaugeFunc reads the real thing at scrape time; a mirrored copy could disagree.
func (r *Registry) GaugeFunc(name, help string, read func() int64) {
	r.register(name, help, "gauge", func(w io.Writer, full string) {
		fmt.Fprintf(w, "%s %d\n", full, read())
	})
}

func (r *Registry) CounterVec(name, help, label string) *CounterVec {
	v := &CounterVec{label: label, m: make(map[string]*Counter)}
	r.register(name, help, "counter", func(w io.Writer, full string) {
		v.mu.RLock()
		defer v.mu.RUnlock()

		values := make([]string, 0, len(v.m))
		for value := range v.m {
			values = append(values, value)
		}
		sort.Strings(values) // so two scrapes diff cleanly

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
		// A panic at startup beats one metric silently shadowing another.
		panic("metrics: duplicate metric " + name)
	}
	r.names[name] = true
	r.entries = append(r.entries, entry{name: name, help: help, kind: kind, read: read})
}

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

func escape(s string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s)
}
