package metrics

import (
	"strings"
	"sync"
	"testing"
)

func scrape(t *testing.T, r *Registry) string {
	t.Helper()
	var out strings.Builder
	if _, err := r.WriteTo(&out); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	return out.String()
}

func TestCounter(t *testing.T) {
	r := New("test_")
	c := r.Counter("things_total", "Things that happened.")

	c.Inc()
	c.Add(4)
	if got := c.Value(); got != 5 {
		t.Fatalf("value = %d, want 5", got)
	}

	out := scrape(t, r)
	for _, want := range []string{
		"# HELP test_things_total Things that happened.",
		"# TYPE test_things_total counter",
		"test_things_total 5",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("scrape is missing %q:\n%s", want, out)
		}
	}
}

func TestGauge(t *testing.T) {
	r := New("test_")
	g := r.Gauge("held", "Things being held.")

	g.Inc()
	g.Inc()
	g.Dec()
	if got := g.Value(); got != 1 {
		t.Fatalf("value = %d, want 1", got)
	}
	g.Set(42)

	if out := scrape(t, r); !strings.Contains(out, "test_held 42") {
		t.Errorf("scrape is missing the gauge:\n%s", out)
	}
}

// A gauge read at scrape time is how something already counted elsewhere
// gets exported without being counted twice.
func TestGaugeFunc(t *testing.T) {
	r := New("test_")
	live := []string{"a", "b", "c"}
	r.GaugeFunc("live", "Live things.", func() int64 { return int64(len(live)) })

	if out := scrape(t, r); !strings.Contains(out, "test_live 3") {
		t.Errorf("scrape is missing the gauge func:\n%s", out)
	}

	live = live[:1]
	if out := scrape(t, r); !strings.Contains(out, "test_live 1") {
		t.Errorf("the gauge func was not re-read:\n%s", out)
	}
}

func TestCounterVec(t *testing.T) {
	r := New("test_")
	v := r.CounterVec("refusals_total", "Refusals by reason.", "code")

	v.With("throttled").Inc()
	v.With("throttled").Inc()
	v.With("muted").Inc()

	out := scrape(t, r)
	for _, want := range []string{
		`test_refusals_total{code="muted"} 1`,
		`test_refusals_total{code="throttled"} 2`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("scrape is missing %q:\n%s", want, out)
		}
	}
	// One HELP and TYPE for the whole vector, not one per label value.
	if n := strings.Count(out, "# TYPE test_refusals_total"); n != 1 {
		t.Errorf("the vector emitted %d TYPE lines, want 1:\n%s", n, out)
	}
}

// Label values come out sorted, so two scrapes can be diffed.
func TestCounterVecOutputIsStable(t *testing.T) {
	r := New("test_")
	v := r.CounterVec("things_total", "Things.", "kind")
	for _, name := range []string{"zebra", "alpha", "middle"} {
		v.With(name).Inc()
	}

	out := scrape(t, r)
	alpha := strings.Index(out, "alpha")
	middle := strings.Index(out, "middle")
	zebra := strings.Index(out, "zebra")
	if !(alpha < middle && middle < zebra) {
		t.Fatalf("label values are not sorted:\n%s", out)
	}
	if out != scrape(t, r) {
		t.Fatal("two scrapes of the same registry differ")
	}
}

// Metrics come out in registration order, so the output reads like the
// code that declares it.
func TestRegistrationOrder(t *testing.T) {
	r := New("test_")
	r.Counter("first_total", "First.")
	r.Counter("second_total", "Second.")

	out := scrape(t, r)
	if strings.Index(out, "first_total") > strings.Index(out, "second_total") {
		t.Fatalf("metrics are out of registration order:\n%s", out)
	}
}

func TestLabelEscaping(t *testing.T) {
	r := New("test_")
	v := r.CounterVec("things_total", "Things.", "kind")
	v.With(`a "quoted" \ thing`).Inc()

	if out := scrape(t, r); !strings.Contains(out, `{kind="a \"quoted\" \\ thing"}`) {
		t.Errorf("the label value was not escaped:\n%s", out)
	}
}

// A duplicate name would silently shadow another metric. Failing at
// startup, which is the only time this can happen, is better.
func TestDuplicateNamePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("registering a duplicate name did not panic")
		}
	}()

	r := New("test_")
	r.Counter("things_total", "Things.")
	r.Counter("things_total", "Things again.")
}

func TestEmptyRegistry(t *testing.T) {
	if out := scrape(t, New("test_")); out != "" {
		t.Fatalf("an empty registry produced %q", out)
	}
}

func TestConcurrentUse(t *testing.T) {
	r := New("test_")
	c := r.Counter("things_total", "Things.")
	g := r.Gauge("held", "Held.")
	v := r.CounterVec("refusals_total", "Refusals.", "code")

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			for j := range 1000 {
				c.Inc()
				g.Inc()
				g.Dec()
				v.With([]string{"a", "b", "c"}[j%3]).Inc()
				if i == 0 && j%100 == 0 {
					scrape(t, r) // scraping races the writers too
				}
			}
		})
	}
	wg.Wait()

	if got := c.Value(); got != 8000 {
		t.Fatalf("counter = %d, want 8000", got)
	}
	total := v.With("a").Value() + v.With("b").Value() + v.With("c").Value()
	if total != 8000 {
		t.Fatalf("vector total = %d, want 8000", total)
	}
}
