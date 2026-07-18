package collector

import (
	"context"
	"errors"
	"testing"
	"time"
)

type stubCollector struct {
	name    string
	avail   bool
	samples []Sample
	err     error
}

func (s stubCollector) Name() string                              { return s.name }
func (s stubCollector) Available() bool                           { return s.avail }
func (s stubCollector) Collect(context.Context) ([]Sample, error) { return s.samples, s.err }

func TestRegistrySkipsUnavailableAndKeepsGoingOnError(t *testing.T) {
	r := NewRegistry()
	r.Register(stubCollector{name: "cpu", avail: true, samples: []Sample{{"sys.cpu.total_pct", 10}}})
	r.Register(stubCollector{name: "psi", avail: false, samples: []Sample{{"sys.psi.cpu_some_pct", 1}}})
	r.Register(stubCollector{name: "disk", avail: true, err: errors.New("stuck mount")})
	r.Register(stubCollector{name: "mem", avail: true, samples: []Sample{{"sys.mem.used_pct", 40}}})

	samples, errs := r.Collect(context.Background())
	if len(samples) != 2 {
		t.Fatalf("want 2 samples (cpu+mem), got %d", len(samples))
	}
	if len(errs) != 1 || errs[0].Collector != "disk" {
		t.Fatalf("want one disk error, got %v", errs)
	}
}

func TestClampPercent(t *testing.T) {
	if ClampPercent(-5) != 0 || ClampPercent(150) != 100 || ClampPercent(42) != 42 {
		t.Fatal("ClampPercent bounds wrong")
	}
}

func TestCounterDelta(t *testing.T) {
	if d, reset := CounterDelta(100, 40); d != 60 || reset {
		t.Fatalf("forward delta wrong: %d reset=%v", d, reset)
	}
	if d, reset := CounterDelta(5, 100); d != 0 || !reset {
		t.Fatalf("counter reset not detected: %d reset=%v", d, reset)
	}
}

func TestRate(t *testing.T) {
	if Rate(100, 2*time.Second) != 50 {
		t.Fatal("rate over 2s wrong")
	}
	if Rate(100, 0) != 0 {
		t.Fatal("rate with zero window must be 0")
	}
}

func TestRatioPercent(t *testing.T) {
	if RatioPercent(50, 200) != 25 {
		t.Fatal("ratio percent wrong")
	}
	if RatioPercent(1, 0) != 0 {
		t.Fatal("ratio with zero whole must be 0")
	}
}
