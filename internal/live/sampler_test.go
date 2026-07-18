package live

import (
	"context"
	"errors"
	"testing"

	"github.com/LIVCK/agent/internal/collector"
)

// fakeCollector is a controllable collector for the sampler test.
type fakeCollector struct {
	name     string
	avail    bool
	interval bool
	samples  []collector.Sample
	err      error
}

func (f fakeCollector) Name() string                                        { return f.name }
func (f fakeCollector) Available() bool                                     { return f.avail }
func (f fakeCollector) Collect(context.Context) ([]collector.Sample, error) { return f.samples, f.err }
func (f fakeCollector) IntervalSampled() bool                               { return f.interval }

func TestRegistrySamplerFlattensCheapCollectors(t *testing.T) {
	reg := collector.NewRegistry()
	reg.Register(fakeCollector{name: "cpu", avail: true, samples: []collector.Sample{
		{Key: "sys.cpu.total_pct", Value: 20},
	}})
	reg.Register(fakeCollector{name: "mem", avail: true, samples: []collector.Sample{
		{Key: "sys.mem.used_pct", Value: 55},
	}})
	// Interval-sampled (gpu/smart analogue): must be skipped on the 2s cadence.
	reg.Register(fakeCollector{name: "gpu", avail: true, interval: true, samples: []collector.Sample{
		{Key: "sys.gpu.util_pct", Value: 99},
	}})
	// Unavailable: skipped.
	reg.Register(fakeCollector{name: "psi", avail: false, samples: []collector.Sample{
		{Key: "sys.psi.some", Value: 1},
	}})
	// Errors are swallowed — a live frame missing one source is fine.
	reg.Register(fakeCollector{name: "net", avail: true, err: errors.New("boom"), samples: []collector.Sample{
		{Key: "sys.net.rx", Value: 1},
	}})

	got := NewRegistrySampler(reg, nil).Sample(context.Background())

	if got["sys.cpu.total_pct"] != 20 || got["sys.mem.used_pct"] != 55 {
		t.Fatalf("missing cheap gauges: %+v", got)
	}
	if _, ok := got["sys.gpu.util_pct"]; ok {
		t.Fatal("interval-sampled collector must be skipped")
	}
	if _, ok := got["sys.psi.some"]; ok {
		t.Fatal("unavailable collector must be skipped")
	}
	if _, ok := got["sys.net.rx"]; ok {
		t.Fatal("errored collector must contribute nothing")
	}
	if len(got) != 2 {
		t.Fatalf("want exactly the 2 cheap gauges, got %d: %+v", len(got), got)
	}
}
