package registry

import (
	"testing"
	"time"

	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/platform"
	"github.com/LIVCK/agent/internal/platform/platformtest"
)

type stubHost struct{}

func (stubHost) Hostname() (string, error) { return "test-host", nil }

func testPlatform() platform.Platform {
	return platform.Platform{
		Clock: platformtest.NewClock(time.Unix(1000, 0)),
		FS:    platformtest.NewMemFS(),
		Host:  stubHost{},
	}
}

func TestBuildRegistersAllCollectors(t *testing.T) {
	r := Build(testPlatform(), config.Defaults, nil)

	names := map[string]bool{}
	for _, c := range r.Collectors() {
		names[c.Name()] = true
	}
	for _, want := range []string{"cpu", "load", "mem", "swap", "psi", "system", "disk", "diskio", "net", "self"} {
		if !names[want] {
			t.Errorf("collector %q not registered", want)
		}
	}
}

func TestDisabledCollectorReportsUnavailable(t *testing.T) {
	cfg := func() *config.Config {
		c := config.Defaults()
		c.Collectors.CPU = false
		return c
	}
	r := Build(testPlatform(), cfg, nil)
	for _, c := range r.Collectors() {
		if c.Name() == "cpu" && c.Available() {
			t.Fatal("disabled cpu collector must report Available()==false")
		}
	}
}
