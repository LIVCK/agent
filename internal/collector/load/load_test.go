package load

import (
	"context"
	"testing"

	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/platform/platformtest"
)

func TestLoadParsesThreeAverages(t *testing.T) {
	fs := platformtest.NewMemFS()
	_ = fs.WriteFileAtomic(loadavgPath, []byte("0.50 1.25 2.00 3/456 78901\n"), 0o644)
	c := New(fs, config.Defaults)

	got, err := c.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]float64{}
	for _, s := range got {
		m[s.Key] = s.Value
	}
	if m["sys.load.1"] != 0.50 || m["sys.load.5"] != 1.25 || m["sys.load.15"] != 2.00 {
		t.Fatalf("load averages wrong: %v", m)
	}
}

func TestLoadDisabled(t *testing.T) {
	cfg := func() *config.Config {
		c := config.Defaults()
		c.Collectors.Load = false
		return c
	}
	if New(platformtest.NewMemFS(), cfg).Available() {
		t.Fatal("load must be unavailable when disabled")
	}
}
