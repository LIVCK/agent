package swap

import (
	"context"
	"testing"

	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/platform/platformtest"
)

const k = 1024

func TestSwapUsage(t *testing.T) {
	fs := platformtest.NewMemFS()
	_ = fs.WriteFileAtomic(meminfoPath, []byte("SwapTotal:      1000 kB\nSwapFree:        250 kB\n"), 0o644)

	got, err := New(fs, config.Defaults).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]float64{}
	for _, s := range got {
		m[s.Key] = s.Value
	}
	if m["sys.swap.total_bytes"] != 1000*k || m["sys.swap.used_bytes"] != 750*k {
		t.Fatalf("swap bytes wrong: %v", m)
	}
	if m["sys.swap.used_pct"] != 75 {
		t.Fatalf("swap used_pct = %v, want 75", m["sys.swap.used_pct"])
	}
}

func TestSwapZeroWhenNoSwap(t *testing.T) {
	fs := platformtest.NewMemFS()
	_ = fs.WriteFileAtomic(meminfoPath, []byte("SwapTotal:            0 kB\nSwapFree:             0 kB\n"), 0o644)

	got, err := New(fs, config.Defaults).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]float64{}
	for _, s := range got {
		m[s.Key] = s.Value
	}
	// A host with no swap reports a real total of zero across all three keys.
	if m["sys.swap.total_bytes"] != 0 || m["sys.swap.used_pct"] != 0 {
		t.Fatalf("no-swap host must report zeros, got %v", m)
	}
	if len(m) != 3 {
		t.Fatalf("want three swap keys, got %v", m)
	}
}
