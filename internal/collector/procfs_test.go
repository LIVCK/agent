package collector

import "testing"

func TestParseMeminfoNormalizesToBytes(t *testing.T) {
	m := ParseMeminfo([]byte("MemTotal:       1000 kB\nMemFree:         100 kB\nHugePages_Total:       0\n"))
	if m["MemTotal"] != 1000*1024 {
		t.Fatalf("MemTotal = %d, want %d", m["MemTotal"], 1000*1024)
	}
	if m["MemFree"] != 100*1024 {
		t.Fatalf("MemFree = %d", m["MemFree"])
	}
	// A count field without a kB unit is kept raw.
	if m["HugePages_Total"] != 0 {
		t.Fatalf("HugePages_Total = %d", m["HugePages_Total"])
	}
}

func TestParseVmstatKey(t *testing.T) {
	data := []byte("nr_free_pages 123\noom_kill 9\npgfault 4567\n")
	if v, ok := ParseVmstatKey(data, "oom_kill"); !ok || v != 9 {
		t.Fatalf("oom_kill = %d ok=%v, want 9 true", v, ok)
	}
	if _, ok := ParseVmstatKey(data, "missing"); ok {
		t.Fatal("missing key must report ok=false")
	}
}
