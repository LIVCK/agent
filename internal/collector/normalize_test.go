package collector

import "testing"

func TestNormalizeSegment(t *testing.T) {
	cases := map[string]string{
		"/":           "_root",
		"/var/log":    "_var_log",
		"sda":         "sda",
		"nvme0n1":     "nvme0n1",
		"dm-0":        "dm-0",
		"eth0":        "eth0",
		"Weird Name!": "weird_name_",
		"a.b_c-d":     "a.b_c-d",
	}
	for in, want := range cases {
		if got := NormalizeSegment(in); got != want {
			t.Errorf("NormalizeSegment(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNormalizeSegmentTruncates(t *testing.T) {
	long := "abcdefghijklmnopqrstuvwxyz0123456789abcdefghij" // 46 chars
	got := NormalizeSegment(long)
	if len(got) != maxSegmentLen {
		t.Fatalf("len = %d, want %d", len(got), maxSegmentLen)
	}
}

func TestDedupeSegment(t *testing.T) {
	used := map[string]bool{}
	if s := DedupeSegment("data", used); s != "data" {
		t.Fatalf("first = %q", s)
	}
	if s := DedupeSegment("data", used); s != "data-2" {
		t.Fatalf("second = %q", s)
	}
	if s := DedupeSegment("data", used); s != "data-3" {
		t.Fatalf("third = %q", s)
	}
}

func TestCapBySizeKeepsLargest(t *testing.T) {
	type item struct {
		name string
		size float64
	}
	items := []item{{"a", 10}, {"b", 30}, {"c", 20}, {"d", 5}}
	got := CapBySize(items, 2, func(i item) string { return i.name }, func(i item) float64 { return i.size })
	if len(got) != 2 {
		t.Fatalf("want 2, got %d", len(got))
	}
	names := map[string]bool{}
	for _, g := range got {
		names[g.name] = true
	}
	if !names["b"] || !names["c"] {
		t.Fatalf("want the two largest (b,c), got %v", names)
	}
}
