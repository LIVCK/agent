package wire

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestMetricBatchRoundTrip(t *testing.T) {
	in := &MetricBatch{
		SchemaVersion:        1,
		IdempotencyKey:       "6f1c2e4a-9b7d-4c3a-8e21-0d5f9a1b2c3d",
		AgentVersion:         "1.0.0",
		AgentInstanceId:      "b2d9f0c1-3e4a-4f5b-9c6d-7e8f9a0b1c2d",
		SentAtUnixMs:         1_752_840_001_000,
		AppliedConfigVersion: 7,
		Fingerprint: map[string]string{
			"machine_id_hash": "abc123",
			"boot_id":         "def456",
			"hostname":        "api-prod-1",
		},
		Reports: []*Report{
			{
				SampledAtUnixMs: 1_752_840_000_000,
				Metrics: map[string]float64{
					"sys.cpu.total_pct": 12.4,
					"sys.mem.used_pct":  61.0,
				},
				Meta: map[string]string{"hostname": "api-prod-1"},
			},
		},
		Events: []*Event{
			{
				EventId:          "a1b2c3d4-e5f6-4a5b-8c6d-7e8f9a0b1c2d",
				OccurredAtUnixMs: 1_752_840_000_500,
				Type:             EventType_EVENT_TYPE_CLEAN_SHUTDOWN,
				Meta:             map[string]string{"type": "reboot", "reason": "kernel update"},
			},
		},
		ServicePublicId: "",
	}

	raw, err := proto.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	out := &MetricBatch{}
	if err := proto.Unmarshal(raw, out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if !proto.Equal(in, out) {
		t.Fatalf("round trip changed the message:\n in: %v\nout: %v", in, out)
	}
}

// An event-only batch carries no reports and each event may hold an empty
// metrics context. Reports themselves may carry an empty metrics map.
func TestEventOnlyBatchRoundTrip(t *testing.T) {
	in := &MetricBatch{
		SchemaVersion:  1,
		IdempotencyKey: "9a8b7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d",
		AgentVersion:   "1.0.0",
		Reports:        nil,
		Events: []*Event{
			{
				EventId:          "0f1e2d3c-4b5a-4968-8778-695a4b3c2d1e",
				OccurredAtUnixMs: 1_752_840_010_000,
				Type:             EventType_EVENT_TYPE_OOM_KILL,
				Meta:             map[string]string{"count": "1"},
			},
		},
	}

	raw, err := proto.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out := &MetricBatch{}
	if err := proto.Unmarshal(raw, out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !proto.Equal(in, out) {
		t.Fatalf("round trip changed the message:\n in: %v\nout: %v", in, out)
	}
	if len(out.Reports) != 0 {
		t.Fatalf("expected no reports, got %d", len(out.Reports))
	}
}

func TestReportEmptyMetricsRoundTrip(t *testing.T) {
	in := &MetricBatch{
		SchemaVersion: 1,
		Reports: []*Report{
			{SampledAtUnixMs: 1_752_840_000_000, Metrics: map[string]float64{}},
		},
	}
	raw, err := proto.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out := &MetricBatch{}
	if err := proto.Unmarshal(raw, out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !proto.Equal(in, out) {
		t.Fatalf("round trip changed the message")
	}
}

// The reserved ranges are the freeze guard: they must be present in the compiled
// descriptor so no future field can reuse those numbers without a deliberate
// contract change. protoreflect ranges are half-open [start, end).
func TestReservedRanges(t *testing.T) {
	cases := []struct {
		name string
		md   protoreflect.MessageDescriptor
		want [][2]int32
	}{
		{"MetricBatch", (&MetricBatch{}).ProtoReflect().Descriptor(), [][2]int32{{20, 30}}},
		{"Report", (&Report{}).ProtoReflect().Descriptor(), [][2]int32{{10, 15}}},
		{"Event", (&Event{}).ProtoReflect().Descriptor(), [][2]int32{{10, 15}}},
	}
	for _, c := range cases {
		ranges := c.md.ReservedRanges()
		got := make(map[[2]int32]bool, ranges.Len())
		for i := 0; i < ranges.Len(); i++ {
			r := ranges.Get(i)
			got[[2]int32{int32(r[0]), int32(r[1])}] = true
		}
		for _, w := range c.want {
			if !got[w] {
				t.Errorf("%s: reserved range [%d,%d) missing (have %v)", c.name, w[0], w[1], got)
			}
		}
	}
}

// Field numbers are part of the frozen contract. Assert the load-bearing ones so
// a reorder or renumber in wire.proto fails here.
func TestFieldNumbers(t *testing.T) {
	batch := (&MetricBatch{}).ProtoReflect().Descriptor().Fields()
	wantBatch := map[string]int32{
		"schema_version":         1,
		"idempotency_key":        2,
		"agent_version":          3,
		"agent_instance_id":      4,
		"sent_at_unix_ms":        5,
		"applied_config_version": 6,
		"fingerprint":            7,
		"reports":                8,
		"events":                 9,
		"service_public_id":      10,
	}
	assertFieldNumbers(t, "MetricBatch", batch, wantBatch)

	report := (&Report{}).ProtoReflect().Descriptor().Fields()
	assertFieldNumbers(t, "Report", report, map[string]int32{
		"sampled_at_unix_ms": 1,
		"metrics":            2,
		"meta":               3,
	})

	event := (&Event{}).ProtoReflect().Descriptor().Fields()
	assertFieldNumbers(t, "Event", event, map[string]int32{
		"event_id":            1,
		"occurred_at_unix_ms": 2,
		"type":                3,
		"meta":                4,
	})
}

func assertFieldNumbers(t *testing.T, msg string, fields protoreflect.FieldDescriptors, want map[string]int32) {
	t.Helper()
	for name, num := range want {
		fd := fields.ByName(protoreflect.Name(name))
		if fd == nil {
			t.Errorf("%s: field %q not found", msg, name)
			continue
		}
		if int32(fd.Number()) != num {
			t.Errorf("%s: field %q number = %d, want %d", msg, name, fd.Number(), num)
		}
	}
}

// The EventType zero value is the proto3 default and stands for "unknown to this
// build".
func TestEventTypeUnspecifiedIsZero(t *testing.T) {
	if EventType_EVENT_TYPE_UNSPECIFIED != 0 {
		t.Fatalf("EVENT_TYPE_UNSPECIFIED = %d, want 0", EventType_EVENT_TYPE_UNSPECIFIED)
	}
	if EventType_EVENT_TYPE_UNIT_FLAPPING != 20 {
		t.Fatalf("EVENT_TYPE_UNIT_FLAPPING = %d, want 20", EventType_EVENT_TYPE_UNIT_FLAPPING)
	}
}
