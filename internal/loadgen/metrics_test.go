package loadgen

import (
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestRecorderPercentiles(t *testing.T) {
	r := NewRecorder()
	for i := 0; i < 99; i++ {
		r.Record(10*time.Millisecond, http.StatusAccepted, false)
	}
	r.Record(5*time.Second, http.StatusAccepted, false) // one slow outlier

	s := r.Snapshot()
	if s.Total != 100 || s.OK != 100 {
		t.Fatalf("total=%d ok=%d, want 100/100", s.Total, s.OK)
	}
	if s.P50Ms < 8 || s.P50Ms > 20 {
		t.Errorf("p50=%.1fms, want ~10ms", s.P50Ms)
	}
	if s.P99Ms < 8 || s.P99Ms > 30 {
		t.Errorf("p99=%.1fms, want ~10ms (the 5s outlier is above p99)", s.P99Ms)
	}
	if s.OKRate() != 1 {
		t.Errorf("ok rate=%.2f, want 1", s.OKRate())
	}
}

func TestRecorderClassifies(t *testing.T) {
	r := NewRecorder()
	r.Record(time.Millisecond, http.StatusAccepted, false)
	r.Record(time.Millisecond, http.StatusTooManyRequests, false)
	r.Record(time.Millisecond, http.StatusServiceUnavailable, false)
	r.Record(time.Millisecond, http.StatusBadRequest, false)
	r.Record(time.Millisecond, http.StatusForbidden, false)
	r.Record(time.Millisecond, 0, true)

	s := r.Snapshot()
	if s.OK != 1 || s.Retry429 != 1 || s.Retry5xx != 1 || s.Drop4xx != 1 || s.Auth != 1 || s.TransportErr != 1 {
		t.Fatalf("misclassified: %+v", s)
	}
	if s.Total != 6 {
		t.Fatalf("total=%d, want 6", s.Total)
	}
}

func TestDeltaSnapshotIsolatesAWindow(t *testing.T) {
	r := NewRecorder()
	// Window 1: 50 fast 202s (should NOT leak into window 2's readout).
	for i := 0; i < 50; i++ {
		r.Record(1*time.Millisecond, http.StatusAccepted, false)
	}
	from := r.Raw()
	// Window 2: 100 at 20ms, of which 10 are 429s.
	for i := 0; i < 90; i++ {
		r.Record(20*time.Millisecond, http.StatusAccepted, false)
	}
	for i := 0; i < 10; i++ {
		r.Record(20*time.Millisecond, http.StatusTooManyRequests, false)
	}
	to := r.Raw()

	s := DeltaSnapshot(from, to)
	if s.Total != 100 {
		t.Fatalf("window total=%d, want 100 (window 1 must not leak in)", s.Total)
	}
	if s.OK != 90 || s.Retry429 != 10 {
		t.Fatalf("window ok=%d 429=%d, want 90/10", s.OK, s.Retry429)
	}
	if s.P50Ms < 15 || s.P50Ms > 40 {
		t.Errorf("window p50=%.1fms, want ~20ms (not the 1ms of window 1)", s.P50Ms)
	}
}

func TestRecorderConcurrent(t *testing.T) {
	r := NewRecorder()
	var wg sync.WaitGroup
	for g := 0; g < 20; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				r.Record(time.Millisecond, http.StatusAccepted, false)
			}
		}()
	}
	wg.Wait()
	if s := r.Snapshot(); s.Total != 20000 || s.OK != 20000 {
		t.Fatalf("concurrent total=%d ok=%d, want 20000/20000", s.Total, s.OK)
	}
}
