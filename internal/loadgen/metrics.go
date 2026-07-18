package loadgen

import (
	"net/http"
	"sort"
	"sync/atomic"
	"time"
)

// latencyBounds are the upper edges (in seconds) of the latency histogram
// buckets, ~1.4x log-spaced from 0.1ms to 60s plus an overflow catch-all. A
// bucketed histogram keeps recording lock-free and bounded in memory at any
// request rate; percentiles are reported at the bucket's upper edge (coarse but
// sufficient to see p50/p99 move under load).
var latencyBounds = buildLatencyBounds()

func buildLatencyBounds() []float64 {
	var b []float64
	for v := 0.0001; v < 60; v *= 1.4 {
		b = append(b, v)
	}
	return append(b, 60, 1e9)
}

// Recorder accumulates ingest request outcomes lock-free (atomics only) so it can
// sit in the hot RoundTrip path of thousands of concurrent senders.
type Recorder struct {
	total        atomic.Int64
	ok           atomic.Int64 // 202
	retry429     atomic.Int64 // 429
	retry5xx     atomic.Int64 // 500-599
	drop4xx      atomic.Int64 // 400/413/422 and other non-auth 4xx
	auth         atomic.Int64 // 401/403/409 (quarantine)
	other        atomic.Int64 // unexpected 2xx/3xx
	transportErr atomic.Int64 // no HTTP response (connect/reset/timeout)
	latSumNanos  atomic.Uint64
	buckets      []atomic.Uint64
}

// NewRecorder builds an empty Recorder.
func NewRecorder() *Recorder {
	return &Recorder{buckets: make([]atomic.Uint64, len(latencyBounds))}
}

// Record folds one request outcome into the recorder. statusCode is ignored when
// transportErr is true (no response arrived).
func (r *Recorder) Record(d time.Duration, statusCode int, transportErr bool) {
	r.total.Add(1)
	if d < 0 {
		d = 0
	}
	r.latSumNanos.Add(uint64(d.Nanoseconds()))
	idx := sort.SearchFloat64s(latencyBounds, d.Seconds())
	if idx >= len(r.buckets) {
		idx = len(r.buckets) - 1
	}
	r.buckets[idx].Add(1)

	switch {
	case transportErr:
		r.transportErr.Add(1)
	case statusCode == http.StatusAccepted:
		r.ok.Add(1)
	case statusCode == http.StatusTooManyRequests:
		r.retry429.Add(1)
	case statusCode >= 500:
		r.retry5xx.Add(1)
	case statusCode == 401 || statusCode == 403 || statusCode == 409:
		r.auth.Add(1)
	case statusCode >= 400:
		r.drop4xx.Add(1)
	default:
		r.other.Add(1)
	}
}

// Raw is a lossless readout of the recorder's counters and histogram buckets at
// one instant, so two Raws can be diffed into a per-window Snapshot (percentiles
// included). It is the basis for measuring one ramp step in isolation instead of
// reporting cumulative totals.
type Raw struct {
	Total, OK, Retry429, Retry5xx, Drop4xx, Auth, Other, TransportErr int64
	SumNanos                                                          uint64
	Buckets                                                           []uint64
}

// Raw reads the current counters and histogram buckets.
func (r *Recorder) Raw() Raw {
	buckets := make([]uint64, len(r.buckets))
	for i := range r.buckets {
		buckets[i] = r.buckets[i].Load()
	}
	return Raw{
		Total:        r.total.Load(),
		OK:           r.ok.Load(),
		Retry429:     r.retry429.Load(),
		Retry5xx:     r.retry5xx.Load(),
		Drop4xx:      r.drop4xx.Load(),
		Auth:         r.auth.Load(),
		Other:        r.other.Load(),
		TransportErr: r.transportErr.Load(),
		SumNanos:     r.latSumNanos.Load(),
		Buckets:      buckets,
	}
}

// DeltaSnapshot returns the readout for the window (from, to]: counts are the
// difference and percentiles come from the difference of the histogram buckets,
// so one ramp step is measured without the earlier steps' history.
func DeltaSnapshot(from, to Raw) Snapshot {
	total := to.Total - from.Total
	s := Snapshot{
		Total:        total,
		OK:           to.OK - from.OK,
		Retry429:     to.Retry429 - from.Retry429,
		Retry5xx:     to.Retry5xx - from.Retry5xx,
		Drop4xx:      to.Drop4xx - from.Drop4xx,
		Auth:         to.Auth - from.Auth,
		Other:        to.Other - from.Other,
		TransportErr: to.TransportErr - from.TransportErr,
	}
	if total > 0 {
		s.MeanMs = float64(to.SumNanos-from.SumNanos) / float64(total) / 1e6
	}
	s.P50Ms = percentileFromBuckets(from.Buckets, to.Buckets, total, 0.50)
	s.P90Ms = percentileFromBuckets(from.Buckets, to.Buckets, total, 0.90)
	s.P99Ms = percentileFromBuckets(from.Buckets, to.Buckets, total, 0.99)
	return s
}

func percentileFromBuckets(from, to []uint64, total int64, p float64) float64 {
	if total <= 0 {
		return 0
	}
	target := uint64(float64(total) * p)
	if target == 0 {
		target = 1
	}
	var cum uint64
	for i := range to {
		cum += to[i] - from[i]
		if cum >= target {
			return latencyBounds[i] * 1000
		}
	}
	return latencyBounds[len(latencyBounds)-1] * 1000
}

// Snapshot is an immutable readout of the recorder at one instant.
type Snapshot struct {
	Total        int64
	OK           int64
	Retry429     int64
	Retry5xx     int64
	Drop4xx      int64
	Auth         int64
	Other        int64
	TransportErr int64
	MeanMs       float64
	P50Ms        float64
	P90Ms        float64
	P99Ms        float64
}

// Snapshot reads the current counters and derives latency percentiles from the
// histogram. Percentiles are the upper edge of the containing bucket.
func (r *Recorder) Snapshot() Snapshot {
	total := r.total.Load()
	s := Snapshot{
		Total:        total,
		OK:           r.ok.Load(),
		Retry429:     r.retry429.Load(),
		Retry5xx:     r.retry5xx.Load(),
		Drop4xx:      r.drop4xx.Load(),
		Auth:         r.auth.Load(),
		Other:        r.other.Load(),
		TransportErr: r.transportErr.Load(),
	}
	if total > 0 {
		s.MeanMs = float64(r.latSumNanos.Load()) / float64(total) / 1e6
	}
	s.P50Ms = r.percentileMs(0.50)
	s.P90Ms = r.percentileMs(0.90)
	s.P99Ms = r.percentileMs(0.99)
	return s
}

// OKRate is the fraction of requests that returned 202 (0..1).
func (s Snapshot) OKRate() float64 {
	if s.Total == 0 {
		return 0
	}
	return float64(s.OK) / float64(s.Total)
}

// percentileMs returns the p-th latency percentile in milliseconds at the
// containing bucket's upper edge.
func (r *Recorder) percentileMs(p float64) float64 {
	total := r.total.Load()
	if total == 0 {
		return 0
	}
	target := uint64(float64(total) * p)
	if target == 0 {
		target = 1
	}
	var cum uint64
	for i := range r.buckets {
		cum += r.buckets[i].Load()
		if cum >= target {
			return latencyBounds[i] * 1000
		}
	}
	return latencyBounds[len(latencyBounds)-1] * 1000
}

// instrumentedRT wraps a RoundTripper, recording latency + status of every trip.
type instrumentedRT struct {
	base http.RoundTripper
	rec  *Recorder
}

func (t *instrumentedRT) RoundTrip(req *http.Request) (*http.Response, error) {
	start := time.Now()
	resp, err := t.base.RoundTrip(req)
	d := time.Since(start)
	if err != nil {
		t.rec.Record(d, 0, true)
		return resp, err
	}
	t.rec.Record(d, resp.StatusCode, false)
	return resp, err
}

// InstrumentClient returns a shallow copy of base whose Transport records every
// request into rec. The original client is not mutated.
func InstrumentClient(base *http.Client, rec *Recorder) *http.Client {
	bt := base.Transport
	if bt == nil {
		bt = http.DefaultTransport
	}
	c := *base
	c.Transport = &instrumentedRT{base: bt, rec: rec}
	return &c
}
