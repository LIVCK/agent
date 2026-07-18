package collector

import "time"

// This file fixes the delta/clamp/rate conventions every rate-based collector
// shares. The rule: work on raw kernel counters with our own math, never a
// Percent() helper. A percent series must stay in [0,100]; a
// counter only advances, so a negative delta is a reset (reboot, wrap, VM
// migration) that re-baselines and emits nothing this sample (no zero-fake).

// ClampPercent clamps v into [0,100]. Percent samples are clamped defensively
// before they leave the collector: pulse drops out-of-range values with reason
// "sanity", and a normalized ratio can land a hair above 100 from rounding.
func ClampPercent(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 100 {
		return 100
	}
	return v
}

// CounterDelta returns the forward delta cur-prev and whether a reset occurred.
// reset is true when cur < prev; the delta is then meaningless and the caller
// must re-baseline and emit nothing for this sample.
func CounterDelta(cur, prev uint64) (delta uint64, reset bool) {
	if cur < prev {
		return 0, true
	}
	return cur - prev, false
}

// Rate returns value per second over dt. A non-positive dt yields 0 (no window
// elapsed), never a divide-by-zero or a spike.
func Rate(value float64, dt time.Duration) float64 {
	sec := dt.Seconds()
	if sec <= 0 {
		return 0
	}
	return value / sec
}

// RatioPercent returns part/whole as a percentage clamped into [0,100]. A
// non-positive whole yields 0. Used for used_pct style keys computed from two
// byte counters.
func RatioPercent(part, whole float64) float64 {
	if whole <= 0 {
		return 0
	}
	return ClampPercent(part / whole * 100)
}
