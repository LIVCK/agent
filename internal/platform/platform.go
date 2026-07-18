// Package platform holds the injectable Clock, FS and Host abstractions the
// agent is built on. Nothing outside this package calls time.Now, touches
// /proc, or reaches the real filesystem directly; everything goes through these
// interfaces so schedulers, backoff, buffering and identity are testable with
// fakes (see the platformtest subpackage). Real() returns the production
// implementation backed by the OS.
package platform

// Platform bundles the host abstractions the agent depends on. It is passed by
// value; the fields are interfaces. Exec is used only by the opt-in
// hardware-telemetry collectors (gpu, smart) and may be nil for a build that
// wires none of them.
type Platform struct {
	Clock Clock
	FS    FS
	Host  Host
	Exec  Exec
}

// Real returns the production platform backed by the OS clock, filesystem,
// hostname and process runner.
func Real() Platform {
	return Platform{
		Clock: SystemClock{},
		FS:    OSFS{},
		Host:  OSHost{},
		Exec:  OSExec{},
	}
}
