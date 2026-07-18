package platform

import "os"

// Host exposes host facts the foundation needs. It is intentionally minimal: the
// full host profile (distro, kernel, arch, virtualization, cpu, ram) is collected
// by the collector layer and hangs off there, not here. The identity layer needs
// the hostname for the enroll fingerprint and report meta only.
type Host interface {
	Hostname() (string, error)
}

// OSHost is the production Host backed by the os package.
type OSHost struct{}

// Hostname returns the kernel hostname.
func (OSHost) Hostname() (string, error) { return os.Hostname() }
