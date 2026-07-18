//go:build !linux

package disk

import "errors"

// realStatfs is a stub for non-Linux build hosts (developer laptops, CI cross
// builds). The agent only ships on Linux; this keeps the package compiling
// elsewhere. Reporting unavailability is honest: there is no /proc-based host to
// probe.
func realStatfs(string) (Statfs, error) {
	return Statfs{}, errors.New("disk: statfs is only implemented on linux")
}
