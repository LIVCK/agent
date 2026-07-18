//go:build linux

package disk

import "syscall"

// realStatfs is the production StatfsFunc: a direct statfs(2) syscall. It is a
// blocking call with no context; the collector guards it with a deadline and a
// one-probe-per-mount rule (see disk.go) so a dead mount cannot wedge the loop.
func realStatfs(path string) (Statfs, error) {
	var s syscall.Statfs_t
	if err := syscall.Statfs(path, &s); err != nil {
		return Statfs{}, err
	}
	return Statfs{
		Blocks: s.Blocks,
		Bfree:  s.Bfree,
		Bavail: s.Bavail,
		Bsize:  uint64(s.Bsize),
		Files:  s.Files,
		Ffree:  s.Ffree,
	}, nil
}
