package disk

// Statfs is the subset of a statfs(2) result the disk collector needs. Block
// counts are in units of Bsize bytes; inode counts are absolute. It is defined
// here (not in platform) because statfs is the one blocking, non-cancellable
// syscall in the agent: keeping it behind an injectable StatfsFunc lets tests
// simulate a hung mount deterministically.
type Statfs struct {
	Blocks uint64 // total data blocks
	Bfree  uint64 // free blocks
	Bavail uint64 // free blocks available to an unprivileged user
	Bsize  uint64 // block size in bytes
	Files  uint64 // total inodes
	Ffree  uint64 // free inodes
}

// StatfsFunc reads filesystem statistics for a mount point. The real
// implementation is a blocking syscall; on a dead network mount it hangs
// forever, which is exactly why the collector runs it off the sample path.
type StatfsFunc func(path string) (Statfs, error)
