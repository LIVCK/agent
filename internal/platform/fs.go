package platform

import (
	"fmt"
	"os"
	"path/filepath"
)

// FS abstracts the pieces of the filesystem the agent needs: reading and
// writing the identity files, the managed token, the config last-good cache and
// the exit spool. It is deliberately small. All agent-owned files live under the
// state directory and are written 0600.
type FS interface {
	ReadFile(name string) ([]byte, error)
	// WriteFileAtomic writes data to a sibling temp file and renames it over
	// name, so a crash mid-write never leaves a torn file. perm is applied to
	// the final file.
	WriteFileAtomic(name string, data []byte, perm os.FileMode) error
	Remove(name string) error
	Stat(name string) (os.FileInfo, error)
	MkdirAll(path string, perm os.FileMode) error
	// ReadDir lists the immediate entries of a directory. The system
	// collector uses it to enumerate /proc entries; it stays on FS so the scan
	// runs against a fake in tests instead of the real /proc.
	ReadDir(name string) ([]os.DirEntry, error)
}

// OSFS is the production FS backed by the os package.
type OSFS struct{}

// ReadFile reads the named file.
func (OSFS) ReadFile(name string) ([]byte, error) { return os.ReadFile(name) }

// WriteFileAtomic writes to a temp file in the same directory and renames it
// over name. The temp file is created with perm; the rename is atomic on the
// same filesystem.
func (OSFS) WriteFileAtomic(name string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(name)
	tmp, err := os.CreateTemp(dir, ".tmp-"+filepath.Base(name)+"-*")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	tmpName := tmp.Name()
	// Best-effort cleanup if we bail before the rename.
	defer func() { _ = os.Remove(tmpName) }()

	if err := tmp.Chmod(perm); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod temp: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp: %w", err)
	}
	if err := os.Rename(tmpName, name); err != nil {
		return fmt.Errorf("rename temp: %w", err)
	}
	return nil
}

// Remove removes the named file. A missing file is not an error.
func (OSFS) Remove(name string) error {
	err := os.Remove(name)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// Stat returns file info for name.
func (OSFS) Stat(name string) (os.FileInfo, error) { return os.Stat(name) }

// MkdirAll creates path and any missing parents.
func (OSFS) MkdirAll(path string, perm os.FileMode) error { return os.MkdirAll(path, perm) }

// ReadDir lists the immediate directory entries of name.
func (OSFS) ReadDir(name string) ([]os.DirEntry, error) { return os.ReadDir(name) }
