package platform

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestOSFSWriteAtomicAndPerm(t *testing.T) {
	dir := t.TempDir()
	fs := OSFS{}
	path := filepath.Join(dir, "secret")
	if err := fs.WriteFileAtomic(path, []byte("token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := fs.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("perm = %v, want 0600", info.Mode().Perm())
	}
	got, err := fs.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "token\n" {
		t.Fatalf("content = %q", got)
	}
	// No temp files linger next to the target.
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatalf("expected exactly the target file, found %d entries", len(entries))
	}
}

func fileOwner(t *testing.T, path string) (uint32, uint32) {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Skip("no syscall.Stat_t on this platform")
	}
	return st.Uid, st.Gid
}

// A written state file must always end up owned by the same uid/gid as its
// directory — otherwise a root-run enroll leaves root-owned files the non-root
// service user cannot read.
func TestOSFSWriteInheritsStateDirOwner(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	if err := (OSFS{}).WriteFileAtomic(path, []byte("t\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	fu, fg := fileOwner(t, path)
	du, dg := fileOwner(t, dir)
	if fu != du || fg != dg {
		t.Fatalf("file owner %d:%d != state dir owner %d:%d", fu, fg, du, dg)
	}
}

// Under root, a file written into a state dir owned by a non-root user must
// inherit that user (the exact fix for the systemd 401-after-manual-enroll bug).
func TestOSFSWriteChownsToNonRootDirOwnerAsRoot(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root to chown a file to another uid")
	}
	const uid, gid = 65534, 65534 // nobody/nogroup on most distros
	dir := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(dir, uid, gid); err != nil {
		t.Fatalf("chown dir: %v", err)
	}
	path := filepath.Join(dir, "token")
	if err := (OSFS{}).WriteFileAtomic(path, []byte("t\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if fu, fg := fileOwner(t, path); fu != uid || fg != gid {
		t.Fatalf("root-written file owner = %d:%d, want %d:%d (should inherit the state dir owner)", fu, fg, uid, gid)
	}
}

func TestOSFSRemoveMissingIsNoError(t *testing.T) {
	if err := (OSFS{}).Remove(filepath.Join(t.TempDir(), "absent")); err != nil {
		t.Fatalf("removing a missing file should be a no-op, got %v", err)
	}
}

func TestSystemClockSleepCancels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := (SystemClock{}).Sleep(ctx, time.Hour); err == nil {
		t.Fatal("Sleep should return the context error when already cancelled")
	}
}
