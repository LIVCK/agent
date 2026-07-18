package platform

import (
	"context"
	"os"
	"path/filepath"
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
