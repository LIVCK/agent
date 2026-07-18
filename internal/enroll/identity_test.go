package enroll

import (
	"testing"

	"github.com/LIVCK/agent/internal/platform/platformtest"
)

const uuidA = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

func newStore() (*Store, *platformtest.MemFS) {
	fs := platformtest.NewMemFS()
	ids := []string{uuidA, "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"}
	i := 0
	gen := func() string {
		id := ids[i%len(ids)]
		i++
		return id
	}
	return NewStore(fs, "/state", gen), fs
}

func TestInstanceIDCreateThenStable(t *testing.T) {
	s, _ := newStore()
	id, err := s.LoadOrCreateInstanceID()
	if err != nil {
		t.Fatal(err)
	}
	if id != uuidA {
		t.Fatalf("first id = %q", id)
	}
	// Second call returns the persisted id, not a fresh one.
	again, err := s.LoadOrCreateInstanceID()
	if err != nil {
		t.Fatal(err)
	}
	if again != uuidA {
		t.Fatalf("instance id changed across calls: %q", again)
	}
}

func TestInstanceIDPersisted0600(t *testing.T) {
	s, fs := newStore()
	if _, err := s.LoadOrCreateInstanceID(); err != nil {
		t.Fatal(err)
	}
	perm, ok := fs.Perm("/state/instance_id")
	if !ok {
		t.Fatal("instance_id not persisted")
	}
	if perm != secretPerm {
		t.Fatalf("instance_id perm = %v, want 0600", perm)
	}
}

func TestInstanceIDCorruptIsNotOverwritten(t *testing.T) {
	fs := platformtest.NewMemFS()
	_ = fs.WriteFileAtomic("/state/instance_id", []byte("not-a-uuid"), secretPerm)
	s := NewStore(fs, "/state", func() string { return uuidA })

	if _, err := s.LoadOrCreateInstanceID(); err != ErrCorruptIdentity {
		t.Fatalf("want ErrCorruptIdentity, got %v", err)
	}
	// The corrupt file must remain untouched (the agent never wipes identity).
	raw, _ := fs.ReadFile("/state/instance_id")
	if string(raw) != "not-a-uuid" {
		t.Fatalf("corrupt identity was overwritten: %q", raw)
	}
}

func TestEnrollmentIDStable(t *testing.T) {
	s, _ := newStore()
	first, err := s.LoadOrCreateEnrollmentID()
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.LoadOrCreateEnrollmentID()
	if err != nil {
		t.Fatal(err)
	}
	if first != again {
		t.Fatalf("enrollment id not stable: %q vs %q", first, again)
	}
}

func TestInstanceIDReadOnlyMissing(t *testing.T) {
	s, _ := newStore()
	if _, err := s.InstanceID(); err == nil {
		t.Fatal("InstanceID should error when no identity exists")
	}
}

func TestEnrollmentIDCorruptRejected(t *testing.T) {
	fs := platformtest.NewMemFS()
	_ = fs.WriteFileAtomic("/state/enrollment_id", []byte("bad"), secretPerm)
	s := NewStore(fs, "/state", func() string { return uuidA })
	if _, err := s.LoadOrCreateEnrollmentID(); err == nil {
		t.Fatal("corrupt enrollment_id should be rejected")
	}
}

func TestTokenMissingErrors(t *testing.T) {
	s, _ := newStore()
	if _, err := s.Token(); err == nil {
		t.Fatal("Token should error when absent")
	}
}

func TestTokenStorage(t *testing.T) {
	s, fs := newStore()
	if s.HasToken() {
		t.Fatal("fresh store should have no token")
	}
	if err := s.SetToken("lvk_secret"); err != nil {
		t.Fatal(err)
	}
	if !s.HasToken() {
		t.Fatal("token should exist after SetToken")
	}
	tok, err := s.Token()
	if err != nil {
		t.Fatal(err)
	}
	if tok != "lvk_secret" {
		t.Fatalf("token = %q", tok)
	}
	if perm, _ := fs.Perm("/state/token"); perm != secretPerm {
		t.Fatalf("token perm = %v, want 0600", perm)
	}
}
