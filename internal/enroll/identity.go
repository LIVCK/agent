// Package enroll owns the agent's persistent identity: the instance_id, the
// enrollment_id, and the managed token, each stored 0600 in the state directory.
// It holds the identity primitives only; the enroll CLI verbs (enroll, reset,
// uninstall) and the enroll HTTP call live elsewhere.
//
// Identity rules enforced here:
//   - instance_id is a fresh UUIDv4 created on first run and never derived from
//     hostname, machine-id or DMI. Clones inherit machine-id; reinstalls change
//     it; DMI is root-only. A UUID in a 0600 file is the only stable identity.
//   - The agent never wipes its own identity. A corrupt instance_id file is an
//     error, not a trigger to regenerate: regenerating would silently mint a new
//     identity (a phantom clone). Only the explicit reset verb wipes it.
package enroll

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/LIVCK/agent/internal/platform"
)

const (
	instanceIDFile   = "instance_id"
	enrollmentIDFile = "enrollment_id"
	tokenFile        = "token"
	ingestURLFile    = "ingest_url"

	// ConfigFile is the last-good config cache filename. The enroll verb writes
	// the server's initial config here and the run loop's config manager seeds
	// from the same file, so both agree on one on-disk config.
	ConfigFile = "config.json"

	secretPerm os.FileMode = 0o600
	dirPerm    os.FileMode = 0o700
)

// uuidRe matches the 8-4-4-4-12 hex UUID shape (any version). The generator
// produces v4; validation stays version-agnostic so it does not reject a valid
// id from a future generator.
var uuidRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// ErrCorruptIdentity is returned when the instance_id file exists but does not
// hold a valid UUID. The caller must not overwrite it.
var ErrCorruptIdentity = errors.New("enroll: instance_id file is corrupt")

// IDGen returns a fresh UUIDv4. Injected for deterministic tests.
type IDGen func() string

// Store reads and writes the identity files under a state directory.
type Store struct {
	fs    platform.FS
	dir   string
	newID IDGen
}

// NewStore builds a Store rooted at dir using newID to mint fresh identifiers.
func NewStore(fs platform.FS, dir string, newID IDGen) *Store {
	return &Store{fs: fs, dir: dir, newID: newID}
}

func (s *Store) path(name string) string { return filepath.Join(s.dir, name) }

func (s *Store) ensureDir() error {
	return s.fs.MkdirAll(s.dir, dirPerm)
}

// LoadOrCreateInstanceID returns the persisted instance_id, generating and
// persisting a fresh UUIDv4 on first run. A present-but-corrupt file yields
// ErrCorruptIdentity and is never overwritten.
func (s *Store) LoadOrCreateInstanceID() (string, error) {
	raw, err := s.fs.ReadFile(s.path(instanceIDFile))
	if err == nil {
		id := strings.TrimSpace(string(raw))
		if !uuidRe.MatchString(id) {
			return "", ErrCorruptIdentity
		}
		return id, nil
	}
	if !os.IsNotExist(err) {
		return "", fmt.Errorf("read instance_id: %w", err)
	}
	return s.create(instanceIDFile)
}

// InstanceID returns the persisted instance_id without creating one. It errors
// if the identity is missing or corrupt.
func (s *Store) InstanceID() (string, error) {
	raw, err := s.fs.ReadFile(s.path(instanceIDFile))
	if err != nil {
		return "", fmt.Errorf("read instance_id: %w", err)
	}
	id := strings.TrimSpace(string(raw))
	if !uuidRe.MatchString(id) {
		return "", ErrCorruptIdentity
	}
	return id, nil
}

// LoadOrCreateEnrollmentID returns the persisted enrollment_id, generating one
// on first call. The enrollment_id is minted before the first enroll attempt so
// that a retried enroll reuses it and the server dedupes to one service instead
// of creating a duplicate.
func (s *Store) LoadOrCreateEnrollmentID() (string, error) {
	raw, err := s.fs.ReadFile(s.path(enrollmentIDFile))
	if err == nil {
		id := strings.TrimSpace(string(raw))
		if !uuidRe.MatchString(id) {
			return "", fmt.Errorf("enroll: enrollment_id file is corrupt")
		}
		return id, nil
	}
	if !os.IsNotExist(err) {
		return "", fmt.Errorf("read enrollment_id: %w", err)
	}
	return s.create(enrollmentIDFile)
}

func (s *Store) create(name string) (string, error) {
	if err := s.ensureDir(); err != nil {
		return "", fmt.Errorf("ensure state dir: %w", err)
	}
	id := s.newID()
	if err := s.fs.WriteFileAtomic(s.path(name), []byte(id+"\n"), secretPerm); err != nil {
		return "", fmt.Errorf("persist %s: %w", name, err)
	}
	return id, nil
}

// Token returns the persisted managed token (lvk_...). A missing token is an
// error: the caller decides whether that means "not enrolled yet".
func (s *Store) Token() (string, error) {
	raw, err := s.fs.ReadFile(s.path(tokenFile))
	if err != nil {
		return "", fmt.Errorf("read token: %w", err)
	}
	return strings.TrimSpace(string(raw)), nil
}

// HasToken reports whether a managed token is stored.
func (s *Store) HasToken() bool {
	_, err := s.fs.Stat(s.path(tokenFile))
	return err == nil
}

// SetToken persists the managed token 0600, replacing any previous one (used on
// enroll and on token rotation).
func (s *Store) SetToken(token string) error {
	if err := s.ensureDir(); err != nil {
		return fmt.Errorf("ensure state dir: %w", err)
	}
	return s.fs.WriteFileAtomic(s.path(tokenFile), []byte(strings.TrimSpace(token)+"\n"), secretPerm)
}

// IngestURL returns the persisted ingest base URL learned at enroll, or an error
// if none is stored. Bootstrap carries only the enroll URL; the ingest URL is
// server-supplied in the enroll response.
func (s *Store) IngestURL() (string, error) {
	raw, err := s.fs.ReadFile(s.path(ingestURLFile))
	if err != nil {
		return "", fmt.Errorf("read ingest_url: %w", err)
	}
	return strings.TrimSpace(string(raw)), nil
}

// SetIngestURL persists the ingest base URL 0600, replacing any previous one.
func (s *Store) SetIngestURL(url string) error {
	if err := s.ensureDir(); err != nil {
		return fmt.Errorf("ensure state dir: %w", err)
	}
	return s.fs.WriteFileAtomic(s.path(ingestURLFile), []byte(strings.TrimSpace(url)+"\n"), secretPerm)
}

// SetConfig persists the server's config document as the last-good cache 0600,
// so the next run seeds from it. The bytes are written verbatim; the run loop's
// config manager re-validates on read.
func (s *Store) SetConfig(raw []byte) error {
	if err := s.ensureDir(); err != nil {
		return fmt.Errorf("ensure state dir: %w", err)
	}
	return s.fs.WriteFileAtomic(s.path(ConfigFile), raw, secretPerm)
}

// Reset removes the persisted identity so the next enroll starts fresh: the
// instance_id, enrollment_id, managed token, learned ingest URL and the last-good
// config cache. It leaves the state directory itself in place and is idempotent —
// a missing file is not an error. This is the local half of a clean re-enroll and
// the only sanctioned way to drop the instance_id (which is otherwise never wiped).
func (s *Store) Reset() error {
	for _, name := range []string{instanceIDFile, enrollmentIDFile, tokenFile, ingestURLFile, ConfigFile} {
		if err := s.fs.Remove(s.path(name)); err != nil {
			return fmt.Errorf("remove %s: %w", name, err)
		}
	}
	return nil
}
