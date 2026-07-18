package config

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LIVCK/agent/internal/platform"
	"github.com/LIVCK/agent/pkg/wire"
)

// DefaultPollInterval is the backstop config-pull cadence. The primary trigger
// is the config_version piggyback in every 202 ingest response; this periodic
// pull only covers a missed piggyback.
const DefaultPollInterval = 5 * time.Minute

// Emitter raises lifecycle events (config_applied, config_error). It matches
// internal/event.Emitter without importing it, keeping config leaf-level.
type Emitter interface {
	Emit(t wire.EventType, meta map[string]string)
}

// Fetcher pulls the raw config document. Fetch returns notModified true on a 304
// (ETag match) and leaves raw/newETag zero. The HTTP implementation is below.
type Fetcher interface {
	Fetch(ctx context.Context, etag string) (raw []byte, newETag string, notModified bool, err error)
}

// Manager holds the current config behind an atomic pointer, applies pulled
// documents with validate-before-swap, keeps a last-good cache on disk, and
// never leaves the agent without a usable config.
type Manager struct {
	current      atomic.Pointer[Config]
	fetcher      Fetcher
	emitter      Emitter
	fs           platform.FS
	lastGoodPath string
	log          *slog.Logger

	mu   sync.Mutex // serializes Apply and guards etag
	etag string
}

// NewManager builds a Manager. It seeds the current config from the last-good
// cache at lastGoodPath, falling back to Defaults() when the cache is missing or
// unusable. fetcher and emitter may be nil in tests that only exercise Apply.
func NewManager(fetcher Fetcher, emitter Emitter, fs platform.FS, lastGoodPath string, log *slog.Logger) *Manager {
	if log == nil {
		log = slog.Default()
	}
	m := &Manager{
		fetcher:      fetcher,
		emitter:      emitter,
		fs:           fs,
		lastGoodPath: lastGoodPath,
		log:          log,
	}
	m.current.Store(m.seed())
	return m
}

func (m *Manager) seed() *Config {
	if m.fs == nil || m.lastGoodPath == "" {
		return Defaults()
	}
	raw, err := m.fs.ReadFile(m.lastGoodPath)
	if err != nil {
		return Defaults()
	}
	cfg, _, err := Validate(raw)
	if err != nil {
		m.log.Warn("last-good config cache is unusable, using defaults", "err", err.Error())
		return Defaults()
	}
	return cfg
}

// Current returns the applied config snapshot. It is never nil.
func (m *Manager) Current() *Config { return m.current.Load() }

// Apply validates raw and, if usable, swaps it in and persists it as last-good.
// A fatal validation error keeps the last-good config and raises exactly one
// config_error. Field-level errors still apply the clamped config but also raise
// a config_error; a clean apply raises config_applied only when the version
// changed.
func (m *Manager) Apply(raw []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cfg, issues, err := Validate(raw)
	if err != nil {
		m.log.Warn("config rejected, keeping last-good", "err", err.Error())
		m.emit(wire.EventType_EVENT_TYPE_CONFIG_ERROR, map[string]string{
			"error": clip(err.Error(), 200),
		})
		return
	}

	prev := m.current.Load()
	m.current.Store(cfg)
	m.persist(raw)

	if HasErrors(issues) {
		m.log.Warn("config applied with corrections", "version", cfg.ConfigVersion, "issues", Summary(issues))
		m.emit(wire.EventType_EVENT_TYPE_CONFIG_ERROR, map[string]string{
			"version": strconv.Itoa(cfg.ConfigVersion),
			"error":   Summary(issues),
		})
	}
	for _, i := range issues {
		if i.Severity == SeverityWarn {
			m.log.Warn("config knob clamped", "field", i.Field, "detail", i.Message)
		}
	}
	if prev == nil || prev.ConfigVersion != cfg.ConfigVersion {
		m.log.Info("config applied", "version", cfg.ConfigVersion, "interval_seconds", cfg.IntervalSeconds)
		m.emit(wire.EventType_EVENT_TYPE_CONFIG_APPLIED, map[string]string{
			"version": strconv.Itoa(cfg.ConfigVersion),
		})
	}
}

func (m *Manager) persist(raw []byte) {
	if m.fs == nil || m.lastGoodPath == "" {
		return
	}
	if err := m.fs.WriteFileAtomic(m.lastGoodPath, raw, 0o600); err != nil {
		m.log.Warn("could not persist last-good config", "err", err.Error())
	}
}

func (m *Manager) emit(t wire.EventType, meta map[string]string) {
	if m.emitter != nil {
		m.emitter.Emit(t, meta)
	}
}

// Pull fetches the config once (sending the current ETag) and applies it unless
// the server answered 304. A fetch error is logged and leaves the current config
// untouched.
func (m *Manager) Pull(ctx context.Context) {
	if m.fetcher == nil {
		return
	}
	m.mu.Lock()
	etag := m.etag
	m.mu.Unlock()

	raw, newETag, notModified, err := m.fetcher.Fetch(ctx, etag)
	if err != nil {
		m.log.Warn("config pull failed", "err", err.Error())
		return
	}
	if notModified {
		return
	}
	m.Apply(raw)
	m.mu.Lock()
	m.etag = newETag
	m.mu.Unlock()
}

// Run drives config pulls until ctx is done: on every trigger signal (the 202
// config_version piggyback) and on a periodic backstop interval. interval <= 0
// uses DefaultPollInterval.
func (m *Manager) Run(ctx context.Context, trigger <-chan struct{}, interval time.Duration) {
	if interval <= 0 {
		interval = DefaultPollInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-trigger:
			m.Pull(ctx)
		case <-t.C:
			m.Pull(ctx)
		}
	}
}
