// Command livck-agent is the LIVCK server monitoring agent. This file is wiring
// only: it assembles the foundation (platform, identity, config, buffer, sender,
// runner) and runs the collect loop until a shutdown signal. It also wires the
// enroll verb (the client-initiated registration handshake) and the doctor verb
// (read-only self-diagnosis); reset and uninstall are not implemented yet.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/LIVCK/agent/internal/buffer"
	"github.com/LIVCK/agent/internal/collector/gpu"
	collectorregistry "github.com/LIVCK/agent/internal/collector/registry"
	"github.com/LIVCK/agent/internal/collector/smart"
	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/doctor"
	"github.com/LIVCK/agent/internal/enroll"
	"github.com/LIVCK/agent/internal/event"
	"github.com/LIVCK/agent/internal/hostinfo"
	"github.com/LIVCK/agent/internal/lifecycle"
	"github.com/LIVCK/agent/internal/live"
	"github.com/LIVCK/agent/internal/platform"
	"github.com/LIVCK/agent/internal/runner"
	"github.com/LIVCK/agent/internal/sender"
)

// version is set at build time via -ldflags "-X main.version=...".
var version = "0.0.0-dev"

// Exit codes the enroll verb returns so an installer script can branch: usage
// and identity/limit failures are FINAL, transport/5xx are retryable, and an
// already-enrolled host without --force is an idempotent no-op.
const (
	exitOK              = 0
	exitRetryable       = 1 // transport, 5xx/429, or an unparseable response
	exitUsage           = 2 // bad flags or a missing token
	exitFatal           = 3 // final 4xx, or a corrupt local identity (needs reset)
	exitAlreadyEnrolled = 4 // managed token already present; pass --force to redo
)

// maxProcs caps the Go runtime's parallelism. A metric sampler is I/O-bound and
// needs no host-scaled concurrency; without this the runtime sizes its scheduler
// threads, GC workers and per-P caches to the host core count, so the SAME binary
// would balloon on a 128-core box. 2 leaves headroom for the concurrent probe
// dials while keeping the footprint identical on a 2-core VM and a 128-core host.
const maxProcs = 2

// capRuntime bounds the runtime footprint independently of how the agent is
// launched (systemd, Docker, or by hand). The systemd unit also pins GOMAXPROCS/
// GOMEMLIMIT, but a container or a manual run would otherwise inherit the host's
// full core count and no heap ceiling. GOMAXPROCS is capped unconditionally (a
// hard ceiling; a lower operator setting via env still wins because Go applies it
// before main, and we only lower from there); the soft heap limit is a default we
// set only when the operator has not provided GOMEMLIMIT.
func capRuntime() {
	if n := runtime.NumCPU(); n < maxProcs {
		runtime.GOMAXPROCS(n)
	} else if runtime.GOMAXPROCS(0) > maxProcs {
		runtime.GOMAXPROCS(maxProcs)
	}
	if os.Getenv("GOMEMLIMIT") == "" {
		debug.SetMemoryLimit(48 << 20) // 48 MiB soft cap, matches the systemd unit
	}
}

func main() {
	capRuntime()

	cmd := "run"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}
	switch cmd {
	case "version":
		fmt.Println("livck-agent " + version)
		return
	case "run":
		if err := run(); err != nil {
			fmt.Fprintln(os.Stderr, "livck-agent: "+err.Error())
			os.Exit(1)
		}
	case "enroll":
		os.Exit(enrollCmd(os.Args[2:]))
	case "doctor":
		os.Exit(doctorCmd(os.Args[2:]))
	default:
		// reset/uninstall are not implemented yet.
		fmt.Fprintln(os.Stderr, "livck-agent: unknown command "+cmd+" (run, enroll, doctor, version)")
		os.Exit(2)
	}
}

// enrollCmd runs the enroll verb: it parses flags, resolves the bootstrap token
// (preferring the 0600 --token-file, which it unlinks after reading), performs
// the handshake, and maps the outcome to a process exit code. The heavy lifting
// lives in internal/enroll so other callers can reuse it.
func enrollCmd(args []string) int {
	fs := flag.NewFlagSet("enroll", flag.ContinueOnError)
	var (
		token     = fs.String("token", "", "enrollment token (lve_...); prefer --token-file")
		tokenFile = fs.String("token-file", "", "path to a 0600 file holding the enrollment token; read then unlinked")
		url       = fs.String("url", envOr("LIVCK_ENROLL_URL", "https://app.livck.cloud"), "enroll endpoint base URL")
		name      = fs.String("name", "", "optional service name (defaults to the hostname)")
		force     = fs.Bool("force", false, "re-enroll even if a managed token already exists")
		tags      = fs.String("tags", "", "comma-separated tags, e.g. env:prod,team:ops")
	)
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}

	plat := platform.Real()
	stateDir := resolveStateDir()
	store := enroll.NewStore(plat.FS, stateDir, uuid.NewString)

	secret, err := resolveToken(plat, *token, *tokenFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "livck-agent: "+err.Error())
		return exitUsage
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	res, err := enroll.Do(ctx, enroll.Options{
		Store:        store,
		HTTP:         &http.Client{Timeout: 30 * time.Second},
		BaseURL:      *url,
		Token:        secret,
		Force:        *force,
		Name:         *name,
		Tags:         splitTags(*tags),
		AgentVersion: version,
		Meta:         hostinfo.Meta(plat),
		Fingerprint:  hostinfo.Fingerprint(plat),
		PrivateIPs:   enroll.LocalPrivateIPs(),
	})
	if err != nil {
		return reportEnrollError(err)
	}

	label := res.ServiceName
	if label == "" {
		label = res.ServicePublicID
	}
	if res.AlreadyEnrolled {
		fmt.Printf("livck-agent: already enrolled as %q (service %s); config refreshed. Reporting to %s\n",
			label, res.ServicePublicID, res.IngestURL)
	} else {
		fmt.Printf("livck-agent: enrolled %q (service %s). Reporting to %s\n",
			label, res.ServicePublicID, res.IngestURL)
	}
	return exitOK
}

// resolveToken reads the bootstrap token from --token-file (preferred: it keeps
// the secret out of the process list) or --token. A token file is unlinked after
// reading, best-effort, so it does not linger on disk.
func resolveToken(plat platform.Platform, token, tokenFile string) (string, error) {
	if tokenFile != "" {
		raw, err := plat.FS.ReadFile(tokenFile)
		if err != nil {
			return "", fmt.Errorf("read token file: %w", err)
		}
		_ = plat.FS.Remove(tokenFile)
		if t := strings.TrimSpace(string(raw)); t != "" {
			return t, nil
		}
		return "", fmt.Errorf("token file %s is empty", tokenFile)
	}
	if t := strings.TrimSpace(token); t != "" {
		return t, nil
	}
	return "", fmt.Errorf("an enrollment token is required (--token or --token-file)")
}

// reportEnrollError prints a legible, secret-free message and returns the exit
// code that classifies the failure for the installer.
func reportEnrollError(err error) int {
	var apiErr *enroll.APIError
	switch {
	case errors.Is(err, enroll.ErrAlreadyEnrolled):
		fmt.Fprintln(os.Stderr, "livck-agent: "+err.Error())
		return exitAlreadyEnrolled
	case errors.Is(err, enroll.ErrCorruptIdentity):
		fmt.Fprintln(os.Stderr, "livck-agent: local identity is corrupt; run 'livck-agent reset' before re-enrolling")
		return exitFatal
	case errors.As(err, &apiErr):
		fmt.Fprintln(os.Stderr, "livck-agent: "+apiErr.Error())
		if apiErr.Limit != nil && apiErr.Used != nil {
			fmt.Fprintf(os.Stderr, "  agents: %d/%d used", *apiErr.Used, *apiErr.Limit)
			if apiErr.UpgradeURL != "" {
				fmt.Fprintf(os.Stderr, " - upgrade: %s", apiErr.UpgradeURL)
			}
			fmt.Fprintln(os.Stderr)
		}
		if apiErr.Retryable() {
			return exitRetryable
		}
		return exitFatal
	default:
		fmt.Fprintln(os.Stderr, "livck-agent: "+err.Error())
		return exitRetryable
	}
}

// doctorCmd runs the read-only self-diagnosis: it resolves the same control and
// ingest endpoints the run loop uses, executes the checks, renders them, and
// exits with the report's code (0 ok, 1 warnings, 2 hard errors, 3 unsupported
// platform) so an install script can branch on the result.
func doctorCmd(args []string) int {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	var (
		controlURL = fs.String("url", resolveControlBase(), "control-plane base URL (/v1/me, agent config)")
		ingestURL  = fs.String("ingest-url", "", "ingest base URL (defaults to the enrolled/packaged value)")
	)
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}

	plat := platform.Real()
	stateDir := resolveStateDir()
	store := enroll.NewStore(plat.FS, stateDir, uuid.NewString)

	ingestBase := strings.TrimSpace(*ingestURL)
	if ingestBase == "" {
		ingestBase = resolveIngestBase(store)
	}

	report := doctor.Run(context.Background(), doctor.Options{
		Platform:     plat,
		Store:        store,
		HTTP:         &http.Client{Timeout: 30 * time.Second},
		ControlBase:  strings.TrimSpace(*controlURL),
		IngestBase:   ingestBase,
		StateDir:     stateDir,
		AgentVersion: version,
	})

	doctor.Render(os.Stdout, report, useColor())
	return report.ExitCode()
}

// resolveControlBase picks the control-plane base URL doctor probes: the config
// URL override wins (it is what the run loop uses), then the enroll URL, then the
// packaged production default.
func resolveControlBase() string {
	return envOr("LIVCK_CONFIG_URL", envOr("LIVCK_ENROLL_URL", "https://app.livck.cloud"))
}

// useColor reports whether to color the doctor output: only when stdout is a
// terminal and NO_COLOR is unset.
func useColor() bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	fi, err := os.Stdout.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

func run() error {
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	plat := platform.Real()
	stateDir := resolveStateDir()
	configURL := envOr("LIVCK_CONFIG_URL", "https://app.livck.cloud") + "/api/v1/agents/config"

	store := enroll.NewStore(plat.FS, stateDir, uuid.NewString)
	instanceID, err := store.LoadOrCreateInstanceID()
	if err != nil {
		return fmt.Errorf("identity: %w", err)
	}

	// The ingest URL is learned at enroll and persisted; an explicit env var still
	// wins for dev. The base gets the /v1/ingest path appended.
	ingestURL := resolveIngestBase(store) + "/v1/ingest"

	// The token is read fresh on every use so a rotation is picked up without a
	// restart. A dev fallback env var keeps the loop runnable before enroll runs.
	tokenFn := func() string {
		if tok, err := store.Token(); err == nil && tok != "" {
			return tok
		}
		return os.Getenv("LIVCK_AGENT_TOKEN")
	}

	client := &http.Client{Timeout: 30 * time.Second}

	queue := event.NewQueue(plat.Clock, uuid.NewString, event.DefaultCapacity)
	ring := buffer.New(plat.Clock, queue, buffer.DefaultMaxBytes, buffer.DefaultHorizon)
	spool := buffer.NewSpool(plat.FS, filepath.Join(stateDir, "spool.pb"), buffer.DefaultMaxBytes)

	fetcher := config.NewHTTPFetcher(client, configURL, tokenFn)
	cfg := config.NewManager(fetcher, queue, plat.FS, filepath.Join(stateDir, enroll.ConfigFile), log)

	registry := collectorregistry.Build(plat, cfg.Current, queue)
	lc := lifecycle.New(plat.FS, plat.Clock, queue, stateDir)

	trigger := make(chan struct{}, 1)

	sndr, err := sender.New(sender.Options{
		Client:       client,
		URL:          ingestURL,
		TokenFn:      tokenFn,
		InstanceID:   instanceID,
		AgentVersion: version,
		Clock:        plat.Clock,
		Buffer:       ring,
		Events:       queue,
		Config:       cfg.Current,
		Emitter:      queue,
		NewID:        uuid.NewString,
		Log:          log,
		Fingerprint:  hostinfo.Fingerprint(plat),
		OnConfigVersion: func(uint32) {
			select {
			case trigger <- struct{}{}:
			default:
			}
		},
	})
	if err != nil {
		return err
	}
	defer sndr.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Live-Watch signal channel: a SECOND registry so its rate collectors
	// keep delta state independent of the report loop. The streamer arms
	// itself on features.live and idles otherwise, so it costs nothing when nobody
	// is watching. The ingest base doubles as the WSS host (/v1/live/agent).
	liveRegistry := collectorregistry.Build(plat, cfg.Current, nil)
	streamer := live.New(live.Options{
		Config:  cfg.Current,
		Sampler: live.NewRegistrySampler(liveRegistry, log),
		Dialer:  live.NewDialer(),
		WSURL:   live.WSURL(resolveIngestBase(store)),
		TokenFn: tokenFn,
		Clock:   plat.Clock,
		Log:     log,
	})
	go streamer.Run(ctx)

	// Seed the report meta with human-readable GPU model names (nvidia-smi, forked ONCE). They ride the
	// bootstrap meta → service_agents.meta → the detail "Devices" panel, so a card reads
	// "NVIDIA GeForce RTX 4080" instead of a bare PCI address. Best-effort: absent on non-NVIDIA hosts.
	hostMeta := hostinfo.Meta(plat)
	for pci, name := range gpu.QueryNames(ctx, plat.Exec) {
		hostMeta["gpu."+pci+".name"] = name
	}
	// Same idea for SMART drives: the model name (nvme0 → "Samsung SSD 9100 PRO 2TB") rides the meta
	// (needs root, like the SMART metrics themselves). Only runs when features.smart is on.
	if cfg.Current().Features.Smart {
		for dev, name := range smart.QueryNames(ctx, plat.Exec) {
			hostMeta["smart."+dev+".name"] = name
		}
	}

	r := runner.New(runner.Options{
		Platform:      plat,
		Config:        cfg,
		Registry:      registry,
		Ring:          ring,
		Events:        queue,
		Sender:        sndr,
		Spool:         spool,
		Lifecycle:     lc,
		ConfigTrigger: trigger,
		Meta:          hostMeta,
		Log:           log,
	})

	log.Info("livck-agent starting", "version", version, "instance_id", instanceID, "state_dir", stateDir,
		"gomaxprocs", runtime.GOMAXPROCS(0), "numcpu", runtime.NumCPU())
	return r.Run(ctx)
}

// resolveStateDir prefers the systemd StateDirectory, then an override, then the
// packaged default.
func resolveStateDir() string {
	if d := os.Getenv("STATE_DIRECTORY"); d != "" {
		// systemd may pass a colon-separated list; take the first.
		if i := strings.IndexByte(d, ':'); i >= 0 {
			d = d[:i]
		}
		return d
	}
	return envOr("LIVCK_STATE_DIR", "/var/lib/livck-agent")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// resolveIngestBase picks the ingest base URL: an explicit LIVCK_INGEST_URL env
// override wins (dev), then the URL learned and persisted at enroll, then the
// packaged production default.
func resolveIngestBase(store *enroll.Store) string {
	if v := os.Getenv("LIVCK_INGEST_URL"); v != "" {
		return v
	}
	if u, err := store.IngestURL(); err == nil && u != "" {
		return u
	}
	return "https://ingest.livck.cloud"
}

// splitTags parses the comma-separated --tags flag into a trimmed, non-empty
// list, or nil when empty.
func splitTags(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var out []string
	for _, part := range strings.Split(raw, ",") {
		if t := strings.TrimSpace(part); t != "" {
			out = append(out, t)
		}
	}
	return out
}
