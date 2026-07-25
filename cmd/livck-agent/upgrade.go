package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/enroll"
	"github.com/LIVCK/agent/internal/platform"
	"github.com/LIVCK/agent/internal/update"
)

// upgradeCmd implements `livck-agent upgrade` (alias: update) — the ROOT-side
// upgrade path (SERVER-AGENT.md §10.8). The hardened non-root daemon never
// replaces its own binary; this verb (run manually or by livck-agent-update.timer
// with --auto) is the only mechanism.
//
// Target resolution: --version X > the server-advertised update.target_version
// (staged rollout) > the distribution host's latest GA tag (manual runs only).
// --auto strictly follows the advertised target: it no-ops when autoupdate is
// off, no target is released for this host's rollout group, the host-local
// update window is closed, or the target is already installed.
func upgradeCmd(args []string) int {
	fs := flag.NewFlagSet("upgrade", flag.ContinueOnError)
	var (
		check       = fs.Bool("check", false, "read-only: show current/target and whether an upgrade is available (exit 5 when one is)")
		auto        = fs.Bool("auto", false, "timer mode: honour autoupdate flag + update window, only ever the advertised target")
		pinVersion  = fs.String("version", "", "install this exact version (vX.Y.Z) instead of the advertised target")
		force       = fs.Bool("force", false, "allow a downgrade or same-version reinstall")
		downloadURL = fs.String("download-url", envOr("LIVCK_DOWNLOAD_URL", "https://get.livck.cloud"), "distribution host base URL")
	)
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if os.Geteuid() != 0 {
		fmt.Fprintln(os.Stderr, "livck-agent: upgrade must run as root (use sudo).")
		return exitUsage
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	plat := platform.Real()
	stateDir := resolveStateDir()
	store := enroll.NewStore(plat.FS, stateDir, uuid.NewString)

	engine := update.New(update.Options{
		BinaryPath:     binaryPath,
		ServiceName:    serviceUnit,
		StateDir:       stateDir,
		DownloadBase:   strings.TrimRight(*downloadURL, "/"),
		Arch:           runtime.GOARCH,
		CurrentVersion: version,
		HTTP:           &http.Client{Timeout: 5 * time.Minute},
	})

	// Fresh config: carries autoupdate, the window, the advertised target and the
	// server-pinned artifact digest. Absent config (not enrolled / control plane
	// down) is fatal for --auto and advertised-target runs, but an explicitly
	// pinned --version may proceed on the same-origin checksum.
	cfg, cfgErr := fetchUpgradeConfig(ctx, store)

	if *auto {
		if cfgErr != nil {
			fmt.Fprintf(os.Stderr, "livck-agent: config unavailable (%v) — retrying on the next timer run.\n", cfgErr)
			return exitOK
		}
		if !cfg.Update.Autoupdate {
			return exitOK // panel opt-out — timer stays a silent no-op
		}
		if cfg.Update.TargetVersion == "" {
			return exitOK // nothing released for this host's rollout group
		}
		if !update.InWindow(time.Now(), cfg.Update.WindowStart, cfg.Update.WindowEnd) {
			return exitOK
		}
	}

	// Resolve the target. Integrity is enforced by the cosign SIGNATURE the engine
	// verifies against the compiled-in release key — not by a config-supplied digest.
	target := strings.TrimSpace(*pinVersion)
	source := "pinned"
	if target == "" && cfg != nil && cfg.Update.TargetVersion != "" {
		target = cfg.Update.TargetVersion
		source = "advertised"
	}
	if target == "" && !*auto {
		if cfgErr != nil && *pinVersion == "" && !*check {
			fmt.Fprintf(os.Stderr, "livck-agent: cannot read the agent config (%v); pass --version X to upgrade anyway.\n", cfgErr)
			return exitRetryable
		}
		latest, err := engine.ResolveLatest(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "livck-agent: cannot resolve the latest release: %v\n", err)
			return exitRetryable
		}
		target = latest
		source = "latest"
	}

	if *check {
		return renderUpgradeCheck(ctx, engine, cfg, cfgErr, target, source)
	}

	if target == "" {
		return exitOK
	}

	release, err := update.Lock(stateDir)
	if err != nil {
		if errors.Is(err, update.ErrLocked) {
			if *auto {
				return exitOK // another run is applying — clean timer no-op
			}
			fmt.Fprintln(os.Stderr, "livck-agent: another upgrade is already running.")
			return exitRetryable
		}
		fmt.Fprintf(os.Stderr, "livck-agent: %v\n", err)
		return exitRetryable
	}
	defer release()

	res, err := engine.Run(ctx, update.Request{
		Target: target,
		Force:  *force,
		Auto:   *auto,
	})
	switch {
	case err != nil:
		fmt.Fprintf(os.Stderr, "livck-agent: upgrade failed: %v\n", err)
		return exitRetryable
	case res.Outcome == "rolled_back":
		fmt.Fprintf(os.Stderr, "livck-agent: %s was unhealthy — rolled back to %s.\n", res.To, res.From)
		if *auto {
			return exitOK // recorded in the result heartbeat; the rollout halt reads it
		}
		return exitRetryable
	case res.Outcome == "skipped":
		fmt.Printf("livck-agent: already on %s.\n", version)
		return exitOK
	default:
		fmt.Printf("livck-agent: upgraded %s -> %s.\n", res.From, res.To)
		return exitOK
	}
}

// fetchUpgradeConfig pulls a FRESH config document with the managed token. The
// token never appears in argv/env/output.
func fetchUpgradeConfig(ctx context.Context, store *enroll.Store) (*config.Config, error) {
	token, err := store.Token()
	if err != nil || token == "" {
		return nil, errors.New("not enrolled (no managed token)")
	}
	url := envOr("LIVCK_CONFIG_URL", "https://app.livck.cloud") + "/api/v1/agents/config"
	fetcher := config.NewHTTPFetcher(&http.Client{Timeout: 30 * time.Second}, url, func() string { return token })
	raw, _, _, err := fetcher.Fetch(ctx, "")
	if err != nil {
		return nil, err
	}
	cfg, _, err := config.Validate(raw)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

// renderUpgradeCheck prints the read-only status. Exit contract: 0 = up to
// date / nothing released, 5 = an upgrade is available (monitoring-friendly).
func renderUpgradeCheck(ctx context.Context, engine *update.Engine, cfg *config.Config, cfgErr error, target, source string) int {
	fmt.Printf("installed:   %s\n", version)
	fmt.Printf("managed by:  %s\n", engine.ManagedBy(ctx))
	if cfgErr != nil {
		fmt.Printf("config:      unavailable (%v)\n", cfgErr)
	} else {
		state := "off"
		if cfg.Update.Autoupdate {
			state = "on"
		}
		now := time.Now()
		inWin := "outside"
		if update.InWindow(now, cfg.Update.WindowStart, cfg.Update.WindowEnd) {
			inWin = "inside"
		}
		fmt.Printf("autoupdate:  %s (window %s-%s host-local, currently %s)\n", state, cfg.Update.WindowStart, cfg.Update.WindowEnd, inWin)
	}
	if target == "" {
		fmt.Println("target:      none released for this host")
		return exitOK
	}
	fmt.Printf("target:      %s (%s)\n", target, source)

	cmp, err := update.CompareSemver(target, version)
	if err == nil && cmp > 0 {
		fmt.Println("status:      upgrade available — run `sudo livck-agent upgrade`")
		return exitUpgradeAvailable
	}
	fmt.Println("status:      up to date")
	return exitOK
}
