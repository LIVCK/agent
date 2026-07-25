package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/LIVCK/agent/internal/config"
	"github.com/LIVCK/agent/internal/enroll"
	"github.com/LIVCK/agent/internal/platform"
	"github.com/LIVCK/agent/internal/update"
)

// statusCmd renders a read-only runtime snapshot: what the agent is, whether it
// can reach and is accepted by the cloud, its effective config, the update
// settings, and the running daemon's own footprint. Unlike `doctor` (a pass/fail
// preflight) this is a "what is it doing right now" view for humans and support.
func statusCmd(args []string) int {
	controlBase := resolveControlBase()
	if len(args) >= 2 && args[0] == "--url" {
		controlBase = args[1]
	}

	plat := platform.Real()
	stateDir := resolveStateDir()
	store := enroll.NewStore(plat.FS, stateDir, uuid.NewString)

	fmt.Println("livck-agent status")
	fmt.Println()

	// ---- Agent ------------------------------------------------------------
	instanceID, _ := store.InstanceID()
	enrolled := store.HasToken() && instanceID != ""
	section("Agent")
	row("version", version)
	if enrolled {
		row("enrollment", "enrolled (instance "+shortID(instanceID)+")")
	} else {
		row("enrollment", "NOT enrolled — run: livck-agent enroll --token lve_...")
	}

	// ---- Cloud reachability (the ping) + fresh config ---------------------
	section("Cloud")
	cfg, httpStatus, reachErr := fetchConfigStatus(controlBase, store)
	switch {
	case reachErr != "":
		row("control plane", "UNREACHABLE — "+reachErr)
	case httpStatus == http.StatusOK:
		row("control plane", fmt.Sprintf("reachable, authorized (HTTP 200) — %s", trimScheme(controlBase)))
	case httpStatus == http.StatusUnauthorized:
		row("control plane", "reachable but TOKEN REJECTED (HTTP 401) — re-enroll needed")
	default:
		row("control plane", fmt.Sprintf("reachable, unexpected HTTP %d", httpStatus))
	}
	row("ingest", trimScheme(resolveIngestBase(store)))

	// ---- Config -----------------------------------------------------------
	// Prefer the fresh pull; fall back to the cached last-good on the disk.
	if cfg == nil {
		cfg = cachedConfig(plat, stateDir)
	}
	if cfg != nil {
		section("Config")
		row("version", strconv.Itoa(cfg.ConfigVersion))
		row("interval", fmt.Sprintf("%ds", cfg.IntervalSeconds))
		row("collectors", collectorSummary(cfg))
		row("features", featureSummary(cfg))
		if n := len(cfg.Collectors.Probe.Targets); n > 0 {
			row("probes", fmt.Sprintf("%d target(s)", n))
		}

		// ---- Updates ------------------------------------------------------
		section("Updates")
		row("autoupdate", onOff(cfg.Update.Autoupdate))
		row("window", fmt.Sprintf("%s–%s (host-local)", cfg.Update.WindowStart, cfg.Update.WindowEnd))
		if cfg.Update.TargetVersion != "" {
			if cmp, err := update.CompareSemver(cfg.Update.TargetVersion, version); err == nil && cmp > 0 {
				row("available", cfg.Update.TargetVersion+"  → run: sudo livck-agent upgrade")
			} else {
				row("available", "up to date (target "+cfg.Update.TargetVersion+")")
			}
		} else {
			row("available", "up to date (no rollout released)")
		}
		row("last run", lastUpdateResult(stateDir))
	}

	// ---- Daemon (own footprint) ------------------------------------------
	section("Daemon (" + serviceUnit + ")")
	renderDaemon()

	fmt.Println()
	return exitOK
}

// fetchConfigStatus does one authenticated GET of the config endpoint and returns
// the parsed config (on 200), the HTTP status, and a reachability error string.
func fetchConfigStatus(controlBase string, store *enroll.Store) (*config.Config, int, string) {
	token, err := store.Token()
	if err != nil || token == "" {
		return nil, 0, "not enrolled"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(controlBase, "/")+"/api/v1/agents/config", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return nil, 0, "network error"
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode, ""
	}
	raw := make([]byte, 0, 4096)
	buf := make([]byte, 4096)
	for {
		n, rerr := resp.Body.Read(buf)
		raw = append(raw, buf[:n]...)
		if rerr != nil || len(raw) > 1<<20 {
			break
		}
	}
	cfg, _, verr := config.Validate(raw)
	if verr != nil {
		return nil, resp.StatusCode, ""
	}
	return cfg, resp.StatusCode, ""
}

// cachedConfig loads the last-good config the run loop persisted, so status still
// shows the effective config when the control plane is down.
func cachedConfig(plat platform.Platform, stateDir string) *config.Config {
	raw, err := plat.FS.ReadFile(filepath.Join(stateDir, enroll.ConfigFile))
	if err != nil || len(raw) == 0 {
		return nil
	}
	cfg, _, verr := config.Validate(raw)
	if verr != nil {
		return nil
	}
	return cfg
}

// renderDaemon shows the running service's liveness + its own footprint.
func renderDaemon() {
	out, err := exec.Command("systemctl", "show", serviceUnit,
		"-p", "ActiveState,SubState,MainPID,NRestarts,ActiveEnterTimestampMonotonic").Output()
	if err != nil {
		row("state", "unknown (systemctl unavailable)")
		return
	}
	f := map[string]string{}
	for _, line := range strings.Split(string(out), "\n") {
		if k, v, ok := strings.Cut(strings.TrimSpace(line), "="); ok {
			f[k] = v
		}
	}
	state := f["ActiveState"]
	if f["SubState"] != "" {
		state += " (" + f["SubState"] + ")"
	}
	row("state", orDash(state))

	pid := f["MainPID"]
	if pid == "" || pid == "0" {
		row("pid", "— (not running)")
		return
	}
	row("pid", pid)
	if rss := procRSS(pid); rss != "" {
		row("memory", rss)
	}
	if up := monotonicUptime(f["ActiveEnterTimestampMonotonic"]); up != "" {
		row("uptime", up)
	}
	row("restarts", orDash(f["NRestarts"]))
}

// procRSS reads VmRSS from /proc/<pid>/status and renders it human-readable.
func procRSS(pid string) string {
	raw, err := os.ReadFile("/proc/" + pid + "/status")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				if kb, err := strconv.ParseFloat(fields[1], 64); err == nil {
					return fmt.Sprintf("%.1f MB", kb/1024)
				}
			}
		}
	}
	return ""
}

// monotonicUptime turns the unit's ActiveEnter monotonic stamp (µs since boot)
// into a compact duration against the current monotonic clock.
func monotonicUptime(usStr string) string {
	us, err := strconv.ParseInt(strings.TrimSpace(usStr), 10, 64)
	if err != nil || us == 0 {
		return ""
	}
	raw, err := os.ReadFile("/proc/uptime")
	if err != nil {
		return ""
	}
	secStr, _, _ := strings.Cut(string(raw), " ")
	bootSec, err := strconv.ParseFloat(secStr, 64)
	if err != nil {
		return ""
	}
	d := time.Duration(bootSec*float64(time.Second)) - time.Duration(us)*time.Microsecond
	if d < 0 {
		return ""
	}
	return compactDuration(d)
}

func compactDuration(d time.Duration) string {
	d = d.Round(time.Second)
	if d >= 24*time.Hour {
		return fmt.Sprintf("%dd %dh", d/(24*time.Hour), (d%(24*time.Hour))/time.Hour)
	}
	if d >= time.Hour {
		return fmt.Sprintf("%dh %dm", d/time.Hour, (d%time.Hour)/time.Minute)
	}
	if d >= time.Minute {
		return fmt.Sprintf("%dm %ds", d/time.Minute, (d%time.Minute)/time.Second)
	}
	return fmt.Sprintf("%ds", d/time.Second)
}

// lastUpdateResult reads the upgrade CLI's result heartbeat.
func lastUpdateResult(stateDir string) string {
	raw, err := os.ReadFile(filepath.Join(stateDir, update.ResultFile))
	if err != nil {
		return "—"
	}
	var res struct {
		Outcome    string `json:"outcome"`
		From       string `json:"from"`
		To         string `json:"to"`
		FinishedAt string `json:"finished_at"`
	}
	if json.Unmarshal(raw, &res) != nil || res.Outcome == "" {
		return "—"
	}
	when := ""
	if t, err := time.Parse(time.RFC3339, res.FinishedAt); err == nil {
		when = " · " + compactDuration(time.Since(t)) + " ago"
	}
	if res.From != "" && res.To != "" {
		return fmt.Sprintf("%s (%s → %s)%s", res.Outcome, res.From, res.To, when)
	}
	return res.Outcome + when
}

// ---- small rendering helpers ------------------------------------------------

func section(name string) { fmt.Printf("  %s\n", name) }

func row(label, value string) { fmt.Printf("    %-14s %s\n", label, value) }

func onOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

func orDash(s string) string {
	if strings.TrimSpace(s) == "" {
		return "—"
	}
	return s
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8] + "…"
	}
	return id
}

func trimScheme(u string) string {
	u = strings.TrimPrefix(u, "https://")
	u = strings.TrimPrefix(u, "http://")
	return strings.TrimRight(u, "/")
}

func collectorSummary(cfg *config.Config) string {
	c := cfg.Collectors
	on := []string{}
	for _, e := range []struct {
		name string
		set  bool
	}{
		{"cpu", c.CPU}, {"load", c.Load}, {"mem", c.Mem}, {"swap", c.Swap},
		{"psi", c.PSI}, {"disk", true}, {"diskio", true}, {"net", true},
	} {
		if e.set {
			on = append(on, e.name)
		}
	}
	if len(on) == 0 {
		return "—"
	}
	return strings.Join(on, ", ")
}

func featureSummary(cfg *config.Config) string {
	on := []string{}
	if cfg.Features.Live {
		on = append(on, "live")
	}
	if cfg.Features.GPU {
		on = append(on, "gpu")
	}
	if cfg.Features.Smart {
		on = append(on, "smart")
	}
	if cfg.Features.Systemd.Enabled {
		on = append(on, "systemd")
	}
	if len(on) == 0 {
		return "—"
	}
	return strings.Join(on, ", ")
}
