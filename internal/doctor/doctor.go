// Package doctor implements the livck-agent `doctor` verb: a read-only,
// side-effect-free self-diagnosis an operator runs to answer "why isn't this
// server reporting?". It checks, in order, the things that actually block a
// healthy agent: the platform is supported
// (systemd, not a container, a known distro/arch), the agent can read /proc and
// write its state directory, the control plane and ingest host resolve and are
// reachable, the managed token authenticates (`GET /v1/me`), the clock is not
// skewed enough to get batches rejected, and PSI is available.
//
// It never mutates state, never enrolls, and never installs anything. The
// outcome is a Report whose ExitCode maps to the contract's four codes: 0 green,
// 1 warnings (PSI off, clock drift), 2 hard runtime errors (unreachable, auth
// failure, unreadable /proc), 3 an unsupported platform. A caller renders the
// report and exits with that code so an install script can branch on it.
package doctor

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/LIVCK/agent/internal/enroll"
	"github.com/LIVCK/agent/internal/platform"
)

// HTTPClient is the subset of *http.Client the network probes need, so tests
// inject a fake transport instead of reaching the real endpoints.
type HTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

// Status is the outcome of a single check. The zero value is StatusOK so a
// check that appends nothing reads as passing.
type Status int

const (
	StatusOK          Status = iota // the check passed
	StatusInfo                      // neutral fact (e.g. "not enrolled yet")
	StatusWarn                      // degraded but not fatal (PSI off, clock drift)
	StatusFail                      // a hard runtime error (unreachable, auth failed)
	StatusUnsupported               // the platform is not supported (systemd/distro/arch/container)
	StatusSkip                      // not run (a prerequisite check failed)
)

// Check is one diagnostic line: what was tested, how it went, a human detail and
// an optional remediation hint.
type Check struct {
	Name   string
	Status Status
	Detail string
	Hint   string
}

// Report is the ordered result of a doctor run.
type Report struct {
	Checks []Check
}

func (r *Report) add(name string, st Status, detail string) {
	r.Checks = append(r.Checks, Check{Name: name, Status: st, Detail: detail})
}

func (r *Report) addHint(name string, st Status, detail, hint string) {
	r.Checks = append(r.Checks, Check{Name: name, Status: st, Detail: detail, Hint: hint})
}

// ExitCode collapses the report to the contract's four exit codes, most severe
// wins: an unsupported platform (3) outranks a hard failure (2), which outranks
// a warning (1); an all-clear run is 0. Unsupported is its own code, not merged
// into "failure", so an install script can tell "wrong platform, stop" apart
// from "right platform, transient problem, maybe retry".
func (r *Report) ExitCode() int {
	var unsupported, failed, warned bool
	for _, c := range r.Checks {
		switch c.Status {
		case StatusUnsupported:
			unsupported = true
		case StatusFail:
			failed = true
		case StatusWarn:
			warned = true
		}
	}
	switch {
	case unsupported:
		return 3
	case failed:
		return 2
	case warned:
		return 1
	default:
		return 0
	}
}

// Options configures a doctor run. Store, ControlBase, IngestBase and StateDir
// are resolved by the caller from the same env/state the run loop uses, so
// doctor diagnoses the exact endpoints the agent would report to.
type Options struct {
	Platform     platform.Platform
	Store        *enroll.Store // identity + token (may be un-enrolled)
	HTTP         HTTPClient
	ControlBase  string        // control-plane base, e.g. https://app.livck.cloud
	IngestBase   string        // ingest base, e.g. https://ingest.livck.cloud
	StateDir     string        // state directory the agent writes 0600 files under
	AgentVersion string        // reported in the probe User-Agent
	Timeout      time.Duration // per-probe timeout; defaults to 10s
}

// Run executes the checks in a fixed, legible order and returns the Report. It
// short-circuits the network and PSI probes when the platform is unsupported:
// on the wrong platform those results are meaningless and the operator's only
// action is to move to a supported host.
func Run(ctx context.Context, opts Options) Report {
	if opts.Timeout <= 0 {
		opts.Timeout = 10 * time.Second
	}

	var r Report

	unsupported := checkPlatform(&r, opts.Platform)
	checkLocal(&r, opts)

	if unsupported {
		r.add("further checks", StatusSkip, "skipped — the platform is not supported for the agent")
		return r
	}

	checkNetwork(ctx, &r, opts)
	checkPSI(&r, opts.Platform)

	return r
}

// ---- rendering ----

const (
	ansiReset  = "\033[0m"
	ansiGreen  = "\033[32m"
	ansiYellow = "\033[33m"
	ansiRed    = "\033[31m"
	ansiGray   = "\033[90m"
)

// marker returns the status glyph and (when color is on) its ANSI color.
func marker(st Status, color bool) string {
	glyph, col := "?", ""
	switch st {
	case StatusOK:
		glyph, col = "✓", ansiGreen
	case StatusInfo:
		glyph, col = "·", ansiGray
	case StatusWarn:
		glyph, col = "!", ansiYellow
	case StatusFail:
		glyph, col = "✗", ansiRed
	case StatusUnsupported:
		glyph, col = "✗", ansiRed
	case StatusSkip:
		glyph, col = "–", ansiGray
	}
	if !color {
		return glyph
	}
	return col + glyph + ansiReset
}

// Render writes the report as an aligned, optionally colored checklist followed
// by a one-line verdict. color should be false when stdout is not a terminal.
func Render(w io.Writer, r Report, color bool) {
	width := 0
	for _, c := range r.Checks {
		if len(c.Name) > width {
			width = len(c.Name)
		}
	}

	var ok, warn, fail int
	for _, c := range r.Checks {
		switch c.Status {
		case StatusOK:
			ok++
		case StatusWarn:
			warn++
		case StatusFail, StatusUnsupported:
			fail++
		}
		_, _ = fmt.Fprintf(w, "  %s  %-*s  %s\n", marker(c.Status, color), width, c.Name, c.Detail)
		if c.Hint != "" {
			_, _ = fmt.Fprintf(w, "     %*s  ↳ %s\n", width, "", c.Hint)
		}
	}

	_, _ = fmt.Fprintln(w)
	verdict := "everything looks healthy"
	switch {
	case fail > 0 && r.ExitCode() == 3:
		verdict = "this host is not supported for the agent"
	case fail > 0:
		verdict = "there are problems that will stop the agent reporting"
	case warn > 0:
		verdict = "working, with warnings worth addressing"
	}
	_, _ = fmt.Fprintf(w, "  %d ok, %d warning(s), %d problem(s) — %s\n", ok, warn, fail, verdict)
}
