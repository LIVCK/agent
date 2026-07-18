package platform

import (
	"context"
	"os/exec"
)

// Exec abstracts running a fixed external tool and capturing its structured
// output. It exists for the opt-in hardware-telemetry collectors (gpu via
// nvidia-smi, smart via smartctl) that read a tool's machine-readable output,
// exactly the structured-exec form these tools expose (systemctl show -p,
// smartctl --json). It is deliberately minimal and carries no shell: callers
// pass a constant program name and fixed arguments, never user or config input,
// so there is no command-injection surface. The agent never installs these
// tools; a collector runs one only when LookPath finds it already present.
type Exec interface {
	// LookPath reports the resolved path of name and whether it exists in PATH.
	// It never forks the tool; it only resolves the binary, so it is cheap
	// enough to call from a collector's Available check every cycle.
	LookPath(name string) (string, bool)
	// Run executes name with args, bounded by ctx, and returns its stdout. The
	// caller derives ctx with a hard per-invocation timeout so a hung tool can
	// never block the collect loop; when ctx fires the process is killed. args
	// must be constant flags (plus tool-produced device paths the caller has
	// validated), never raw user input. stderr is discarded: these tools carry
	// their result on stdout and a non-zero exit yields an error the collector
	// degrades to "no samples".
	Run(ctx context.Context, name string, args ...string) ([]byte, error)
}

// OSExec is the production Exec backed by os/exec.
type OSExec struct{}

// LookPath resolves name against PATH.
func (OSExec) LookPath(name string) (string, bool) {
	p, err := exec.LookPath(name)
	return p, err == nil
}

// Run runs name with args under ctx and returns stdout. exec.CommandContext
// kills the process when ctx is cancelled or its deadline passes, so the hard
// timeout the caller sets is enforced by the OS. Only stdout is returned; a
// non-zero exit surfaces as a non-nil error (via *exec.ExitError).
func (OSExec) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	return cmd.Output()
}
