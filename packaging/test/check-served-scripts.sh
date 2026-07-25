#!/bin/sh
# Validates the served-script contract: install.sh is served by get.livck.cloud
# with its {{SYSTEMD_*}} placeholders substituted by the real systemd unit files
# (see laravel ScriptFetchService). The SERVED script — not the raw source — is
# what customers pipe into `sh`, so THAT is what must be valid POSIX shell.
#
# This reproduces the substitution and syntax-checks both forms. It exists because
# a greedy str_replace once injected a unit (containing apostrophes: "host's",
# "'yes'") into install.sh's `grep -qF '{{SYSTEMD_UNIT}}'` guard, breaking the
# served script with `Syntax error: "}" unexpected` — while the raw source parsed
# fine, so nothing caught it.
#
# The substitution here is LINE-ANCHORED (only a placeholder alone on its own line,
# i.e. the heredoc body, is replaced) — it MUST mirror ScriptFetchService so this
# test reflects what the server actually serves. POSIX sh, ASCII only.
set -eu

# Repo root = two levels up from this script (packaging/test/).
root="$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)"
install_sh="$root/packaging/install.sh"

# Placeholder -> unit-file map. Keep in sync with config/distribution.php includes.
# (encoded as a newline list of "PLACEHOLDER|relative/path" pairs)
includes='{{SYSTEMD_UNIT}}|packaging/systemd/livck-agent.service
{{SYSTEMD_UPDATE_SERVICE}}|packaging/systemd/livck-agent-update.service
{{SYSTEMD_UPDATE_TIMER}}|packaging/systemd/livck-agent-update.timer'

fail=0
note() { printf '%s\n' "$*" >&2; }

# syntax_check <label> <file>: dash/sh -n (hard) + shellcheck errors (if present).
syntax_check() {
    _label="$1"; _file="$2"
    if command -v dash >/dev/null 2>&1; then _sh='dash'; else _sh='sh'; fi
    if "$_sh" -n "$_file"; then
        note "  ok: $_sh -n ($_label)"
    else
        note "  FAIL: $_sh -n rejected $_label"; fail=1
    fi
    if command -v shellcheck >/dev/null 2>&1; then
        # --severity=error: only genuine errors (e.g. unterminated quotes) fail the
        # build; pre-existing style/info nits do not. This is the class the served
        # script broke on.
        if shellcheck -s sh --severity=error "$_file"; then
            note "  ok: shellcheck --severity=error ($_label)"
        else
            note "  FAIL: shellcheck flagged errors in $_label"; fail=1
        fi
    fi
}

# assemble: line-anchored placeholder substitution, mirroring ScriptFetchService.
assemble() {
    _src="$1"; _out="$2"
    cp "$_src" "$_out"
    printf '%s\n' "$includes" | while IFS='|' read -r _ph _rel; do
        [ -n "$_ph" ] || continue
        _unit="$root/$_rel"
        [ -f "$_unit" ] || { note "  FAIL: unit file missing: $_rel"; return 1; }
        _tmp="$(mktemp)"
        # Replace ONLY a line that is EXACTLY the placeholder (the heredoc body).
        # An inline occurrence (the grep guard) is left verbatim — same contract
        # the server relies on.
        awk -v ph="$_ph" -v f="$_unit" '
            $0 == ph { while ((getline line < f) > 0) print line; close(f); next }
            { print }
        ' "$_out" > "$_tmp"
        mv "$_tmp" "$_out"
    done
}

note "== raw install.sh =="
[ -f "$install_sh" ] || { note "install.sh not found at $install_sh"; exit 1; }
syntax_check "raw install.sh" "$install_sh"

note "== assembled (served) install.sh =="
assembled="$(mktemp)"
trap 'rm -f "$assembled"' EXIT
assemble "$install_sh" "$assembled"

# The guard's literal placeholder MUST survive (proves the substitution did not
# clobber it — the exact thing that broke).
if grep -qF "grep -qF '{{SYSTEMD_UNIT}}'" "$assembled"; then
    note "  ok: grep guard preserved in the served script"
else
    note "  FAIL: the grep guard's placeholder was clobbered by substitution"; fail=1
fi
# ...and no placeholder may remain in the heredoc bodies.
if grep -qE '^\{\{SYSTEM' "$assembled"; then
    note "  FAIL: an un-substituted placeholder line remains in the served script"; fail=1
else
    note "  ok: all heredoc-body placeholders substituted"
fi
syntax_check "assembled install.sh" "$assembled"

if [ "$fail" -ne 0 ]; then
    note "served-script check FAILED"
    exit 1
fi
note "served-script check passed"
