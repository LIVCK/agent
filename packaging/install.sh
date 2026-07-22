#!/bin/sh
# LIVCK Server Monitoring Agent - installer.
#
# Served from https://get.livck.cloud. Typical use (panel one-liner writes the
# short-lived enrollment token to a 0600 temp file first, so it never lands in
# argv / /proc/cmdline):
#
#   sudo sh <<'EOF'
#   umask 077
#   tf=$(mktemp); trap 'rm -f "$tf"' EXIT
#   cat > "$tf" <<'TOKEN'
#   lve_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
#   TOKEN
#   curl -fsSL https://get.livck.cloud/install.sh | sh -s -- --token-file "$tf" --name web-01
#   EOF
#
# It also accepts --token directly for manual use (less safe: the token is then
# visible in this script's argv while it runs).
#
# What it does: platform preflight (systemd, not-a-container, supported distro +
# arch) -> download and integrity-check the deb/rpm (or the raw binary as a
# fallback) -> install -> enroll -> enable + start. Idempotent: re-running
# upgrades in place and re-enroll is a no-op unless --force.
#
# POSIX sh (works under sh and bash). No `pipefail` (not portable to dash): all
# downloads go to a file and are checked explicitly. ASCII only.

set -eu

# ---- defaults (public hosts only; overridable via flags/env) ----------------
DOWNLOAD_BASE="${LIVCK_DOWNLOAD_URL:-https://get.livck.cloud}"
ENROLL_BASE="${LIVCK_ENROLL_URL:-https://app.livck.cloud}"
VERSION="${LIVCK_AGENT_VERSION:-latest}"

BINARY_PATH="/usr/bin/livck-agent"
UNIT_PATH="/usr/lib/systemd/system/livck-agent.service"
STATE_DIR="/var/lib/livck-agent"
CONFIG_DIR="/etc/livck-agent"
USER_NAME="livck-agent"
SERVICE="livck-agent.service"

# ---- arg state --------------------------------------------------------------
TOKEN=""
TOKEN_FILE=""
NAME=""
TAGS=""
FORCE=""
SKIP_ENROLL=""

# ---- logging (stderr; never prints the token) -------------------------------
log()  { printf 'livck-agent: %s\n' "$1" >&2; }
warn() { printf 'livck-agent: WARNING: %s\n' "$1" >&2; }
# die <exit-code> <message>
die()  { printf 'livck-agent: ERROR: %s\n' "$2" >&2; exit "$1"; }

usage() {
    cat >&2 <<'EOF'
Usage: install.sh [options]

  --token <lve_...>       enrollment token (prefer --token-file)
  --token-file <path>     file holding the enrollment token (0600 recommended)
  --name <name>           service name (defaults to the hostname)
  --tag <k:v>             tag, repeatable (e.g. --tag env:prod --tag team:ops)
  --tags <k:v,k:v>        comma-separated tags (alternative to --tag)
  --version <v>           agent version to install (default: latest)
  --url <url>             enroll base URL (default: https://app.livck.cloud)
  --download-url <url>    download base URL (default: https://get.livck.cloud)
  --force                 re-enroll even if this host is already enrolled
  --skip-enroll           install only; do not enroll/start (advanced)
  -h, --help              this help
EOF
}

# ---- argument parsing (supports --flag value and --flag=value) --------------
while [ $# -gt 0 ]; do
    case "$1" in
        --token)         TOKEN="${2:-}"; shift 2 ;;
        --token=*)       TOKEN="${1#*=}"; shift ;;
        --token-file)    TOKEN_FILE="${2:-}"; shift 2 ;;
        --token-file=*)  TOKEN_FILE="${1#*=}"; shift ;;
        --name)          NAME="${2:-}"; shift 2 ;;
        --name=*)        NAME="${1#*=}"; shift ;;
        --tag)           TAGS="${TAGS:+$TAGS,}${2:-}"; shift 2 ;;
        --tag=*)         TAGS="${TAGS:+$TAGS,}${1#*=}"; shift ;;
        --tags)          TAGS="${TAGS:+$TAGS,}${2:-}"; shift 2 ;;
        --tags=*)        TAGS="${TAGS:+$TAGS,}${1#*=}"; shift ;;
        --version)       VERSION="${2:-}"; shift 2 ;;
        --version=*)     VERSION="${1#*=}"; shift ;;
        --url)           ENROLL_BASE="${2:-}"; shift 2 ;;
        --url=*)         ENROLL_BASE="${1#*=}"; shift ;;
        --download-url)  DOWNLOAD_BASE="${2:-}"; shift 2 ;;
        --download-url=*) DOWNLOAD_BASE="${1#*=}"; shift ;;
        --force)         FORCE=1; shift ;;
        --skip-enroll)   SKIP_ENROLL=1; shift ;;
        -h|--help)       usage; exit 0 ;;
        *)               usage; die 2 "unknown argument: $1" ;;
    esac
done

# ---- preflight: privileges + required tools ---------------------------------
[ "$(id -u)" = "0" ] || die 2 "must run as root (use sudo)."
command -v curl >/dev/null 2>&1 || die 2 "curl is required."
command -v uname >/dev/null 2>&1 || die 2 "uname is required."
command -v systemctl >/dev/null 2>&1 || die 3 "systemctl not found; systemd is required."

# ---- preflight: platform (unsupported => exit 3) ----------------------------
# systemd present and PID 1.
[ -d /run/systemd/system ] || die 3 "systemd is required (no /run/systemd/system)."
if [ "$(cat /proc/1/comm 2>/dev/null || echo unknown)" != "systemd" ]; then
    die 3 "PID 1 is not systemd; unsupported init system."
fi

# Not a container / LXC / Docker (namespaced /proc makes host KPIs ambiguous).
in_container() {
    # --quiet returns the verdict as the exit status (0 = in a container) with no
    # output. Parsing the printed string was wrong: on a bare host the command
    # prints "none" AND exits non-zero, so `... || echo none` doubled it to
    # "none\nnone" and `[ "$v" != "none" ]` fired — a false positive that blocked
    # every real host (dedicated servers, KVM VMs, …).
    if command -v systemd-detect-virt >/dev/null 2>&1 && systemd-detect-virt --container --quiet 2>/dev/null; then
        return 0
    fi
    [ -f /.dockerenv ] && return 0
    if [ -r /proc/1/cgroup ] && grep -qE '(docker|lxc|kubepods|containerd)' /proc/1/cgroup 2>/dev/null; then
        return 0
    fi
    return 1
}
if in_container; then
    die 3 "containers/LXC are not supported in v1 (namespaced /proc). See the roadmap."
fi

# Supported distro (Ubuntu 22.04/24.04, Debian 12, RHEL 9). RHEL-9 ABI rebuilds
# (Rocky/Alma/CentOS Stream 9) are best-effort: WARN and continue.
[ -r /etc/os-release ] || die 3 "cannot read /etc/os-release."
# shellcheck disable=SC1091
. /etc/os-release
OS_ID="${ID:-unknown}"
OS_VER="${VERSION_ID:-0}"
OS_MAJOR="${OS_VER%%.*}"
supported=0
case "$OS_ID" in
    ubuntu) case "$OS_VER" in 22.04|24.04) supported=1 ;; esac ;;
    debian) [ "$OS_MAJOR" = "12" ] && supported=1 ;;
    rhel)   [ "$OS_MAJOR" = "9" ] && supported=1 ;;
esac
if [ "$supported" -ne 1 ]; then
    if printf '%s' "${ID_LIKE:-}" | grep -qw rhel && [ "$OS_MAJOR" = "9" ]; then
        warn "distro $OS_ID $OS_VER is a RHEL-9 rebuild: best-effort, untested. Continuing."
    else
        die 3 "unsupported distro: $OS_ID $OS_VER (supported: Ubuntu 22.04/24.04, Debian 12, RHEL 9)."
    fi
fi

# Supported architecture.
case "$(uname -m)" in
    x86_64)  ARCH="amd64";  RAW_ARCH="x86_64" ;;
    aarch64) ARCH="arm64";  RAW_ARCH="aarch64" ;;
    *)       die 3 "unsupported architecture: $(uname -m) (supported: x86_64, aarch64)." ;;
esac

# ---- token requirement ------------------------------------------------------
if [ -z "$SKIP_ENROLL" ] && [ -z "$TOKEN" ] && [ -z "$TOKEN_FILE" ]; then
    usage
    die 2 "an enrollment token is required (--token-file or --token), or pass --skip-enroll."
fi
if [ -n "$TOKEN_FILE" ] && [ ! -r "$TOKEN_FILE" ]; then
    die 2 "token file not readable: $TOKEN_FILE"
fi

# ---- temp workspace ---------------------------------------------------------
umask 077
WORKDIR="$(mktemp -d)"
cleanup() { rm -rf "$WORKDIR" 2>/dev/null || :; }
trap cleanup EXIT INT TERM
DL="$WORKDIR/dl"

# curl_to <url> <dest> : download with retries; returns non-zero on failure.
curl_to() {
    curl -fsSL --retry 3 --retry-delay 2 --connect-timeout 15 -o "$2" "$1"
}

# sha256_of <file> : print the hex digest, or empty if no tool is available.
sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$1" | awk '{print $1}'
    else
        echo ""
    fi
}

# verify_checksum <file> <checksum-url> : 0 ok, 1 mismatch, 2 unavailable.
verify_checksum() {
    _expected="$(curl -fsSL --connect-timeout 15 "$2" 2>/dev/null | awk '{print $1}' | head -n1 || true)"
    [ -n "$_expected" ] || return 2
    _actual="$(sha256_of "$1")"
    [ -n "$_actual" ] || { warn "no sha256 tool; cannot verify integrity."; return 2; }
    [ "$_expected" = "$_actual" ]
}

# ---- install via native package if possible, else raw binary ----------------
install_via_package() {
    _fmt="$1"        # deb | rpm
    _pkgarch="$2"    # amd64/arm64 (deb) or x86_64/aarch64 (rpm)
    _url="$DOWNLOAD_BASE/$_fmt/$VERSION/livck-agent_${_pkgarch}.$_fmt"
    _pkg="$DL.$_fmt"
    log "downloading $_fmt package ($_url)"
    if ! curl_to "$_url" "$_pkg"; then
        return 1
    fi
    if verify_checksum "$_pkg" "$_url.sha256"; then
        log "checksum verified."
    else
        _rc=$?
        if [ "$_rc" -eq 1 ]; then die 1 "checksum mismatch for $_url"; fi
        warn "no published checksum for the package; skipping integrity check."
    fi
    case "$_fmt" in
        deb)
            if ! dpkg -i "$_pkg" >&2; then
                # only dep is systemd (present); attempt a repair anyway.
                command -v apt-get >/dev/null 2>&1 && apt-get -y -f install >&2 || :
                dpkg -i "$_pkg" >&2
            fi
            ;;
        rpm)
            if command -v dnf >/dev/null 2>&1; then
                dnf install -y "$_pkg" >&2
            elif command -v yum >/dev/null 2>&1; then
                yum install -y "$_pkg" >&2
            else
                rpm -Uvh "$_pkg" >&2
            fi
            ;;
    esac
    return 0
}

# systemd unit for the raw-binary fallback (runs only when no deb/rpm package is
# available). The unit has a single source of truth: packaging/systemd/livck-agent.service,
# the same file the deb/rpm ship. get.livck.cloud substitutes the placeholder below
# with that file's content when it serves this script, so there is no second copy to
# drift. If the script is run straight from source the placeholder is untouched, and
# we fetch the unit from get.livck.cloud instead.
write_embedded_unit() {
    cat > "$UNIT_PATH" <<'UNIT'
{{SYSTEMD_UNIT}}
UNIT
    if grep -qF '{{SYSTEMD_UNIT}}' "$UNIT_PATH" 2>/dev/null; then
        log "fetching systemd unit ($DOWNLOAD_BASE/livck-agent.service)"
        curl_to "$DOWNLOAD_BASE/livck-agent.service" "$UNIT_PATH" \
            || die 1 "could not fetch the systemd unit; install the deb/rpm package instead"
    fi
    chmod 0644 "$UNIT_PATH"
}

pick_nologin() {
    if [ -x /usr/sbin/nologin ]; then echo /usr/sbin/nologin
    elif [ -x /sbin/nologin ]; then echo /sbin/nologin
    else echo /bin/false; fi
}

create_user() {
    if ! getent group "$USER_NAME" >/dev/null 2>&1; then
        groupadd --system "$USER_NAME"
    fi
    if ! getent passwd "$USER_NAME" >/dev/null 2>&1; then
        useradd --system --gid "$USER_NAME" --home-dir "$STATE_DIR" \
            --no-create-home --shell "$(pick_nologin)" \
            --comment "LIVCK Monitoring Agent" "$USER_NAME"
    fi
}

install_via_binary() {
    _url="$DOWNLOAD_BASE/binary/$VERSION/livck-agent_linux_${ARCH}"
    _bin="$DL.bin"
    log "downloading raw binary ($_url)"
    curl_to "$_url" "$_bin" || die 1 "download failed: $_url"
    if verify_checksum "$_bin" "$_url.sha256"; then
        log "checksum verified."
    else
        _rc=$?
        if [ "$_rc" -eq 1 ]; then die 1 "checksum mismatch for $_url"; fi
        warn "no published checksum for the binary; skipping integrity check."
    fi
    install -m 0755 "$_bin" "$BINARY_PATH"
    create_user
    write_embedded_unit
    systemctl daemon-reload >/dev/null 2>&1 || :
}

install_agent() {
    # Prefer the native package format for this distro; fall back to the binary.
    if command -v dpkg >/dev/null 2>&1; then
        install_via_package deb "$ARCH" && return 0
        warn "deb package unavailable; falling back to the raw binary."
    elif command -v rpm >/dev/null 2>&1; then
        install_via_package rpm "$RAW_ARCH" && return 0
        warn "rpm package unavailable; falling back to the raw binary."
    fi
    install_via_binary
}

log "installing livck-agent $VERSION for $OS_ID $OS_VER ($ARCH)"
install_agent
[ -x "$BINARY_PATH" ] || die 3 "installation failed: $BINARY_PATH not found."
log "installed $("$BINARY_PATH" version 2>/dev/null || echo 'livck-agent')"

# ---- enroll -----------------------------------------------------------------
if [ -n "$SKIP_ENROLL" ]; then
    log "--skip-enroll set; not enrolling or starting. Enroll later with 'livck-agent enroll'."
    exit 0
fi

# State dir must exist and be owned by the agent user before first start so the
# enroll writes (identity/token, 0600) are readable by the service. systemd's
# StateDirectory will adopt this exact dir on start.
install -d -m 0700 -o "$USER_NAME" -g "$USER_NAME" "$STATE_DIR"

# Copy the token into a fresh 0600 file the enroll verb reads then unlinks. This
# never touches the caller's original --token-file.
TF="$WORKDIR/token"
: > "$TF"
chmod 0600 "$TF"
if [ -n "$TOKEN_FILE" ]; then
    cat "$TOKEN_FILE" > "$TF"
else
    printf '%s' "$TOKEN" > "$TF"
fi

# Build enroll args (token never appears here; it is in the file).
set -- enroll --token-file "$TF" --url "$ENROLL_BASE"
[ -n "$NAME" ] && set -- "$@" --name "$NAME"
[ -n "$TAGS" ] && set -- "$@" --tags "$TAGS"
[ -n "$FORCE" ] && set -- "$@" --force

# Run enroll as root; it writes into $STATE_DIR. Fix ownership afterwards so the
# non-root service can read the 0600 identity/token files.
log "enrolling with the LIVCK control plane..."
set +e
"$BINARY_PATH" "$@"
enroll_rc=$?
set -e
chown -R "$USER_NAME:$USER_NAME" "$STATE_DIR" 2>/dev/null || :

case "$enroll_rc" in
    0) log "enrolled." ;;
    4) log "already enrolled (config refreshed). Use --force to re-enroll." ;;
    2) die 2 "enroll rejected the arguments." ;;
    3) die 3 "enroll failed permanently (see the message above)." ;;
    *) die 1 "enroll failed (retryable). Re-run the installer to try again." ;;
esac

# ---- enable + start ---------------------------------------------------------
log "enabling and starting $SERVICE"
systemctl enable --now "$SERVICE" >&2

log "done. Check status with: systemctl status $SERVICE"
exit 0
