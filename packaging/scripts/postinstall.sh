#!/bin/sh
# postinstall for livck-agent (deb + rpm, one script).
#
# Creates the dedicated system user and reloads systemd. It deliberately does
# NOT enable or start the service on a clean install: the agent needs an
# enrollment token first, which the get.livck.cloud install.sh supplies via
# `livck-agent enroll` before it enables the unit. ASCII only.
set -e

USER_NAME=livck-agent
GROUP_NAME=livck-agent
STATE_DIR=/var/lib/livck-agent

# --- normalize the packager-specific argument into install|upgrade -----------
# deb: $1=configure, $2=<old version> on upgrade (empty on clean install).
# rpm: $1=1 on install, $1=2 on upgrade.
action="$1"
if [ "$1" = "configure" ] && [ -z "${2:-}" ]; then
    action="install"
elif [ "$1" = "configure" ] && [ -n "${2:-}" ]; then
    action="upgrade"
fi

pick_nologin() {
    if [ -x /usr/sbin/nologin ]; then
        echo /usr/sbin/nologin
    elif [ -x /sbin/nologin ]; then
        echo /sbin/nologin
    else
        echo /bin/false
    fi
}

create_user() {
    # Idempotent: safe on reinstall/upgrade.
    if ! getent group "$GROUP_NAME" >/dev/null 2>&1; then
        groupadd --system "$GROUP_NAME"
    fi
    if ! getent passwd "$USER_NAME" >/dev/null 2>&1; then
        useradd --system \
            --gid "$GROUP_NAME" \
            --home-dir "$STATE_DIR" \
            --no-create-home \
            --shell "$(pick_nologin)" \
            --comment "LIVCK Monitoring Agent" \
            "$USER_NAME"
    fi
}

create_user
systemctl daemon-reload >/dev/null 2>&1 || :

case "$action" in
    "1"|"install")
        # Clean install: unit is on disk but stays stopped/disabled until enroll.
        echo "livck-agent installed. Enroll with a token to start it, e.g.:"
        echo "  curl -fsSL https://get.livck.cloud/install.sh | sudo sh -s -- --token lve_..."
        ;;
    "2"|"upgrade")
        # Upgrade: pick up the new binary only if the service is already running.
        # try-restart is a no-op when the unit is inactive, so we never start an
        # un-enrolled agent.
        systemctl try-restart "$USER_NAME.service" >/dev/null 2>&1 || :
        ;;
    *)
        # Fallback (e.g. unexpected arg): behave like a clean install.
        :
        ;;
esac

exit 0
