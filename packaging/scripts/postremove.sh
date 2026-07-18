#!/bin/sh
# postremove for livck-agent (deb + rpm, one script).
#
# Always reloads systemd so the removed unit disappears. On a deb `purge` it
# also deletes the config, the persisted state (identity/token), and the system
# user. A plain remove or an rpm erase KEEPS state so a reinstall re-adopts the
# same identity. ASCII.
set -e

USER_NAME=livck-agent
GROUP_NAME=livck-agent
STATE_DIR=/var/lib/livck-agent
CONFIG_DIR=/etc/livck-agent

systemctl daemon-reload >/dev/null 2>&1 || :

# deb passes a word ("remove"/"purge"/"upgrade"); rpm passes a count
# ("0"=erase, "1"=upgrade). Only the deb "purge" phase wipes user data.
case "$1" in
    "purge")
        rm -rf "$STATE_DIR" "$CONFIG_DIR"
        if getent passwd "$USER_NAME" >/dev/null 2>&1; then
            userdel "$USER_NAME" >/dev/null 2>&1 || :
        fi
        if getent group "$GROUP_NAME" >/dev/null 2>&1; then
            groupdel "$GROUP_NAME" >/dev/null 2>&1 || :
        fi
        ;;
    *)
        # remove / upgrade / rpm erase / rpm upgrade: keep state and the user.
        :
        ;;
esac

exit 0
