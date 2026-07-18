#!/bin/sh
# preremove for livck-agent (deb + rpm, one script).
#
# On a real removal it stops and disables the service. On an upgrade it does
# nothing (the new package's postinstall try-restarts the running unit). ASCII.
set -e

SERVICE=livck-agent.service

# --- normalize the packager-specific argument into remove|upgrade ------------
# deb: $1=remove or $1=upgrade.
# rpm: $1=0 on final erase, $1=1 on upgrade.
action="$1"
if [ "$1" = "0" ]; then
    action="remove"
elif [ "$1" = "1" ]; then
    action="upgrade"
fi

case "$action" in
    "remove")
        systemctl disable --now "$SERVICE" >/dev/null 2>&1 || :
        ;;
    "upgrade")
        # Leave the running service alone; postinstall handles the restart.
        :
        ;;
    *)
        :
        ;;
esac

exit 0
