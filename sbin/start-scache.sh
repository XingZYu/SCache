#!/bin/bash

set -euo pipefail

usage() {
    echo "Usage: start-scache"
    exit 1
}

sbin=`dirname "$0"`
sbin=`cd "$sbin"; pwd`

. "$sbin/config.sh"


SCACHE_CLIENT_SCRIPT_OPTS="${SCACHE_CLIENT_SCRIPT_OPTS:-}"


SLAVES_FILE="${SCACHE_CONF_DIR}/slaves"
if [[ ! -f "$SLAVES_FILE" ]]; then
    echo "ERROR: slaves file not found: $SLAVES_FILE" >&2
    exit 1
fi


echo "start master"
"$sbin/start-master.sh"
sleep 5

echo "start clients"
BASE_PORT="${SCACHE_CLIENT_BASE_PORT:-15678}"
client_idx=0
while IFS= read -r slave || [[ -n "$slave" ]]; do
    # ignore blanks and comments
    [[ -z "${slave// /}" ]] && continue
    [[ "$slave" =~ ^# ]] && continue
    port=$((BASE_PORT + client_idx))
    echo "start Scache on $slave (port=$port idx=$client_idx)"

        if [[ "$slave" == "localhost" || "$slave" == "127.0.0.1" || "$slave" == "::1" ||
                    "$slave" == "$(hostname)" || "$slave" == "$(hostname -f 2>/dev/null || hostname)" ]]; then
            if [[ -n "${SCACHE_CLIENT_SCRIPT_OPTS// /}" ]]; then
                # shellcheck disable=SC2086
                "${SCACHE_HOME}/sbin/start-client.sh" --ip "$slave" --port "$port" ${SCACHE_CLIENT_SCRIPT_OPTS} &
            else
                "${SCACHE_HOME}/sbin/start-client.sh" --ip "$slave" --port "$port" &
            fi
        else
            # -n and </dev/null prevent ssh from reading stdin (which can stall loops and jobs)
            ssh -n $SCACHE_SSH_OPTS "$slave" \
                "${SCACHE_HOME}/sbin/start-client.sh --ip $slave --port $port ${SCACHE_CLIENT_SCRIPT_OPTS}" </dev/null &
        fi
    client_idx=$((client_idx + 1))
done < "$SLAVES_FILE"

wait
