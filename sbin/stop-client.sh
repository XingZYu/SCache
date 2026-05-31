#!/bin/bash

set -euo pipefail

sbin=`dirname "$0"`
sbin=`cd "$sbin"; pwd`

. "$sbin/config.sh"

MAIN_CLASS="org.scache.deploy.ScacheClient"

# Support multi-client mode: look for port-specific PID files.
# If --port is provided, stop only that client; otherwise stop all matched clients.
CLIENT_PORT_SUFFIX=""
for _a in "$@"; do
    if [[ -n "${_prev_port:-}" ]]; then
        CLIENT_PORT_SUFFIX="-${_a}"
        break
    fi
    [[ "$_a" == "--port" || "$_a" == "-p" ]] && _prev_port=1
done
PID_FILE="$SCACHE_PID_DIR/scache-client${CLIENT_PORT_SUFFIX}.pid"

is_pid_running() {
	local pid="$1"
	[[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

pid_matches_main() {
	local pid="$1"
	ps -p "$pid" -o args= 2>/dev/null | grep -Fq "$MAIN_CLASS"
}

kill_and_wait() {
	local pid="$1"
	local timeout_secs="${2:-15}"

	if ! is_pid_running "$pid"; then
		return 0
	fi

	echo "Stopping ScacheClient (pid=$pid)"
	kill "$pid" 2>/dev/null || true
	local waited=0
	while is_pid_running "$pid" && [[ $waited -lt $timeout_secs ]]; do
		sleep 1
		waited=$((waited + 1))
	done
	if is_pid_running "$pid"; then
		echo "ScacheClient did not stop in ${timeout_secs}s; sending SIGKILL" >&2
		kill -9 "$pid" 2>/dev/null || true
	fi
}

pid=""
if [[ -f "$PID_FILE" ]]; then
	pid=$(cat "$PID_FILE" 2>/dev/null || true)
fi

if [[ -n "$pid" ]] && is_pid_running "$pid" && pid_matches_main "$pid"; then
	kill_and_wait "$pid"
	rm -f "$PID_FILE"
	echo "ScacheClient stopped"
	exit 0
fi

# Fallback: locate by process command line.
pid=$(pgrep -u "$(id -u)" -f "$MAIN_CLASS" | head -n 1 || true)
if [[ -n "$pid" ]]; then
	kill_and_wait "$pid"
	rm -f "$PID_FILE" || true
	echo "ScacheClient stopped"
	exit 0
fi

echo "ScacheClient not running"
