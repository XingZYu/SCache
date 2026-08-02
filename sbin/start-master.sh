#!/bin/bash

set -euo pipefail

usage() {
    echo "Usage: start-master"
    exit 1
}

sbin=`dirname "$0"`
sbin=`cd "$sbin"; pwd`


. "$sbin/config.sh"

mkdir -p "$SCACHE_LOG_DIR" "$SCACHE_PID_DIR"

MAIN_CLASS="org.scache.deploy.ScacheMaster"
PID_FILE="$SCACHE_PID_DIR/scache-master.pid"
LOG_FILE="$SCACHE_LOG_DIR/scache-master.out"

find_assembly_jar() {
    if [[ -n "${SCACHE_JAR:-}" ]]; then
        echo "$SCACHE_JAR"
        return 0
    fi

    # Prefer the Spark 3 / Celeborn-compatible Scala ABI, then fall back to any build.
    local jar
    jar=$(find "$SCACHE_HOME/target" -type f -path "*/scala-2.12/*" -name "SCache-assembly-*.jar" 2>/dev/null | sort | tail -n 1 || true)
    if [[ -z "$jar" ]]; then
        jar=$(find "$SCACHE_HOME/target" -type f -path "*/scala-*/*" -name "SCache-assembly-*.jar" 2>/dev/null | sort | tail -n 1 || true)
    fi
    if [[ -z "$jar" ]]; then
        echo "ERROR: Could not find assembly jar under $SCACHE_HOME/target. Run 'sbt assembly' first or set SCACHE_JAR." >&2
        exit 1
    fi
    echo "$jar"
}

is_pid_running() {
    local pid="$1"
    [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

pid_matches_main() {
    local pid="$1"
    ps -p "$pid" -o args= 2>/dev/null | grep -Fq "$MAIN_CLASS"
}

if [[ -f "$PID_FILE" ]]; then
    pid=$(cat "$PID_FILE" 2>/dev/null || true)
    if is_pid_running "$pid" && pid_matches_main "$pid"; then
        echo "ScacheMaster already running (pid=$pid)"
        exit 0
    fi
    rm -f "$PID_FILE"
fi

JAR="$(find_assembly_jar)"

echo "Starting ScacheMaster using $JAR"
nohup java $SCACHE_JAVA_OPTS -cp "$JAR" "$MAIN_CLASS" >>"$LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"
echo "ScacheMaster started (pid=$(cat "$PID_FILE"))"
