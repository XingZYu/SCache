#!/bin/bash

set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  start-client.sh [script-options] [--] [ScacheClient options]

Script options:
  --cpu-node N         Bind client CPU to NUMA node N (uses numactl)
  --cpu-list LIST      Bind client to an exact physical CPU list (uses numactl)
  --mem-node N         Bind client memory allocations to NUMA node N (uses numactl)
  --numa-node N        Alias for --mem-node
  --numactl-opts OPTS  Raw opts passed to numactl (overrides --cpu-node/--mem-node)

Env (alternatives to script options):
  SCACHE_CLIENT_CPU_NODE
  SCACHE_CLIENT_MEM_NODE (or SCACHE_CLIENT_NUMA_NODE)
  SCACHE_CLIENT_NUMACTL_OPTS

Any remaining arguments are forwarded to org.scache.deploy.ScacheClient (e.g. --ip/--port/--master).
EOF
    exit 1
}

sbin=`dirname "$0"`
sbin=`cd "$sbin"; pwd`



. "$sbin/config.sh"

mkdir -p "$SCACHE_LOG_DIR" "$SCACHE_PID_DIR"

MAIN_CLASS="org.scache.deploy.ScacheClient"
PID_FILE="$SCACHE_PID_DIR/scache-client.pid"
LOG_FILE="$SCACHE_LOG_DIR/scache-client.out"

find_assembly_jar() {
    if [[ -n "${SCACHE_JAR:-}" ]]; then
        echo "$SCACHE_JAR"
        return 0
    fi

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
        echo "ScacheClient already running (pid=$pid)"
        exit 0
    fi
    rm -f "$PID_FILE"
fi

JAR="$(find_assembly_jar)"

echo "Starting ScacheClient using $JAR"

# Optional: prefix the client JVM with numactl to control NUMA placement.
# Example (Spark pinned to CPU node 0, memory pool on node 1):
#   ./sbin/start-client.sh --cpu-node 0 --mem-node 1
CLIENT_ARGS=()
CPU_NODE="${SCACHE_CLIENT_CPU_NODE:-}"
CPU_LIST="${SCACHE_CLIENT_CPU_LIST:-}"
MEM_NODE="${SCACHE_CLIENT_MEM_NODE:-${SCACHE_CLIENT_NUMA_NODE:-}}"
NUMACTL_OPTS="${SCACHE_CLIENT_NUMACTL_OPTS:-}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            usage
            ;;
        --cpu-node)
            [[ $# -ge 2 ]] || usage
            CPU_NODE="$2"
            shift 2
            ;;
        --cpu-list)
            [[ $# -ge 2 ]] || usage
            CPU_LIST="$2"
            shift 2
            ;;
        --mem-node|--numa-node)
            [[ $# -ge 2 ]] || usage
            MEM_NODE="$2"
            shift 2
            ;;
        --numactl-opts)
            [[ $# -ge 2 ]] || usage
            NUMACTL_OPTS="$2"
            shift 2
            ;;
        --)
            shift
            CLIENT_ARGS+=("$@")
            break
            ;;
        *)
            CLIENT_ARGS+=("$1")
            shift
            ;;
    esac
done

CMD_PREFIX=()
if [[ -n "${NUMACTL_OPTS}" ]]; then
    command -v numactl >/dev/null 2>&1 || {
        echo "ERROR: numactl is required for --numactl-opts/SCACHE_CLIENT_NUMACTL_OPTS" >&2
        exit 1
    }
    # shellcheck disable=SC2206
    CMD_PREFIX=(numactl ${NUMACTL_OPTS})
else
    NUMA_ARGS=()
    if [[ -n "${CPU_LIST}" ]]; then
        NUMA_ARGS+=(--physcpubind="${CPU_LIST}")
    elif [[ -n "${CPU_NODE}" ]]; then
        NUMA_ARGS+=(--cpunodebind="${CPU_NODE}")
    fi
    if [[ -n "${MEM_NODE}" ]]; then
        NUMA_ARGS+=(--membind="${MEM_NODE}")
    fi
    if [[ ${#NUMA_ARGS[@]} -gt 0 ]]; then
        command -v numactl >/dev/null 2>&1 || {
            echo "ERROR: numactl is required for --cpu-node/--mem-node" >&2
            exit 1
        }
        CMD_PREFIX=(numactl "${NUMA_ARGS[@]}")
    fi
fi

nohup "${CMD_PREFIX[@]}" java $SCACHE_JAVA_OPTS -cp "$JAR" "$MAIN_CLASS" "${CLIENT_ARGS[@]}" >>"$LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"
echo "ScacheClient started (pid=$(cat "$PID_FILE"))"
