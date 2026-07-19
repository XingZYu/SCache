#!/usr/bin/env bash
# Java/JNI URMA test matrix — runs all mandatory operation/size combinations.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="$HERE/../build"
SCACHE_ROOT="$(cd "$HERE/../../.." && pwd)"
ASSEMBLY_JAR="$SCACHE_ROOT/target/scala-2.13/SCache-assembly-0.1.0-SNAPSHOT.jar"
INTEGRATION_JAR="$SCACHE_ROOT/target/scala-2.13/SCache-integration-0.1.0-SNAPSHOT.jar"
JAVA_CLASSES="$INTEGRATION_JAR:$ASSEMBLY_JAR"
DEVICE="${PP_DEV:-openurma0}"
SERVER_PORT=19090
TIMEOUT_SEC=120

# URMA environment
UMDK_BUILD="${OU_UMDK_BUILD:?OU_UMDK_BUILD must point to the built UMDK tree}"
TIER_S="${OU_TIER_S:?OU_TIER_S must point to the Tier-S provider artifacts}"
export OU_LIBURMA="$UMDK_BUILD/urma/lib/urma/core"
export OU_LIBCOMMON="$UMDK_BUILD/urma/common"
export OU_SHIM="$TIER_S/openurma_shim.so"
# Fix paths
OU_LIBURMA=$(readlink -f "$OU_LIBURMA" 2>/dev/null || echo "$OU_LIBURMA")
OU_LIBCOMMON=$(readlink -f "$OU_LIBCOMMON" 2>/dev/null || echo "$OU_LIBCOMMON")
export LD_LIBRARY_PATH="$OU_LIBURMA:$OU_LIBCOMMON:${LD_LIBRARY_PATH:-}"

LOG_DIR=${SCACHE_JNI_TEST_LOG_DIR:-$(mktemp -d /tmp/scache_java_jni.XXXXXX)}
mkdir -p "$LOG_DIR"

echo "=== Java/JNI URMA Test Matrix ==="
echo "Device: $DEVICE | Build: $BUILD_DIR"

RESULTS="$LOG_DIR/matrix.csv"
echo "operation,payload_bytes,chunks,iterations,submitted,completed,timeouts,chk_errors,tcp_fb,result" > "$RESULTS"

run_test() {
    local op=$1 payload=$2 iters=$3
    echo "--- $op ${payload}B x${iters} ---"

    # Start server
    OPENURMA_FAKE_ROOT=/tmp/rootA LD_PRELOAD="$OU_SHIM" java \
        -cp "$JAVA_CLASSES" -Djava.library.path="$BUILD_DIR" \
        org.scache.network.ub.UrmaServer \
        --device "$DEVICE" --control-port $SERVER_PORT --buffer-bytes 1048576 \
        > "$LOG_DIR/server_${op}_${payload}.log" 2>&1 &
    SPID=$!
    sleep 3

    if ! kill -0 $SPID 2>/dev/null; then
        echo "SERVER_FAIL"
        echo "$op,$payload,1,$iters,0,0,1,0,0,FAIL" >> "$RESULTS"
        return 1
    fi

    # Run client
    set +e
    OPENURMA_FAKE_ROOT=/tmp/rootA LD_PRELOAD="$OU_SHIM" timeout $TIMEOUT_SEC java \
        -cp "$JAVA_CLASSES" -Djava.library.path="$BUILD_DIR" \
        org.scache.network.ub.UrmaClient \
        --server 127.0.0.1:$SERVER_PORT --device "$DEVICE" \
        --operation "$op" --payload-bytes "$payload" --iterations "$iters" \
        > "$LOG_DIR/client_${op}_${payload}.log" 2>&1
    RC=$?
    set -e

    kill $SPID 2>/dev/null || true

    # Parse result
    if grep -q "result=PASS" "$LOG_DIR/client_${op}_${payload}.log" 2>/dev/null; then
        echo "PASS"
    else
        echo "FAIL (exit=$RC)"
    fi
}

# Run all required tests
for op in write read send; do
    for sz in 64 4096; do
        run_test "$op" "$sz" 20 2>&1
    done
done
for sz in 65536 1048576; do
    run_test "write" "$sz" 5 2>&1
    run_test "read" "$sz" 5 2>&1
done

echo ""
echo "=== Matrix complete ==="
echo "Logs: $LOG_DIR"
echo "Results: $RESULTS"
