#!/usr/bin/env bash
# Clean production + integration artifact build with package-content gates.
set -euo pipefail

SCACHE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_ROOT="${SCACHE_RELEASE_BUILD_ROOT:-$(mktemp -d /tmp/scache_release.XXXXXX)}"
INSTALL_DIR="${SCACHE_RELEASE_INSTALL_DIR:-$SCACHE_ROOT/target/release}"
JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
SBT_BIN="${SBT_BIN:-sbt}"
SBT_LAUNCH_JAR="${SBT_LAUNCH_JAR:-}"
TARGET_DIR="$BUILD_ROOT/target"
SCALA_DIR="$TARGET_DIR/scala-2.13"
ASSEMBLY="$SCALA_DIR/SCache-assembly-0.1.0-SNAPSHOT.jar"
INTEGRATION="$SCALA_DIR/SCache-integration-0.1.0-SNAPSHOT.jar"

mkdir -p "$BUILD_ROOT" "$INSTALL_DIR"
cd "$SCACHE_ROOT"
if [[ -n "$SBT_LAUNCH_JAR" ]]; then
  [[ -f "$SBT_LAUNCH_JAR" ]] || {
    echo "missing SBT_LAUNCH_JAR: $SBT_LAUNCH_JAR" >&2
    exit 1
  }
  SBT_COMMAND=("$JAVA_HOME/bin/java" -jar "$SBT_LAUNCH_JAR")
  SBT_OPTIONS=()
else
  SBT_COMMAND=("$SBT_BIN")
  SBT_OPTIONS=(-batch)
fi
"${SBT_COMMAND[@]}" "${SBT_OPTIONS[@]}" \
  "set target := file(\"$TARGET_DIR\")" \
  clean assembly "Integration / packageBin"

[[ -f "$ASSEMBLY" && -f "$INTEGRATION" ]]
FORBIDDEN='org/scache/(deploy/(DaemonClientIpcIntegrationTest|DaemonRpcLifecycleIntegrationNode|NettyBlockTransferIntegrationNode|PoolIpcSelfTest)|network/ub/(SCacheUrmaTest|UbPhase4ContractTest|UbPhase4LifecycleTest|UbPhase4ServiceInitTest|UrmaClient|UrmaLocalContractTest|UrmaRegistrationLifecycleTest|UrmaRuntimeLifecycleTest|UrmaServer)).*class$'
if "$JAVA_HOME/bin/jar" tf "$ASSEMBLY" | grep -Eq "$FORBIDDEN"; then
  echo "production assembly contains integration/diagnostic entry classes" >&2
  exit 1
fi
"$JAVA_HOME/bin/jar" tf "$INTEGRATION" | grep -q 'org/scache/deploy/NettyBlockTransferIntegrationNode.class'

cp "$ASSEMBLY" "$INSTALL_DIR/"
cp "$INTEGRATION" "$INSTALL_DIR/"
sha256sum "$INSTALL_DIR/$(basename "$ASSEMBLY")" \
  "$INSTALL_DIR/$(basename "$INTEGRATION")" | tee "$INSTALL_DIR/SHA256SUMS"
printf 'build_root=%s\ninstall_dir=%s\n' "$BUILD_ROOT" "$INSTALL_DIR"
