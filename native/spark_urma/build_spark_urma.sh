#!/usr/bin/env bash
# Reproducible Release build for the production JNI library.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
URMA_ROOT="${OU_UMDK_BUILD:?OU_UMDK_BUILD must point to the built UMDK tree}"
BUILD_DIR="${SPARK_URMA_BUILD_DIR:-$HERE/build}"
INSTALL_PREFIX="${SPARK_URMA_INSTALL_PREFIX:-$BUILD_DIR/install}"
JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
JOBS="${SPARK_URMA_BUILD_JOBS:-2}"

cmake -S "$HERE" -B "$BUILD_DIR" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="$INSTALL_PREFIX" \
  -DURMA_ROOT="$URMA_ROOT" \
  -DBUILD_TESTS=OFF \
  -DBUILD_STANDALONE=OFF \
  -DJAVA_HOME="$JAVA_HOME"
cmake --build "$BUILD_DIR" --target SparkUrma --parallel "$JOBS"
cmake --install "$BUILD_DIR"

LIBRARY="$INSTALL_PREFIX/lib/libSparkUrma.so"
[[ -f "$LIBRARY" ]] || { echo "missing installed library: $LIBRARY" >&2; exit 1; }
if readelf -d "$LIBRARY" | grep -Eq 'RPATH|RUNPATH'; then
  echo "unexpected RPATH/RUNPATH in $LIBRARY" >&2
  exit 1
fi
sha256sum "$LIBRARY"
