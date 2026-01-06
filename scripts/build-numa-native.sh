#!/usr/bin/env bash

set -euo pipefail

sbin="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
scache_home="$(cd "${sbin}/.." && pwd)"

out_dir="${scache_home}/native"
src="${out_dir}/scache_numa.c"
out="${out_dir}/libscache_numa.so"

if [[ ! -f "${src}" ]]; then
  echo "ERROR: source not found: ${src}" >&2
  exit 1
fi

java_home="${JAVA_HOME:-}"
if [[ -z "${java_home}" ]]; then
  javac_path="$(command -v javac || true)"
  if [[ -n "${javac_path}" ]]; then
    java_home="$(cd "$(dirname "${javac_path}")/.." && pwd)"
  fi
fi

if [[ -z "${java_home}" || ! -d "${java_home}/include" ]]; then
  echo "ERROR: JAVA_HOME not set and could not infer JDK include paths." >&2
  exit 1
fi

mkdir -p "${out_dir}"

echo "Building ${out}"
gcc -shared -fPIC -O2 \
  -I"${java_home}/include" \
  -I"${java_home}/include/linux" \
  -o "${out}" \
  "${src}"

echo "Built ${out}"
echo
echo "To enable NUMA binding:"
echo "  export SCACHE_JAVA_OPTS=\"\${SCACHE_JAVA_OPTS:-} -Djava.library.path=${out_dir}\""
echo "  # and set in conf/scache.conf:"
echo "  # scache.memory.offHeap.numaNode=1"

