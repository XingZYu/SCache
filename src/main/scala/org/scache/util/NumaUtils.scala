package org.scache.util

import java.nio.ByteBuffer

import sun.nio.ch.DirectBuffer

/**
 * Helpers for binding off-heap allocations to a NUMA node (CXL simulation).
 *
 * Requires the optional JNI library `libscache_numa.so` to be available in `java.library.path`
 * (or `-Dscache.numa.library.path=/absolute/path/to/libscache_numa.so`).
 */
private[scache] object NumaUtils extends Logging {
  @volatile private var warnedLoadFailure = false

  def bindOffHeapIfEnabled(buffer: ByteBuffer, conf: ScacheConf): Unit = {
    if (buffer == null || !buffer.isDirect) return

    val node = conf.getInt("scache.memory.offHeap.numaNode", -1)
    if (node < 0) return

    NumaNative.ensureLoaded()
    if (!NumaNative.isLoaded()) {
      if (!warnedLoadFailure) {
        warnedLoadFailure = true
        val err = NumaNative.loadError()
        val msg = if (err == null) "unknown error" else s"${err.getClass.getName}: ${err.getMessage}"
        logWarning(s"NUMA binding requested (scache.memory.offHeap.numaNode=$node) but native library failed to load: $msg")
      }
      return
    }

    val address = buffer.asInstanceOf[DirectBuffer].address()
    val length = buffer.capacity().toLong
    val rc = NumaNative.mbind(address, length, node)
    if (rc != 0) {
      logWarning(s"mbind(addr=0x${java.lang.Long.toHexString(address)}, len=$length, node=$node) failed: rc=$rc")
    }
  }
}

