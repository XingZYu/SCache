package org.scache.util;

/**
 * Minimal JNI wrapper for NUMA memory policy syscalls (Linux).
 *
 * This is intentionally optional: if the native library cannot be loaded, callers should
 * gracefully fall back to default allocation behavior.
 */
public final class NumaNative {
  private static volatile boolean loadAttempted = false;
  private static volatile boolean loaded = false;
  private static volatile Throwable loadError = null;

  private NumaNative() {}

  public static synchronized void ensureLoaded() {
    if (loadAttempted) {
      return;
    }
    loadAttempted = true;
    try {
      final String explicitPath = System.getProperty("scache.numa.library.path");
      if (explicitPath != null && !explicitPath.isBlank()) {
        System.load(explicitPath);
      } else {
        System.loadLibrary("scache_numa");
      }
      loaded = true;
    } catch (Throwable t) {
      loadError = t;
      loaded = false;
    }
  }

  public static boolean isLoaded() {
    return loaded;
  }

  public static Throwable loadError() {
    return loadError;
  }

  /**
   * Bind the address range to the given NUMA node via mbind(MPOL_BIND).
   *
   * @return 0 on success, otherwise negative errno (e.g., -EINVAL, -ENOSYS).
   */
  public static native int mbind(long address, long length, int node);
}

