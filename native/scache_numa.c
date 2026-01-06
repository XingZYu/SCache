#include <jni.h>

#include <errno.h>
#include <stdint.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <linux/mempolicy.h>

JNIEXPORT jint JNICALL Java_org_scache_util_NumaNative_mbind(
    JNIEnv* env, jclass clazz, jlong address, jlong length, jint node) {
  (void)env;
  (void)clazz;

  if (address == 0 || length <= 0 || node < 0) {
    return -EINVAL;
  }

  // Support NUMA node IDs in [0, BITS_PER_ULONG).
  const int bits = (int)(8 * sizeof(unsigned long));
  if (node >= bits) {
    return -EINVAL;
  }

  unsigned long nodemask[1];
  nodemask[0] = 1UL << node;

  const unsigned long maxnode = (unsigned long)bits;

  long ret = syscall(
      __NR_mbind,
      (void*)(uintptr_t)address,
      (unsigned long)length,
      MPOL_BIND,
      nodemask,
      maxnode,
      MPOL_MF_MOVE);

  if (ret != 0) {
    return -errno;
  }
  return 0;
}

