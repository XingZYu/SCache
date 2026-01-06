package org.scache.io

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean

/**
 * A [[ChunkedByteBuffer]] whose backing bytes live in the daemon/client IPC pool.
 *
 * The pool slice must not be freed immediately after putting the block into the BlockManager;
 * otherwise the allocator may reuse the same region and overwrite the data. This buffer keeps
 * ownership of the slice and releases it only when [[dispose()]] is called by the MemoryStore.
 *
 * Callers should avoid returning this object directly to readers. Use [[nonOwningDuplicate()]]
 * to return a view that does not release the pool slice when disposed.
 */
private[scache] final class IpcPoolChunkedByteBuffer(
    chunks0: Array[ByteBuffer],
    onDispose: () => Unit)
  extends ChunkedByteBuffer(chunks0) {

  require(onDispose != null, "onDispose must not be null")

  private[this] val released = new AtomicBoolean(false)

  override def dispose(): Unit = {
    if (released.compareAndSet(false, true)) {
      onDispose()
    }
  }

  /**
   * Returns a view over the same bytes which does not release the underlying pool slice when
   * disposed. This is used so callers may call `dispose()` (e.g. replication code paths) without
   * invalidating the cached copy held by the MemoryStore.
   */
  def nonOwningDuplicate(): ChunkedByteBuffer = {
    val dups = this.chunks.map { b =>
      val dup = b.duplicate()
      dup.position(0)
      dup
    }
    new IpcPoolChunkedByteBuffer(dups, () => ())
  }
}
