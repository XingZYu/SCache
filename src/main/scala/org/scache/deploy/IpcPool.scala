package org.scache.deploy

import java.io.File
import java.nio.ByteBuffer
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.StandardOpenOption
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

private[deploy] final class MmapPoolFile private(
    val path: String,
    val sizeBytes: Long,
    private val mapChunkBytes: Int,
    private val segments: Array[MappedByteBuffer]) {

  private def segmentIndex(offset: Long): Int = (offset / mapChunkBytes).toInt

  def slice(offset: Long, length: Int): ByteBuffer = {
    if (length < 0) throw new IllegalArgumentException(s"length must be >= 0, got $length")
    if (offset < 0) throw new IllegalArgumentException(s"offset must be >= 0, got $offset")
    if (offset + length > sizeBytes) {
      throw new IllegalArgumentException(
        s"slice out of bounds: offset=$offset length=$length poolSize=$sizeBytes")
    }
    if (length == 0) return ByteBuffer.allocate(0)

    val idx = segmentIndex(offset)
    val segOffset = (offset % mapChunkBytes).toInt
    val seg = segments(idx)
    if (segOffset + length > seg.capacity()) {
      throw new IllegalArgumentException(
        s"slice crosses segment boundary: offset=$offset length=$length segCapacity=${seg.capacity()} mapChunkBytes=$mapChunkBytes")
    }

    val dup = seg.duplicate()
    dup.position(segOffset)
    dup.limit(segOffset + length)
    dup.slice()
  }

  def write(offset: Long, data: Array[Byte], length: Int): Unit = {
    require(data != null, "data must not be null")
    require(length >= 0, s"length must be >= 0, got $length")
    require(length <= data.length, s"length ($length) must be <= data.length (${data.length})")
    if (length == 0) return
    val buf = slice(offset, length)
    buf.put(data, 0, length)
  }
}

private[deploy] object MmapPoolFile {
  private def mapAll(
      channel: FileChannel,
      sizeBytes: Long,
      mapChunkBytes: Int): Array[MappedByteBuffer] = {
    require(sizeBytes > 0, s"pool size must be > 0, got $sizeBytes")
    require(mapChunkBytes > 0, s"mapChunkBytes must be > 0, got $mapChunkBytes")

    val segments = new ArrayBuffer[MappedByteBuffer]()
    var offset = 0L
    while (offset < sizeBytes) {
      val remaining = sizeBytes - offset
      val len = Math.min(mapChunkBytes.toLong, remaining).toInt
      segments += channel.map(MapMode.READ_WRITE, offset, len)
      offset += len
    }
    segments.toArray
  }

  def open(path: String, mapChunkBytes: Int): MmapPoolFile = {
    val f = new File(path)
    if (!f.exists()) {
      throw new IllegalStateException(s"IPC pool file does not exist: $path")
    }
    val channel = FileChannel.open(f.toPath, StandardOpenOption.READ, StandardOpenOption.WRITE)
    try {
      val sizeBytes = channel.size()
      val segments = mapAll(channel, sizeBytes, mapChunkBytes)
      new MmapPoolFile(path, sizeBytes, mapChunkBytes, segments)
    } finally {
      channel.close()
    }
  }

  def createOrOpen(
      path: String,
      desiredSizeBytes: Long,
      mapChunkBytes: Int): MmapPoolFile = {
    require(desiredSizeBytes > 0, s"desiredSizeBytes must be > 0, got $desiredSizeBytes")
    val f = new File(path)
    val parent = f.getParentFile
    if (parent != null && !parent.exists() && !parent.mkdirs()) {
      throw new IllegalStateException(s"Failed to create IPC pool directory: ${parent.getAbsolutePath}")
    }

    val channel = FileChannel.open(
      f.toPath,
      StandardOpenOption.READ,
      StandardOpenOption.WRITE,
      StandardOpenOption.CREATE)
    try {
      val currentSize = channel.size()
      val targetSize = Math.max(currentSize, desiredSizeBytes)
      if (currentSize < targetSize) {
        channel.position(targetSize - 1)
        channel.write(ByteBuffer.wrap(Array[Byte](0)))
      }
      val sizeBytes = channel.size()
      val segments = mapAll(channel, sizeBytes, mapChunkBytes)
      new MmapPoolFile(path, sizeBytes, mapChunkBytes, segments)
    } finally {
      channel.close()
    }
  }
}

private[deploy] final class PoolAllocator(
    poolSizeBytes: Long,
    chunkSizeBytes: Long,
    alignBytes: Int) {

  require(poolSizeBytes > 0, s"poolSizeBytes must be > 0, got $poolSizeBytes")
  require(chunkSizeBytes > 0, s"chunkSizeBytes must be > 0, got $chunkSizeBytes")
  require(alignBytes > 0, s"alignBytes must be > 0, got $alignBytes")

  private val free = new util.TreeMap[java.lang.Long, java.lang.Long]()
  private var nextOffset = 0L

  private def alignUp(value: Long): Long = {
    val mask = alignBytes.toLong - 1
    (value + mask) & ~mask
  }

  private def chunkEnd(offset: Long): Long = {
    val start = (offset / chunkSizeBytes) * chunkSizeBytes
    Math.min(start + chunkSizeBytes, poolSizeBytes)
  }

  private def addFree(offset: Long, length: Long): Unit = {
    if (length <= 0) return
    insertFree(offset, length)
  }

  def allocate(size: Int): Option[Long] = synchronized {
    if (size < 0) return None
    if (size == 0) return Some(0L)
    if (size.toLong > chunkSizeBytes) return None

    allocateFromFree(size) match {
      case some @ Some(_) => return some
      case None =>
    }

    var off = nextOffset

    val aligned = alignUp(off)
    if (aligned > off) addFree(off, aligned - off)
    off = aligned

    while (off < poolSizeBytes && off + size > chunkEnd(off)) {
      val end = chunkEnd(off)
      addFree(off, end - off)
      off = alignUp(end)
    }

    if (off + size <= poolSizeBytes) {
      nextOffset = off + size
      return Some(off)
    }

    // nextOffset is a high-water mark, not a circular cursor.  Once the virgin portion of the
    // pool is exhausted, only ranges explicitly returned through free() may be reused.  Resetting
    // nextOffset to zero here used to allocate over live slices after the first wrap, leaving the
    // block metadata valid while silently corrupting the bytes underneath it.
    if (off < poolSizeBytes) {
      addFree(off, poolSizeBytes - off)
    }
    allocateFromFree(size)
  }

  private def allocateFromFree(size: Int): Option[Long] = {
    var entry = free.firstEntry()
    while (entry != null) {
      val segOffset = entry.getKey.longValue()
      val segLen = entry.getValue.longValue()
      val segEnd = segOffset + segLen

      val candidate1 = alignUp(segOffset)
      val candidate = if (candidate1 + size <= segEnd && candidate1 + size <= chunkEnd(candidate1)) {
        candidate1
      } else {
        val candidate2 = alignUp(chunkEnd(candidate1))
        if (candidate2 + size <= segEnd && candidate2 + size <= chunkEnd(candidate2)) candidate2 else -1L
      }

      if (candidate >= 0) {
        free.remove(entry.getKey)
        if (candidate > segOffset) {
          free.put(segOffset, candidate - segOffset)
        }
        val allocatedEnd = candidate + size
        if (allocatedEnd < segEnd) {
          free.put(allocatedEnd, segEnd - allocatedEnd)
        }
        return Some(candidate)
      }

      entry = free.higherEntry(entry.getKey)
    }
    None
  }

  def free(offset: Long, size: Int): Unit = synchronized {
    insertFree(offset, size.toLong)
  }

  private def insertFree(offset: Long, length: Long): Unit = {
    if (length <= 0) return
    if (offset < 0 || offset + length > poolSizeBytes) {
      throw new IllegalArgumentException(
        s"free out of bounds: offset=$offset size=$length poolSize=$poolSizeBytes")
    }

    var start = offset
    var end = offset + length

    val prev = free.floorEntry(start)
    if (prev != null) {
      val prevStart = prev.getKey.longValue()
      val prevEnd = prevStart + prev.getValue.longValue()
      if (prevEnd >= start) {
        start = prevStart
        end = Math.max(end, prevEnd)
        free.remove(prev.getKey)
      }
    }

    var next = free.ceilingEntry(start)
    while (next != null) {
      val nextStart = next.getKey.longValue()
      val nextEnd = nextStart + next.getValue.longValue()
      if (nextStart <= end) {
        end = Math.max(end, nextEnd)
        free.remove(next.getKey)
        next = free.ceilingEntry(start)
      } else {
        next = null
      }
    }

    free.put(start, end - start)
  }

  def statsString: String = synchronized {
    val freeBytes = free.values().asScala.map(_.longValue()).sum
    s"PoolAllocator(nextOffset=$nextOffset freeBytes=$freeBytes freeSegments=${free.size()})"
  }
}
