/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.scache.storage

import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.util.{HashMap => JHashMap, TreeMap => JTreeMap}

import org.scache.MapOutputTrackerMaster

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import org.scache.util.{ScacheConf, Logging, Utils, ThreadUtils}
import org.scache.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.scache.storage.BlockManagerMessages._

/**
 * BlockManagerMasterEndpoint is an [[ThreadSafeRpcEndpoint]] on the master node to track statuses
 * of all slaves' block managers.
 */
private[scache]
class BlockManagerMasterEndpoint(
    override val rpcEnv: RpcEnv,
    val isLocal: Boolean,
    val mapOutputTrackerMaster: MapOutputTrackerMaster,
    conf: ScacheConf)
  extends ThreadSafeRpcEndpoint with Logging {

  // Mapping from block manager id to the block manager's information.
  private val blockManagerInfo = new mutable.HashMap[BlockManagerId, BlockManagerInfo]

  // Mapping from executor ID to block manager ID.
  private val blockManagerIdByExecutor = new mutable.HashMap[String, BlockManagerId]

  // Mapping from block id to the set of block managers that have the block.
  private val blockLocations = new JHashMap[BlockId, mutable.HashSet[BlockManagerId]]

  // Shared CXL (fsdax) pool metadata: block -> (poolPath, offset, length).
  private val cxlSharedEnabled =
    conf.getBoolean("scache.storage.cxl.shared.enabled", false)
  private val cxlSharedPoolPath =
    conf.getString("scache.storage.cxl.shared.pool.path", "").trim
  private val cxlSharedPoolSizeBytes =
    conf.getSizeAsBytes("scache.storage.cxl.shared.pool.size", "0b")
  private val cxlSharedPoolAlignBytes =
    conf.getInt("scache.storage.cxl.shared.pool.align", 4096)

  private val cxlStateLock = new Object
  private val cxlBlocks = new mutable.HashMap[BlockId, CxlBlockLocation]()
  private val cxlAllocator: Option[CxlPoolAllocator] = {
    if (cxlSharedEnabled && cxlSharedPoolPath.nonEmpty && cxlSharedPoolSizeBytes > 0) {
      val ensuredSize = ensurePoolFileSize(cxlSharedPoolPath, cxlSharedPoolSizeBytes)
      if (ensuredSize <= 0) {
        logWarning(
          s"Shared CXL pool is enabled but has invalid size=$ensuredSize; disabling shared pool.")
        None
      } else {
        Some(new CxlPoolAllocator(ensuredSize, cxlSharedPoolAlignBytes))
      }
    } else {
      if (cxlSharedEnabled) {
        logWarning(
          "Shared CXL pool is enabled but scache.storage.cxl.shared.pool.path/size are not set; " +
            "disabling shared pool.")
      }
      None
    }
  }

  private val askThreadPool = ThreadUtils.newDaemonCachedThreadPool("block-manager-ask-thread-pool")
  private implicit val askExecutionContext: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(askThreadPool)

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterBlockManager(blockManagerId, maxMemSize, slaveEndpoint) =>
      register(blockManagerId, maxMemSize, slaveEndpoint)
      context.reply(true)

    case _updateBlockInfo @
        UpdateBlockInfo(blockManagerId, blockId, storageLevel, deserializedSize, size) =>
      context.reply(updateBlockInfo(blockManagerId, blockId, storageLevel, deserializedSize, size))
      // listenerBus.post(ScacheListenerBlockUpdated(BlockUpdatedInfo(_updateBlockInfo)))

    case GetLocations(blockId) =>
      context.reply(getLocations(blockId))

    case GetLocationsMultipleBlockIds(blockIds) =>
      context.reply(getLocationsMultipleBlockIds(blockIds))

    case GetPeers(blockManagerId) =>
      context.reply(getPeers(blockManagerId))

    case GetExecutorEndpointRef(executorId) =>
      context.reply(getExecutorEndpointRef(executorId))

    case GetBlockManagerId(executorId) =>
      context.reply(getBlockManagerId(executorId))

    case GetMemoryStatus =>
      context.reply(memoryStatus)

    case GetStorageStatus =>
      context.reply(storageStatus)

    case GetBlockStatus(blockId, askSlaves) =>
      context.reply(blockStatus(blockId, askSlaves))

    case GetMatchingBlockIds(filter, askSlaves) =>
      context.reply(getMatchingBlockIds(filter, askSlaves))

    case AllocateCxlBlock(length) =>
      context.reply(allocateCxlBlock(length))

    case RegisterCxlBlock(blockId, location) =>
      context.reply(registerCxlBlock(blockId, location))

    case RegisterCxlBlocks(blockIds, locations) =>
      context.reply(registerCxlBlocks(blockIds, locations))

    case GetCxlBlock(blockId) =>
      context.reply(getCxlBlock(blockId))

    case GetCxlBlocks(blockIds) =>
      context.reply(getCxlBlocks(blockIds))

    case ReleaseCxlBlock(blockId) =>
      context.reply(releaseCxlBlock(blockId))

    case RemoveRdd(rddId) =>
      context.reply(removeRdd(rddId))

    case RemoveShuffle(shuffleId) =>
      context.reply(removeShuffle(shuffleId))

    case RemoveBroadcast(broadcastId, removeFromDriver) =>
      context.reply(removeBroadcast(broadcastId, removeFromDriver))

    case RemoveBlock(blockId) =>
      releaseCxlBlock(blockId)
      removeBlockFromWorkers(blockId)
      context.reply(true)

    case RemoveExecutor(execId) =>
      removeExecutor(execId)
      context.reply(true)

    case StopBlockManagerMaster =>
      context.reply(true)
      stop()

    case BlockManagerHeartbeat(blockManagerId) =>
      context.reply(heartbeatReceived(blockManagerId))

    case HasCachedBlocks(executorId) =>
      blockManagerIdByExecutor.get(executorId) match {
        case Some(bm) =>
          if (blockManagerInfo.contains(bm)) {
            val bmInfo = blockManagerInfo(bm)
            context.reply(bmInfo.cachedBlocks.nonEmpty)
          } else {
            context.reply(false)
          }
        case None => context.reply(false)
      }
  }

  private def allocateCxlBlock(length: Int): Option[CxlBlockLocation] = {
    if (length < 0) return None
    cxlAllocator match {
      case Some(allocator) =>
        allocator.allocate(length).map { offset =>
          CxlBlockLocation(cxlSharedPoolPath, offset, length)
        }
      case None =>
        None
    }
  }

  private def registerCxlBlock(blockId: BlockId, location: CxlBlockLocation): Boolean = {
    if (blockId == null || location == null) return false
    if (location.length < 0 || location.offset < 0L) return false
    if (!cxlSharedEnabled) return false
    if (cxlSharedPoolPath.isEmpty) return false
    if (location.poolPath != cxlSharedPoolPath) return false
    cxlStateLock.synchronized {
      cxlBlocks.put(blockId, location)
    }
    true
  }

  private def registerCxlBlocks(
      blockIds: Array[BlockId],
      locations: Array[CxlBlockLocation]): Boolean = {
    if (blockIds == null || locations == null || blockIds.length != locations.length) return false
    if (!cxlSharedEnabled || cxlSharedPoolPath.isEmpty) return false
    var i = 0
    while (i < blockIds.length) {
      val blockId = blockIds(i)
      val location = locations(i)
      if (blockId == null || location == null || location.length < 0 ||
          location.offset < 0L || location.poolPath != cxlSharedPoolPath) {
        return false
      }
      i += 1
    }
    cxlStateLock.synchronized {
      i = 0
      while (i < blockIds.length) {
        cxlBlocks.put(blockIds(i), locations(i))
        i += 1
      }
    }
    true
  }

  private def getCxlBlock(blockId: BlockId): Option[CxlBlockLocation] = {
    if (blockId == null) return None
    cxlStateLock.synchronized {
      cxlBlocks.get(blockId)
    }
  }

  private def getCxlBlocks(blockIds: Array[BlockId]): Array[Option[CxlBlockLocation]] = {
    if (blockIds == null) return Array.empty
    cxlStateLock.synchronized {
      blockIds.map(cxlBlocks.get)
    }
  }

  private def releaseCxlBlock(blockId: BlockId): Boolean = {
    if (blockId == null) return false
    val removed = cxlStateLock.synchronized {
      cxlBlocks.remove(blockId)
    }
    (removed, cxlAllocator) match {
      case (Some(loc), Some(allocator)) =>
        allocator.free(loc.offset, loc.length)
        true
      case (Some(_), None) =>
        true
      case (None, _) =>
        false
    }
  }

  private def ensurePoolFileSize(path: String, desiredSizeBytes: Long): Long = {
    if (path == null || path.trim.isEmpty) return 0L
    if (desiredSizeBytes <= 0) return 0L

    val f = new File(path)
    val parent = f.getParentFile
    if (parent != null && !parent.exists() && !parent.mkdirs()) {
      logWarning(s"Failed to create shared CXL pool directory: ${parent.getAbsolutePath}")
      return 0L
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
      channel.size()
    } catch {
      case e: Exception =>
        logWarning(s"Failed to create/resize shared CXL pool file: $path", e)
        0L
    } finally {
      channel.close()
    }
  }

  private def removeRdd(rddId: Int): Future[Seq[Int]] = {
    // First remove the metadata for the given RDD, and then asynchronously remove the blocks
    // from the slaves.

    // Find all blocks for the given RDD, remove the block from both blockLocations and
    // the blockManagerInfo that is tracking the blocks.
    val blocks = blockLocations.asScala.keys.flatMap(_.asRDDId).filter(_.rddId == rddId)
    blocks.foreach { blockId =>
      val bms: mutable.HashSet[BlockManagerId] = blockLocations.get(blockId)
      bms.foreach(bm => blockManagerInfo.get(bm).foreach(_.removeBlock(blockId)))
      blockLocations.remove(blockId)
    }

    // Ask the slaves to remove the RDD, and put the result in a sequence of Futures.
    // The dispatcher is used as an implicit argument into the Future sequence construction.
    val removeMsg = RemoveRdd(rddId)
    Future.sequence(
      blockManagerInfo.values.map { bm =>
        bm.slaveEndpoint.ask[Int](removeMsg)
      }.toSeq
    )
  }

  private def removeShuffle(shuffleId: Int): Future[Seq[Boolean]] = {
    releaseCxlBlocksForShuffle(shuffleId)
    val removeMsg = RemoveShuffle(shuffleId)
    Future.sequence(
      blockManagerInfo.values.map { bm =>
        bm.slaveEndpoint.ask[Boolean](removeMsg)
      }.toSeq
    )
  }

  private def releaseCxlBlocksForShuffle(shuffleId: Int): Unit = {
    if (!cxlSharedEnabled) return
    val removedLocations = cxlStateLock.synchronized {
      val matches = cxlBlocks.collect {
        case (bid: ScacheBlockId, loc) if bid.shuffleId == shuffleId => (bid, loc)
      }.toArray
      matches.foreach { case (bid, _) => cxlBlocks.remove(bid) }
      matches.map(_._2).toSeq
    }
    cxlAllocator.foreach { allocator =>
      removedLocations.foreach(loc => allocator.free(loc.offset, loc.length))
    }
  }

  /**
   * Delegate RemoveBroadcast messages to each BlockManager because the master may not notified
   * of all broadcast blocks. If removeFromDriver is false, broadcast blocks are only removed
   * from the executors, but not from the driver.
   */
  private def removeBroadcast(broadcastId: Long, removeFromDriver: Boolean): Future[Seq[Int]] = {
    val removeMsg = RemoveBroadcast(broadcastId, removeFromDriver)
    val requiredBlockManagers = blockManagerInfo.values.filter { info =>
      removeFromDriver || !info.blockManagerId.isDriver
    }
    Future.sequence(
      requiredBlockManagers.map { bm =>
        bm.slaveEndpoint.ask[Int](removeMsg)
      }.toSeq
    )
  }

  private def removeBlockManager(blockManagerId: BlockManagerId) {
    val info = blockManagerInfo(blockManagerId)

    // Remove the block manager from blockManagerIdByExecutor.
    blockManagerIdByExecutor -= blockManagerId.executorId

    // Remove it from blockManagerInfo and remove all the blocks.
    blockManagerInfo.remove(blockManagerId)
    val iterator = info.blocks.keySet.iterator
    while (iterator.hasNext) {
      val blockId = iterator.next
      val locations = blockLocations.get(blockId)
      locations -= blockManagerId
      if (locations.isEmpty) {
        blockLocations.remove(blockId)
      }
    }
    // listenerBus.post(ScacheListenerBlockManagerRemoved(System.currentTimeMillis(), blockManagerId))
    logInfo(s"Removing block manager $blockManagerId")
  }

  private def removeExecutor(execId: String) {
    logInfo("Trying to remove executor " + execId + " from BlockManagerMaster.")
    blockManagerIdByExecutor.get(execId).foreach(removeBlockManager)
  }

  /**
   * Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   */
  private def heartbeatReceived(blockManagerId: BlockManagerId): Boolean = {
    if (!blockManagerInfo.contains(blockManagerId)) {
      blockManagerId.isDriver && !isLocal
    } else {
      blockManagerInfo(blockManagerId).updateLastSeenMs()
      true
    }
  }

  // Remove a block from the slaves that have it. This can only be used to remove
  // blocks that the master knows about.
  private def removeBlockFromWorkers(blockId: BlockId) {
    val locations = blockLocations.get(blockId)
    if (locations != null) {
      locations.foreach { blockManagerId: BlockManagerId =>
        val blockManager = blockManagerInfo.get(blockManagerId)
        if (blockManager.isDefined) {
          // Remove the block from the slave's BlockManager.
          // Doesn't actually wait for a confirmation and the message might get lost.
          // If message loss becomes frequent, we should add retry logic here.
          blockManager.get.slaveEndpoint.ask[Boolean](RemoveBlock(blockId))
        }
      }
    }
  }

  // Return a map from the block manager id to max memory and remaining memory.
  private def memoryStatus: Map[BlockManagerId, (Long, Long)] = {
    blockManagerInfo.map { case(blockManagerId, info) =>
      (blockManagerId, (info.maxMem, info.remainingMem))
    }.toMap
  }

  private def storageStatus: Array[StorageStatus] = {
    blockManagerInfo.map { case (blockManagerId, info) =>
      new StorageStatus(blockManagerId, info.maxMem, info.blocks.asScala)
    }.toArray
  }

  /**
   * Return the block's status for all block managers, if any. NOTE: This is a
   * potentially expensive operation and should only be used for testing.
   *
   * If askSlaves is true, the master queries each block manager for the most updated block
   * statuses. This is useful when the master is not informed of the given block by all block
   * managers.
   */
  private def blockStatus(
      blockId: BlockId,
      askSlaves: Boolean): Map[BlockManagerId, Future[Option[BlockStatus]]] = {
    val getBlockStatus = GetBlockStatus(blockId)
    /*
     * Rather than blocking on the block status query, master endpoint should simply return
     * Futures to avoid potential deadlocks. This can arise if there exists a block manager
     * that is also waiting for this master endpoint's response to a previous message.
     */
    blockManagerInfo.values.map { info =>
      val blockStatusFuture =
        if (askSlaves) {
          info.slaveEndpoint.ask[Option[BlockStatus]](getBlockStatus)
        } else {
          Future { info.getStatus(blockId) }
        }
      (info.blockManagerId, blockStatusFuture)
    }.toMap
  }

  /**
   * Return the ids of blocks present in all the block managers that match the given filter.
   * NOTE: This is a potentially expensive operation and should only be used for testing.
   *
   * If askSlaves is true, the master queries each block manager for the most updated block
   * statuses. This is useful when the master is not informed of the given block by all block
   * managers.
   */
  private def getMatchingBlockIds(
      filter: BlockId => Boolean,
      askSlaves: Boolean): Future[Seq[BlockId]] = {
    val getMatchingBlockIds = GetMatchingBlockIds(filter)
    Future.sequence(
      blockManagerInfo.values.map { info =>
        val future =
          if (askSlaves) {
            info.slaveEndpoint.ask[Seq[BlockId]](getMatchingBlockIds)
          } else {
            Future { info.blocks.asScala.keys.filter(filter).toSeq }
          }
        future
      }
    ).map(_.flatten.toSeq)
  }

  private def register(id: BlockManagerId, maxMemSize: Long, slaveEndpoint: RpcEndpointRef) {
    val time = System.currentTimeMillis()
    if (!blockManagerInfo.contains(id)) {
      blockManagerIdByExecutor.get(id.executorId) match {
        case Some(oldId) =>
          // A block manager of the same executor already exists, so remove it (assumed dead)
          logError("Got two different block manager registrations on same executor - "
              + s" will replace old one $oldId with new one $id")
          removeExecutor(id.executorId)
        case None =>
      }
      logInfo("Registering block manager %s with %s RAM, %s".format(
        id.hostPort, Utils.bytesToString(maxMemSize), id))

      blockManagerIdByExecutor(id.executorId) = id

      blockManagerInfo(id) = new BlockManagerInfo(
        id, System.currentTimeMillis(), maxMemSize, slaveEndpoint)
    }
    // listenerBus.post(ScacheListenerBlockManagerAdded(time, id, maxMemSize))
  }

  private def updateBlockInfo(
      blockManagerId: BlockManagerId,
      blockId: BlockId,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long): Boolean = {

    if (!blockManagerInfo.contains(blockManagerId)) {
      if (blockManagerId.isDriver && !isLocal) {
        // We intentionally do not register the master (except in local mode),
        // so we should not indicate failure.
        return true
      } else {
        return false
      }
    }

    if (blockId == null) {
      blockManagerInfo(blockManagerId).updateLastSeenMs()
      return true
    }

    blockManagerInfo(blockManagerId).updateBlockInfo(blockId, storageLevel, memSize, diskSize)

    var locations: mutable.HashSet[BlockManagerId] = null
    if (blockLocations.containsKey(blockId)) {
      locations = blockLocations.get(blockId)
    } else {
      locations = new mutable.HashSet[BlockManagerId]
      blockLocations.put(blockId, locations)
      // update block status in mapoutputtracker, it may trigger the map pre-fetch
      if (mapOutputTrackerMaster.updateMapBlocksStatus(blockId) == 0) {
        val sbId = blockId.asInstanceOf[ScacheBlockId]
        logInfo(s"Start map fetch notification for ${sbId.app}_${sbId.shuffleId}_${sbId.mapId}")
        Future {
          for (info <- blockManagerInfo.values) {
            val res = info.slaveEndpoint.askWithRetry[Boolean](StartMapFetch(blockManagerId, sbId.app, sbId.jobId, sbId.shuffleId, sbId.mapId))
            if (!res) {
              logError(s"Start map fetch notification failed on ${info.blockManagerId.host}")
            }
          }
        }
      }
    }

    if (storageLevel.isValid) {
      locations.add(blockManagerId)
    } else {
      locations.remove(blockManagerId)
    }

    // Remove the block from master tracking if it has been removed on all slaves.
    if (locations.size == 0) {
      blockLocations.remove(blockId)
    }
    true
  }

  private def getLocations(blockId: BlockId): Seq[BlockManagerId] = {
    if (blockLocations.containsKey(blockId)) blockLocations.get(blockId).toSeq else Seq.empty
  }

  private def getLocationsMultipleBlockIds(
      blockIds: Array[BlockId]): IndexedSeq[Seq[BlockManagerId]] = {
    blockIds.map(blockId => getLocations(blockId))
  }

  /** Get the list of the peers of the given block manager */
  private def getPeers(blockManagerId: BlockManagerId): Seq[BlockManagerId] = {
    val blockManagerIds = blockManagerInfo.keySet
    if (blockManagerIds.contains(blockManagerId)) {
      blockManagerIds.filterNot { _.isDriver }.filterNot { _ == blockManagerId }.toSeq
    } else {
      Seq.empty
    }
  }

  /**
   * Returns an [[RpcEndpointRef]] of the [[BlockManagerSlaveEndpoint]] for sending RPC messages.
   */
  private def getExecutorEndpointRef(executorId: String): Option[RpcEndpointRef] = {
    for (
      blockManagerId <- blockManagerIdByExecutor.get(executorId);
      info <- blockManagerInfo.get(blockManagerId)
    ) yield {
      info.slaveEndpoint
    }
  }

  private def getBlockManagerId(executorId: String): Option[BlockManagerId] = {
    blockManagerIdByExecutor.get(executorId)
  }

  override def onStop(): Unit = {
    askThreadPool.shutdownNow()
  }
}

/**
 * Simple master-managed allocator for a single shared file-backed pool.
 *
 * This is intentionally lightweight: it allocates contiguous (offset, length) ranges with
 * alignment and supports best-effort free with coalescing.
 */
private final class CxlPoolAllocator(poolSizeBytes: Long, alignBytes: Int) {
  require(poolSizeBytes > 0, s"poolSizeBytes must be > 0, got $poolSizeBytes")
  require(alignBytes > 0, s"alignBytes must be > 0, got $alignBytes")

  private val free = new JTreeMap[java.lang.Long, java.lang.Long]()
  private var nextOffset = 0L

  private def alignUp(value: Long): Long = {
    val a = alignBytes.toLong
    if (a <= 1) return value
    ((value + a - 1) / a) * a
  }

  def allocate(size: Int): Option[Long] = synchronized {
    if (size < 0) return None
    if (size == 0) return Some(0L)
    if (size.toLong > poolSizeBytes) return None

    allocateFromFree(size) match {
      case some @ Some(_) => return some
      case None =>
    }

    val originalNext = nextOffset
    var off = nextOffset

    val aligned = alignUp(off)
    if (aligned > off) insertFree(off, aligned - off)
    off = aligned

    if (off + size <= poolSizeBytes) {
      nextOffset = off + size
      return Some(off)
    }

    // Wrap-around: add remaining tail as free and retry from free list.
    if (originalNext < poolSizeBytes) {
      insertFree(originalNext, poolSizeBytes - originalNext)
    }
    nextOffset = 0L
    allocateFromFree(size)
  }

  private def allocateFromFree(size: Int): Option[Long] = {
    var entry = free.firstEntry()
    while (entry != null) {
      val segOffset = entry.getKey.longValue()
      val segLen = entry.getValue.longValue()
      val segEnd = segOffset + segLen

      val candidate = alignUp(segOffset)
      if (candidate >= segOffset && candidate + size <= segEnd) {
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
    if (offset < 0 || offset + length > poolSizeBytes) return

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
    while (next != null && next.getKey.longValue() <= end) {
      val nextStart = next.getKey.longValue()
      val nextEnd = nextStart + next.getValue.longValue()
      end = Math.max(end, nextEnd)
      free.remove(next.getKey)
      next = free.ceilingEntry(start)
    }

    free.put(start, end - start)
  }
}

case class BlockStatus(storageLevel: StorageLevel, memSize: Long, diskSize: Long) {
  def isCached: Boolean = memSize + diskSize > 0
}

object BlockStatus {
  def empty: BlockStatus = BlockStatus(StorageLevel.NONE, memSize = 0L, diskSize = 0L)
}

private[scache] class BlockManagerInfo(
    val blockManagerId: BlockManagerId,
    timeMs: Long,
    val maxMem: Long,
    val slaveEndpoint: RpcEndpointRef)
  extends Logging {

  private var _lastSeenMs: Long = timeMs
  private var _remainingMem: Long = maxMem

  // Mapping from block id to its status.
  private val _blocks = new JHashMap[BlockId, BlockStatus]

  // Cached blocks held by this BlockManager. This does not include broadcast blocks.
  private val _cachedBlocks = new mutable.HashSet[BlockId]

  def getStatus(blockId: BlockId): Option[BlockStatus] = Option(_blocks.get(blockId))

  def updateLastSeenMs() {
    _lastSeenMs = System.currentTimeMillis()
  }

  def updateBlockInfo(
      blockId: BlockId,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long) {

    updateLastSeenMs()

    if (_blocks.containsKey(blockId)) {
      // The block exists on the slave already.
      val blockStatus: BlockStatus = _blocks.get(blockId)
      val originalLevel: StorageLevel = blockStatus.storageLevel
      val originalMemSize: Long = blockStatus.memSize

      if (originalLevel.useMemory) {
        _remainingMem += originalMemSize
      }
    }

    if (storageLevel.isValid) {
      /* isValid means it is either stored in-memory or on-disk.
       * The memSize here indicates the data size in or dropped from memory,
       * externalBlockStoreSize here indicates the data size in or dropped from externalBlockStore,
       * and the diskSize here indicates the data size in or dropped to disk.
       * They can be both larger than 0, when a block is dropped from memory to disk.
       * Therefore, a safe way to set BlockStatus is to set its info in accurate modes. */
      var blockStatus: BlockStatus = null
      if (storageLevel.useMemory) {
        blockStatus = BlockStatus(storageLevel, memSize = memSize, diskSize = 0)
        _blocks.put(blockId, blockStatus)
        _remainingMem -= memSize
        logInfo("Added %s in memory on %s (size: %s, free: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(memSize),
          Utils.bytesToString(_remainingMem)))
      }
      if (storageLevel.useDisk) {
        blockStatus = BlockStatus(storageLevel, memSize = 0, diskSize = diskSize)
        _blocks.put(blockId, blockStatus)
        logInfo("Added %s on disk on %s (size: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(diskSize)))
      }
      if (!blockId.isBroadcast && blockStatus.isCached) {
        _cachedBlocks += blockId
      }
    } else if (_blocks.containsKey(blockId)) {
      // If isValid is not true, drop the block.
      val blockStatus: BlockStatus = _blocks.get(blockId)
      _blocks.remove(blockId)
      _cachedBlocks -= blockId
      if (blockStatus.storageLevel.useMemory) {
        logInfo("Removed %s on %s in memory (size: %s, free: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(blockStatus.memSize),
          Utils.bytesToString(_remainingMem)))
      }
      if (blockStatus.storageLevel.useDisk) {
        logInfo("Removed %s on %s on disk (size: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(blockStatus.diskSize)))
      }
    }
  }

  def removeBlock(blockId: BlockId) {
    if (_blocks.containsKey(blockId)) {
      _remainingMem += _blocks.get(blockId).memSize
      _blocks.remove(blockId)
    }
    _cachedBlocks -= blockId
  }

  def remainingMem: Long = _remainingMem

  def lastSeenMs: Long = _lastSeenMs

  def blocks: JHashMap[BlockId, BlockStatus] = _blocks

  // This does not include broadcast blocks.
  def cachedBlocks: collection.Set[BlockId] = _cachedBlocks

  override def toString: String = "BlockManagerInfo " + timeMs + " " + _remainingMem

  def clear() {
    _blocks.clear()
  }
}
