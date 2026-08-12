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
import java.util.concurrent.atomic.AtomicLong

import org.scache.MapOutputTrackerMaster

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}
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
  private val prefetchSequence = new AtomicLong(0L)
  private val prefetchResults = new mutable.ArrayBuffer[PrefetchResult]()
  // UB shared-pool reducers fetch the published arena slice through the ranged URMA
  // path. The legacy map-completion prefetch below materializes a byte[] and calls
  // putBytes for the same block; with the direct pool path that races the reducer
  // lookup and can publish the same logical block twice. Keep legacy prefetch for
  // Netty/CXL, but disable it for the UB pool backend where it is redundant and adds
  // an avoidable copy.
  private val ubSharedPoolEnabled = conf.getBoolean("scache.ub.pool.enabled", false)

  // Shared CXL (fsdax) pool metadata: block -> (poolPath, offset, length).
  private val cxlSharedEnabled =
    conf.getBoolean("scache.storage.cxl.shared.enabled", false)
  private val cxlSharedPoolPath =
    conf.getString("scache.storage.cxl.shared.pool.path", "").trim
  private val cxlSharedPoolSizeBytes =
    conf.getSizeAsBytes("scache.storage.cxl.shared.pool.size", "0b")
  private val cxlSharedPoolAlignBytes =
    conf.getInt("scache.storage.cxl.shared.pool.align", 4096)
  private val cxlSharedMapChunkBytes =
    conf.getSizeAsBytes("scache.daemon.ipc.pool.mapChunk", "256m")

  private val cxlStateLock = new Object
  private val cxlBlocks = new mutable.HashMap[BlockId, CxlBlockLocation]()
  private val cxlReservations = new mutable.HashMap[BlockId, CxlBlockLocation]()

  // Multi-domain CXL support: domainId -> CxlDomainState
  private val cxlDomains: mutable.Map[String, CxlDomainState] = mutable.Map.empty
  // hostname -> domainId mapping for CXL-aware consumer locality
  private val hostToCxlDomain: mutable.Map[String, String] = mutable.Map.empty

  // Backward-compatible default domain initialization from legacy config.
  if (cxlSharedEnabled && cxlSharedPoolPath.nonEmpty && cxlSharedPoolSizeBytes > 0) {
    val ensuredSize = ensurePoolFileSize(cxlSharedPoolPath, cxlSharedPoolSizeBytes)
    if (ensuredSize <= 0) {
      logWarning(
        s"Shared CXL pool is enabled but has invalid size=$ensuredSize; disabling shared pool.")
    } else {
      val domainMembers = conf.getString(
        "scache.storage.cxl.domain.members", "").trim
      val memberSet: Set[String] =
        if (domainMembers.nonEmpty) domainMembers.split(",").map(_.trim).filter(_.nonEmpty).toSet
        else Set.empty
      val defaultDomain = CxlMemoryDomain(
        domainId = "default",
        poolPath = cxlSharedPoolPath,
        poolSizeBytes = ensuredSize,
        poolAlignBytes = cxlSharedPoolAlignBytes,
        memberHosts = memberSet)
      val allocator = new CxlPoolAllocator(
        ensuredSize, cxlSharedPoolAlignBytes, cxlSharedMapChunkBytes)
      cxlDomains.put("default", CxlDomainState(defaultDomain, allocator, new File(cxlSharedPoolPath)))
      memberSet.foreach { host =>
        hostToCxlDomain.put(host, "default")
      }
      logInfo(s"Initialized default CXL domain: path=$cxlSharedPoolPath size=$ensuredSize " +
        s"members=${memberSet.mkString(",")}")
    }
  } else if (cxlSharedEnabled) {
    logWarning(
      "Shared CXL pool is enabled but scache.storage.cxl.shared.pool.path/size are not set; " +
        "disabling shared pool.")
  }

  /** Returns the first available CXL domain, if any. */
  private def defaultCxlDomainId: Option[String] = cxlDomains.keys.headOption

  /** Resolve domainId: use provided value if non-empty, otherwise fall back to default domain. */
  private def resolveCxlDomain(domainId: String): Option[CxlDomainState] = {
    val effectiveId = if (domainId.nonEmpty) domainId else defaultCxlDomainId.getOrElse("")
    if (effectiveId.isEmpty) None else cxlDomains.get(effectiveId)
  }

  private val askThreadPool = ThreadUtils.newDaemonCachedThreadPool("block-manager-ask-thread-pool")
  private implicit val askExecutionContext: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(askThreadPool)

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterBlockManager(blockManagerId, maxMemSize, slaveEndpoint, capability) =>
      register(blockManagerId, maxMemSize, slaveEndpoint, capability)
      context.reply(true)

    case GetBlockManagerCapabilities =>
      context.reply(blockManagerInfo.values.map(_.capability).toSeq)

    case GetPrefetchResults =>
      context.reply(prefetchResults.synchronized(prefetchResults.toVector))

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

    case AllocateCxlBlock(domainId, length) =>
      context.reply(allocateCxlBlock(domainId, length))

    case AllocateCxlBlocks(domainId, lengths) =>
      context.reply(allocateCxlBlocks(domainId, lengths))

    case ReserveCxlBlock(blockId, domainId, length) =>
      context.reply(reserveCxlBlock(blockId, domainId, length))

    case ReserveCxlBlocks(blockIds, domainId, lengths) =>
      context.reply(reserveCxlBlocks(blockIds, domainId, lengths))

    case RegisterCxlBlock(blockId, location) =>
      context.reply(registerCxlBlock(blockId, location))

    case GetCxlBlock(blockId) =>
      context.reply(getCxlBlock(blockId))

    case ReleaseCxlBlock(blockId) =>
      context.reply(releaseCxlBlock(blockId))

    case ReleaseCxlAppShuffle(appName, shuffleId, jobId) =>
      context.reply(releaseCxlBlocksForAppShuffle(appName, shuffleId, jobId))

    case ReleaseCxlApplication(appName) =>
      context.reply(releaseCxlBlocksForApplication(appName))

    case RegisterCxlDomain(domainId, poolPath, poolSize, poolAlign, memberHosts) =>
      context.reply(registerCxlDomain(domainId, poolPath, poolSize, poolAlign, memberHosts))

    case GetCxlDomainTopology =>
      context.reply(cxlDomains.values.map(_.domain).toSeq)

    case GetCxlPoolStats(domainId) =>
      context.reply(getCxlPoolStats(domainId))

    case RemoveRdd(rddId) =>
      removeRdd(rddId).onComplete {
        case Success(result) => context.reply(result)
        case Failure(error) => context.sendFailure(error)
      }(askExecutionContext)

    case RemoveShuffle(shuffleId, appName, jobId) =>
      // Fan-out asks include the requesting client itself.  Complete the
      // outer RPC asynchronously so that the endpoint dispatcher remains
      // available to process that client's RemoveShuffle message.
      removeShuffle(shuffleId, appName, jobId).onComplete {
        case Success(result) => context.reply(result)
        case Failure(error) => context.sendFailure(error)
      }(askExecutionContext)

    case RemoveApplication(appName) =>
      removeApplication(appName).onComplete {
        case Success(result) => context.reply(result)
        case Failure(error) => context.sendFailure(error)
      }(askExecutionContext)

    case RemoveBroadcast(broadcastId, removeFromDriver) =>
      removeBroadcast(broadcastId, removeFromDriver).onComplete {
        case Success(result) => context.reply(result)
        case Failure(error) => context.sendFailure(error)
      }(askExecutionContext)

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

  private def allocateCxlBlock(domainId: String, length: Int): Option[CxlBlockLocation] = {
    if (length < 0) return None
    resolveCxlDomain(domainId) match {
      case Some(state) =>
        state.allocator.allocate(length).map { offset =>
          CxlBlockLocation(state.domain.poolPath, offset, length)
        }
      case None =>
        None
    }
  }

  private def allocateCxlBlocks(domainId: String, lengths: Seq[Int]): Seq[Option[CxlBlockLocation]] = {
    resolveCxlDomain(domainId) match {
      case Some(state) =>
        val poolPath = state.domain.poolPath
        val offsets = state.allocator.allocateMultiple(lengths)
        offsets.zip(lengths).map {
          case (Some(offset), len) => Some(CxlBlockLocation(poolPath, offset, len))
          case (None, _) => None
        }
      case None =>
        lengths.map(_ => None)
    }
  }

  private def freeCxlLocation(location: CxlBlockLocation): Unit = {
    cxlDomains.values.find(_.domain.poolPath == location.poolPath).foreach { state =>
      state.allocator.free(location.offset, location.length)
    }
  }

  private def trackCxlReservation(
      blockId: BlockId, location: CxlBlockLocation): CxlBlockLocation = {
    val replaced = cxlStateLock.synchronized {
      cxlReservations.put(blockId, location)
    }
    replaced.filter(_ != location).foreach(freeCxlLocation)
    location
  }

  private def reserveCxlBlock(
      blockId: BlockId, domainId: String, length: Int): Option[CxlBlockLocation] = {
    if (blockId == null) return None
    allocateCxlBlock(domainId, length).map(trackCxlReservation(blockId, _))
  }

  private def reserveCxlBlocks(
      blockIds: Seq[BlockId],
      domainId: String,
      lengths: Seq[Int]): Seq[Option[CxlBlockLocation]] = {
    if (blockIds.size != lengths.size) return lengths.map(_ => None)
    allocateCxlBlocks(domainId, lengths).zip(blockIds).map {
      case (Some(location), blockId) if blockId != null =>
        Some(trackCxlReservation(blockId, location))
      case _ => None
    }
  }

  private def registerCxlDomain(
      domainId: String, poolPath: String, poolSize: Long, poolAlign: Int,
      memberHosts: Seq[String]): Boolean = {
    if (domainId == null || domainId.trim.isEmpty) return false
    if (poolPath == null || poolPath.trim.isEmpty) return false
    if (poolSize <= 0) return false
    if (!cxlSharedEnabled) {
      logWarning(s"CXL shared pool is not enabled; ignoring domain registration for $domainId")
      return false
    }
    val ensuredSize = ensurePoolFileSize(poolPath, poolSize)
    if (ensuredSize <= 0) {
      logWarning(s"Failed to ensure CXL pool file size for domain $domainId: path=$poolPath size=$poolSize")
      return false
    }
    val domain = CxlMemoryDomain(
      domainId = domainId, poolPath = poolPath, poolSizeBytes = ensuredSize,
      poolAlignBytes = poolAlign, memberHosts = memberHosts.toSet)
    val allocator = new CxlPoolAllocator(ensuredSize, poolAlign, cxlSharedMapChunkBytes)
    val state = CxlDomainState(domain, allocator, new File(poolPath))
    cxlDomains.put(domainId, state)
    memberHosts.foreach { host =>
      hostToCxlDomain.put(host, domainId)
    }
    logInfo(s"Registered CXL domain $domainId: path=$poolPath size=$ensuredSize members=${memberHosts.mkString(",")}")
    true
  }

  private def getCxlPoolStats(domainId: String): Option[CxlPoolStats] = {
    val effectiveId = if (domainId.nonEmpty) domainId else defaultCxlDomainId.getOrElse("")
    cxlDomains.get(effectiveId).map { state =>
      val freeBytes = state.allocator.freeBytes
      val totalBytes = state.domain.poolSizeBytes
      val fragmentationRatio = if (totalBytes > 0) {
        val usedBytes = totalBytes - freeBytes
        usedBytes.toDouble / totalBytes.toDouble
      } else 0.0
      CxlPoolStats(
        domainId = effectiveId,
        freeBytes = freeBytes,
        totalBytes = totalBytes,
        allocationCount = state.allocator.allocationCount,
        freeSegmentCount = state.allocator.freeSegmentCount,
        fragmentationRatio = fragmentationRatio)
    }
  }

  private def registerCxlBlock(blockId: BlockId, location: CxlBlockLocation): Boolean = {
    if (blockId == null || location == null) return false
    if (location.length < 0 || location.offset < 0L) return false
    if (!cxlSharedEnabled) return false
    // Verify the pool path belongs to a registered domain.
    val domainOpt = cxlDomains.values.find(_.domain.poolPath == location.poolPath)
    if (domainOpt.isEmpty) {
      logWarning(s"CXL block registration rejected: pool path ${location.poolPath} does not match any domain")
      return false
    }
    val (reservation, replaced) = cxlStateLock.synchronized {
      (cxlReservations.remove(blockId), cxlBlocks.put(blockId, location))
    }
    // A retried/recomputed map task can publish the same logical block again. The metadata
    // map keeps only the newest slice, so the superseded allocation must be returned here.
    Seq(reservation, replaced).flatten.filter(_ != location).distinct.foreach(freeCxlLocation)
    true
  }

  private def getCxlBlock(blockId: BlockId): Option[CxlBlockLocation] = {
    if (blockId == null) return None
    cxlStateLock.synchronized {
      cxlBlocks.get(blockId)
    }
  }

  private def releaseCxlBlock(blockId: BlockId): Boolean = {
    if (blockId == null) return false
    val removed = cxlStateLock.synchronized {
      Seq(cxlBlocks.remove(blockId), cxlReservations.remove(blockId)).flatten.distinct
    }
    removed.foreach(freeCxlLocation)
    removed.nonEmpty
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

  private def removeShuffle(
      shuffleId: Int,
      appName: String,
      jobId: Int): Future[Seq[Int]] = {
    if (appName == null || appName.isEmpty) {
      releaseCxlBlocksForShuffle(shuffleId)
    } else {
      releaseCxlBlocksForAppShuffle(appName, shuffleId, jobId)
    }
    val removeMsg = RemoveShuffle(shuffleId, Option(appName).getOrElse(""), jobId)
    Future.sequence(
      blockManagerInfo.values.map { bm =>
        bm.slaveEndpoint.ask[Int](removeMsg)
      }.toSeq
    )
  }

  private def removeApplication(appName: String): Future[Seq[Int]] = {
    releaseCxlBlocksForApplication(appName)
    val removeMsg = RemoveApplication(appName)
    Future.sequence(
      blockManagerInfo.values.map { bm =>
        bm.slaveEndpoint.ask[Int](removeMsg)
      }.toSeq
    )
  }

  private def releaseCxlBlocksForShuffle(shuffleId: Int): Unit = {
    if (!cxlSharedEnabled) return
    val removedLocations = cxlStateLock.synchronized {
      val committed = cxlBlocks.collect {
        case (bid: ScacheBlockId, loc) if bid.shuffleId == shuffleId => (bid, loc)
      }.toArray
      val reserved = cxlReservations.collect {
        case (bid: ScacheBlockId, loc) if bid.shuffleId == shuffleId => (bid, loc)
      }.toArray
      committed.foreach { case (bid, _) => cxlBlocks.remove(bid) }
      reserved.foreach { case (bid, _) => cxlReservations.remove(bid) }
      (committed.map(_._2) ++ reserved.map(_._2)).distinct.toSeq
    }
    removedLocations.foreach(freeCxlLocation)
  }

  private def releaseCxlBlocksForAppShuffle(
      appName: String,
      shuffleId: Int,
      jobId: Int = -1): Int = {
    if (!cxlSharedEnabled || appName == null || appName.isEmpty) return 0
    val removedLocations = cxlStateLock.synchronized {
      val committed = cxlBlocks.collect {
        case (bid: ScacheBlockId, loc)
            if bid.app == appName && bid.shuffleId == shuffleId &&
              (jobId < 0 || bid.jobId == jobId) => (bid, loc)
      }.toArray
      val reserved = cxlReservations.collect {
        case (bid: ScacheBlockId, loc)
            if bid.app == appName && bid.shuffleId == shuffleId &&
              (jobId < 0 || bid.jobId == jobId) => (bid, loc)
      }.toArray
      committed.foreach { case (bid, _) => cxlBlocks.remove(bid) }
      reserved.foreach { case (bid, _) => cxlReservations.remove(bid) }
      (committed.map(_._2) ++ reserved.map(_._2)).distinct.toSeq
    }
    removedLocations.foreach(freeCxlLocation)
    removedLocations.size
  }

  private def releaseCxlBlocksForApplication(appName: String): Int = {
    if (!cxlSharedEnabled || appName == null || appName.isEmpty) return 0
    val removedLocations = cxlStateLock.synchronized {
      val committed = cxlBlocks.collect {
        case (bid: ScacheBlockId, loc) if bid.app == appName => (bid, loc)
      }.toArray
      val reserved = cxlReservations.collect {
        case (bid: ScacheBlockId, loc) if bid.app == appName => (bid, loc)
      }.toArray
      committed.foreach { case (bid, _) => cxlBlocks.remove(bid) }
      reserved.foreach { case (bid, _) => cxlReservations.remove(bid) }
      (committed.map(_._2) ++ reserved.map(_._2)).distinct.toSeq
    }
    removedLocations.foreach(freeCxlLocation)
    removedLocations.size
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

  private def validateCapability(id: BlockManagerId, capability: BlockManagerCapability): Unit = {
    require(capability != null, s"Missing capability for $id")
    require(capability.protocolVersion == 1,
      s"Unsupported BlockManager capability protocol ${capability.protocolVersion} for $id")
    require(capability.blockManagerId == id,
      s"Capability BlockManagerId ${capability.blockManagerId} does not match registration $id")
    require(capability.nodeEpoch != null && capability.nodeEpoch.nonEmpty,
      s"Missing node epoch for $id")
    require(Set("netty", "ub").contains(capability.backend),
      s"Unsupported backend ${capability.backend} for $id")
    require(capability.remoteFetchSupported == capability.networkEnabled,
      s"Contradictory network capability for $id: networkEnabled=${capability.networkEnabled} " +
        s"remoteFetchSupported=${capability.remoteFetchSupported}")
  }

  private def register(
      id: BlockManagerId,
      maxMemSize: Long,
      slaveEndpoint: RpcEndpointRef,
      capability: BlockManagerCapability) {
    validateCapability(id, capability)
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
        id, System.currentTimeMillis(), maxMemSize, slaveEndpoint, capability)
    } else {
      blockManagerInfo(id).capability = capability
    }
    logDebug(s"SCACHE_CAPABILITY_REGISTERED blockManagerId=$id " +
      s"protocolVersion=${capability.protocolVersion} nodeEpoch=${capability.nodeEpoch} " +
      s"backend=${capability.backend} networkEnabled=${capability.networkEnabled} " +
      s"remoteFetchSupported=${capability.remoteFetchSupported} " +
      s"sharedCxlEnabled=${capability.sharedCxlEnabled} " +
      s"rpc=${capability.rpcHost}:${capability.rpcPort}")
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
      val mapComplete = mapOutputTrackerMaster.updateMapBlocksStatus(blockId) == 0
      if (mapComplete && !ubSharedPoolEnabled) {
        val sbId = blockId.asInstanceOf[ScacheBlockId]
        val correlationId = s"prefetch-${sbId.app}-${sbId.jobId}-${sbId.shuffleId}-" +
          s"${sbId.mapId}-${prefetchSequence.incrementAndGet()}"
        logDebug(s"SCACHE_PREFETCH_DECISION correlationId=$correlationId decision=MAP_COMPLETE " +
          s"source=$blockManagerId app=${sbId.app} jobId=${sbId.jobId} " +
          s"shuffleId=${sbId.shuffleId} mapId=${sbId.mapId}")
        Future {
          for (info <- blockManagerInfo.values) {
            val result = if (info.blockManagerId == blockManagerId) {
              PrefetchResult(correlationId, "SKIPPED_LOCAL_SOURCE", blockManagerId,
                info.blockManagerId, 0, 0, 0, 0, 0L, 0L)
            } else if (!info.capability.remoteFetchSupported || !info.capability.networkEnabled) {
              PrefetchResult(correlationId, "SKIPPED_CAPABILITY_DISABLED", blockManagerId,
                info.blockManagerId, 0, 0, 0, 0, 0L, 0L,
                "RemoteFetchDisabled", "registered capability disables remote fetch")
            } else {
              try {
                info.slaveEndpoint.askWithRetry[PrefetchResult](StartMapFetch(
                  blockManagerId, sbId.app, sbId.jobId, sbId.shuffleId, sbId.mapId, correlationId))
              } catch {
                case NonFatal(e) =>
                  var root = e
                  while (root.getCause != null && (root.getCause ne root)) root = root.getCause
                  val attempts = conf.getInt("scache.rpc.numRetries", 3) + 1
                  PrefetchResult(correlationId, "FAILED", blockManagerId,
                    info.blockManagerId, 0, 0, 0, 1, 0L, 0L,
                    root.getClass.getName,
                    s"operation=start-map-fetch source=$blockManagerId " +
                      s"target=${info.blockManagerId} " +
                      s"targetEndpoint=${info.capability.rpcHost}:${info.capability.rpcPort} " +
                      s"attempt=$attempts/$attempts " +
                      s"configuredTimeout=${conf.getString("scache.rpc.askTimeout", "120s")} " +
                      s"rootCause=${root.getClass.getName}:" +
                      Option(root.getMessage).getOrElse(""))
              }
            }
            recordPrefetchResult(result)
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

  private def recordPrefetchResult(result: PrefetchResult): Unit = {
    prefetchResults.synchronized {
      prefetchResults += result
      if (prefetchResults.size > 10000) prefetchResults.remove(0, prefetchResults.size - 10000)
    }
    logDebug(s"SCACHE_PREFETCH_MASTER_RESULT correlationId=${result.correlationId} " +
      s"status=${result.status} source=${result.sourceBlockManagerId} " +
      s"target=${result.targetBlockManagerId} submitted=${result.submitted} " +
      s"completed=${result.completed} failed=${result.failed} " +
      s"payloadBytes=${result.payloadBytes} elapsedMs=${result.elapsedMs} " +
      s"errorType=${result.errorType} errorMessage=${result.errorMessage}")
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
private[storage] final class CxlPoolAllocator(
    poolSizeBytes: Long,
    alignBytes: Int,
    mapChunkBytes: Long) {
  require(poolSizeBytes > 0, s"poolSizeBytes must be > 0, got $poolSizeBytes")
  require(alignBytes > 0, s"alignBytes must be > 0, got $alignBytes")
  require(mapChunkBytes > 0, s"mapChunkBytes must be > 0, got $mapChunkBytes")

  private val free = new JTreeMap[java.lang.Long, java.lang.Long]()
  private var nextOffset = 0L
  // Before the first wrap, [nextOffset, poolSizeBytes) is virgin space and is not
  // represented in `free`. After wrap, the whole pool is represented by `free`;
  // counting the tail again would make freeBytes exceed poolSizeBytes.
  private var wrapped = false
  private var _allocationCount: Long = 0L

  def allocationCount: Long = _allocationCount
  def freeBytes: Long = synchronized {
    (if (wrapped) 0L else poolSizeBytes - nextOffset) +
      free.values().asScala.foldLeft(0L)((sum, len) => sum + len)
  }
  def freeSegmentCount: Int = synchronized { free.size() }

  private def alignUp(value: Long): Long = {
    val a = alignBytes.toLong
    if (a <= 1) return value
    ((value + a - 1) / a) * a
  }

  private def chunkEnd(offset: Long): Long = {
    val start = (offset / mapChunkBytes) * mapChunkBytes
    Math.min(start + mapChunkBytes, poolSizeBytes)
  }

  /** Batch-allocate multiple slices in a single synchronized block. */
  def allocateMultiple(sizes: Seq[Int]): Seq[Option[Long]] = synchronized {
    sizes.map(allocateUnsafe)
  }

  /** Allocate without acquiring the lock — caller must hold `synchronized`. */
  private def allocateUnsafe(size: Int): Option[Long] = {
    if (size < 0) return None
    if (size == 0) return Some(0L)
    if (size.toLong > poolSizeBytes || size.toLong > mapChunkBytes) return None

    allocateFromFree(size) match {
      case some @ Some(_) => return some
      case None =>
    }

    val originalNext = nextOffset
    var off = nextOffset

    val aligned = alignUp(off)
    if (aligned > off) insertFree(off, aligned - off)
    off = aligned

    while (off < poolSizeBytes && off + size > chunkEnd(off)) {
      val end = chunkEnd(off)
      insertFree(off, end - off)
      off = alignUp(end)
    }

    if (off + size <= poolSizeBytes) {
      nextOffset = off + size
      _allocationCount += 1
      return Some(off)
    }

    // Wrap-around: add remaining tail as free and retry from free list.
    if (originalNext < poolSizeBytes) {
      insertFree(originalNext, poolSizeBytes - originalNext)
    }
    nextOffset = 0L
    wrapped = true
    allocateFromFree(size)
  }

  def allocate(size: Int): Option[Long] = synchronized {
    allocateUnsafe(size)
  }

  private def allocateFromFree(size: Int): Option[Long] = {
    var entry = free.firstEntry()
    while (entry != null) {
      val segOffset = entry.getKey.longValue()
      val segLen = entry.getValue.longValue()
      val segEnd = segOffset + segLen

      val candidate1 = alignUp(segOffset)
      val candidate =
        if (candidate1 + size <= segEnd && candidate1 + size <= chunkEnd(candidate1)) {
          candidate1
        } else {
          alignUp(chunkEnd(candidate1))
        }
      if (candidate >= segOffset && candidate + size <= segEnd &&
          candidate + size <= chunkEnd(candidate)) {
        free.remove(entry.getKey)

        if (candidate > segOffset) {
          free.put(segOffset, candidate - segOffset)
        }
        val allocatedEnd = candidate + size
        if (allocatedEnd < segEnd) {
          free.put(allocatedEnd, segEnd - allocatedEnd)
        }
        _allocationCount += 1
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
    val slaveEndpoint: RpcEndpointRef,
    var capability: BlockManagerCapability)
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
