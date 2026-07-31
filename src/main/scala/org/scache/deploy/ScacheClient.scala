package org.scache.deploy

import java.io.File
import java.lang.Exception
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.StandardOpenOption
import java.util.concurrent.{TimeoutException, TimeUnit}

import org.apache.commons.httpclient.util.TimeoutController.TimeoutException
import org.scache.deploy.DeployMessages._
import org.scache.io.ChunkedByteBuffer
import org.scache.io.IpcPoolChunkedByteBuffer
import org.scache.network.netty.NettyBlockTransferService
import org.scache.storage._
import org.scache.storage.memory.{MemoryManager, StaticMemoryManager, UnifiedMemoryManager}
import org.scache.{MapOutputTracker, MapOutputTrackerMaster, MapOutputTrackerWorker}
import org.scache.rpc._
import org.scache.serializer.{JavaSerializer, SerializerManager}
import org.scache.util._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.control.Exception
import scala.util.{Failure, Random, Success}


/**
 * Created by frankfzw on 16-9-19.
 */
class ScacheClient(
  val rpcEnv: RpcEnv,
  val hostname: String,
  val masterHostname: String,
  val port: Int,
  conf: ScacheConf) extends ThreadSafeRpcEndpoint with Logging {
   conf.set("scache.client.port", rpcEnv.address.port.toString)

  private val ipcBackend = conf.getString("scache.daemon.ipc.backend", "files").trim.toLowerCase
  private val ipcMode = conf.getString("scache.daemon.ipc.mode", "remote").trim.toLowerCase
  private val ipcDirRemote = conf.getString(
    "scache.daemon.ipc.dir.remote",
    conf.getString("scache.daemon.ipc.dir", ScacheConf.scacheLocalDir))
  private val ipcDirLocal = conf.getString("scache.daemon.ipc.dir.local", ipcDirRemote)
  private val ipcDirGet = conf.getString("scache.daemon.ipc.dir.get", ipcDirLocal)
  private val ipcPretouch = conf.getBoolean("scache.daemon.ipc.pretouch", false)
  private val ipcPretouchPageSize = conf.getInt("scache.daemon.ipc.pretouch.pageSize", 4096)

  private val ipcPoolPathDefault =
    new File(ScacheConf.scacheLocalDir, "scache-ipc.pool").getAbsolutePath
  private val ipcPoolPath = conf.getString("scache.daemon.ipc.pool.path", ipcPoolPathDefault)
  private val ipcPoolSizeBytes = conf.getSizeAsBytes("scache.daemon.ipc.pool.size", "1024m")
  private val ipcPoolMapChunkBytes = {
    val bytes = conf.getSizeAsBytes("scache.daemon.ipc.pool.mapChunk", "256m")
    Math.min(bytes, Int.MaxValue.toLong).toInt
  }
  private val ipcPoolAlignBytes = conf.getInt("scache.daemon.ipc.pool.align", 4096)
  private val mapRegionLocalCommitParallelism =
    Math.max(1, conf.getInt("scache.shuffle.mapRegion.localCommitParallelism", 4))
  private val ipcPoolZeroCopyPut =
    conf.getBoolean("scache.daemon.ipc.pool.zeroCopy.put", false)

  // Shared CXL (fsdax) pool used for cross-node shuffle blocks. When enabled, non-local-consumer
  // blocks can be written directly into a global pool slice and later read by the reduce host
  // without client-to-client TCP transfers.
  private val cxlSharedEnabled =
    conf.getBoolean("scache.storage.cxl.shared.enabled", false)
  private val cxlSharedPoolPath =
    conf.getString("scache.storage.cxl.shared.pool.path", "").trim
  private val cxlSharedForceAll =
    conf.getBoolean("scache.storage.cxl.shared.forceAll", false)
  // Reserving one shared-pool slice per shuffle partition makes every map writer perform a
  // synchronous RPC against the single master allocator. Reserve a larger arena per client and
  // sub-allocate locally instead. Individual blocks are still registered and released with their
  // exact ranges, so the existing metadata and reclamation semantics remain unchanged.
  private val cxlSharedArenaSizeBytes = {
    val configured = conf.getSizeAsBytes("scache.storage.cxl.shared.arena.size", "256m")
    Math.max(0L, Math.min(configured, Int.MaxValue.toLong)).toInt
  }
  private val cxlSharedAlignBytes =
    Math.max(1, conf.getInt("scache.storage.cxl.shared.pool.align", 4096))
  private val cxlArenaLock = new Object
  private var cxlArenaNextOffset = 0L
  private var cxlArenaEndOffset = 0L

  private def alignCxlOffset(offset: Long): Long = {
    val alignment = cxlSharedAlignBytes.toLong
    ((offset + alignment - 1L) / alignment) * alignment
  }

  private def allocateCxlBlockFromArena(size: Int): Option[BlockManagerMessages.CxlBlockLocation] = {
    if (size < 0) return None
    if (size == 0) {
      return blockManagerMaster.allocateCxlBlock(0)
    }
    if (cxlSharedArenaSizeBytes <= 0 || size > cxlSharedArenaSizeBytes) {
      return blockManagerMaster.allocateCxlBlock(size)
    }

    cxlArenaLock.synchronized {
      val alignedOffset = alignCxlOffset(cxlArenaNextOffset)
      if (alignedOffset + size > cxlArenaEndOffset) {
        val arena = blockManagerMaster.allocateCxlBlock(cxlSharedArenaSizeBytes)
        if (arena.isEmpty) return None
        cxlArenaNextOffset = arena.get.offset
        cxlArenaEndOffset = arena.get.offset + arena.get.length
      }

      val blockOffset = alignCxlOffset(cxlArenaNextOffset)
      if (blockOffset + size > cxlArenaEndOffset) {
        return None
      }
      cxlArenaNextOffset = blockOffset + size
      Some(BlockManagerMessages.CxlBlockLocation(cxlSharedPoolPath, blockOffset, size))
    }
  }

  @volatile private var poolAllocator: PoolAllocator = null
  @volatile private var poolFile: MmapPoolFile = null

  private def getOrCreatePool(path: String): (MmapPoolFile, PoolAllocator) = {
    val currentFile = poolFile
    val currentAllocator = poolAllocator
    if (currentFile != null && currentAllocator != null && currentFile.path == path) {
      return (currentFile, currentAllocator)
    }
    synchronized {
      val againFile = poolFile
      val againAllocator = poolAllocator
      if (againFile != null && againAllocator != null && againFile.path == path) {
        return (againFile, againAllocator)
      }

      val created = MmapPoolFile.createOrOpen(
        path,
        desiredSizeBytes = ipcPoolSizeBytes,
        mapChunkBytes = ipcPoolMapChunkBytes)
      val allocator = new PoolAllocator(
        poolSizeBytes = created.sizeBytes,
        chunkSizeBytes = ipcPoolMapChunkBytes.toLong,
        alignBytes = ipcPoolAlignBytes)
      poolFile = created
      poolAllocator = allocator
      (created, allocator)
    }
  }

  private def parseStorageLevel(key: String, defaultValue: StorageLevel): StorageLevel = {
    val levelName = conf.getString(key, "").trim
    if (levelName.isEmpty) return defaultValue
    val upper = levelName.toUpperCase
    try {
      StorageLevel.fromString(upper)
    } catch {
      case _: IllegalArgumentException =>
        logWarning(s"Invalid $key=$upper; falling back to $defaultValue")
        defaultValue
    }
  }

  private val daemonPutStorageLevelDefault =
    parseStorageLevel("scache.daemon.putBlock.storageLevel", StorageLevel.MEMORY_ONLY)
  private val daemonPutStorageLevelLocal =
    parseStorageLevel("scache.daemon.putBlock.storageLevel.local", daemonPutStorageLevelDefault)
  private val daemonPutStorageLevelRemote =
    parseStorageLevel("scache.daemon.putBlock.storageLevel.remote", daemonPutStorageLevelDefault)

  private def mkdirsOrWarn(path: String): File = {
    val dir = new File(path)
    if (!dir.exists() && !dir.mkdirs()) {
      logWarning(s"Failed to create daemon IPC directory: $path")
    }
    dir
  }

  private val ipcDirRemoteFile = mkdirsOrWarn(ipcDirRemote)
  private val ipcDirLocalFile = mkdirsOrWarn(ipcDirLocal)
  private val ipcDirGetFile = mkdirsOrWarn(ipcDirGet)

  private def putIpcFileIn(dir: File, blockName: String): File = new File(dir, blockName)
  private def getIpcFile(blockName: String): File = new File(ipcDirGetFile, blockName)

  private def isLocalConsumer(blockId: BlockId): Boolean = blockId match {
    case bId: ScacheBlockId =>
      val shuffleKey = ShuffleKey(bId.app, bId.jobId, bId.shuffleId)
      val status = mapOutputTracker.getShuffleStatuses(shuffleKey)
      if (status == null) {
        false
      } else if (bId.reduceId < 0 || bId.reduceId >= status.reduceArray.length) {
        false
      } else {
        status.reduceArray(bId.reduceId).host == hostname
      }
    case _ =>
      false
  }

  private def putIpcFileCandidates(blockId: BlockId): Seq[File] = {
    val name = blockId.toString
    val localFile = putIpcFileIn(ipcDirLocalFile, name)
    val remoteFile = putIpcFileIn(ipcDirRemoteFile, name)

    val preferLocal = ipcMode match {
      case "local" => true
      case "remote" => false
      case "auto" => isLocalConsumer(blockId)
      case other =>
        logWarning(s"Unknown scache.daemon.ipc.mode=$other; defaulting to remote")
        false
    }

    if (preferLocal) Seq(localFile, remoteFile) else Seq(remoteFile, localFile)
  }

  val numUsableCores = conf.getInt("scache.cores", 1)
  val serializer = new JavaSerializer(conf)
  val serializerManager = new SerializerManager(serializer, conf)
  var clientId: Int = -1

  @volatile var master: RpcEndpointRef = null

  val mapOutputTracker = new MapOutputTrackerWorker(conf)
  mapOutputTracker.trackerEndpoint = RpcUtils.makeDriverRef(MapOutputTracker.ENDPOINT_NAME, conf, rpcEnv)
  logInfo("Registering " + MapOutputTracker.ENDPOINT_NAME)

  val useLegacyMemoryManager = conf.getBoolean("scache.memory.useLegacyMode", false)
  val memoryManager: MemoryManager =
      if (useLegacyMemoryManager) {
        new StaticMemoryManager(conf, numUsableCores)
      } else {
        UnifiedMemoryManager(conf, numUsableCores)
      }

  val blockTransferService = new NettyBlockTransferService(conf, hostname, numUsableCores)


  val blockManagerMasterEndpoint = RpcUtils.makeDriverRef(BlockManagerMaster.DRIVER_ENDPOINT_NAME, conf, rpcEnv)
  val blockManagerMaster = new BlockManagerMaster(blockManagerMasterEndpoint, conf, false)
  var blockManager:BlockManager = null

  override def onStart(): Unit = {
    logInfo("Client connecting to master " + masterHostname)
    master = RpcUtils.makeDriverRef("ScacheMaster", conf, rpcEnv)
    clientId = master.askWithRetry[Int](RegisterClient(hostname, port, self))
    blockManager = new BlockManager(clientId.toString, rpcEnv, blockManagerMaster,
      serializerManager, conf, memoryManager, mapOutputTracker, blockTransferService, numUsableCores)
    logInfo(s"Got ID ${clientId} from master")
    blockManager.initialize()
  }


  // meta of shuffle tracking
  // val shuffleOutputStatus = new mutable.HashMap[ShuffleKey, ShuffleStatus]()
  // create the future context for client
  private val asyncThreadPool =
    ThreadUtils.newDaemonCachedThreadPool("block-manager-slave-async-thread-pool")
  private implicit val asyncExecutionContext: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(asyncThreadPool)


  override def onStop(): Unit = {
    asyncThreadPool.shutdown()
  }


  override def receive: PartialFunction[Any, Unit] = {
    // from deamon
    case MapEnd(appName, jobId, shuffleId, mapId) =>
      mapEnd(appName, jobId, shuffleId, mapId)
    // from master
    case _ =>
      logError("Empty message received !")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case PreparePutBlock(blockId, size) =>
      doAsync[IpcLocation](s"Prepare IPC location for $blockId from daemon", context) {
        preparePutBlockFromDaemon(blockId, size)
      }
    case PrepareMapRegion(blockIds, sizes) =>
      doAsync[Array[IpcPoolSlice]](
          s"Prepare map region with ${blockIds.length} slices", context) {
        if (blockIds.length != sizes.length) {
          Array.empty[IpcPoolSlice]
        } else {
          blockIds.indices.map { i =>
            preparePutBlockFromDaemon(blockIds(i), sizes(i)) match {
              case slice: IpcPoolSlice => slice
              case other => throw new IllegalStateException(
                s"Expected pool slice for ${blockIds(i)}, got $other")
            }
          }.toArray
        }
      }
    case PutBlock(blockId, size, ipc) =>
      doAsync[Boolean](s"Read block $blockId from daemon", context) {
        readBlockFromDaemon(context, blockId, size, ipc)
      }
    case PutMapRegion(blockIds, sizes, slices) =>
      doAsync[Boolean](s"Commit map region with ${blockIds.length} slices", context) {
        readMapRegionFromDaemon(blockIds, sizes, slices)
      }
    case RegisterShuffle(appName, jobId, shuffleId, numMapTask, numReduceTask) =>
      context.reply(registerShuffle(appName, jobId, shuffleId, numMapTask, numReduceTask))
    case GetShuffleStatus(appName, jobId, shuffleId) =>
      context.reply(getShuffleStatus(appName, jobId, shuffleId))
    case GetBlock(blockId) =>
      doAsync[Int](s"Fetch block ${blockId} from daemon", context) {
        sendBlockToDaemon(context, blockId)
      }
    case GetBlockIpc(blockId) =>
      doAsync[Option[IpcBlock]](s"Fetch IPC location for block ${blockId} from daemon", context) {
        getBlockIpcFromDaemon(context, blockId)
      }
    case GetBlocksIpc(blockIds) =>
      doAsync[Array[Option[IpcBlock]]](
          s"Fetch IPC locations for ${blockIds.length} blocks", context) {
        getBlocksIpcFromDaemon(context, blockIds)
      }
    case _ =>
      logError("Empty message received !")
  }

  def registerShuffle(appName: String, jobId: Int, ids: Array[Int], numMaps: Array[Int], numReduces: Array[Int]): Boolean = {
    if (ids.length == 1) {
      val res = mapOutputTracker.registerShuffle(appName, jobId, ids(0), numMaps(0), numReduces(0))
      logInfo(s"Trying to register shuffle $appName, $jobId, ${ids(0)} with map ${numMaps(0)} and reduce ${numReduces(0)}, get $res")
      return res
    } else {
      val r = numReduces(0)
      for (numR <- numReduces) {
        if (numR != r) {
          logError(s"Register shuffle $appName, $jobId, ${ids(0)} with map ${numMaps(0)} and " +
            s"reduce ${numReduces(0)} fail, Reduce inconsistency")
          return false
        }
      }
      val res = mapOutputTracker.registerShuffles(appName, jobId, ids, numMaps, r)
      res
    }
  }

  def mapEnd(appName: String, jobId: Int, shuffleId: Int, mapId: Int): Unit = {
    logInfo(s"Map $appName:$jobId:$shuffleId:$mapId finished")
    // master.ask(MapEndToMaster(appName, jobId, shuffleId, mapId))
  }

  // def startMapFetch(blockManagerId: BlockManagerId, appName: String, jobId: Int, shuffleId: Int, mapId: Int): Unit = {
  //   // only pre-fetch remote bytes
  //   if (blockManagerId.executorId.equals(clientId.toString)) {
  //     return
  //   }
  //   logDebug(s"Start to fetch ${appName}_${jobId}_${shuffleId}_${mapId} from ${blockManagerId.host}")
  //   val shuffleKey = ShuffleKey(appName, jobId, shuffleId)
  //   val shuffleStatus = mapOutputTracker.getShuffleStatuses(shuffleKey)
  //   val bIds = new ArrayBuffer[String]()
  //   for (r <- shuffleStatus.reduceArray) {
  //     if (r.host.equals(hostname)) {
  //       // TODO start fetch and add call back to store block in memory
  //       val bId = ScacheBlockId(appName, jobId, shuffleId, mapId, r.id)
  //       bIds.append(bId.toString)
  //     }
  //   }
  //   blockManager.asyncGetRemoteBlock(blockManagerId, bIds.toArray)
  // }

  def readBlockFromDaemon(context: RpcCallContext, blockId: BlockId, size: Int, ipc: IpcLocation): Boolean= {
    val localConsumer = isLocalConsumer(blockId) && !cxlSharedForceAll
    val storageLevel =
      if (localConsumer) daemonPutStorageLevelLocal else daemonPutStorageLevelRemote

    ipc match {
      case IpcPoolSlice(poolPath0, offset, _) if !localConsumer && cxlSharedEnabled &&
          cxlSharedPoolPath.nonEmpty =>
        val poolPath = if (poolPath0.nonEmpty) poolPath0 else ipcPoolPath
        if (poolPath == cxlSharedPoolPath) {
          try {
            blockManagerMaster.registerCxlBlock(
              blockId,
              BlockManagerMessages.CxlBlockLocation(poolPath, offset, size))
            logDebug(s"Registered shared CXL location for block $blockId: $poolPath@$offset+$size")
            return true
          } catch {
            case e: Exception =>
              logWarning(s"Failed to register shared CXL mapping for block $blockId", e)
          }
        }
      case _ =>
    }

    if (size == 0) {
      val chunkedBuffer = new ChunkedByteBuffer(Array(ByteBuffer.allocate(0)))
      blockManager.putBytes(blockId, chunkedBuffer, storageLevel)
      return true
    }
    try {
        val chunkedBuffer: ChunkedByteBuffer = ipc match {
          case IpcPoolSlice(poolPath, offset, _) if ipcPoolZeroCopyPut &&
            storageLevel.useMemory && !storageLevel.useDisk && !storageLevel.deserialized =>
            val (pool, allocator) =
              getOrCreatePool(if (poolPath.nonEmpty) poolPath else ipcPoolPath)
            val buffer = pool.slice(offset, size)
            new IpcPoolChunkedByteBuffer(
              Array(buffer),
              () => allocator.free(offset, size))

          case IpcPoolSlice(poolPath, offset, _) =>
            val (pool, allocator) =
              getOrCreatePool(if (poolPath.nonEmpty) poolPath else ipcPoolPath)
            try {
              val buffer = pool.slice(offset, size)
              val array = new Array[Byte](size)
              buffer.get(array)
              val buf = ByteBuffer.wrap(array)
              new ChunkedByteBuffer(Array(buf))
            } finally {
              allocator.free(offset, size)
            }

          case IpcFile(path) =>
            val channel = FileChannel.open(
              new File(path).toPath,
              StandardOpenOption.READ,
              StandardOpenOption.DELETE_ON_CLOSE)
            try {
              val buffer = channel.map(MapMode.READ_ONLY, 0, size)
              val array = new Array[Byte](size)
              buffer.get(array)
              val buf = ByteBuffer.wrap(array)
              new ChunkedByteBuffer(Array(buf))
            } finally {
              channel.close()
            }
        }

        val stored = try {
          blockManager.putBytes(blockId, chunkedBuffer, storageLevel)
        } catch {
          case e: Exception =>
            try chunkedBuffer.dispose() catch { case _: Exception => }
            throw e
        }

        if (!stored) {
          try chunkedBuffer.dispose() catch { case _: Exception => }
          logWarning(s"Failed to store block $blockId in BlockManager (level=$storageLevel)")
          false
        } else {
          logDebug(s"Put block $blockId with size $size successfully (level=$storageLevel)")
          true
        }

        // start block transmission immediately
        // val shuffleStatus = getShuffleStatus(blockId)
        // val statuses = mapOutputTracker.getShuffleStatuses(ShuffleKey.fromString(blockId.toString))

      } catch {
        case e: Exception =>
          logError(s"Copy block $blockId error, ${e.getMessage}")
          false
      }
  }

  private def readMapRegionFromDaemon(
      blockIds: Array[BlockId],
      sizes: Array[Int],
      slices: Array[IpcPoolSlice]): Boolean = {
    if (blockIds == null || sizes == null || slices == null ||
        blockIds.length != sizes.length || sizes.length != slices.length) {
      return false
    }

    val cxlIds = new ArrayBuffer[BlockId]()
    val cxlLocations = new ArrayBuffer[BlockManagerMessages.CxlBlockLocation]()
    val localSlices = new ArrayBuffer[(BlockId, Int, IpcPoolSlice)]()
    var i = 0
    while (i < blockIds.length) {
      val blockId = blockIds(i)
      val slice = slices(i)
      if (sizes(i) != slice.length || sizes(i) < 0) return false
      if ((!isLocalConsumer(blockId) || cxlSharedForceAll) && cxlSharedEnabled &&
          cxlSharedPoolPath.nonEmpty && slice.poolPath == cxlSharedPoolPath) {
        cxlIds += blockId
        cxlLocations += BlockManagerMessages.CxlBlockLocation(
          slice.poolPath, slice.offset, slice.length)
      } else {
        localSlices += ((blockId, sizes(i), slice))
      }
      i += 1
    }

    val cxlOk = cxlIds.isEmpty ||
      blockManagerMaster.registerCxlBlocks(cxlIds.toArray, cxlLocations.toArray)
    if (!cxlOk) return false

    // Preserve the old writer's bounded parallelism without issuing one RPC per reduce slice.
    localSlices.grouped(mapRegionLocalCommitParallelism).forall { group =>
      Await.result(
        Future.sequence(group.map { case (blockId, size, slice) =>
          Future(readBlockFromDaemon(null, blockId, size, slice))
        }),
        Duration.Inf).forall(identity)
    }
  }

  private def preparePutBlockFromDaemon(blockId: BlockId, size: Int): IpcLocation = {
    if (size < 0) return IpcFile("")

    if (ipcBackend == "pool") {
      val useSharedCxlPool =
        cxlSharedEnabled && cxlSharedPoolPath.nonEmpty &&
          (!isLocalConsumer(blockId) || cxlSharedForceAll)

      if (useSharedCxlPool) {
        allocateCxlBlockFromArena(size) match {
          case Some(loc) =>
            return IpcPoolSlice(loc.poolPath, loc.offset, loc.length)
          case None =>
            logWarning(
              s"Failed to allocate shared CXL pool slice; falling back to local IPC pool " +
                s"for block $blockId (size=$size)")
        }
      }

      if (size == 0) return IpcPoolSlice(ipcPoolPath, 0L, 0)
      val (pool, allocator) = getOrCreatePool(ipcPoolPath)
      allocator.allocate(size) match {
        case Some(offset) =>
          if (ipcPretouch && size > 0) {
            try {
              val buffer = pool.slice(offset, size)
              val step = Math.max(ipcPretouchPageSize, 1)
              var i = 0
              while (i < size) {
                buffer.put(i, 0.toByte)
                i += step
              }
            } catch {
              case e: Exception =>
                logWarning(s"Failed to pretouch IPC pool slice for block $blockId", e)
            }
          }
          return IpcPoolSlice(ipcPoolPath, offset, size)
        case None =>
          logWarning(s"IPC pool is full; falling back to file IPC for block $blockId (size=$size)")
      }
    }

    val f = putIpcFileCandidates(blockId).head
    try {
      val channel = FileChannel.open(
        f.toPath,
        StandardOpenOption.READ,
        StandardOpenOption.WRITE,
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING)
      try {
        if (size == 0) {
          channel.truncate(0)
          return IpcFile(f.getAbsolutePath)
        }
        val buffer = channel.map(MapMode.READ_WRITE, 0, size.toLong)
        val doPretouch = ipcPretouch && f.getParentFile.getAbsolutePath == ipcDirLocalFile.getAbsolutePath
        if (doPretouch) {
          val step = Math.max(ipcPretouchPageSize, 1)
          var i = 0
          while (i < size) {
            buffer.put(i, 0.toByte)
            i += step
          }
        }
        IpcFile(f.getAbsolutePath)
      } finally {
        channel.close()
      }
    } catch {
      case e: Exception =>
        logWarning(s"Failed to prepare IPC file for block $blockId", e)
        IpcFile("")
    }
  }

  def sendBlockToDaemon(context: RpcCallContext, blockId: BlockId): Int= {
    val sleepMS = 100
    val retryTimes = conf.getInt("scache.block.fetching.retry", 5)
    val networkEnabled = conf.getBoolean("scache.storage.network.enabled", true)
    var times = 0
    while (times < retryTimes) {
      logDebug(s"Try to fetch block ${blockId} at ${times} time")
      val localBytes = blockManager.getLocalBytes(blockId)
      val (bufferOpt, fetchedRemotely) = localBytes match {
        case Some(local) => (Some(local), false)
        case None =>
          val cxlLocationOpt =
            if (cxlSharedEnabled && cxlSharedPoolPath.nonEmpty) {
              try {
                blockManagerMaster.getCxlBlock(blockId)
              } catch {
                case e: Exception =>
                  logWarning(s"Failed to query shared CXL metadata for $blockId", e)
                  None
              }
            } else {
              None
            }

          cxlLocationOpt match {
            case Some(loc) if loc.length >= 0 && loc.offset >= 0L &&
                loc.poolPath != null && loc.poolPath.nonEmpty =>
              try {
                val f = getIpcFile(blockId.toString)
                val outCh = FileChannel.open(
                  f.toPath,
                  StandardOpenOption.READ,
                  StandardOpenOption.WRITE,
                  StandardOpenOption.CREATE,
                  StandardOpenOption.TRUNCATE_EXISTING)
                try {
                  if (loc.length == 0) {
                    outCh.truncate(0)
                    return 0
                  }
                  val outBuf = outCh.map(MapMode.READ_WRITE, 0, loc.length.toLong)
                  val inCh = FileChannel.open(new File(loc.poolPath).toPath, StandardOpenOption.READ)
                  try {
                    var copied = 0L
                    while (outBuf.hasRemaining()) {
                      val n = inCh.read(outBuf, loc.offset + copied)
                      if (n < 0) {
                        throw new java.io.IOException(
                          s"Unexpected EOF while reading shared CXL pool for $blockId " +
                            s"(path=${loc.poolPath} offset=${loc.offset} length=${loc.length})")
                      }
                      copied += n
                    }
                  } finally {
                    inCh.close()
                  }
                  return loc.length
                } finally {
                  outCh.close()
                }
              } catch {
                case e: Exception =>
                  logWarning(s"Failed to read $blockId from shared CXL pool; will retry.", e)
              }
            case _ =>
          }

          if (!networkEnabled) {
            return -1
          }
          try {
            (blockManager.getRemoteBytes(blockId), true)
          } catch {
            case e: Exception =>
              logWarning(s"Failed to fetch remote block $blockId while serving daemon GetBlock", e)
              (None, true)
          }
      }

      bufferOpt match {
        case Some(buffer) =>
          val bytes = try {
            val chunks = buffer.getChunks()
            assert(chunks.size == 1)
            val arr = new Array[Byte](chunks(0).remaining())
            chunks(0).get(arr)
            arr
          } finally {
            if (fetchedRemotely) {
              try buffer.dispose() catch { case _: Exception => }
            }
          }

          val f = getIpcFile(blockId.toString)
          val channel = FileChannel.open(
            f.toPath,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING)
          try {
            if (bytes.isEmpty) {
              channel.truncate(0)
              return 0
            }
            val writeBuf = channel.map(MapMode.READ_WRITE, 0, bytes.length)
            writeBuf.put(bytes, 0, bytes.length)
            return bytes.length
          } finally {
            channel.close()
          }
        case None =>
          Thread.sleep(sleepMS)
          times += 1
      }
    }
    return -1

  }

  private def getBlockIpcFromDaemon(context: RpcCallContext, blockId: BlockId): Option[IpcBlock] = {
    if (blockId == null) return None

    if (cxlSharedEnabled && cxlSharedPoolPath.nonEmpty) {
      try {
        blockManagerMaster.getCxlBlock(blockId) match {
          case Some(loc) if loc.poolPath != null && loc.poolPath.nonEmpty &&
              loc.length >= 0 && loc.offset >= 0L =>
            return Some(IpcBlock(loc.length, IpcPoolSlice(loc.poolPath, loc.offset, loc.length)))
          case _ =>
        }
      } catch {
        case e: Exception =>
          logWarning(s"Failed to query shared CXL metadata for $blockId while serving GetBlockIpc", e)
      }
    }

    val size = sendBlockToDaemon(context, blockId)
    if (size < 0) return None
    if (size == 0) return Some(IpcBlock(0, IpcFile("")))
    Some(IpcBlock(size, IpcFile(getIpcFile(blockId.toString).getAbsolutePath)))
  }

  private def getBlocksIpcFromDaemon(
      context: RpcCallContext,
      blockIds: Array[BlockId]): Array[Option[IpcBlock]] = {
    if (blockIds == null) return Array.empty
    val cxlLocations =
      if (cxlSharedEnabled && cxlSharedPoolPath.nonEmpty) {
        try {
          blockManagerMaster.getCxlBlocks(blockIds)
        } catch {
          case e: Exception =>
            logWarning("Failed batch CXL metadata lookup; falling back to block lookups", e)
            Array.fill[Option[BlockManagerMessages.CxlBlockLocation]](blockIds.length)(None)
        }
      } else {
        Array.fill[Option[BlockManagerMessages.CxlBlockLocation]](blockIds.length)(None)
      }

    blockIds.indices.map { i =>
      cxlLocations(i) match {
        case Some(loc) if loc.poolPath != null && loc.poolPath.nonEmpty &&
            loc.length >= 0 && loc.offset >= 0L =>
          Some(IpcBlock(loc.length, IpcPoolSlice(loc.poolPath, loc.offset, loc.length)))
        case _ =>
          getBlockIpcFromDaemon(context, blockIds(i))
      }
    }.toArray
  }

  private def getShuffleStatus(blockId: BlockId): ShuffleStatus = {
    val shuffleKey = ShuffleKey.fromString(blockId.toString)
    mapOutputTracker.getShuffleStatuses(shuffleKey)
  }

  private def getShuffleStatus(appName: String, shuffleId: Int, jobId: Int): ShuffleStatus = {
    val shuffleKey = ShuffleKey(appName, shuffleId, jobId)
    mapOutputTracker.getShuffleStatuses(shuffleKey)
  }

  def runTest(): Unit = {
    val blockIda1 = new ScacheBlockId("scache", 1, 1, 1, 1)
    val blockIda2 = new ScacheBlockId("scache", 1, 1, 1, 2)
    val blockIda3 = new ScacheBlockId("scache", 1, 1, 2, 1)

    // Checking whether master knows about the blocks or not
    assert(blockManagerMaster.getLocations(blockIda1).size > 0, "master was not told about a1")
    assert(blockManagerMaster.getLocations(blockIda2).size > 0, "master was not told about a2")
    assert(blockManagerMaster.getLocations(blockIda3).size == 0, "master was told about a3")

    // Try to fetch remote blocks
    assert(blockManager.getRemoteBytes(blockIda1).size > 0, "fail to get a1")
    assert(blockManager.getRemoteBytes(blockIda2).size > 0, "fail to get a2")

    blockManager.getLocalBytes(blockIda1) match {
      case Some(buffer) =>
        logInfo(s"The size of ${blockIda1} is ${buffer.size}")
      case None =>
        logError(s"Wrong fetch result")
    }

    // shuffle register test
    Thread.sleep(Random.nextInt(1000))
    val res = registerShuffle("scache", 0, Array(1), Array(5), Array(2))
    logInfo(s"TEST: register shuffle got ${res}")
    Thread.sleep(Random.nextInt(1000))
    val statuses = getShuffleStatus(ScacheBlockId("scache", 0, 1, 0, 0))
    for (rs <- statuses.reduceArray) {
      logInfo(s"TEST: shuffle status of ${statuses.shffleId}: reduce ${rs.id} on ${rs.host}")
    }
  }

  private def doAsync[T](actionMessage: String, context: RpcCallContext)(body: => T): Unit = {
    val future = Future {
      logDebug(actionMessage)
      body
    }
    future.onComplete {
      case Success(response) =>
        logDebug("Done " + actionMessage + ", response is " + response)
        context.reply(response)
        logDebug("Sent response: " + response + " to " + context.senderAddress)
      case Failure(t) =>
        logError("Error in " + actionMessage, t)
        context.sendFailure(t)
    }
  }
}

object ScacheClient extends Logging{
  def main(args: Array[String]): Unit = {
    val conf = new ScacheConf()
    val arguements = new ClientArguments(args, conf)

    val ipcBackend = conf.getString("scache.daemon.ipc.backend", "files").trim.toLowerCase
    if (ipcBackend == "pool") {
      val isLoopback = try {
        java.net.InetAddress.getByName(arguements.host).isLoopbackAddress
      } catch {
        case _: Exception => false
      }
      if (isLoopback) {
        val configuredPoolPath = conf.getString(
          "scache.daemon.ipc.pool.path",
          new File(ScacheConf.scacheLocalDir, "scache-ipc.pool").getAbsolutePath)
        val suffix = arguements.host.replace(':', '_').replace('.', '_')
        val desiredPoolPath =
          if (configuredPoolPath.endsWith("." + suffix)) configuredPoolPath
          else configuredPoolPath + "." + suffix
        if (desiredPoolPath != configuredPoolPath) {
          conf.set("scache.daemon.ipc.pool.path", desiredPoolPath)
        }
      }
    }

    val hostName = Utils.findLocalInetAddress().getHostName
    System.setProperty("SCACHE_DAEMON", s"client-${hostName}")
    conf.set("scache.rpc.askTimeout", "10")
    logInfo("Start Client")
    conf.set("scache.driver.host", arguements.masterIp)
    conf.set("scache.driver.port", arguements.masterPort.toString)
    conf.set("scache.app.id", "test")

    val masterRpcAddress = RpcAddress(arguements.masterIp, arguements.masterPort)

    val rpcEnv = RpcEnv.create("scache.client", arguements.host, arguements.port, conf)
    val clientEndpoint = rpcEnv.setupEndpoint("ScacheClient",
      new ScacheClient(rpcEnv, arguements.host, RpcEndpointAddress(masterRpcAddress, "ScacheMaster").toString, arguements.port, conf)
    )
    rpcEnv.awaitTermination()
  }
}
