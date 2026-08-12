package org.scache.deploy

import java.io.File
import java.lang.Exception
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.StandardOpenOption
import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, Semaphore, TimeoutException, TimeUnit}

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
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future, Promise}
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
  val nodeEpoch: String = conf.getString("scache.client.nodeEpoch", UUID.randomUUID().toString)
  conf.set("scache.client.nodeEpoch", nodeEpoch, slient = true)

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
  private val ipcPoolZeroCopyPut =
    conf.getBoolean("scache.daemon.ipc.pool.zeroCopy.put", false)

  // Shared CXL (fsdax) pool used for cross-node shuffle blocks. When enabled, non-local-consumer
  // blocks can be written directly into a global pool slice and later read by the reduce host
  // without client-to-client TCP transfers.
  private val cxlSharedEnabled =
    conf.getBoolean("scache.storage.cxl.shared.enabled", false)
  private val cxlSharedPoolPath =
    conf.getString("scache.storage.cxl.shared.pool.path", "").trim
  // Force CXL mode: bypass isLocalConsumer check and use CXL pool for ALL pool allocations.
  // For single-node CXL emulation testing only.
  private val cxlSharedForce =
    conf.getBoolean("scache.storage.cxl.shared.force", false)

  // UB distributed shared arena is deliberately independent from the CXL-like mmap pool.  A
  // reducer receives only a local destination slice; the owner path is never treated as a remote
  // filesystem path.
  private lazy val ubSharedPoolEnabled =
    conf.getBoolean("scache.ub.pool.enabled", false)
  private lazy val ubSharedPoolStrict =
    conf.getBoolean("spark.scache.strict", false) ||
      conf.getBoolean("spark.urma.strict", false)
  // A previous experiment changed GetBlocksIpc to unbounded Future.traverse. That allowed two
  // requests for the same logical block to reserve/commit the same arena entry concurrently.
  // Keep the safe serial default for all other backends, and use a small bounded degree only for
  // the UB pool path. The single-flight table makes duplicate block ids share one fetch.
  private lazy val ubPoolFetchParallelism =
    math.max(1, conf.getInt("scache.ub.pool.fetchParallelism", 4))
  private lazy val ubPoolFetchSlots = new Semaphore(ubPoolFetchParallelism)
  private lazy val ubPoolFetches =
    new ConcurrentHashMap[String, Future[Option[IpcBlock]]]()

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

  // CXL domain topology: hostname -> domainId, lazily populated from master.
  private val cxlDomainForHost: mutable.Map[String, String] = mutable.Map.empty
  private var cxlDomainTopologyLoaded = false

  private def loadCxlDomainTopology(): Unit = {
    if (cxlDomainTopologyLoaded || !cxlSharedEnabled) return
    try {
      val domains = blockManagerMaster.getCxlDomainTopology
      for (d <- domains; host <- d.memberHosts) {
        cxlDomainForHost.put(host, d.domainId)
      }
      if (domains.nonEmpty) {
        logInfo(s"Loaded CXL domain topology: ${domains.size} domains, " +
          s"${cxlDomainForHost.size} host mappings")
      }
    } catch {
      case e: Exception =>
        logWarning("Failed to load CXL domain topology from master", e)
    }
    cxlDomainTopologyLoaded = true
  }

  private def isLocalConsumer(blockId: BlockId): Boolean = blockId match {
    case bId: ScacheBlockId =>
      val shuffleKey = ShuffleKey(bId.app, bId.jobId, bId.shuffleId)
      val status = mapOutputTracker.getShuffleStatuses(shuffleKey)
      if (status == null) {
        false
      } else if (bId.reduceId < 0 || bId.reduceId >= status.reduceArray.length) {
        false
      } else {
        val reduceHost = status.reduceArray(bId.reduceId).host
        if (reduceHost == hostname) {
          true
        } else if (cxlSharedEnabled) {
          // CXL-aware: if consumer and producer share a CXL domain, treat as local.
          loadCxlDomainTopology()
          val consumerDomain = cxlDomainForHost.get(hostname)
          val producerDomain = cxlDomainForHost.get(reduceHost)
          consumerDomain.isDefined && consumerDomain == producerDomain
        } else {
          false
        }
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

  private val configuredBackend =
    conf.getString("scache.blockTransfer.backend", "netty").trim.toLowerCase
  val blockTransferService = configuredBackend match {
    case "ub" =>
      logInfo("Using UB (URMA) BlockTransferService backend")
      new org.scache.network.ub.UBBlockTransferService(conf, hostname, numUsableCores)
    case "netty" =>
      new NettyBlockTransferService(conf, hostname, numUsableCores)
    case other =>
      throw new IllegalArgumentException(s"Unsupported scache.blockTransfer.backend=$other")
  }

  private def ubSharedTransport: Option[org.scache.network.ub.UBBlockTransferService] = {
    if (!ubSharedPoolEnabled) None
    else blockTransferService match {
      case ub: org.scache.network.ub.UBBlockTransferService => Some(ub)
      case _ =>
        throw new IllegalStateException(
          "scache.ub.pool.enabled=true requires scache.blockTransfer.backend=ub")
    }
  }


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
    val networkEnabled = conf.getBoolean("scache.storage.network.enabled", true)
    logInfo(s"SCACHE_CLIENT_READY nodeEpoch=$nodeEpoch clientId=$clientId rpc=${rpcEnv.address} " +
      s"blockManagerId=${blockManager.blockManagerId} backend=$configuredBackend " +
      s"networkEnabled=$networkEnabled remoteFetchSupported=$networkEnabled " +
      s"sharedCxlEnabled=$cxlSharedEnabled")
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
    case ClientHello(protocolVersion, correlationId) =>
      if (protocolVersion != 1) {
        context.sendFailure(new IllegalArgumentException(
          s"Unsupported daemon protocolVersion=$protocolVersion correlationId=$correlationId"))
      } else if (blockManager == null || blockManager.blockManagerId == null || clientId < 0) {
        context.sendFailure(new IllegalStateException(
          s"ScacheClient is not ready correlationId=$correlationId nodeEpoch=$nodeEpoch"))
      } else {
        val networkEnabled = conf.getBoolean("scache.storage.network.enabled", true)
        val identity = ClientIdentity(
          protocolVersion = 1,
          nodeEpoch = nodeEpoch,
          clientId = clientId,
          rpcHost = rpcEnv.address.host,
          rpcPort = rpcEnv.address.port,
          blockManagerId = blockManager.blockManagerId,
          backend = configuredBackend,
          networkEnabled = networkEnabled,
          remoteFetchSupported = networkEnabled,
          sharedCxlEnabled = cxlSharedEnabled)
        logDebug(s"SCACHE_CLIENT_HELLO correlationId=$correlationId nodeEpoch=$nodeEpoch " +
          s"clientId=$clientId rpc=${rpcEnv.address} blockManagerId=${blockManager.blockManagerId} " +
          s"backend=$configuredBackend networkEnabled=$networkEnabled " +
          s"remoteFetchSupported=$networkEnabled sharedCxlEnabled=$cxlSharedEnabled")
        context.reply(identity)
      }

    case PreparePutBlock(blockId, size) =>
      doAsync[IpcLocation](s"Prepare IPC location for $blockId from daemon", context) {
        preparePutBlockFromDaemon(blockId, size)
      }
    case PreparePutBlocks(blockIds, sizes) =>
      doAsync[Seq[Option[IpcLocation]]](s"Batch prepare ${blockIds.size} blocks", context) {
        preparePutBlocksFromDaemon(blockIds, sizes).map {
          case loc: IpcLocation => Some(loc)
          case _ => None
        }
      }
    case AbortPutBlock(blockId, size, ipc) =>
      doAsync[Boolean](s"Abort block reservation $blockId from daemon", context) {
        abortPutBlockFromDaemon(blockId, size, ipc)
      }
    case PutBlock(blockId, size, ipc) =>
      doAsync[Boolean](s"Read block $blockId from daemon", context) {
        readBlockFromDaemon(context, blockId, size, ipc)
      }
    case PutBlocks(msgs) =>
      doAsync[Seq[Boolean]](s"Batch read ${msgs.size} blocks from daemon", context) {
        msgs.map { case PutBlock(blockId, size, ipc) =>
          readBlockFromDaemon(context, blockId, size, ipc)
        }
      }
    case RegisterShuffle(appName, jobId, shuffleId, numMapTask, numReduceTask) =>
      context.reply(registerShuffle(appName, jobId, shuffleId, numMapTask, numReduceTask))
    case ReleaseShuffle(appName, jobId, shuffleId) =>
      // Release is application-wide: the driver daemon may be connected to
      // one client while shuffle blocks live on every executor client.
      // Broadcast through the SCache master so every client withdraws its UB
      // descriptors before its arena is reused.
      // The master broadcast includes this client endpoint.  Waiting from the
      // RPC dispatcher thread would therefore wait for a message that cannot
      // be dispatched until this handler returns.  Run the blocking wait on
      // the existing async pool so the endpoint remains responsive.
      doAsync[Int](s"Removing shuffle $appName:$jobId:$shuffleId", context) {
        blockManagerMaster.removeShuffle(
          shuffleId, blocking = true, appName = appName, jobId = jobId)
        blockManagerMaster.releaseCxlAppShuffle(appName, shuffleId, jobId)
      }
    case ReleaseApplication(appName) =>
      doAsync[Int](s"Removing application $appName", context) {
        blockManagerMaster.removeApplication(appName, blocking = true)
        blockManagerMaster.releaseCxlApplication(appName)
      }
    case GetShuffleStatus(appName, jobId, shuffleId) =>
      context.reply(getShuffleStatus(appName, jobId, shuffleId))
    case GetBlock(blockId) =>
      doAsync[Int](s"Fetch block ${blockId} from daemon", context) {
        sendBlockToDaemon(context, blockId)
      }
    case GetBlockIpc(blockId) =>
      doAsync[Option[IpcBlock]](s"Fetch IPC location for block ${blockId} from daemon", context) {
        if (ubSharedPoolEnabled) {
          Await.result(ubPoolFetchSingleFlight(context, blockId), Duration.Inf)
        } else {
          getBlockIpcFromDaemon(context, blockId)
        }
      }
    case GetBlocksIpc(blockIds) =>
      doAsync[Seq[Option[IpcBlock]]](s"Fetch IPC locations for ${blockIds.size} blocks from daemon", context) {
        if (ubSharedPoolEnabled) {
          // Different blocks can make progress concurrently, but the semaphore bounds URMA and
          // arena/control pressure. Duplicate logical block ids are coalesced by single-flight.
          Await.result(Future.traverse(blockIds)(blockId =>
            ubPoolFetchSingleFlight(context, blockId)), Duration.Inf)
        } else {
          blockIds.map { blockId =>
            getBlockIpcFromDaemon(context, blockId)
          }
        }
      }
    case ReleaseImportedBlock(blockId) =>
      doAsync[Boolean](s"Release imported UB pool block ${blockId}", context) {
        ubSharedPoolEnabled && ubSharedTransport.exists(_.releaseImportedPoolBlock(blockId))
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
    val localConsumer = isLocalConsumer(blockId)
    val storageLevel =
      if (localConsumer) daemonPutStorageLevelLocal else daemonPutStorageLevelRemote

    // UB pool commit publishes the bytes already written by Spark into a long-lived registered
    // arena.  Do not materialize them into BlockManager memory; only register a directory/status
    // entry so reducers can discover the owner BlockManagerId.
    ipc match {
      case IpcPoolSlice(poolPath, offset, length) if ubSharedPoolEnabled =>
        val ub = ubSharedTransport.get
        if (length != size || offset < 0L) {
          logError(s"UB pool slice mismatch block=$blockId size=$size offset=$offset length=$length")
          return false
        }
        try {
          val committed = ub.commitPoolBlock(blockId, poolPath, offset, size)
          if (!committed) return false
          val registered = blockManager.registerExternalBlock(
            blockId, size, StorageLevel.MEMORY_ONLY, tellMaster = true)
          if (!registered) {
            ub.unpublishBlock(blockId)
          }
          return registered
        } catch {
          case e: Exception =>
            try ub.unpublishBlock(blockId) catch { case _: Exception => }
            logError(s"Failed to commit UB shared pool block $blockId", e)
            if (ubSharedPoolStrict) throw e
            return false
        }
      case _ =>
    }

    ipc match {
      case IpcPoolSlice(poolPath0, offset, _) if (!localConsumer || cxlSharedForce) &&
          cxlSharedEnabled && cxlSharedPoolPath.nonEmpty =>
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
            val resolvedPoolPath = if (poolPath.nonEmpty) poolPath else ipcPoolPath
            val (pool, allocator) =
              getOrCreatePool(resolvedPoolPath)
            val buffer = pool.slice(offset, size)
            new IpcPoolChunkedByteBuffer(
              Array(buffer),
              () => allocator.free(offset, size),
              resolvedPoolPath, offset, size)

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

  private def preparePutBlockFromDaemon(blockId: BlockId, size: Int): IpcLocation = {
    if (size < 0) return IpcFile("")

    if (ubSharedPoolEnabled) {
      val ub = ubSharedTransport.get
      ub.preparePoolBlock(blockId, size) match {
        case Some(slice) =>
          return IpcPoolSlice(slice.path, slice.offset, slice.length)
        case None =>
          if (ubSharedPoolStrict) {
            throw new IllegalStateException(
              s"UB shared pool is enabled but could not reserve block=$blockId size=$size")
          }
      }
    }

    if (ipcBackend == "pool") {
      val useSharedCxlPool =
        cxlSharedEnabled && cxlSharedPoolPath.nonEmpty &&
        (cxlSharedForce || !isLocalConsumer(blockId))

      if (useSharedCxlPool) {
        blockManagerMaster.reserveCxlBlock(blockId, "", size) match {
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

  /** Release a writer-side UB reservation without publishing a block location. */
  private def abortPutBlockFromDaemon(
      blockId: BlockId, size: Int, ipc: IpcLocation): Boolean = {
    ipc match {
      case IpcPoolSlice(poolPath, offset, length) if ubSharedPoolEnabled =>
        if (size < 0 || length != size || offset < 0L) {
          throw new IllegalArgumentException(
            s"Invalid UB pool abort slice block=$blockId size=$size offset=$offset length=$length")
        }
        ubSharedTransport.get.abortPoolBlock(blockId, poolPath, offset, size)
      case _ =>
        // Existing file/CXL paths have their own commit-time ownership rules.  The new abort
        // message is intentionally a no-op for them so enabling this cleanup hook does not
        // change baseline behavior.
        true
    }
  }

  /** Batch allocate CXL pool slices: one RPC to Master for all blocks. */
  def preparePutBlocksFromDaemon(blockIds: Seq[BlockId], sizes: Seq[Int]): Seq[IpcLocation] = {
    require(blockIds.size == sizes.size, s"blockIds.size=${blockIds.size} != sizes.size=${sizes.size}")
    if (ubSharedPoolEnabled) {
      val ub = ubSharedTransport.get
      return ub.preparePoolBlocks(blockIds, sizes).map {
        case Some(slice) => IpcPoolSlice(slice.path, slice.offset, slice.length)
        case None if ubSharedPoolStrict =>
          throw new IllegalStateException("UB shared pool batch reservation failed")
        case None => IpcFile("")
      }
    }
    if (cxlSharedEnabled && cxlSharedPoolPath.nonEmpty) {
      val locs = blockManagerMaster.reserveCxlBlocks(blockIds, "", sizes.toSeq)
      return locs.zip(sizes).map {
        case (Some(loc), len) => IpcPoolSlice(loc.poolPath, loc.offset, len)
        case (None, len) =>
          logWarning(s"Batch CXL pool allocation failed for size=$len; falling back to file IPC")
          IpcFile("")
      }
    }
    // Fallback: allocate individually from local IPC pool
    blockIds.zip(sizes).map { case (bid, sz) => preparePutBlockFromDaemon(bid, sz) }
  }

  def sendBlockToDaemon(context: RpcCallContext, blockId: BlockId): Int = {
    sendBlockToDaemonFile(context, blockId, getIpcFile(blockId.toString))
  }

  private def sendBlockToDaemonFile(
      context: RpcCallContext,
      blockId: BlockId,
      ipcFile: File): Int = {
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
                val outCh = FileChannel.open(
                  ipcFile.toPath,
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
            } else {
              blockManager.releaseLock(blockId)
            }
          }

          val channel = FileChannel.open(
            ipcFile.toPath,
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

    if (ubSharedPoolEnabled) {
      val ub = ubSharedTransport.get
      ub.localPoolBlock(blockId) match {
        case Some(slice) =>
          return Some(IpcBlock(slice.length,
            IpcPoolSlice(slice.path, slice.offset, slice.length)))
        case None =>
      }

      // The BlockManager master directory tells us which UB control endpoint owns the block.  A
      // successful fetch is cached in this executor's local arena; subsequent Spark reads use the
      // local slice and do not repeat URMA READ.
      val localId = blockManager.blockManagerId
      val locations = blockManagerMaster.getLocations(blockId)
      locations.iterator.filterNot(_ == localId).foreach { location =>
        try {
          ub.fetchBlockToPool(location.host, location.port, blockId.toString) match {
            case Some(slice) =>
              return Some(IpcBlock(slice.length,
                IpcPoolSlice(slice.path, slice.offset, slice.length)))
            case None =>
          }
        } catch {
          case e: Exception =>
            logWarning(s"UB shared pool fetch failed block=$blockId source=$location", e)
        }
      }
      if (ubSharedPoolStrict) {
        throw new IllegalStateException(
          s"Strict UB shared pool could not fetch block=$blockId from ${locations.mkString(",")}")
      }
    }

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

    // If the block is stored locally as a zero-copy pool buffer, return the pool slice directly
    // without materializing to a temp file.
    blockManager.getLocalBytes(blockId) match {
      case Some(chunkedBuffer: IpcPoolChunkedByteBuffer) =>
        try {
          return Some(IpcBlock(
            chunkedBuffer.poolLength,
            IpcPoolSlice(chunkedBuffer.poolPath, chunkedBuffer.poolOffset, chunkedBuffer.poolLength)))
        } finally {
          blockManager.releaseLock(blockId)
        }
      case Some(_) =>
        blockManager.releaseLock(blockId)
      case _ =>
    }

    // GetBlockIpc responses hand ownership of the materialized file to the caller, which deletes
    // it after consumption. A fixed per-block filename lets concurrent readers delete or truncate
    // each other's file, so every response must use its own path.
    val ipcFile = getIpcFile(s"${blockId.toString}-${UUID.randomUUID()}")
    val size = sendBlockToDaemonFile(context, blockId, ipcFile)
    if (size < 0) {
      ipcFile.delete()
      return None
    }
    if (size == 0) {
      ipcFile.delete()
      return Some(IpcBlock(0, IpcFile("")))
    }
    Some(IpcBlock(size, IpcFile(ipcFile.getAbsolutePath)))
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

  /**
   * Coalesce concurrent requests for one logical block while allowing a bounded number of
   * different UB pool blocks to fetch concurrently. The promise is installed before the
   * worker starts, so retries/duplicate GetBlocksIpc entries cannot reserve the same slice twice.
   */
  private def ubPoolFetchSingleFlight(
      context: RpcCallContext, blockId: BlockId): Future[Option[IpcBlock]] = {
    val key = if (blockId == null) "<null>" else blockId.toString
    val ub = ubSharedTransport.get
    // Reserve before joining a completed single-flight Future. The reservation transfers to
    // Spark's ManagedBuffer release callback; failed or missing fetches cancel it here.
    ub.reservePoolConsumer(blockId)
    val promise = Promise[Option[IpcBlock]]()
    val existing = ubPoolFetches.putIfAbsent(key, promise.future)
    val shared =
      if (existing != null) {
        existing
      } else {
        Future {
          ubPoolFetchSlots.acquire()
          try getBlockIpcFromDaemon(context, blockId)
          finally ubPoolFetchSlots.release()
        }.onComplete { result =>
          promise.tryComplete(result)
          ubPoolFetches.remove(key, promise.future)
        }
        promise.future
      }
    shared.andThen {
      case Success(Some(_)) =>
      case _ => ub.cancelPoolConsumer(blockId)
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
