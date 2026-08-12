package org.scache.deploy

import java.io.{ByteArrayOutputStream, File, ObjectOutputStream}
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.util.concurrent.atomic.AtomicLong

import org.scache.deploy.DeployMessages._
import org.scache.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.scache.storage.{BlockId, ScacheBlockId}
import org.scache.util._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

/**
  * Created by frankfzw on 16-10-31.
  */


case class DaemonEndpointConfig(
    bindHost: Option[String] = None,
    bindPort: Option[Int] = None,
    clientHost: Option[String] = None,
    clientPort: Option[Int] = None,
    correlationPrefix: String = "daemon")

class Daemon(
  scacheHome: String,
  platform: String,
  endpointConfig: DaemonEndpointConfig) extends Logging {

  def this(scacheHome: String, platform: String) =
    this(scacheHome, platform, DaemonEndpointConfig())

  System.setProperty("SCACHE_DAEMON", s"daemon-${Utils.findLocalInetAddress().getHostName}")
  private val asyncThreadPool =
    ThreadUtils.newDaemonCachedThreadPool("scache-daemon-async-thread-pool")
  private implicit val asyncExecutionContext: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(asyncThreadPool)

  private val conf = new ScacheConf(scacheHome)
  private val clientPort: Int = {
    endpointConfig.clientPort.getOrElse {
      val envPort = System.getenv("SCACHE_CLIENT_PORT")
      if (envPort != null && envPort.nonEmpty) envPort.toInt
      else Integer.getInteger("scache.client.port", conf.getInt("scache.client.port", 5678))
    }
  }
  private val daemonPort = endpointConfig.bindPort.getOrElse(
    conf.getInt("scache.daemon.bind.port", conf.getInt("scache.daemon.port", 0)))
  private val host = endpointConfig.bindHost.getOrElse(
    conf.getString("scache.daemon.bind.host", Utils.findLocalInetAddress().getHostAddress))
  private val rpcEnv = RpcEnv.create("scache.daemon", host, daemonPort, conf, false)
  if (daemonPort > 0 && rpcEnv.address.port != daemonPort) {
    val actualPort = rpcEnv.address.port
    rpcEnv.shutdown()
    asyncExecutionContext.shutdownNow()
    throw new IllegalStateException(
      s"Daemon RPC bind failed operation=bind requested=$host:$daemonPort " +
        s"actual=$host:$actualPort rootCause=java.net.BindException:Address_already_in_use")
  }
  private val clientHost = endpointConfig.clientHost.getOrElse(
    conf.getString("scache.client.host",
      Option(System.getenv("SCACHE_CLIENT_HOST")).filter(_.nonEmpty).getOrElse(host)))
  private val helloCorrelationId =
    s"${endpointConfig.correlationPrefix}-hello-${ProcessHandle.current().pid()}-${System.nanoTime()}"
  private val clientRef: RpcEndpointRef = try {
    rpcEnv.setupEndpointRef(RpcAddress(clientHost, clientPort), "ScacheClient")
  } catch {
    case NonFatal(e) =>
      rpcEnv.shutdown()
      asyncExecutionContext.shutdownNow()
      throw endpointFailure("lookup", 1, 1,
        conf.getString("scache.rpc.lookupTimeout", "120s"), e)
  }
  val clientIdentity: ClientIdentity = try {
    val identity = clientRef.askWithRetry[ClientIdentity](ClientHello(1, helloCorrelationId))
    require(identity.protocolVersion == 1,
      s"Unsupported client protocolVersion=${identity.protocolVersion}")
    require(identity.rpcHost == clientHost && identity.rpcPort == clientPort,
      s"Client identity endpoint ${identity.rpcHost}:${identity.rpcPort} does not match " +
        s"configured $clientHost:$clientPort")
    identity
  } catch {
    case NonFatal(e) =>
      rpcEnv.shutdown()
      asyncExecutionContext.shutdownNow()
      throw endpointFailure("hello", 1, conf.getInt("scache.rpc.numRetries", 3) + 1,
        conf.getString("scache.rpc.askTimeout", "120s"), e)
  }
  val daemonAddress: RpcAddress = rpcEnv.address
  logInfo(s"SCACHE_DAEMON_READY correlationId=$helloCorrelationId pid=${ProcessHandle.current().pid()} " +
    s"daemonAddress=$daemonAddress target=$clientHost:$clientPort " +
    s"clientId=${clientIdentity.clientId} nodeEpoch=${clientIdentity.nodeEpoch} " +
    s"blockManagerId=${clientIdentity.blockManagerId} backend=${clientIdentity.backend} " +
    s"networkEnabled=${clientIdentity.networkEnabled} " +
    s"remoteFetchSupported=${clientIdentity.remoteFetchSupported} " +
    s"sharedCxlEnabled=${clientIdentity.sharedCxlEnabled}")

  private val putBlockAsync = conf.getBoolean("scache.daemon.putBlock.async", true)
  private val requestSequence = new AtomicLong(0L)

  private def endpointFailure(
      operation: String,
      attempt: Int,
      totalAttempts: Int,
      configuredTimeout: String,
      cause: Throwable): IllegalStateException = {
    var root = cause
    while (root.getCause != null && (root.getCause ne root)) root = root.getCause
    new IllegalStateException(
      s"Daemon Client RPC failed correlationId=$helloCorrelationId operation=$operation " +
        s"target=$clientHost:$clientPort attempt=$attempt/$totalAttempts " +
        s"configuredTimeout=$configuredTimeout rootCause=${root.getClass.getName}:" +
        Option(root.getMessage).getOrElse(""), cause)
  }

  private def askClient[T: ClassTag](operation: String, message: Any): T = {
    val correlationId = s"${endpointConfig.correlationPrefix}-$operation-${requestSequence.incrementAndGet()}"
    val totalAttempts = conf.getInt("scache.rpc.numRetries", 3) + 1
    try {
      val result = clientRef.askWithRetry[T](message)
      logDebug(s"SCACHE_DAEMON_RPC_COMPLETED correlationId=$correlationId operation=$operation " +
        s"target=$clientHost:$clientPort attempt=1/$totalAttempts")
      result
    } catch {
      case NonFatal(e) =>
        var root = e
        while (root.getCause != null && (root.getCause ne root)) root = root.getCause
        throw new IllegalStateException(
          s"Daemon Client RPC failed correlationId=$correlationId operation=$operation " +
            s"target=$clientHost:$clientPort attempt=$totalAttempts/$totalAttempts " +
            s"configuredTimeout=${conf.getString("scache.rpc.askTimeout", "120s")} " +
            s"rootCause=${root.getClass.getName}:${Option(root.getMessage).getOrElse("")}", e)
    }
  }

  private val ipcBackend = conf.getString("scache.daemon.ipc.backend", "files").trim.toLowerCase
  private val ipcMode = conf.getString("scache.daemon.ipc.mode", "remote").trim.toLowerCase
  private val ipcDirRemote = conf.getString(
    "scache.daemon.ipc.dir.remote",
    conf.getString("scache.daemon.ipc.dir", ScacheConf.scacheLocalDir))
  private val ipcDirGet = conf.getString(
    "scache.daemon.ipc.dir.get",
    conf.getString("scache.daemon.ipc.dir.local", ipcDirRemote))
  private val ipcPrepare = conf.getBoolean(
    "scache.daemon.ipc.prepare",
    ipcBackend == "pool" || ipcMode != "remote")

  private val ipcPoolPath = conf.getString(
    "scache.daemon.ipc.pool.path",
    new File(ScacheConf.scacheLocalDir, "scache-ipc.pool").getAbsolutePath)
  private val ipcPoolMapChunkBytes = {
    val bytes = conf.getSizeAsBytes("scache.daemon.ipc.pool.mapChunk", "256m")
    Math.min(bytes, Int.MaxValue.toLong).toInt
  }

  @volatile private var poolWriter: MmapPoolFile = null
  private val ipcDirRemoteFile = mkdirsOrWarn(ipcDirRemote)
  private val ipcDirGetFile = mkdirsOrWarn(ipcDirGet)

  // start daemon rpc thread
  doAsync[Unit]("Start Scache Daemon") {
    logInfo("Start deamon")
    rpcEnv.awaitTermination()
  }

  private def mkdirsOrWarn(path: String): File = {
    val dir = new File(path)
    if (!dir.exists() && !dir.mkdirs()) {
      logWarning(s"Failed to create daemon IPC directory: $path")
    }
    dir
  }

  private def putIpcFallbackFile(blockName: String): File = new File(ipcDirRemoteFile, blockName)
  private def getIpcFile(blockName: String): File = new File(ipcDirGetFile, blockName)

  private def getOrCreatePoolWriter(poolPath: String): MmapPoolFile = {
    val current = poolWriter
    if (current != null && current.path == poolPath) return current
    synchronized {
      val again = poolWriter
      if (again != null && again.path == poolPath) return again
      val created = MmapPoolFile.open(poolPath, mapChunkBytes = ipcPoolMapChunkBytes)
      poolWriter = created
      created
    }
  }

  /**
   * Allocate an IPC pool slice for the given block. Spark can write shuffle bytes directly
   * into the returned (poolPath, offset, length) region and then call `commitPutBlockPool`
   * to publish the block to the SCache client.
   */
  def preparePutBlockPool(blockId: String, size: Int): IpcPoolSlice = {
    val scacheBlockId = BlockId.apply(blockId)
    val ipc = askClient[IpcLocation]("prepare-put-pool", PreparePutBlock(scacheBlockId, size))
    ipc match {
      case slice: IpcPoolSlice => slice
      case other =>
        throw new IllegalStateException(
          s"Expected IpcPoolSlice for block $blockId (size=$size), but got $other")
    }
  }

  /**
   * Allocate a generic IPC location for the given block. Unlike `preparePutBlockPool`, this may
   * return either a detached file-backed location or a pool slice, depending on current daemon /
   * client configuration.
   */
  def preparePutBlock(blockId: String, size: Int): IpcLocation = {
    val scacheBlockId = BlockId.apply(blockId)
    askClient[IpcLocation]("prepare-put", PreparePutBlock(scacheBlockId, size))
  }

  /**
   * Publish a previously-prepared IPC pool slice as the final contents of the given block.
   * The SCache client will read the bytes from the IPC region and store them.
   */
  def commitPutBlockPool(blockId: String, size: Int, poolPath: String, offset: Long): Boolean = {
    val scacheBlockId = BlockId.apply(blockId)
    val resolvedPoolPath = if (poolPath != null && poolPath.nonEmpty) poolPath else ipcPoolPath
    askClient[Boolean]("commit-put-pool", PutBlock(
      scacheBlockId,
      size,
      IpcPoolSlice(resolvedPoolPath, offset, size)))
  }

  /**
   * Release a pool reservation when the Spark writer aborts before commit.  The payload is not
   * copied and no block-directory entry is created by this message.
   */
  def abortPutBlockPool(blockId: String, size: Int, poolPath: String, offset: Long): Boolean = {
    val scacheBlockId = BlockId.apply(blockId)
    val resolvedPoolPath = if (poolPath != null && poolPath.nonEmpty) poolPath else ipcPoolPath
    askClient[Boolean]("abort-put-pool", AbortPutBlock(
      scacheBlockId,
      size,
      IpcPoolSlice(resolvedPoolPath, offset, size)))
  }

  /**
   * Publish a previously prepared generic IPC location as the final contents of the given block.
   */
  def commitPutBlock(blockId: String, size: Int, ipc: IpcLocation): Boolean = {
    val scacheBlockId = BlockId.apply(blockId)
    askClient[Boolean]("commit-put", PutBlock(scacheBlockId, size, ipc))
  }

  /** Batch allocate pool slices: one RPC to Client for all blocks. */
  def preparePutBlocksPool(blockIds: Seq[String], sizes: Seq[Int]): Seq[Option[IpcPoolSlice]] = {
    require(blockIds.size == sizes.size,
      s"batch sizes mismatch: ids=${blockIds.size} sizes=${sizes.size}")
    val scacheBlockIds = blockIds.map(BlockId.apply)
    askClient[Seq[Option[IpcLocation]]]("prepare-put-batch",
      PreparePutBlocks(scacheBlockIds, sizes)).map {
      case Some(slice: IpcPoolSlice) => Some(slice)
      case _ => None
    }
  }

  /** Batch commit pool slices: one RPC to Client for all blocks. */
  def commitPutBlocksPool(specs: Seq[(String, Int, IpcPoolSlice)]): Seq[Boolean] = {
    val msgs = specs.map { case (blockId, size, slice) =>
      val scacheBlockId = BlockId.apply(blockId)
      PutBlock(scacheBlockId, size, slice)
    }
    askClient[Seq[Boolean]]("commit-put-batch", PutBlocks(msgs))
  }

  /** Batch putBlocks using pool slices: batch allocate → write → batch commit. */
  def putBlocksPool(
      scacheBlockIds: Array[String],
      dataArrays: Array[Array[Byte]]): Array[Boolean] = {
    require(scacheBlockIds.length == dataArrays.length,
      s"batch size mismatch: ids=${scacheBlockIds.length} data=${dataArrays.length}")
    if (scacheBlockIds.isEmpty) return Array.empty

    val sizes = dataArrays.map(_.length)
    val slices = preparePutBlocksPool(scacheBlockIds.toIndexedSeq, sizes.toIndexedSeq)

    val results = new Array[Boolean](scacheBlockIds.length)
    val validSpecs = Array.newBuilder[(String, Int, IpcPoolSlice)]

    var i = 0
    while (i < scacheBlockIds.length) {
      slices(i) match {
        case Some(slice) =>
          val data = dataArrays(i)
          val poolPath = if (slice.poolPath != null && slice.poolPath.nonEmpty) slice.poolPath
                         else ipcPoolPath
          val writer = getOrCreatePoolWriter(poolPath)
          writer.write(slice.offset, data, data.length)
          validSpecs += ((scacheBlockIds(i), data.length, slice))
          results(i) = true
        case None =>
          logWarning(s"Batch pool allocate failed for block ${scacheBlockIds(i)}; falling back")
          results(i) = false
          try {
            putBlock(scacheBlockIds(i), dataArrays(i), dataArrays(i).length, dataArrays(i).length)
            results(i) = true
          } catch {
            case NonFatal(e) =>
              logError(s"Fallback putBlock failed for ${scacheBlockIds(i)}", e)
          }
      }
      i += 1
    }

    val specsArr = validSpecs.result()
    if (specsArr.nonEmpty) {
      // A task attempt can fail after PREPARE (or after a partial COMMIT).  Return every
      // reservation that did not reach a successful commit before Spark retries the task; without
      // this, the next attempt sees the same logical block id and the arena keeps a stale lease.
      def abortSpec(spec: (String, Int, IpcPoolSlice)): Unit = {
        try abortPutBlockPool(spec._1, spec._2, spec._3.poolPath, spec._3.offset)
        catch { case NonFatal(e) => logWarning(s"Failed to abort pool reservation ${spec._1}", e) }
      }
      val commits = try {
        commitPutBlocksPool(specsArr.toIndexedSeq)
      } catch {
        case NonFatal(e) =>
          specsArr.foreach(abortSpec)
          throw e
      }
      // Map commit results back to original indices for pool-slice blocks
      var commitIdx = 0
      i = 0
      while (i < scacheBlockIds.length && commitIdx < specsArr.length) {
        if (slices(i).isDefined) {
          if (!commits(commitIdx)) {
            results(i) = false
            logWarning(s"Batch commit failed for block ${scacheBlockIds(i)}")
            abortSpec(specsArr(commitIdx))
          }
          commitIdx += 1
        }
        i += 1
      }
    }
    results
  }

  def putBlock(blockId: String, data: Array[Byte], rawLen: Int, compressedLen: Int): Unit = {
    val scacheBlockId = BlockId.apply(blockId)
    if (!scacheBlockId.isInstanceOf[ScacheBlockId]) {
      logError(s"Unexpected block type, except ScacheBlockId, got ${scacheBlockId.getClass.getSimpleName}")
    }
    logDebug(s"Start copying block $blockId with size $rawLen")

    def doPut(): Unit = {
      val preparedIpc: Option[IpcLocation] = if (ipcPrepare) {
        try {
          Some(askClient[IpcLocation]("prepare-put", PreparePutBlock(scacheBlockId, data.length)))
        } catch {
          case e: Exception =>
            logWarning(s"PreparePutBlock failed for $blockId; falling back to direct file write", e)
            None
        }
      } else None

      val preparedValid = preparedIpc.filter {
        case IpcFile(path) => path != null && path.trim.nonEmpty
        case IpcPoolSlice(_, offset, length) => offset >= 0 && length >= 0
      }

      val (ipc, prepared) = preparedValid match {
        case Some(loc) => (loc, true)
        case None => (IpcFile(putIpcFallbackFile(blockId).getAbsolutePath), false)
      }

      ipc match {
        case IpcPoolSlice(poolPath, offset, length) =>
          if (length > 0) {
            val writer = getOrCreatePoolWriter(if (poolPath.nonEmpty) poolPath else ipcPoolPath)
            writer.write(offset, data, length)
          }

        case IpcFile(path) =>
          val f = new File(path)
          val channelOptions =
            if (prepared) {
              Array(StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)
            } else {
              Array(StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
            }

          val channel = FileChannel.open(f.toPath, channelOptions: _*)
          try {
            if (data.isEmpty) {
              channel.truncate(0)
            } else {
              val buf = channel.map(FileChannel.MapMode.READ_WRITE, 0, data.length)
              buf.put(data)
            }
          } finally {
            channel.close()
          }
      }

      val res = askClient[Boolean]("put", PutBlock(scacheBlockId, data.length, ipc))
      if (res) {
        logDebug(s"Copy block $blockId succeeded")
      } else {
        throw new IllegalStateException(s"ScacheClient rejected PutBlock for $blockId")
      }
    }

    if (putBlockAsync) {
      doAsync[Unit](s"Copy block $blockId") {
        doPut()
      }
    } else {
      doPut()
    }
  }
  def getBlock(blockId: String): Option[Array[Byte]] = {
    val scacheBlockId = BlockId.apply(blockId)
    if (!scacheBlockId.isInstanceOf[ScacheBlockId]) {
      logError(s"Unexpected block type, except ScacheBlockId, got ${scacheBlockId.getClass.getSimpleName}")
      return None
    }
    val size = askClient[Int]("get", GetBlock(scacheBlockId))
    if (size < 0) {
      return None
    }
    if (size == 0) {
      return Some(new Array[Byte](0))
    }
    val f = getIpcFile(blockId)
    try {
      val channel = FileChannel.open(f.toPath, StandardOpenOption.READ, StandardOpenOption.DELETE_ON_CLOSE)
      try {
        val buf = channel.map(FileChannel.MapMode.READ_ONLY, 0, size)
        val arrayBuf = new Array[Byte](size)
        buf.get(arrayBuf)
        Some(arrayBuf)
      } finally {
        channel.close()
      }
    } catch {
      case e: Exception =>
        logWarning(s"Failed to read block $blockId from IPC file", e)
        None
    }
  }

  /**
   * Return an IPC location for the given block without materializing a large byte[].
   *
   * This may return either:
   *   - IpcFile(path): per-block IPC file prepared by the SCache client, or
   *   - IpcPoolSlice(poolPath, offset, length): a slice in a shared pool file (e.g., fsdax/CXL).
   */
  def getBlockIpc(blockId: String): Option[IpcBlock] = {
    val scacheBlockId = BlockId.apply(blockId)
    if (!scacheBlockId.isInstanceOf[ScacheBlockId]) {
      return None
    }
    askClient[Option[IpcBlock]]("get-ipc", GetBlockIpc(scacheBlockId))
  }

  def getBlocksIpc(blockIds: Seq[String]): Seq[Option[IpcBlock]] = {
    val scacheBlockIds = blockIds.map(BlockId.apply).filter(_.isInstanceOf[ScacheBlockId])
    if (scacheBlockIds.isEmpty) return Seq.empty
    askClient[Seq[Option[IpcBlock]]]("get-ipc-batch", GetBlocksIpc(scacheBlockIds))
  }

  /**
   * Release only a reducer-imported UB destination slice. Owner blocks are deliberately a no-op:
   * their lifetime remains shuffle-scoped and they may still be needed by another reducer.
   */
  def releaseImportedBlock(blockId: String): Boolean = {
    val parsed = BlockId.apply(blockId)
    if (!parsed.isInstanceOf[ScacheBlockId]) return false
    askClient[Boolean]("release-imported-block", ReleaseImportedBlock(parsed))
  }

  def registerShuffles(jobId: Int, shuffleIds: Array[Int], maps: Array[Int], reduces: Array[Int]): Unit = {
    // Registration must be synchronous to ensure the shuffle is registered before returning.
    // Using doAsync here causes a race condition where tasks may try to read shuffle data
    // before the shuffle is registered, leading to "Shuffle is unregistered" errors.
    val res = askClient[Boolean]("register-shuffle",
      RegisterShuffle(platform, jobId, shuffleIds, maps, reduces))
    if (res) {
      logInfo(s"Register shuffles ${shuffleIds.mkString(",")} succeeded")
    } else {
      logWarning(s"Register shuffles ${shuffleIds.mkString(",")} failed")
    }
  }

  def registerShufflesChecked(
      jobId: Int,
      shuffleIds: Array[Int],
      maps: Array[Int],
      reduces: Array[Int]): Boolean = {
    val res = askClient[Boolean]("register-shuffle",
      RegisterShuffle(platform, jobId, shuffleIds, maps, reduces))
    if (!res) {
      throw new IllegalStateException(
        s"ScacheClient rejected RegisterShuffle platform=$platform jobId=$jobId " +
          s"shuffleIds=${shuffleIds.mkString(",")}")
    }
    res
  }
  def mapEnd(jobId: Int, shuffleId: Int, mapId: Int): Unit = {
    doAsync[Unit] ("Map End") {
      clientRef.send(MapEnd(platform, jobId, shuffleId, mapId))
    }
  }
  def getShuffleStatus(jobId: Int, shuffleId: Int): mutable.HashMap[Int, Array[String]] = {
    val statuses = askClient[ShuffleStatus]("get-shuffle-status",
      GetShuffleStatus(platform, jobId, shuffleId))
    val ret = new mutable.HashMap[Int, Array[String]]
    for (rs <- statuses.reduceArray) {
      val hosts = Array(rs.host) ++ rs.backups
      ret += (rs.id -> hosts)
    }
    ret
  }

  def releaseShuffle(appName: String, jobId: Int, shuffleId: Int): Int = {
    askClient[Int]("release-shuffle", ReleaseShuffle(appName, jobId, shuffleId))
  }

  def releaseApplication(appName: String): Int = {
    askClient[Int]("release-application", ReleaseApplication(appName))
  }

  def stop(): Unit = {
    rpcEnv.shutdown()
    asyncThreadPool.shutdown()
    logInfo(s"SCACHE_DAEMON_STOPPED pid=${ProcessHandle.current().pid()} daemonAddress=$daemonAddress " +
      s"target=$clientHost:$clientPort")
  }

  private def doAsync[T](actionMessage: String)(body: => T): Unit = {
    val future = Future {
      logDebug(actionMessage)
      body
    }
    future.onComplete {
      case Success(response) =>
        logDebug("Done " + actionMessage + ", response is " + response)
      case Failure(t) =>
        logError("Error in " + actionMessage, t)
    }(ThreadUtils.sameThread)
  }
}
