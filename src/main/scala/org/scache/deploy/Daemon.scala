package org.scache.deploy

import java.io.{ByteArrayOutputStream, File, ObjectOutputStream}
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption

import org.scache.deploy.DeployMessages._
import org.scache.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.scache.storage.{BlockId, ScacheBlockId}
import org.scache.util._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.{Failure, Success}

/**
  * Created by frankfzw on 16-10-31.
  */


class Daemon(
  scacheHome: String,
  platform: String) extends Logging {

  System.setProperty("SCACHE_DAEMON", s"daemon-${Utils.findLocalInetAddress().getHostName}")
  private val asyncThreadPool =
    ThreadUtils.newDaemonCachedThreadPool("scache-daemon-async-thread-pool")
  private implicit val asyncExecutionContext: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(asyncThreadPool)

  private val conf = new ScacheConf(scacheHome)
  private val clientPort = conf.getInt("scache.client.port", 5678)
  private val daemonPort = conf.getInt("scache.daemon.port", 12345)
  private val host = Utils.findLocalInetAddress().getHostAddress
  private val rpcEnv = RpcEnv.create("scache.daemon", host, daemonPort, conf, true)
  private val clientRef = rpcEnv.setupEndpointRef(RpcAddress(host, clientPort), "ScacheClient")

  private val putBlockAsync = conf.getBoolean("scache.daemon.putBlock.async", true)

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
    val ipc = clientRef.askWithRetry[IpcLocation](PreparePutBlock(scacheBlockId, size))
    ipc match {
      case slice: IpcPoolSlice => slice
      case other =>
        throw new IllegalStateException(
          s"Expected IpcPoolSlice for block $blockId (size=$size), but got $other")
    }
  }

  /**
   * Publish a previously-prepared IPC pool slice as the final contents of the given block.
   * The SCache client will read the bytes from the IPC region and store them.
   */
  def commitPutBlockPool(blockId: String, size: Int, poolPath: String, offset: Long): Boolean = {
    val scacheBlockId = BlockId.apply(blockId)
    val resolvedPoolPath = if (poolPath != null && poolPath.nonEmpty) poolPath else ipcPoolPath
    clientRef.askWithRetry[Boolean](PutBlock(
      scacheBlockId,
      size,
      IpcPoolSlice(resolvedPoolPath, offset, size)))
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
          Some(clientRef.askWithRetry[IpcLocation](PreparePutBlock(scacheBlockId, data.length)))
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
          val writer = getOrCreatePoolWriter(if (poolPath.nonEmpty) poolPath else ipcPoolPath)
          writer.write(offset, data, length)

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
            val buf = channel.map(FileChannel.MapMode.READ_WRITE, 0, data.length)
            buf.put(data)
          } finally {
            channel.close()
          }
      }

      val res = clientRef.askWithRetry[Boolean](PutBlock(scacheBlockId, data.length, ipc))
      if (res) {
        logDebug(s"Copy block $blockId succeeded")
      } else {
        logDebug(s"Copy block $blockId failed")
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
    val size = clientRef.askWithRetry[Int](GetBlock(scacheBlockId))
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
    clientRef.askWithRetry[Option[IpcBlock]](GetBlockIpc(scacheBlockId))
  }
  def registerShuffles(jobId: Int, shuffleIds: Array[Int], maps: Array[Int], reduces: Array[Int]): Unit = {
    doAsync[Unit] ("Register Shuffles") {
      val res = clientRef.askWithRetry[Boolean](RegisterShuffle(platform, jobId, shuffleIds, maps, reduces))
      if (res) {
        logInfo(s"Register shuffles ${shuffleIds} succeeded")
      } else {
        logInfo(s"Register shuffles ${shuffleIds} failed")
      }
    }
  }
  def mapEnd(jobId: Int, shuffleId: Int, mapId: Int): Unit = {
    doAsync[Unit] ("Map End") {
      clientRef.send(MapEnd(platform, jobId, shuffleId, mapId))
    }
  }
  def getShuffleStatus(jobId: Int, shuffleId: Int): mutable.HashMap[Int, Array[String]] = {
    val statuses = clientRef.askWithRetry[ShuffleStatus](GetShuffleStatus(platform, jobId, shuffleId))
    val ret = new mutable.HashMap[Int, Array[String]]
    for (rs <- statuses.reduceArray) {
      val hosts = Array(rs.host) ++ rs.backups
      ret += (rs.id -> hosts)
    }
    ret
  }

  def stop(): Unit = {
    rpcEnv.shutdown()
    asyncThreadPool.shutdown()
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

object Daemon {
  def main(args: Array[String]): Unit = {
    val daemon = new Daemon("/home/spark/SCache", "test")
    daemon.putBlock(ScacheBlockId("test", 0, 0, 0, 0).toString, Array[Byte](5), 5, 5)
    Thread.sleep(10000)
  }
}
