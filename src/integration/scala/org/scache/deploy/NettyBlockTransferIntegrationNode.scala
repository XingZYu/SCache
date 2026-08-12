package org.scache.deploy

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import java.lang.management.ManagementFactory
import java.net.{InetAddress, ServerSocket, Socket}
import java.nio.ByteBuffer
import java.nio.file.{Files, Paths}
import java.util.concurrent.{Executors, TimeUnit}
import java.util.zip.CRC32

import org.scache.io.ChunkedByteBuffer
import org.scache.network.netty.NettyBlockTransferService
import org.scache.network.ub.UBBlockTransferService
import org.scache.rpc.{RpcAddress, RpcEndpointAddress, RpcEnv}
import org.scache.storage.{BlockId, ScacheBlockId, StorageLevel}
import org.scache.util.{Logging, ScacheConf}

import scala.util.control.NonFatal

/**
 * Multi-process integration-test worker.
 *
 * Each invocation creates one real ScacheClient, BlockManager and NettyBlockTransferService in its
 * own OS process. A tiny line-oriented control socket generates deterministic bytes locally, so
 * test control traffic never carries block payload. Remote reads still go through
 * BlockManager.getRemoteBytes -> NettyBlockTransferService.fetchBlocks.
 */
private[scache] object NettyBlockTransferIntegrationNode extends Logging {

  private final case class Options(
      name: String = "",
      host: String = "",
      rpcPort: Int = 0,
      controlPort: Int = 0,
      masterHost: String = "127.0.0.1",
      masterPort: Int = 0,
      backend: String = "netty",
      wireRole: String = "",
      wirePath: String = "",
      ubControlPort: Int = 0,
      ubDevice: String = "",
      arenaBytes: Int = 0,
      arenaCount: Int = 2,
      queueDepth: Int = 128,
      chunkSize: Int = 4096,
      leaseTimeoutMs: Int = 60000,
      testReadDelayMs: Int = 0,
      timeoutMs: Long = 30000L)

  @volatile private var running = true

  def main(args: Array[String]): Unit = {
    val options = parseArgs(args.toList, Options())
    require(options.name.nonEmpty, "--name is required")
    require(options.host.nonEmpty, "--host is required")
    require(options.rpcPort > 0, "--rpc-port must be positive")
    require(options.controlPort > 0, "--control-port must be positive")
    require(options.masterPort > 0, "--master-port must be positive")

    val conf = new ScacheConf()
    conf.set("scache.driver.host", options.masterHost, slient = true)
    conf.set("scache.driver.port", options.masterPort.toString, slient = true)
    conf.set("scache.app.id", s"integration-${options.backend}", slient = true)
    conf.set("scache.blockTransfer.backend", options.backend, slient = true)
    if (options.backend == "ub") {
      require(options.wireRole == "listen" || options.wireRole == "connect",
        "--wire-role listen|connect is required for backend=ub")
      require(options.wirePath.nonEmpty, "--wire-path is required for backend=ub")
      conf.set("spark.urma.strict", "true", slient = true)
      conf.set("spark.urma.wireRole", options.wireRole, slient = true)
      conf.set("spark.urma.wirePath", options.wirePath, slient = true)
      conf.set("spark.urma.controlBindHost", options.host, slient = true)
      conf.set("spark.urma.controlAdvertiseHost", options.host, slient = true)
      conf.set("spark.urma.controlPort", options.ubControlPort.toString, slient = true)
      conf.set("spark.urma.nodeId", options.name, slient = true)
      if (options.ubDevice.nonEmpty) {
        conf.set(s"spark.urma.device.${options.wireRole}", options.ubDevice, slient = true)
      }
      if (options.arenaBytes > 0) conf.set("spark.urma.arenaBytes", options.arenaBytes.toString, slient = true)
      conf.set("spark.urma.arenaCount", options.arenaCount.toString, slient = true)
      // These values must be applied to the SCache client itself. Spark executor
      // --conf values only configure the Spark-side transport; without copying them
      // here the client silently retained the historical 4 KiB default while the
      // executor advertised a larger operation, causing avoidable fragmentation.
      require(options.queueDepth > 0, "--queue-depth must be positive")
      require(options.chunkSize > 0, "--chunk-size must be positive")
      conf.set("spark.urma.queueDepth", options.queueDepth.toString, slient = true)
      conf.set("spark.urma.chunkSize", options.chunkSize.toString, slient = true)
      conf.set("spark.urma.leaseTimeoutMs", options.leaseTimeoutMs.toString, slient = true)
      conf.set("spark.urma.testReadDelayMs", options.testReadDelayMs.toString, slient = true)
      conf.set("spark.urma.transferTimeoutMs", options.timeoutMs.toString, slient = true)
      conf.set("spark.urma.unpublishTimeoutMs", options.timeoutMs.toString, slient = true)
    }

    val rpcEnv = RpcEnv.create(s"scache.client.integration.${options.name}",
      options.host, options.rpcPort, conf)
    val masterAddress = RpcAddress(options.masterHost, options.masterPort)
    val endpoint = new ScacheClient(
      rpcEnv,
      options.host,
      RpcEndpointAddress(masterAddress, "ScacheMaster").toString,
      options.rpcPort,
      conf)
    rpcEnv.setupEndpoint("ScacheClient", endpoint)

    awaitReady(endpoint, options.timeoutMs)
    val server = new ServerSocket(options.controlPort, 64, InetAddress.getByName(options.host))
    server.setSoTimeout(1000)
    val workers = Executors.newFixedThreadPool(16)
    val pid = ProcessHandle.current().pid()
    val backend = conf.getString("scache.blockTransfer.backend", "netty")
    val networkEnabled = conf.getBoolean("scache.storage.network.enabled", true)
    val sharedCxl = conf.getBoolean("scache.storage.cxl.shared.enabled", false)
    println(s"SCACHE_TEST_NODE_READY name=${options.name} pid=$pid clientId=${endpoint.clientId} " +
      s"rpc=${options.host}:${options.rpcPort} control=${options.host}:${options.controlPort} " +
      s"blockManagerId=${endpoint.blockManager.blockManagerId} backend=$backend " +
      s"networkEnabled=$networkEnabled sharedCxl=$sharedCxl " +
      s"urmaQueueDepth=${conf.getInt("spark.urma.queueDepth", 128)} " +
      s"urmaChunkSize=${conf.getInt("spark.urma.chunkSize", 4096)}")
    System.out.flush()

    try {
      while (running) {
        try {
          val socket = server.accept()
          workers.execute(new Runnable {
            override def run(): Unit = handle(socket, endpoint, options)
          })
        } catch {
          case _: java.net.SocketException if !running =>
          case _: java.net.SocketTimeoutException =>
        }
      }
    } finally {
      running = false
      try server.close() catch { case NonFatal(_) => }
      workers.shutdown()
      workers.awaitTermination(10, TimeUnit.SECONDS)
      if (endpoint.blockManager != null) endpoint.blockManager.stop()
      rpcEnv.shutdown()
      rpcEnv.awaitTermination()
      println(s"SCACHE_TEST_NODE_STOPPED name=${options.name} pid=$pid")
    }
  }

  private def handle(socket: Socket, endpoint: ScacheClient, options: Options): Unit = {
    try {
      socket.setSoTimeout(options.timeoutMs.toInt)
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, "UTF-8"))
      val writer = new PrintWriter(socket.getOutputStream, true)
      val line = reader.readLine()
      if (line == null) return
      val parts = line.trim.split("\\s+").toList
      try {
        writer.println(execute(parts, endpoint, options))
      } catch {
        case NonFatal(e) =>
          // The integration launcher may deliberately use a minimal SCACHE_HOME with no log4j
          // appender; preserve the root cause in its process log in that case.
          e.printStackTrace(System.err)
          writer.println(s"ERROR type=${e.getClass.getName} message=${sanitize(e.getMessage)}")
          logError(s"SCACHE_TEST_CONTROL_ERROR node=${options.name} command=$line", e)
      }
    } finally {
      try socket.close() catch { case NonFatal(_) => }
    }
  }

  private def execute(
      command: List[String], endpoint: ScacheClient, options: Options): String = command match {
    case "INFO" :: Nil =>
      val bmId = endpoint.blockManager.blockManagerId
      val conf = ScacheConf.getConf()
      s"OK name=${options.name} pid=${ProcessHandle.current().pid()} clientId=${endpoint.clientId} " +
        s"nodeEpoch=${endpoint.nodeEpoch} " +
        s"blockManagerId=${sanitize(bmId.toString)} nettyHost=${bmId.host} nettyPort=${bmId.port} " +
        s"backend=${conf.getString("scache.blockTransfer.backend", "netty")} " +
        s"networkEnabled=${conf.getBoolean("scache.storage.network.enabled", true)} " +
        s"sharedCxl=${conf.getBoolean("scache.storage.cxl.shared.enabled", false)}"

    case "REGISTER" :: app :: job :: shuffle :: maps :: reduces :: Nil =>
      val ok = endpoint.registerShuffle(
        app, job.toInt, Array(shuffle.toInt), Array(maps.toInt), Array(reduces.toInt))
      s"OK registered=$ok app=$app job=$job shuffle=$shuffle maps=$maps reduces=$reduces"

    case "PUT" :: id :: size :: seed :: Nil =>
      val blockId = requireScacheBlock(id)
      val bytes = deterministicBytes(size.toInt, seed.toLong)
      val crc = crc32(bytes)
      val stored = endpoint.blockManager.putBytes[Array[Byte]](
        blockId, new ChunkedByteBuffer(ByteBuffer.wrap(bytes)), StorageLevel.MEMORY_ONLY_SER)
      s"OK stored=$stored blockId=$id length=${bytes.length} crc=$crc"

    case "LOCAL" :: id :: Nil =>
      val blockId = requireScacheBlock(id)
      endpoint.blockManager.getLocalBytes(blockId) match {
        case Some(buffer) =>
          try {
            val bytes = buffer.toArray
            s"OK localHit=true blockId=$id length=${bytes.length} crc=${crc32(bytes)}"
          } finally {
            endpoint.blockManager.releaseLock(blockId)
          }
        case None => s"OK localHit=false blockId=$id length=-1 crc=-1"
      }

    case "LOCATIONS" :: id :: Nil =>
      val blockId = requireScacheBlock(id)
      val locations = endpoint.blockManager.master.getLocations(blockId)
      s"OK blockId=$id count=${locations.size} locations=${sanitize(locations.mkString(","))}"

    case "FETCH" :: id :: size :: seed :: Nil =>
      val blockId = requireScacheBlock(id)
      val localBefore = endpoint.blockManager.getLocalBytes(blockId)
      if (localBefore.isDefined) {
        endpoint.blockManager.releaseLock(blockId)
        throw new IllegalStateException(s"consumer local hit before remote fetch for $id")
      }
      val expected = deterministicBytes(size.toInt, seed.toLong)
      val started = System.nanoTime()
      val remote = endpoint.blockManager.getRemoteBytes(blockId).getOrElse(
        throw new IllegalStateException(s"remote block not found: $id"))
      val actual = remote.toArray
      val elapsedNs = System.nanoTime() - started
      val expectedCrc = crc32(expected)
      val actualCrc = crc32(actual)
      if (!java.util.Arrays.equals(expected, actual)) {
        throw new IllegalStateException(
          s"payload mismatch for $id expectedLength=${expected.length} actualLength=${actual.length} " +
            s"expectedCrc=$expectedCrc actualCrc=$actualCrc")
      }
      s"OK fetched=true localMissBeforeFetch=true blockId=$id expectedLength=${expected.length} " +
        s"actualLength=${actual.length} expectedCrc=$expectedCrc actualCrc=$actualCrc elapsedNs=$elapsedNs"

    case "UPLOAD" :: id :: remoteHost :: remotePort :: Nil =>
      val blockId = requireScacheBlock(id)
      val data = endpoint.blockManager.getBlockData(blockId)
      try {
        endpoint.blockTransferService.uploadBlockSync(remoteHost, remotePort.toInt, "", blockId,
          data, StorageLevel.MEMORY_ONLY_SER, scala.reflect.ClassTag.Any)
      } finally {
        endpoint.blockManager.releaseLock(blockId)
      }
      s"OK uploaded=true blockId=$id remote=$remoteHost:$remotePort"

    case "DESCRIPTOR" :: remoteHost :: remotePort :: id :: Nil =>
      endpoint.blockTransferService match {
        case ub: UBBlockTransferService =>
          s"OK blockId=$id ${ub.probeRemoteDescriptor(remoteHost, remotePort.toInt, id)}"
        case other => throw new IllegalStateException(s"descriptor probe requires UB backend, got ${other.getClass.getName}")
      }

    case "CONTROL_FAULT" :: remoteHost :: remotePort :: caseId :: id :: Nil =>
      endpoint.blockTransferService match {
        case ub: UBBlockTransferService =>
          s"OK ${ub.runControlFault(remoteHost, remotePort.toInt, caseId, id)}"
        case other => throw new IllegalStateException(s"control fault requires UB backend, got ${other.getClass.getName}")
      }

    case "STALE_EPOCH" :: remoteHost :: remotePort :: id :: staleEpoch :: Nil =>
      endpoint.blockTransferService match {
        case ub: UBBlockTransferService =>
          s"OK ${ub.assertStaleRemoteEpoch(remoteHost, remotePort.toInt, id, staleEpoch.toLong)}"
        case other => throw new IllegalStateException(s"stale epoch probe requires UB backend, got ${other.getClass.getName}")
      }

    case "METRICS" :: Nil =>
      endpoint.blockTransferService match {
        case netty: NettyBlockTransferService =>
          val fields = netty.metricsSnapshot.toSeq.sortBy(_._1)
            .map { case (key, value) => s"$key=$value" }.mkString(" ")
          s"OK $fields"
        case ub: UBBlockTransferService =>
          val fields = ub.urmaMetrics.toSeq.sortBy(_._1)
            .map { case (key, value) => s"$key=$value" }.mkString(" ")
          s"OK $fields"
        case other =>
          throw new IllegalStateException(s"expected Netty backend, got ${other.getClass.getName}")
      }

    case "CXLSTATS" :: Nil =>
      endpoint.blockManager.master.getCxlPoolStats("") match {
        case Some(stats) =>
          s"OK domainId=${stats.domainId} freeBytes=${stats.freeBytes} " +
            s"totalBytes=${stats.totalBytes} allocationCount=${stats.allocationCount} " +
            s"freeSegmentCount=${stats.freeSegmentCount} " +
            f"fragmentationRatio=${stats.fragmentationRatio}%.9f"
        case None =>
          throw new IllegalStateException("shared CXL pool is not registered")
      }

    case "RESOURCES" :: Nil =>
      val status = Files.readAllLines(Paths.get("/proc/self/status"))
      val rss = (0 until status.size()).map(status.get).find(_.startsWith("VmRSS:"))
        .map(_.trim.replaceAll("\\s+", "_")).getOrElse("VmRSS:_unknown")
      val fdStream = Files.list(Paths.get("/proc/self/fd"))
      val fd = try fdStream.count() finally fdStream.close()
      val threads = Thread.getAllStackTraces.size()
      val cpuNs = ProcessHandle.current().info().totalCpuDuration()
        .map(_.toNanos).orElse(-1L)
      val threadBean = ManagementFactory.getThreadMXBean
      val allocatedBytes = threadBean match {
        case bean: com.sun.management.ThreadMXBean if bean.isThreadAllocatedMemorySupported =>
          if (!bean.isThreadAllocatedMemoryEnabled) bean.setThreadAllocatedMemoryEnabled(true)
          bean.getAllThreadIds.iterator.map(bean.getThreadAllocatedBytes).filter(_ >= 0L).sum
        case _ => -1L
      }
      val ubFields = endpoint.blockTransferService match {
        case ub: UBBlockTransferService => ub.urmaMetrics.toSeq.sortBy(_._1)
          .map { case (key, value) => s"$key=$value" }.mkString(" ")
        case _ => "urma.activeLeases=0 urma.registeredRegions=0 urma.activeImports=0 urma.activeTransports=0"
      }
      s"OK pid=${ProcessHandle.current().pid()} fd=$fd threads=$threads cpuNs=$cpuNs " +
        s"allocatedBytes=$allocatedBytes $rss $ubFields"

    case "CAPABILITIES" :: Nil =>
      val capabilities = endpoint.blockManager.master.getBlockManagerCapabilities
        .sortBy(_.clientId)
        .map { capability =>
          s"clientId=${capability.clientId},nodeEpoch=${capability.nodeEpoch}," +
            s"blockManagerId=${sanitize(capability.blockManagerId.toString)}," +
            s"backend=${capability.backend},networkEnabled=${capability.networkEnabled}," +
            s"remoteFetchSupported=${capability.remoteFetchSupported}," +
            s"sharedCxlEnabled=${capability.sharedCxlEnabled}," +
            s"rpc=${capability.rpcHost}:${capability.rpcPort}"
        }.mkString(";")
      s"OK count=${endpoint.blockManager.master.getBlockManagerCapabilities.size} entries=$capabilities"

    case "PREFETCH_RESULTS" :: Nil =>
      val results = endpoint.blockManager.master.getPrefetchResults.map { result =>
        s"correlationId=${result.correlationId},status=${result.status}," +
          s"sourceExecutorId=${result.sourceBlockManagerId.executorId}," +
          s"targetExecutorId=${result.targetBlockManagerId.executorId}," +
          s"source=${sanitize(result.sourceBlockManagerId.toString)}," +
          s"target=${sanitize(result.targetBlockManagerId.toString)}," +
          s"blockCount=${result.blockCount},submitted=${result.submitted}," +
          s"completed=${result.completed},failed=${result.failed}," +
          s"payloadBytes=${result.payloadBytes},elapsedMs=${result.elapsedMs}," +
          s"errorType=${sanitize(result.errorType)},errorMessage=${sanitize(result.errorMessage)}"
      }.mkString(";")
      s"OK count=${endpoint.blockManager.master.getPrefetchResults.size} entries=$results"

    case "REMOVE" :: id :: Nil =>
      endpoint.blockManager.removeBlock(requireScacheBlock(id))
      s"OK removed=true blockId=$id"

    case "STOP" :: Nil =>
      running = false
      s"OK stopping=true name=${options.name}"

    case _ =>
      throw new IllegalArgumentException(s"unknown command: ${command.mkString(" ")}")
  }

  private def requireScacheBlock(id: String): ScacheBlockId = BlockId(id) match {
    case blockId: ScacheBlockId => blockId
    case other => throw new IllegalArgumentException(s"expected ScacheBlockId, got $other")
  }

  private def deterministicBytes(size: Int, seed: Long): Array[Byte] = {
    require(size >= 0, s"negative size: $size")
    val bytes = new Array[Byte](size)
    val random = new java.util.Random(seed)
    random.nextBytes(bytes)
    bytes
  }

  private def crc32(bytes: Array[Byte]): Long = {
    val crc = new CRC32
    crc.update(bytes)
    crc.getValue
  }

  private def awaitReady(endpoint: ScacheClient, timeoutMs: Long): Unit = {
    val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs)
    while ((endpoint.blockManager == null || endpoint.blockManager.blockManagerId == null ||
        endpoint.clientId < 0) && System.nanoTime() < deadline) {
      Thread.sleep(20L)
    }
    if (endpoint.blockManager == null || endpoint.blockManager.blockManagerId == null ||
        endpoint.clientId < 0) {
      throw new IllegalStateException(s"Timed out after ${timeoutMs}ms waiting for ScacheClient ready")
    }
  }

  private def sanitize(value: String): String =
    Option(value).getOrElse("").replaceAll("\\s+", "_")

  private def parseArgs(args: List[String], options: Options): Options = args match {
    case "--name" :: value :: tail => parseArgs(tail, options.copy(name = value))
    case "--host" :: value :: tail => parseArgs(tail, options.copy(host = value))
    case "--rpc-port" :: value :: tail => parseArgs(tail, options.copy(rpcPort = value.toInt))
    case "--control-port" :: value :: tail => parseArgs(tail, options.copy(controlPort = value.toInt))
    case "--master-host" :: value :: tail => parseArgs(tail, options.copy(masterHost = value))
    case "--master-port" :: value :: tail => parseArgs(tail, options.copy(masterPort = value.toInt))
    case "--backend" :: value :: tail => parseArgs(tail, options.copy(backend = value.trim.toLowerCase))
    case "--ub-transport" :: value :: tail =>
      require(value.trim.equalsIgnoreCase("real"), "--ub-transport must be real")
      parseArgs(tail, options)
    case "--wire-role" :: value :: tail => parseArgs(tail, options.copy(wireRole = value.trim.toLowerCase))
    case "--wire-path" :: value :: tail => parseArgs(tail, options.copy(wirePath = value))
    case "--ub-control-port" :: value :: tail => parseArgs(tail, options.copy(ubControlPort = value.toInt))
    case "--ub-device" :: value :: tail => parseArgs(tail, options.copy(ubDevice = value))
    case "--arena-bytes" :: value :: tail => parseArgs(tail, options.copy(arenaBytes = value.toInt))
    case "--arena-count" :: value :: tail => parseArgs(tail, options.copy(arenaCount = value.toInt))
    case "--queue-depth" :: value :: tail => parseArgs(tail, options.copy(queueDepth = value.toInt))
    case "--chunk-size" :: value :: tail => parseArgs(tail, options.copy(chunkSize = value.toInt))
    case "--lease-timeout-ms" :: value :: tail => parseArgs(tail, options.copy(leaseTimeoutMs = value.toInt))
    case "--test-read-delay-ms" :: value :: tail => parseArgs(tail, options.copy(testReadDelayMs = value.toInt))
    case "--timeout-ms" :: value :: tail => parseArgs(tail, options.copy(timeoutMs = value.toLong))
    case Nil => options
    case other => throw new IllegalArgumentException(s"unrecognized arguments: ${other.mkString(" ")}")
  }
}
