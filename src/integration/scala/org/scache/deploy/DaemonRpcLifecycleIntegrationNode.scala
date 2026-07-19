package org.scache.deploy

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import java.net.{InetAddress, ServerSocket, Socket}
import java.util.concurrent.{Executors, TimeUnit}
import java.util.zip.CRC32

import scala.util.control.NonFatal

import org.scache.util.Logging

/**
 * External phase-2 probe for the real Daemon -> ScacheClient RPC/IPC path.
 *
 * The line control protocol carries only block id, size and seed. Payload is generated in this
 * JVM, then sent through Daemon's production IPC path rather than through the control socket.
 */
private[scache] object DaemonRpcLifecycleIntegrationNode extends Logging {

  private final case class Options(
      name: String = "",
      scacheHome: String = "",
      platform: String = "integration",
      bindHost: String = "",
      daemonPort: Int = 0,
      clientHost: String = "",
      clientPort: Int = 0,
      controlHost: String = "",
      controlPort: Int = 0,
      timeoutMs: Long = 10000L)

  @volatile private var running = true

  def main(args: Array[String]): Unit = {
    val options = parseArgs(args.toList, Options())
    require(options.name.nonEmpty, "--name is required")
    require(options.scacheHome.nonEmpty, "--scache-home is required")
    require(options.bindHost.nonEmpty, "--bind-host is required")
    require(options.clientHost.nonEmpty, "--client-host is required")
    require(options.clientPort > 0, "--client-port must be positive")
    require(options.controlHost.nonEmpty, "--control-host is required")
    require(options.controlPort > 0, "--control-port must be positive")

    val daemon = new Daemon(
      options.scacheHome,
      options.platform,
      DaemonEndpointConfig(
        bindHost = Some(options.bindHost),
        bindPort = Some(options.daemonPort),
        clientHost = Some(options.clientHost),
        clientPort = Some(options.clientPort),
        correlationPrefix = options.name))
    val server = new ServerSocket(
      options.controlPort, 64, InetAddress.getByName(options.controlHost))
    server.setSoTimeout(1000)
    val workers = Executors.newFixedThreadPool(8)
    val pid = ProcessHandle.current().pid()
    val identity = daemon.clientIdentity
    println(s"SCACHE_TEST_DAEMON_PROBE_READY name=${options.name} pid=$pid " +
      s"daemonRpc=${daemon.daemonAddress} control=${options.controlHost}:${options.controlPort} " +
      s"target=${options.clientHost}:${options.clientPort} clientId=${identity.clientId} " +
      s"nodeEpoch=${identity.nodeEpoch} blockManagerId=${identity.blockManagerId} " +
      s"backend=${identity.backend} networkEnabled=${identity.networkEnabled} " +
      s"remoteFetchSupported=${identity.remoteFetchSupported} " +
      s"sharedCxlEnabled=${identity.sharedCxlEnabled}")
    System.out.flush()

    try {
      while (running) {
        try {
          val socket = server.accept()
          workers.execute(new Runnable {
            override def run(): Unit = handle(socket, daemon, options)
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
      daemon.stop()
      println(s"SCACHE_TEST_DAEMON_PROBE_STOPPED name=${options.name} pid=$pid")
    }
  }

  private def handle(socket: Socket, daemon: Daemon, options: Options): Unit = {
    try {
      socket.setSoTimeout(options.timeoutMs.toInt)
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, "UTF-8"))
      val writer = new PrintWriter(socket.getOutputStream, true)
      val line = reader.readLine()
      if (line == null) return
      try {
        writer.println(execute(line.trim.split("\\s+").toList, daemon, options))
      } catch {
        case NonFatal(e) =>
          writer.println(s"ERROR type=${e.getClass.getName} message=${sanitize(e.getMessage)}")
          logError(s"SCACHE_TEST_DAEMON_CONTROL_ERROR name=${options.name} command=$line", e)
      }
    } finally {
      try socket.close() catch { case NonFatal(_) => }
    }
  }

  private def execute(command: List[String], daemon: Daemon, options: Options): String = command match {
    case "INFO" :: Nil =>
      val identity = daemon.clientIdentity
      s"OK name=${options.name} pid=${ProcessHandle.current().pid()} " +
        s"daemonRpcHost=${daemon.daemonAddress.host} daemonRpcPort=${daemon.daemonAddress.port} " +
        s"clientId=${identity.clientId} nodeEpoch=${identity.nodeEpoch} " +
        s"blockManagerId=${sanitize(identity.blockManagerId.toString)} " +
        s"clientRpcHost=${identity.rpcHost} clientRpcPort=${identity.rpcPort} " +
        s"backend=${identity.backend} networkEnabled=${identity.networkEnabled} " +
        s"remoteFetchSupported=${identity.remoteFetchSupported} " +
        s"sharedCxlEnabled=${identity.sharedCxlEnabled}"

    case "REGISTER" :: job :: shuffle :: maps :: reduces :: Nil =>
      val registered = daemon.registerShufflesChecked(
        job.toInt, Array(shuffle.toInt), Array(maps.toInt), Array(reduces.toInt))
      s"OK registered=$registered platform=${options.platform} job=$job shuffle=$shuffle " +
        s"maps=$maps reduces=$reduces"

    case "PUT" :: blockId :: size :: seed :: Nil =>
      val expected = deterministicBytes(size.toInt, seed.toLong)
      val started = System.nanoTime()
      daemon.putBlock(blockId, expected, expected.length, expected.length)
      val elapsedNs = System.nanoTime() - started
      s"OK stored=true blockId=$blockId length=${expected.length} crc=${crc32(expected)} " +
        s"elapsedNs=$elapsedNs"

    case "GET" :: blockId :: size :: seed :: Nil =>
      val expected = deterministicBytes(size.toInt, seed.toLong)
      val started = System.nanoTime()
      val actual = daemon.getBlock(blockId).getOrElse(
        throw new IllegalStateException(s"block not found: $blockId"))
      val elapsedNs = System.nanoTime() - started
      if (!java.util.Arrays.equals(expected, actual)) {
        throw new IllegalStateException(
          s"payload mismatch blockId=$blockId expectedLength=${expected.length} " +
            s"actualLength=${actual.length} expectedCrc=${crc32(expected)} actualCrc=${crc32(actual)}")
      }
      s"OK found=true blockId=$blockId expectedLength=${expected.length} " +
        s"actualLength=${actual.length} expectedCrc=${crc32(expected)} " +
        s"actualCrc=${crc32(actual)} elapsedNs=$elapsedNs"

    case "MISS" :: blockId :: Nil =>
      val started = System.nanoTime()
      val actual = daemon.getBlock(blockId)
      val elapsedNs = System.nanoTime() - started
      if (actual.isDefined) throw new IllegalStateException(s"unexpected block hit: $blockId")
      s"OK found=false blockId=$blockId elapsedNs=$elapsedNs"

    case "STOP" :: Nil =>
      running = false
      s"OK stopping=true name=${options.name}"

    case _ => throw new IllegalArgumentException(s"unknown command: ${command.mkString(" ")}")
  }

  private def deterministicBytes(size: Int, seed: Long): Array[Byte] = {
    require(size >= 0, s"negative size: $size")
    val bytes = new Array[Byte](size)
    new java.util.Random(seed).nextBytes(bytes)
    bytes
  }

  private def crc32(bytes: Array[Byte]): Long = {
    val crc = new CRC32
    crc.update(bytes)
    crc.getValue
  }

  private def sanitize(value: String): String =
    Option(value).getOrElse("").replaceAll("\\s+", "_")

  private def parseArgs(args: List[String], options: Options): Options = args match {
    case "--name" :: value :: tail => parseArgs(tail, options.copy(name = value))
    case "--scache-home" :: value :: tail => parseArgs(tail, options.copy(scacheHome = value))
    case "--platform" :: value :: tail => parseArgs(tail, options.copy(platform = value))
    case "--bind-host" :: value :: tail => parseArgs(tail, options.copy(bindHost = value))
    case "--daemon-port" :: value :: tail => parseArgs(tail, options.copy(daemonPort = value.toInt))
    case "--client-host" :: value :: tail => parseArgs(tail, options.copy(clientHost = value))
    case "--client-port" :: value :: tail => parseArgs(tail, options.copy(clientPort = value.toInt))
    case "--control-host" :: value :: tail => parseArgs(tail, options.copy(controlHost = value))
    case "--control-port" :: value :: tail => parseArgs(tail, options.copy(controlPort = value.toInt))
    case "--timeout-ms" :: value :: tail => parseArgs(tail, options.copy(timeoutMs = value.toLong))
    case Nil => options
    case other => throw new IllegalArgumentException(s"unrecognized arguments: ${other.mkString(" ")}")
  }
}
