package org.scache.deploy

import java.io.File
import java.net.ServerSocket

import org.scache.rpc.RpcEnv
import org.scache.storage.{ScacheBlockId, StorageLevel}
import org.scache.util.{ScacheConf, Utils}

import scala.util.Random

/**
 * End-to-end integration test for Daemon -> ScacheClient IPC.
 *
 * This starts a real ScacheMaster + ScacheClient (Netty RPC), then uses the in-process Daemon
 * to push shuffle blocks into the client and verifies the bytes are readable from the client's
 * BlockManager.
 *
 * Run (example):
 *   sbt "runMain org.scache.deploy.DaemonClientIpcIntegrationTest --backend pool --poolPath /dev/shm/scache-ipc.pool.itest"
 */
	object DaemonClientIpcIntegrationTest {

  private final case class Options(
      scacheHome: String,
      host: String,
      backend: String,
      poolPath: String,
      poolSize: String,
      poolChunk: String,
      align: Int,
      blocks: Int,
      maxBlock: String,
      seed: Long,
      timeoutMs: Long,
      keepPool: Boolean)

	  private def usageAndExit(): Nothing = {
	    // scalastyle:off println
	    System.err.println(
	      "DaemonClientIpcIntegrationTest options:\n" +
	        "  --scacheHome PATH   SCache home (default: $SCACHE_HOME or ~/SCache)\n" +
	        "  --host HOST         Bind host (default: auto-detected local IP)\n" +
	        "  --backend NAME      IPC backend: pool|files (default: pool)\n" +
	        "  --poolPath PATH     Pool file path (default: /dev/shm/scache-ipc.pool.itest)\n" +
	        "  --poolSize SIZE     Pool size (default: 256m)\n" +
	        "  --poolChunk SIZE    mmap chunk (default: 64m)\n" +
        "  --align N           Allocation alignment bytes (default: 4096)\n" +
        "  --blocks N          Number of blocks (default: 64)\n" +
        "  --maxBlock SIZE     Max bytes per block (default: 1m)\n" +
        "  --seed N            RNG seed (default: 1)\n" +
        "  --timeoutMs N       Timeout in ms (default: 30000)\n" +
        "  --keepPool          Keep pool file after finishing\n")
    // scalastyle:on println
    sys.exit(1)
  }

	  private def parseArgs(args: Array[String]): Options = {
	    val defaultHome = new File(System.getProperty("user.home"), "SCache").getAbsolutePath
	    var scacheHome = sys.env.getOrElse("SCACHE_HOME", defaultHome)
	    var host = Utils.findLocalInetAddress().getHostAddress
	    var backend = "pool"
	    var poolPath = "/dev/shm/scache-ipc.pool.itest"
	    var poolSize = "256m"
	    var poolChunk = "64m"
	    var align = 4096
    var blocks = 64
    var maxBlock = "1m"
    var seed = 1L
    var timeoutMs = 30000L
    var keepPool = false

    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--scacheHome" =>
          i += 1; if (i >= args.length) usageAndExit()
          scacheHome = args(i)
        case "--host" =>
          i += 1; if (i >= args.length) usageAndExit()
          host = args(i)
        case "--backend" =>
          i += 1; if (i >= args.length) usageAndExit()
          backend = args(i)
        case "--poolPath" =>
          i += 1; if (i >= args.length) usageAndExit()
          poolPath = args(i)
        case "--poolSize" =>
          i += 1; if (i >= args.length) usageAndExit()
          poolSize = args(i)
        case "--poolChunk" =>
          i += 1; if (i >= args.length) usageAndExit()
          poolChunk = args(i)
        case "--align" =>
          i += 1; if (i >= args.length) usageAndExit()
          align = args(i).toInt
        case "--blocks" =>
          i += 1; if (i >= args.length) usageAndExit()
          blocks = args(i).toInt
        case "--maxBlock" =>
          i += 1; if (i >= args.length) usageAndExit()
          maxBlock = args(i)
        case "--seed" =>
          i += 1; if (i >= args.length) usageAndExit()
          seed = args(i).toLong
        case "--timeoutMs" =>
          i += 1; if (i >= args.length) usageAndExit()
          timeoutMs = args(i).toLong
        case "--keepPool" =>
          keepPool = true
        case "-h" | "--help" =>
          usageAndExit()
        case other =>
          System.err.println(s"Unknown arg: $other")
          usageAndExit()
      }
      i += 1
    }

    Options(
      scacheHome = scacheHome,
      host = host,
      backend = backend.trim.toLowerCase,
      poolPath = poolPath,
      poolSize = poolSize,
      poolChunk = poolChunk,
      align = align,
      blocks = blocks,
      maxBlock = maxBlock,
      seed = seed,
      timeoutMs = timeoutMs,
      keepPool = keepPool)
  }

  private def freePort(): Int = {
    val socket = new ServerSocket(0)
    try socket.getLocalPort finally socket.close()
  }

  private def awaitCondition(description: String, timeoutMs: Long)(condition: => Boolean): Unit = {
    val deadline = System.nanoTime() + timeoutMs * 1000000L
    var last = false
    while (!last && System.nanoTime() < deadline) {
      last = condition
      if (!last) Thread.sleep(20)
    }
    if (!last) throw new IllegalStateException(s"Timed out waiting for $description")
  }

	  def main(args: Array[String]): Unit = {
		    val opts = parseArgs(args)
		    System.setProperty("SCACHE_HOME", opts.scacheHome)
		    val conf = new ScacheConf(opts.scacheHome)

	    val masterPort = freePort()
	    val clientPort = freePort()
	    val daemonPort = freePort()

	    conf.set("scache.app.id", "itest", slient = true)
	    conf.set("scache.driver.mode", "false", slient = true)
	    conf.set("scache.master.ip", opts.host, slient = true)
	    conf.set("scache.master.port", masterPort.toString, slient = true)
	    conf.set("scache.client.port", clientPort.toString, slient = true)
	    conf.set("scache.daemon.port", daemonPort.toString, slient = true)
	    conf.set("scache.driver.host", opts.host, slient = true)
	    conf.set("scache.driver.port", masterPort.toString, slient = true)

    // Keep the daemon->client ingestion in memory for easy verification.
    conf.set("scache.daemon.putBlock.storageLevel.local", "MEMORY_ONLY", slient = true)
    conf.set("scache.daemon.putBlock.storageLevel.remote", "MEMORY_ONLY", slient = true)
    conf.set("scache.daemon.putBlock.storageLevel", "MEMORY_ONLY", slient = true)

    opts.backend match {
      case "pool" =>
        val poolFile = new File(opts.poolPath)
        val parent = poolFile.getParentFile
        if (parent != null && !parent.exists() && !parent.mkdirs()) {
          throw new IllegalStateException(s"Failed to create pool dir: ${parent.getAbsolutePath}")
        }
        conf.set("scache.daemon.ipc.backend", "pool", slient = true)
        conf.set("scache.daemon.ipc.pool.path", opts.poolPath, slient = true)
        conf.set("scache.daemon.ipc.pool.size", opts.poolSize, slient = true)
        conf.set("scache.daemon.ipc.pool.mapChunk", opts.poolChunk, slient = true)
        conf.set("scache.daemon.ipc.pool.align", opts.align.toString, slient = true)

      case "files" =>
        conf.set("scache.daemon.ipc.backend", "files", slient = true)

      case other =>
        throw new IllegalArgumentException(s"Unknown --backend: $other (expected pool|files)")
    }

    val maxBlockBytes = Utils.byteStringAsBytes(opts.maxBlock).toInt
    val rnd = new Random(opts.seed)

    // Start master + client.
    val masterEnv = RpcEnv.create("scache.master.itest", opts.host, masterPort, conf)
    val masterEndpoint = new ScacheMaster(masterEnv, opts.host, conf, isDriver = true, isLocal = true)
    masterEnv.setupEndpoint("ScacheMaster", masterEndpoint)

    val clientEnv = RpcEnv.create("scache.client.itest", opts.host, clientPort, conf)
    val clientEndpoint = new ScacheClient(clientEnv, opts.host, "itest-master", clientPort, conf)
    clientEnv.setupEndpoint("ScacheClient", clientEndpoint)

	    awaitCondition("client BlockManager initialization", opts.timeoutMs) {
	      clientEndpoint.blockManager != null && clientEndpoint.clientId >= 0
	    }

	    val daemon = new Daemon(opts.scacheHome, "itest")

	    // Push blocks via the real daemon->client IPC path.
	    val appName = "itest"
	    val jobId = 1
	    val shuffleId = 1
	    val mapId = 1

	    val registered = clientEndpoint.registerShuffle(
	      appName,
	      jobId,
	      Array(shuffleId),
	      Array(mapId + 1),
	      Array(opts.blocks))
	    if (!registered) {
	      throw new IllegalStateException(s"Failed to register shuffle $appName:$jobId:$shuffleId")
	    }

	    val blocks = (0 until opts.blocks).map { reduceId =>
	      val size = 1 + rnd.nextInt(Math.max(1, maxBlockBytes))
	      val bytes = new Array[Byte](size)
	      rnd.nextBytes(bytes)
	      val blockId = ScacheBlockId(appName, jobId, shuffleId, mapId, reduceId)
	      daemon.putBlock(blockId.toString, bytes, size, size)
	      (blockId, bytes)
	    }

    // Verify the bytes landed in the client's BlockManager.
    blocks.foreach { case (blockId, expected) =>
      awaitCondition(s"block $blockId present", opts.timeoutMs) {
        clientEndpoint.blockManager.getLocalBytes(blockId).exists { buf =>
          val got = buf.toArray
          java.util.Arrays.equals(got, expected)
        }
      }
    }

    // scalastyle:off println
	    println(
	      s"PASS daemon-client-ipc: backend=${opts.backend} blocks=${opts.blocks} host=${opts.host} " +
	        s"masterPort=$masterPort clientPort=$clientPort daemonPort=$daemonPort storageLevel=${StorageLevel.MEMORY_ONLY}")
	    // scalastyle:on println

    daemon.stop()
    clientEnv.shutdown()
    masterEnv.shutdown()
    clientEnv.awaitTermination()
    masterEnv.awaitTermination()

    if (opts.backend == "pool" && !opts.keepPool) {
      new File(opts.poolPath).delete()
    }
  }
}
