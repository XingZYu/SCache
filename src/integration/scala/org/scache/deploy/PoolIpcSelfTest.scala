package org.scache.deploy

import java.io.File

import org.scache.util.Utils

import scala.util.Random

/**
 * Quick sanity test for the "single mmap pool file + offset/len" IPC backend.
 *
 * Runs entirely in-process (no RPC / sockets), but uses two independent mmaps of the same file
 * to emulate cross-process visibility.
 *
 * Usage:
 *   sbt "runMain org.scache.deploy.PoolIpcSelfTest --path /dev/shm/scache-ipc.pool --size 1g --chunk 256m"
 */
object PoolIpcSelfTest {

  private final case class Options(
      path: String,
      sizeBytes: Long,
      chunkBytes: Long,
      alignBytes: Int,
      rounds: Int,
      maxBlockBytes: Int,
      batch: Int,
      seed: Long,
      keep: Boolean,
      dumpNumaMaps: Boolean)

  private def usageAndExit(): Nothing = {
    // scalastyle:off println
    System.err.println(
      "PoolIpcSelfTest options:\n" +
        "  --path PATH        Pool file path (default: <tmp>/scache-ipc.pool.selftest)\n" +
        "  --size SIZE        Pool size, e.g. 1024m / 1g (default: 512m)\n" +
        "  --chunk SIZE       mmap chunk size, e.g. 256m (default: 128m)\n" +
        "  --align N          Allocation alignment bytes (default: 4096)\n" +
        "  --rounds N         Number of rounds (default: 200)\n" +
        "  --batch N          Allocations per round (default: 16)\n" +
        "  --maxBlock N       Max block bytes (default: 4m)\n" +
        "  --seed N           RNG seed (default: 0)\n" +
        "  --keep             Keep the pool file after finishing\n" +
        "  --dumpNumaMaps      Print /proc/self/numa_maps lines for the pool mapping\n")
    // scalastyle:on println
    sys.exit(1)
  }

  private def parseArgs(args: Array[String]): Options = {
    val tmp = System.getProperty("java.io.tmpdir")
    var path = new File(tmp, "scache-ipc.pool.selftest").getAbsolutePath
    var sizeBytes = Utils.byteStringAsBytes("512m")
    var chunkBytes = Utils.byteStringAsBytes("128m")
    var alignBytes = 4096
    var rounds = 200
    var batch = 16
    var maxBlockBytes = Utils.byteStringAsBytes("4m").toInt
    var seed = 0L
    var keep = false
    var dumpNumaMaps = false

    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--path" =>
          i += 1; if (i >= args.length) usageAndExit()
          path = args(i)
        case "--size" =>
          i += 1; if (i >= args.length) usageAndExit()
          sizeBytes = Utils.byteStringAsBytes(args(i))
        case "--chunk" =>
          i += 1; if (i >= args.length) usageAndExit()
          chunkBytes = Utils.byteStringAsBytes(args(i))
        case "--align" =>
          i += 1; if (i >= args.length) usageAndExit()
          alignBytes = args(i).toInt
        case "--rounds" =>
          i += 1; if (i >= args.length) usageAndExit()
          rounds = args(i).toInt
        case "--batch" =>
          i += 1; if (i >= args.length) usageAndExit()
          batch = args(i).toInt
        case "--maxBlock" =>
          i += 1; if (i >= args.length) usageAndExit()
          maxBlockBytes = Utils.byteStringAsBytes(args(i)).toInt
        case "--seed" =>
          i += 1; if (i >= args.length) usageAndExit()
          seed = args(i).toLong
        case "--keep" =>
          keep = true
        case "--dumpNumaMaps" =>
          dumpNumaMaps = true
        case "-h" | "--help" =>
          usageAndExit()
        case other =>
          System.err.println(s"Unknown arg: $other")
          usageAndExit()
      }
      i += 1
    }

    Options(
      path = path,
      sizeBytes = sizeBytes,
      chunkBytes = chunkBytes,
      alignBytes = alignBytes,
      rounds = rounds,
      maxBlockBytes = maxBlockBytes,
      batch = batch,
      seed = seed,
      keep = keep,
      dumpNumaMaps = dumpNumaMaps)
  }

  private final case class Allocation(offset: Long, size: Int, data: Array[Byte])

  private def dumpNumaMapsForPath(path: String): Unit = {
    try {
      val src = scala.io.Source.fromFile("/proc/self/numa_maps")
      try {
        val needle = s"file=${path}"
        val lines = src.getLines().filter(_.contains(needle)).toArray
        // scalastyle:off println
        println(s"---- /proc/self/numa_maps for $needle (${lines.length} lines) ----")
        lines.foreach(println)
        println("---- end numa_maps ----")
        // scalastyle:on println
      } finally {
        src.close()
      }
    } catch {
      case t: Throwable =>
        // scalastyle:off println
        System.err.println(s"Failed to read /proc/self/numa_maps: ${t.getClass.getName}: ${t.getMessage}")
        // scalastyle:on println
    }
  }

  def main(args: Array[String]): Unit = {
    val opts = parseArgs(args)
    require(opts.sizeBytes > 0, s"--size must be > 0, got ${opts.sizeBytes}")
    require(opts.chunkBytes > 0, s"--chunk must be > 0, got ${opts.chunkBytes}")
    require(opts.chunkBytes <= Int.MaxValue.toLong, s"--chunk must be <= 2g, got ${opts.chunkBytes}")
    require(opts.sizeBytes >= opts.chunkBytes, s"--size must be >= --chunk (${opts.chunkBytes})")
    require(opts.alignBytes > 0, s"--align must be > 0, got ${opts.alignBytes}")
    require(opts.rounds > 0, s"--rounds must be > 0, got ${opts.rounds}")
    require(opts.batch > 0, s"--batch must be > 0, got ${opts.batch}")

    val maxBlock = Math.min(opts.maxBlockBytes, opts.chunkBytes.toInt)
    require(maxBlock > 0, s"--maxBlock must be > 0, got ${opts.maxBlockBytes}")

    val poolPathFile = new File(opts.path)
    val parent = poolPathFile.getParentFile
    if (parent != null && !parent.exists() && !parent.mkdirs()) {
      throw new IllegalStateException(s"Failed to create parent dir: ${parent.getAbsolutePath}")
    }

    // Create (or extend) the pool file, then open two independent mmaps for writer/reader.
    val created = MmapPoolFile.createOrOpen(opts.path, desiredSizeBytes = opts.sizeBytes, mapChunkBytes = opts.chunkBytes.toInt)
    val writer = MmapPoolFile.open(opts.path, mapChunkBytes = opts.chunkBytes.toInt)
    val reader = MmapPoolFile.open(opts.path, mapChunkBytes = opts.chunkBytes.toInt)

    val allocator = new PoolAllocator(
      poolSizeBytes = created.sizeBytes,
      chunkSizeBytes = opts.chunkBytes,
      alignBytes = opts.alignBytes)

    // Basic boundary test: a slice must not cross the mmap chunk boundary.
    val crossOffset = opts.chunkBytes - 1
    try {
      reader.slice(crossOffset, 2)
      throw new IllegalStateException("Expected slice to fail when crossing chunk boundary, but it succeeded.")
    } catch {
      case _: IllegalArgumentException =>
        // expected
    }

    val rnd = new Random(opts.seed)
    var totalBytes = 0L
    var allocations = 0L

    var round = 0
    while (round < opts.rounds) {
      val batchAllocs = new Array[Allocation](opts.batch)
      var bi = 0
      while (bi < opts.batch) {
        val size = 1 + rnd.nextInt(maxBlock)
        val offsetOpt = allocator.allocate(size)
        if (offsetOpt.isEmpty) {
          throw new IllegalStateException(
            s"Pool is full (round=$round batchIndex=$bi size=$size). ${allocator.statsString}")
        }
        val offset = offsetOpt.get
        val data = new Array[Byte](size)
        rnd.nextBytes(data)
        batchAllocs(bi) = Allocation(offset, size, data)
        bi += 1
      }

      // Simulate daemon writes.
      bi = 0
      while (bi < batchAllocs.length) {
        val a = batchAllocs(bi)
        writer.write(a.offset, a.data, a.size)
        totalBytes += a.size
        allocations += 1
        bi += 1
      }

      // Simulate client reads, then free.
      bi = 0
      while (bi < batchAllocs.length) {
        val a = batchAllocs(bi)
        val buf = reader.slice(a.offset, a.size)
        val got = new Array[Byte](a.size)
        buf.get(got)
        assert(
          java.util.Arrays.equals(got, a.data),
          s"Data mismatch at offset=${a.offset} size=${a.size} round=$round batchIndex=$bi")
        allocator.free(a.offset, a.size)
        bi += 1
      }

      round += 1
    }

    if (opts.dumpNumaMaps) {
      dumpNumaMapsForPath(opts.path)
    }

    // scalastyle:off println
    println(
      s"PASS pool-ipc: path=${opts.path} poolSize=${created.sizeBytes} chunk=${opts.chunkBytes} " +
        s"rounds=${opts.rounds} batch=${opts.batch} allocations=$allocations bytes=$totalBytes")
    // scalastyle:on println

    if (!opts.keep) {
      poolPathFile.delete()
    }
  }
}
