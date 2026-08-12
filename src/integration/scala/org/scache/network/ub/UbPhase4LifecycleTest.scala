package org.scache.network.ub

import java.nio.file.{Files, Paths}

/** Registered-arena lifecycle pressure using the production arena implementation. */
private[scache] object UbPhase4LifecycleTest {
  private def fdCount: Long = {
    val stream = Files.list(Paths.get("/proc/self/fd"))
    try stream.count() finally stream.close()
  }
  private def sample(round: Int, arena: UBBlockTransferService.RegisteredArena): Unit = {
    val runtime = Runtime.getRuntime
    val status = Files.readAllLines(Paths.get("/proc/self/status"))
    val rssKb = (0 until status.size()).map(status.get).find(_.startsWith("VmRSS:"))
      .flatMap(_.split("\\s+").lift(1)).map(_.toLong).getOrElse(-1L)
    val native = arena.transportMetrics
    def backend(name: String): Long = Option(native.backendCounters.get(name))
      .map(_.longValue()).getOrElse(0L)
    println(s"RESOURCE round=$round fd=$fdCount threads=${Thread.getAllStackTraces.size()} " +
      s"vmRssKb=$rssKb usedHeap=${runtime.totalMemory - runtime.freeMemory} freeBytes=${arena.freeBytes} " +
      s"published=${arena.publishedBlocks} leases=${arena.activeLeases} scratch=${arena.activeScratch} " +
      s"regions=${native.registeredRegions} imports=${backend("activeImports")} " +
      s"transports=${backend("activeTransports")} " +
      s"providerOutstanding=${backend("providerOutstandingRequests")}")
  }
  def main(args: Array[String]): Unit = {
    val device = args.headOption.getOrElse("openurma0")
    val role = Option(System.getenv("OPENURMA_WIRE_ROLE")).getOrElse("listen")
    val transport = RealUbTransport.open(device, 64, 4096, true, role)
    val arena = new UBBlockTransferService.RegisteredArena(transport, 1024 * 1024, "lifecycle", 1L)
    try {
      sample(0, arena)
      var previousGeneration = 0L
      for (i <- 1 to 1000) {
        val id = s"block-$i"; val bytes = Array.tabulate[Byte](64)(j => (i + j).toByte)
        arena.publish(id, bytes)
        val descriptor = arena.acquire(id).getOrElse(throw new IllegalStateException(s"missing $id"))
        if (descriptor.offset != 0 || descriptor.generation <= previousGeneration) {
          throw new IllegalStateException(s"address/generation reuse failure at $i")
        }
        previousGeneration = descriptor.generation
        if (!arena.release(descriptor.leaseId, descriptor.generation)) throw new IllegalStateException(s"release failed $i")
        arena.unpublish(id, 1000)
        if (i % 100 == 0) sample(i, arena)
      }
      if (arena.freeBytes != arena.capacity || arena.publishedBlocks != 0 || arena.activeLeases != 0) {
        throw new IllegalStateException("arena did not return to its initial state")
      }
      println("SCACHE_TEST_ARENA_LIFECYCLE=PASS cycles=1000 addressReuse=1000 generationMonotonic=true")
    } finally {
      arena.close(1000)
      transport.close()
    }
  }
}
