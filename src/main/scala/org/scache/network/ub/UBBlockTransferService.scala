/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 */
package org.scache.network.ub

import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream, EOFException, File}
import java.net.{InetAddress, ServerSocket, Socket}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.UUID
import java.util.concurrent.{ExecutorService, Executors}
import java.util.concurrent.atomic.AtomicLong
import java.util.zip.CRC32

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.scache.network.{BlockDataManager, BlockTransferService}
import org.scache.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.scache.network.transfer.BlockFetchingListener
import org.scache.storage.{BlockId, ScacheBlockId, StorageLevel}
import org.scache.util.ScacheConf

/**
 * The formal SCache URMA transport.  TCP carries only bounded control frames;
 * block bytes are transferred exclusively by URMA READ/WRITE against a single
 * registered direct-memory arena on each node.
 */
private[scache] class UBBlockTransferService(
    conf: ScacheConf,
    override val hostName: String,
    numCores: Int) extends BlockTransferService {

  import UBBlockTransferService._

  private val strictMode = conf.getBoolean("spark.urma.strict", false)
  private val diagnosticsLevel =
    conf.getString("scache.ub.diagnostics.level", "off").trim.toLowerCase(java.util.Locale.ROOT)
  require(Set("off", "summary", "trace").contains(diagnosticsLevel),
    s"scache.ub.diagnostics.level must be off, summary, or trace; got $diagnosticsLevel")
  private val summaryDiagnostics = diagnosticsLevel == "summary" || diagnosticsLevel == "trace"
  private val traceDiagnostics = diagnosticsLevel == "trace"
  private val traceChunkSampleLimit =
    math.max(0, conf.getInt("scache.ub.diagnostics.traceChunkSampleLimit", 8))
  private val traceEventLimit =
    math.max(0, conf.getInt("scache.ub.diagnostics.traceEventLimit", 256))
  private val timeoutMs = conf.getInt("spark.urma.transferTimeoutMs", 60000)
  private val unpublishTimeoutMs = conf.getInt("spark.urma.unpublishTimeoutMs", timeoutMs)
  private val leaseTimeoutMs = conf.getInt("spark.urma.leaseTimeoutMs", timeoutMs)
  private val arenaBytes = conf.getInt("spark.urma.arenaBytes",
    conf.getInt("spark.urma.bufferBytes", 64 * 1024 * 1024))
  private val arenaCount = conf.getInt("spark.urma.arenaCount", 2)
  // When enabled, registered arenas are backed by per-node mmap files. Spark's executor-side
  // writer can map the same file and write the bytes before the client publishes the MR. The
  // file is only a local shared-memory bridge; the cross-node payload still uses URMA READ.
  private val sharedPoolEnabled = conf.getBoolean("scache.ub.pool.enabled", false)
  private val sharedPoolDirectory = conf.getString("scache.ub.pool.path", "").trim
  private val sharedPoolForceOnCommit =
    conf.getBoolean("scache.ub.pool.forceOnCommit", false)
  // Full CRC scans remain enabled for functional validation. Performance profiles may
  // disable them after the transport/descriptor gates have passed; bounds, generation
  // and completion checks are intentionally unaffected.
  private val sharedPoolChecksumEnabled =
    conf.getBoolean("scache.ub.checksum.enabled", true)
  private val transportBackend =
    UbTransportFactory.normalizeBackend(conf.getString("scache.ub.transport", "real"))
  require(transportBackend == "real",
    s"production SCache supports only scache.ub.transport=real; got $transportBackend")
  private val wireRole = conf.getString("spark.urma.wireRole", "").trim
  private val wirePath = conf.getString("spark.urma.wirePath", "").trim
  private val controlBindHost = conf.getString("spark.urma.controlBindHost", hostName).trim
  private val controlAdvertiseHost = conf.getString("spark.urma.controlAdvertiseHost", hostName).trim
  private val nodeId = conf.getString("spark.urma.nodeId", s"$hostName-${UUID.randomUUID}")
  private val epoch = System.nanoTime()
  private val controlBytes = new AtomicLong(0L)
  private val nextControlRequestId = new AtomicLong(1L)
  private val payloadBytes = new AtomicLong(0L) // Must remain zero: TCP is control-only.
  private val readRequests = new AtomicLong(0L)
  private val writeRequests = new AtomicLong(0L)
  private val completedChunks = new AtomicLong(0L)
  private val failedChunks = new AtomicLong(0L)
  private val checksumErrors = new AtomicLong(0L)
  private val leaseErrors = new AtomicLong(0L)
  private val generationErrors = new AtomicLong(0L)
  private val sharedPoolPrepares = new AtomicLong(0L)
  private val sharedPoolCommits = new AtomicLong(0L)
  private val sharedPoolAborts = new AtomicLong(0L)
  private val sharedPoolImports = new AtomicLong(0L)
  private val sharedPoolDestinationReads = new AtomicLong(0L)
  private val sharedPoolDestinationBytes = new AtomicLong(0L)
  private val sharedPoolDestinationIdentityMatches = new AtomicLong(0L)
  private val sharedPoolDestinationIdentityMismatches = new AtomicLong(0L)
  private val sharedPoolConsumerReservations = new AtomicLong(0L)
  private val sharedPoolMultiConsumerReservations = new AtomicLong(0L)
  private val sharedPoolDeferredReleases = new AtomicLong(0L)
  private val activeControlConnections = new AtomicLong(0L)
  private val lastNativeRequestId = new AtomicLong(0L)
  private val nextTransferId = new AtomicLong(1L)
  private val traceEvents = new AtomicLong(0L)

  @volatile private var closed = false
  @volatile private var dataManager: BlockDataManager = _
  @volatile private var transport: UbTransport = _
  @volatile private var arena: RegisteredArenaPool = _
  @volatile private var endpoint: Array[Byte] = _
  @volatile private var server: ServerSocket = _
  @volatile private var acceptThread: Thread = _
  private val workers: ExecutorService = Executors.newCachedThreadPool()
  private val connectLock = new Object
  @volatile private var connectedPeer: Array[Byte] = _
  // Blocks imported into this node's destination arena are not represented as local BlockManager
  // payloads. Keep their ids so application cleanup can retire only imported slices; owner blocks
  // are retired through BlockManager.removeBlock/unpublishBlock.
  private val importedSharedBlocks = mutable.HashSet.empty[String]
  // A destination can be returned to multiple Spark task attempts. Keep it published until the
  // final ManagedBuffer consumer releases it; otherwise an arena reuse can corrupt decompression.
  private val sharedPoolConsumerRefs = mutable.HashMap.empty[String, Int]

  override def init(blockDataManager: BlockDataManager): Unit = {
    require(controlBindHost.nonEmpty && controlAdvertiseHost.nonEmpty,
      "UB control bind and advertise hosts must be explicit")
    val device = {
      require(wireRole == "listen" || wireRole == "connect",
        "spark.urma.wireRole must be explicitly listen or connect")
      require(wirePath.nonEmpty, "spark.urma.wirePath must be explicitly configured")
      val launcherRole = Option(System.getenv("OPENURMA_WIRE_ROLE")).getOrElse("")
      require(launcherRole == wireRole,
        s"OPENURMA_WIRE_ROLE=$launcherRole does not match spark.urma.wireRole=$wireRole")
      val launcherWirePath = Option(System.getenv("OPENURMA_WIRE_PATH")).getOrElse("")
      require(launcherWirePath == wirePath,
        s"OPENURMA_WIRE_PATH=$launcherWirePath does not match spark.urma.wirePath=$wirePath")
      wireRole match {
        case "listen" => conf.getString("spark.urma.device.listen", "openurma0")
        case "connect" => conf.getString("spark.urma.device.connect", "openurma1")
      }
    }
    try {
      dataManager = blockDataManager
      transport = UbTransportFactory.open(transportBackend, device,
        conf.getInt("spark.urma.queueDepth", 128), conf.getInt("spark.urma.chunkSize", 4096),
        strictMode, wireRole)
      require(arenaCount > 0, "spark.urma.arenaCount must be positive")
      if (sharedPoolEnabled) {
        require(sharedPoolDirectory.nonEmpty,
          "scache.ub.pool.path must be configured when scache.ub.pool.enabled=true")
        Files.createDirectories(Paths.get(sharedPoolDirectory))
      }
      arena = new RegisteredArenaPool(
        transport, arenaBytes, arenaCount, nodeId, epoch, leaseTimeoutMs,
        if (sharedPoolEnabled) Some(sharedPoolDirectory) else None,
        sharedPoolForceOnCommit, sharedPoolChecksumEnabled)
      endpoint = transport.localEndpoint()
      server = new ServerSocket(conf.getInt("spark.urma.controlPort", 0), 64,
        InetAddress.getByName(controlBindHost))
      acceptThread = new Thread(() => acceptLoop(), s"ub-control-$nodeId")
      acceptThread.setDaemon(true)
      acceptThread.start()
      summary(s"UB BlockTransferService ready node=$nodeId epoch=$epoch backend=$transportBackend role=$wireRole " +
        s"device=$device wirePath=$wirePath control=$controlAdvertiseHost:${server.getLocalPort} " +
          s"arenaBytes=${arena.capacity} arenas=${arena.arenaCount}")
    } catch {
      case NonFatal(e) =>
        close()
        if (strictMode) throw new RuntimeException("URMA strict initialization failed", e)
        throw new RuntimeException("UB backend does not provide a Netty fallback", e)
    }
  }

  override def port: Int = if (server == null) 0 else server.getLocalPort

  /** Called by BlockManager only after its normal put path has succeeded. */
  private[scache] def publishBlock(blockId: BlockId, bytes: Array[Byte]): Unit = {
    requireReady()
    arena.publish(blockId.toString, bytes)
    trace(s"SCACHE_UB_PUBLISH nodeId=$nodeId nodeEpoch=$epoch blockId=$blockId " +
      s"length=${bytes.length} crc=${crc(ByteBuffer.wrap(bytes), bytes.length)} terminal=SUCCESS")
  }

  /**
   * Reserve a long-lived, registered arena slice that Spark can map locally. The returned path is
   * never sent to a remote reducer; it is only the producer-side executor/client shared-memory
   * bridge. A stable URMA descriptor is published only by commitPoolBlock.
   */
  private[scache] def preparePoolBlock(
      blockId: BlockId, size: Int): Option[PoolSlice] = {
    requireReady()
    if (!sharedPoolEnabled) return None
    val slice = arena.prepareShared(blockId.toString, size)
    sharedPoolPrepares.incrementAndGet()
    Some(slice)
  }

  private[scache] def preparePoolBlocks(
      blockIds: Seq[BlockId], sizes: Seq[Int]): Seq[Option[PoolSlice]] = {
    require(blockIds.size == sizes.size,
      s"UB pool prepare sizes mismatch ids=${blockIds.size} sizes=${sizes.size}")
    if (!sharedPoolEnabled) return sizes.map(_ => None)
    val prepared = mutable.ArrayBuffer.empty[(BlockId, PoolSlice)]
    try {
      blockIds.zip(sizes).map { case (blockId, size) =>
        val slice = arena.prepareShared(blockId.toString, size)
        sharedPoolPrepares.incrementAndGet()
        prepared += ((blockId, slice))
        Some(slice)
      }
    } catch {
      case NonFatal(e) =>
        prepared.foreach { case (blockId, slice) =>
          try arena.abortShared(blockId.toString, slice.path, slice.offset, slice.length)
          catch { case NonFatal(_) => }
        }
        throw e
    }
  }

  /** Publish a slice that Spark filled through the local mmap bridge, without copying its bytes. */
  private[scache] def commitPoolBlock(
      blockId: BlockId, poolPath: String, offset: Long, size: Int): Boolean = {
    requireReady()
    if (!sharedPoolEnabled) return false
    val committed = arena.commitShared(blockId.toString, poolPath, offset, size)
    if (committed) sharedPoolCommits.incrementAndGet()
    committed
  }

  /**
   * Cancel a Spark-side reservation that never reached PUBLISHED.  This is intentionally a
   * local operation: the owner has not advertised a descriptor yet, so no remote lease exists.
   * Returning a Boolean lets the daemon distinguish an already-removed/unknown reservation from
   * a successful release without adding another control-plane round trip.
   */
  private[scache] def abortPoolBlock(
      blockId: BlockId, poolPath: String, offset: Long, size: Int): Boolean = {
    requireReady()
    if (!sharedPoolEnabled) return false
    val published = arena.localShared(blockId.toString).exists { slice =>
      slice.path == poolPath && slice.offset == offset && slice.length == size
    }
    if (published) {
      // A cancelled Spark task can race an asynchronous COMMIT.  Treat the abort as a revoke in
      // that case so a late commit cannot leave an unreachable published slice behind.
      arena.unpublish(blockId.toString, unpublishTimeoutMs)
      sharedPoolAborts.incrementAndGet()
      return true
    }
    arena.abortShared(blockId.toString, poolPath, offset, size)
    sharedPoolAborts.incrementAndGet()
    !arena.localShared(blockId.toString).exists { slice =>
      slice.offset == offset && slice.length == size
    }
  }

  private[scache] def localPoolBlock(
      blockId: BlockId): Option[PoolSlice] = {
    if (!sharedPoolEnabled || arena == null) None
    else arena.localShared(blockId.toString).map { slice =>
      slice
    }
  }

  /**
   * Fetch a remote block directly into a destination slice in this node's registered shared arena.
   * The destination remains published until shuffle/application cleanup; Spark reads the same local
   * mmap slice, so no URMA scratch-to-heap staging copy is needed.
   */
  private[scache] def fetchBlockToPool(
      host: String, remotePort: Int, blockId: String): Option[PoolSlice] = {
    requireReady()
    if (!sharedPoolEnabled) return None
    localPoolBlock(BlockId(blockId)) match {
      case Some(local) => return Some(local)
      case None =>
    }
    withControl(host, remotePort) { (channel, peer) =>
      channel.exchange(Acquire(blockId)) match {
        case Grant(descriptor) =>
          validateDescriptor(descriptor, peer, blockId)
          val destination = arena.prepareShared(blockId, descriptor.length, ownerPeer = "remote-fetch")
          val buffer = arena.sharedBuffer(blockId)
          sharedPoolDestinationReads.incrementAndGet()
          sharedPoolDestinationBytes.addAndGet(descriptor.length.toLong)
          if (destination.length != descriptor.length || !buffer.isDirect ||
              buffer.capacity() < descriptor.length) {
            sharedPoolDestinationIdentityMismatches.incrementAndGet()
            throw new IllegalStateException(
              "UB shared destination identity mismatch block=" + blockId +
                " length=" + descriptor.length + " sliceLength=" + destination.length +
                " direct=" + buffer.isDirect + " capacity=" + buffer.capacity())
          }
          sharedPoolDestinationIdentityMatches.incrementAndGet()
          try {
            transferRead(descriptor, buffer, s"pool-fetch-$blockId", blockId)
            val actual = if (sharedPoolChecksumEnabled) crc(buffer, descriptor.length) else -1L
            if (sharedPoolChecksumEnabled && actual != descriptor.crc) {
              checksumErrors.incrementAndGet()
              throw new IllegalStateException(
                s"CRC mismatch for shared UB pool fetch block=$blockId expected=${descriptor.crc} actual=$actual")
            }
            if (!arena.commitShared(blockId, destination.path, destination.offset,
                destination.length, ownerPeer = "remote-fetch", expectedCrc = actual)) {
              throw new IllegalStateException(s"failed to publish shared UB destination for $blockId")
            }
            importedSharedBlocks.synchronized { importedSharedBlocks += blockId }
            sharedPoolImports.incrementAndGet()
            channel.exchange(Release(descriptor.leaseId, descriptor.generation)) match {
              case Ack =>
                Some(destination)
              case other => throw new IllegalStateException(s"remote UB RELEASE failed: $other")
            }
          } catch {
            case NonFatal(e) =>
              arena.abortShared(blockId, destination.path, destination.offset, destination.length,
                ownerPeer = "remote-fetch")
              try channel.exchange(Release(descriptor.leaseId, descriptor.generation))
              catch { case NonFatal(_) => }
              throw e
          }
        case NotFound => None
        case Stale => generationErrors.incrementAndGet(); throw new IllegalStateException(
          s"stale UB descriptor for $blockId")
        case ErrorResponse(message) => throw new IllegalStateException(message)
        case other => throw new IllegalStateException(s"unexpected ACQUIRE response $other")
      }
    }
  }

  /** Reserve one eventual Spark consumer before entering the single-flight fetch. */
  private[scache] def reservePoolConsumer(blockId: BlockId): Unit = {
    require(blockId != null, "UB pool consumer blockId must not be null")
    val name = blockId.toString
    importedSharedBlocks.synchronized {
      val previous = sharedPoolConsumerRefs.getOrElse(name, 0)
      sharedPoolConsumerRefs.update(name, previous + 1)
      sharedPoolConsumerReservations.incrementAndGet()
      if (previous > 0) sharedPoolMultiConsumerReservations.incrementAndGet()
    }
  }

  /** Cancel a reservation whose fetch returned None or failed before Spark received a buffer. */
  private[scache] def cancelPoolConsumer(blockId: BlockId): Unit = {
    if (blockId == null) return
    val name = blockId.toString
    importedSharedBlocks.synchronized {
      sharedPoolConsumerRefs.get(name).foreach { refs =>
        if (refs <= 1) sharedPoolConsumerRefs -= name
        else sharedPoolConsumerRefs.update(name, refs - 1)
      }
    }
  }

  /** Called by BlockManager before memory/disk removal, so no stale descriptor survives it. */
  private[scache] def unpublishBlock(blockId: BlockId): Unit = {
    importedSharedBlocks.synchronized {
      importedSharedBlocks -= blockId.toString
      sharedPoolConsumerRefs -= blockId.toString
    }
    if (arena != null) arena.unpublish(blockId.toString, unpublishTimeoutMs)
  }

  /**
   * Release one reducer destination consumer. Owner blocks remain shuffle-scoped; only imported
   * destinations are unpublished when their final consumer exits.
   */
  private[scache] def releaseImportedPoolBlock(blockId: BlockId): Boolean = {
    if (arena == null || blockId == null) return false
    val name = blockId.toString
    val (accepted, retireImported) = importedSharedBlocks.synchronized {
      sharedPoolConsumerRefs.get(name) match {
        case Some(refs) if refs > 1 =>
          sharedPoolConsumerRefs.update(name, refs - 1)
          sharedPoolDeferredReleases.incrementAndGet()
          (true, false)
        case Some(_) =>
          sharedPoolConsumerRefs -= name
          if (importedSharedBlocks.contains(name)) {
            try {
              arena.unpublish(name, unpublishTimeoutMs)
              importedSharedBlocks -= name
              (true, true)
            } catch {
              case NonFatal(e) =>
                sharedPoolConsumerRefs.update(name, 1)
                throw e
            }
          } else {
            (true, false)
          }
        case None =>
          (false, false)
      }
    }
    if (!accepted) return false
    if (retireImported) {
      val noImportedSlices = importedSharedBlocks.synchronized(importedSharedBlocks.isEmpty)
      if (noImportedSlices && transport != null) {
        transport.releaseCachedRemoteImports()
      }
    }
    true
  }

  /**
   * Application-release hook: all Spark/SCache leases must already be drained.
   * This releases cached peer MR imports without restarting the client or transport.
   */
  private[scache] def releaseCachedRemoteImports(): Unit =
    releaseCachedRemoteImportsWhere(_ => true)

  /**
   * Shuffle-scoped cleanup for imported destination slices.  The old application-wide hook is
   * retained, but removeShuffle must not leave a completed shuffle pinned until application exit.
   * Native MR imports are released only when this client has no remaining imported slices; this
   * avoids invalidating another shuffle's hot-path registration cache.
   */
  private[scache] def releaseCachedRemoteImportsForShuffle(
      appName: String, shuffleId: Int, jobId: Int): Unit = {
    releaseCachedRemoteImportsWhere { blockName =>
      try {
        BlockId(blockName) match {
          case ScacheBlockId(app, job, shuffle, _, _) =>
            (appName == null || appName.isEmpty || app == appName) &&
              shuffle == shuffleId && (jobId < 0 || job == jobId)
          case _ => false
        }
      } catch {
        case _: Throwable => false
      }
    }
  }

  private def releaseCachedRemoteImportsWhere(predicate: String => Boolean): Unit = {
    if (arena == null) return
    val candidates = importedSharedBlocks.synchronized {
      importedSharedBlocks.toVector.filter(predicate)
    }
    candidates.foreach { blockId =>
      try {
        arena.unpublish(blockId, unpublishTimeoutMs)
        importedSharedBlocks.synchronized {
          importedSharedBlocks -= blockId
          sharedPoolConsumerRefs -= blockId
        }
      } catch {
        case NonFatal(e) =>
          summary(s"UB imported slice cleanup failed block=$blockId: ${e.getMessage}")
      }
    }
    val noImportedSlices = importedSharedBlocks.synchronized(importedSharedBlocks.isEmpty)
    if (noImportedSlices && transport != null) {
      transport.releaseCachedRemoteImports()
      trace(s"SCACHE_UB_REMOTE_IMPORTS_RELEASED nodeId=$nodeId nodeEpoch=$epoch")
    }
  }

  /** Phase-4 negative-test probe: exercises the real control ACQUIRE/RELEASE path without payload. */
  private[scache] def probeRemoteDescriptor(host: String, remotePort: Int, blockId: String): String = {
    withControl(host, remotePort) { (channel, peer) =>
      channel.exchange(Acquire(blockId)) match {
        case Grant(descriptor) =>
          validateDescriptor(descriptor, peer, blockId)
          channel.exchange(Release(descriptor.leaseId, descriptor.generation)) match {
            case Ack =>
              s"producerNodeId=${descriptor.nodeId} producerEpoch=${descriptor.epoch} arenaId=${descriptor.arenaId} " +
                s"offset=${descriptor.offset} length=${descriptor.length} token=${descriptor.token} " +
                s"generation=${descriptor.generation} leaseId=${descriptor.leaseId}"
            case other => throw new IllegalStateException(s"probe RELEASE failed: $other")
          }
        case other => throw new IllegalStateException(s"probe ACQUIRE failed: $other")
      }
    }
  }

  /** Formal phase-4 negative probes; all cases use this BTS instance and its real control channel. */
  private[scache] def runControlFault(host: String, remotePort: Int, caseId: String, blockId: String): String = {
    def expectError(message: Message, expected: String): Unit = message match {
      case ErrorResponse(text) if text.toLowerCase.contains(expected.toLowerCase) =>
      case other => throw new IllegalStateException(s"expected typed $expected error, got $other")
    }
    withControl(host, remotePort) { (channel, peer) =>
      caseId match {
        case "unknown_lease" =>
          expectError(channel.exchange(Release(UUID.randomUUID().toString, 1L)), "unknown lease")
        case "stale_generation" =>
          channel.exchange(Acquire(blockId)) match {
            case Grant(d) =>
              expectError(channel.exchange(Release(d.leaseId, d.generation + 1)), "stale lease generation")
              if (channel.exchange(Release(d.leaseId, d.generation)) != Ack) throw new IllegalStateException("cleanup release failed")
            case other => throw new IllegalStateException(s"ACQUIRE failed: $other")
          }
        case "double_release" =>
          channel.exchange(Acquire(blockId)) match {
            case Grant(d) =>
              if (channel.exchange(Release(d.leaseId, d.generation)) != Ack ||
                  channel.exchange(Release(d.leaseId, d.generation)) != Ack) {
                throw new IllegalStateException("idempotent RELEASE did not ACK")
              }
            case other => throw new IllegalStateException(s"ACQUIRE failed: $other")
          }
        case "expired_lease" =>
          channel.exchange(Acquire(blockId)) match {
            case Grant(d) =>
              Thread.sleep(leaseTimeoutMs.toLong + 100L)
              expectError(channel.exchange(Release(d.leaseId, d.generation)), "expired lease")
            case other => throw new IllegalStateException(s"ACQUIRE failed: $other")
          }
        case "stale_producer_epoch" =>
          channel.exchange(Acquire(blockId)) match {
            case Grant(d) =>
              var rejected = false
              try UBBlockTransferService.validateDescriptorIdentity(d, PeerIdentity(peer.nodeId, peer.epoch + 1), blockId)
              catch { case _: IllegalStateException => rejected = true }
              if (!rejected) throw new IllegalStateException("stale producer epoch was accepted")
              if (channel.exchange(Release(d.leaseId, d.generation)) != Ack) throw new IllegalStateException("cleanup release failed")
            case other => throw new IllegalStateException(s"ACQUIRE failed: $other")
          }
        case "remote_bounds" | "invalid_token" =>
          channel.exchange(Acquire(blockId)) match {
            case Grant(d) =>
              val localArena = arena
              val local = localArena.allocateScratch(math.max(1, d.length))
              try {
                var rejected = false
                try {
                  val remote = if (caseId == "remote_bounds")
                    new RemoteBuffer(d.remoteAddress, math.max(1L, d.length.toLong - 1L), d.token, d.segmentGeneration)
                  else new RemoteBuffer(d.remoteAddress, math.max(1L, d.length.toLong), d.token + 1, d.segmentGeneration)
                  val request = transport.read(remote, 0L, local.buffer, 0, math.max(1, d.length))
                  rejected = transport.waitFor(request, timeoutMs) != 0
                } catch { case NonFatal(_) => rejected = true }
                if (!rejected) throw new IllegalStateException(s"$caseId descriptor was accepted")
              } finally {
                localArena.freeScratch(local)
                channel.exchange(Release(d.leaseId, d.generation))
              }
            case other => throw new IllegalStateException(s"ACQUIRE failed: $other")
          }
        case "crc_mismatch" =>
          val localArena = arena
          val local = localArena.allocateScratch(64)
          try {
            var i = 0; while (i < 64) { local.buffer.put(i, (i + 1).toByte); i += 1 }
            val actualCrc = crc(local.buffer, 64)
            channel.exchange(Prepare(blockId + "-crc-fault", 64, actualCrc + 1L, 4, 1, "java.lang.Object")) match {
              case Grant(d) =>
                validateDescriptor(d, peer, blockId + "-crc-fault")
                // Deliberately commit the untouched remote slice against the CRC of non-zero
                // source bytes. This is a deterministic payload-corruption fault without a hook.
                expectError(channel.exchange(Commit(d.leaseId, d.generation)), "CRC mismatch")
              case other => throw new IllegalStateException(s"PREPARE failed: $other")
            }
          } finally localArena.freeScratch(local)
        case other => throw new IllegalArgumentException(s"unknown formal control fault $other")
      }
      s"caseId=$caseId rootCause=typed_negative_assertion peer=${peer.key}"
    }
  }

  private[scache] def assertStaleRemoteEpoch(
      host: String, remotePort: Int, blockId: String, staleEpoch: Long): String = {
    withControl(host, remotePort) { (channel, peer) =>
      channel.exchange(Acquire(blockId)) match {
        case Grant(d) =>
          var rejected = false
          try UBBlockTransferService.validateDescriptorIdentity(d, PeerIdentity(peer.nodeId, staleEpoch), blockId)
          catch { case _: IllegalStateException => rejected = true }
          if (!rejected || staleEpoch == d.epoch) throw new IllegalStateException(
            s"stale epoch assertion invalid stale=$staleEpoch current=${d.epoch}")
          if (channel.exchange(Release(d.leaseId, d.generation)) != Ack) throw new IllegalStateException("cleanup release failed")
          s"staleEpoch=$staleEpoch currentEpoch=${d.epoch} rejected=true"
        case other => throw new IllegalStateException(s"ACQUIRE failed: $other")
      }
    }
  }

  override def fetchBlocks(host: String, remotePort: Int, execId: String,
      blockIds: Array[String], listener: BlockFetchingListener): Unit = {
    blockIds.foreach { id =>
      try listener.onBlockFetchSuccess(id, fetchOne(host, remotePort, id))
      catch { case NonFatal(e) => listener.onBlockFetchFailure(id, e) }
    }
  }

  private def fetchOne(host: String, remotePort: Int, blockId: String): ManagedBuffer = {
    requireReady()
    readRequests.incrementAndGet()
    val transferId = s"$nodeId-$epoch-${nextTransferId.getAndIncrement()}"
    trace(s"SCACHE_UB_FETCH_START transferId=$transferId consumerNodeId=$nodeId consumerEpoch=$epoch " +
      s"producerControl=$host:$remotePort blockId=$blockId")
    withControl(host, remotePort) { (channel, peer) =>
      val acquireResult = channel.exchange(Acquire(blockId))
      val acquireRequestId = channel.lastExchangeId
      acquireResult match {
        case Grant(descriptor) =>
          validateDescriptor(descriptor, peer, blockId)
          trace(s"SCACHE_UB_DESCRIPTOR transferId=$transferId controlRequestId=$acquireRequestId " +
            s"blockId=$blockId producerNodeId=${descriptor.nodeId} producerEpoch=${descriptor.epoch} " +
            s"consumerNodeId=$nodeId consumerEpoch=$epoch arenaId=${descriptor.arenaId} " +
            s"baseAddress=${descriptor.baseAddress} offset=${descriptor.offset} remoteAddress=${descriptor.remoteAddress} " +
            s"length=${descriptor.length} token=${descriptor.token} generation=${descriptor.generation} " +
            s"crc=${descriptor.crc} leaseId=${descriptor.leaseId} leaseOwner=$nodeId/$epoch " +
            s"leaseTimeoutMs=$leaseTimeoutMs terminal=GRANT")
          val localArena = arena
          val local = localArena.allocateScratch(descriptor.length)
          try {
            transferRead(descriptor, local.buffer, transferId, blockId)
            val actual = if (sharedPoolChecksumEnabled) crc(local.buffer, descriptor.length) else -1L
            if (sharedPoolChecksumEnabled && actual != descriptor.crc) {
              checksumErrors.incrementAndGet()
              throw new IllegalStateException(s"CRC mismatch for $blockId expected=${descriptor.crc} actual=$actual")
            }
            val copied = ByteBuffer.allocate(descriptor.length)
            val source = local.buffer.duplicate(); source.position(0); source.limit(descriptor.length)
            copied.put(source); copied.flip()
            channel.exchange(Release(descriptor.leaseId, descriptor.generation)) match {
              case Ack =>
                trace(s"SCACHE_UB_FETCH_TERMINAL transferId=$transferId controlRequestId=${channel.lastExchangeId} " +
                  s"blockId=$blockId length=${descriptor.length} expectedCrc=${descriptor.crc} actualCrc=$actual " +
                  s"leaseId=${descriptor.leaseId} generation=${descriptor.generation} terminal=SUCCESS")
                new NioManagedBuffer(copied)
              case ErrorResponse(message) => throw new IllegalStateException(message)
              case other => throw new IllegalStateException(s"unexpected RELEASE response $other")
            }
          } finally {
            localArena.freeScratch(local)
          }
        case NotFound => throw new java.io.FileNotFoundException(s"remote UB block $blockId was not published")
        case Stale => generationErrors.incrementAndGet(); throw new IllegalStateException(s"stale UB descriptor for $blockId")
        case ErrorResponse(message) => throw new IllegalStateException(message)
        case other => throw new IllegalStateException(s"unexpected ACQUIRE response $other")
      }
    }
  }

  override def uploadBlock(hostname: String, remotePort: Int, execId: String, blockId: BlockId,
      data: ManagedBuffer, level: StorageLevel, classTag: ClassTag[_]): Future[Unit] = {
    implicit val ec: ExecutionContext = ExecutionContext.global
    Future {
      requireReady()
      require(!level.deserialized,
        "UB upload accepts serialized block data only; deserialized StorageLevel has no wire class contract")
      val sourceBytes = {
        val input = data.nioByteBuffer(); val result = new Array[Byte](input.remaining()); input.get(result); result
      }
      val localArena = arena
      val source = localArena.allocateScratch(sourceBytes.length)
      try {
        source.buffer.put(sourceBytes); source.buffer.position(0)
        writeRequests.incrementAndGet()
        withControl(hostname, remotePort) { (channel, peer) =>
          channel.exchange(Prepare(blockId.toString, sourceBytes.length,
            if (sharedPoolChecksumEnabled) crc(source.buffer, sourceBytes.length) else -1L,
            level.toInt, level.replication, classTag.runtimeClass.getName)) match {
            case Grant(descriptor) =>
              validateDescriptor(descriptor, peer, blockId.toString)
              try {
                transferWrite(descriptor, source.buffer, sourceBytes.length)
                channel.exchange(Commit(descriptor.leaseId, descriptor.generation)) match {
                  case Ack => ()
                  case ErrorResponse(message) => throw new IllegalStateException(message)
                  case other => throw new IllegalStateException(s"unexpected COMMIT response $other")
                }
              } catch {
                case NonFatal(e) =>
                  try { channel.exchange(Abort(descriptor.leaseId, descriptor.generation)) }
                  catch { case NonFatal(_) => }
                  throw e
              }
            case ErrorResponse(message) => throw new IllegalStateException(message)
            case other => throw new IllegalStateException(s"unexpected PREPARE response $other")
          }
        }
      } finally localArena.freeScratch(source)
    }
  }

  /** Split a block into backend-neutral logical calls; one transport call remains one operation. */
  private def transferRead(
      descriptor: Descriptor, destination: ByteBuffer, transferId: String, blockId: String): Unit = {
    transferLogical(descriptor.length) { (offset, length, index, count) =>
      val request = transport.read(descriptor.remote, offset.toLong, destination, offset, length)
      trace(s"SCACHE_UB_LOGICAL_READ transferId=$transferId blockId=$blockId " +
        s"requestId=$request operationIndex=$index operationCount=$count offset=$offset " +
        s"length=$length terminal=SUBMITTED")
      await(request, length, isRead = true)
    }
  }

  private def transferWrite(descriptor: Descriptor, source: ByteBuffer, blockLength: Int): Unit = {
    transferLogical(blockLength) { (offset, length, _, _) =>
      val request = transport.write(descriptor.remote, offset.toLong, source, offset, length)
      await(request, length, isRead = false)
    }
  }

  private def transferLogical(blockLength: Int)(operation: (Int, Int, Int, Int) => Unit): Unit = {
    require(blockLength >= 0, s"negative block length $blockLength")
    if (blockLength == 0) return
    val advertised = transport.capabilities().maxLogicalOperationBytes
    require(advertised > 0, s"transport advertised invalid logical limit $advertised")
    val logicalLimit = math.min(Int.MaxValue.toLong, advertised).toInt
    val operationCount = ((blockLength.toLong + logicalLimit - 1L) / logicalLimit).toInt
    var offset = 0
    var index = 0
    while (offset < blockLength) {
      val length = math.min(logicalLimit, blockLength - offset)
      operation(offset, length, index, operationCount)
      offset = Math.addExact(offset, length)
      index += 1
    }
    require(index == operationCount && offset == blockLength,
      s"logical transfer accounting mismatch operations=$index/$operationCount bytes=$offset/$blockLength")
  }

  private def await(request: Long, bytes: Int, isRead: Boolean): Unit = {
    lastNativeRequestId.set(request)
    try {
      val status = transport.waitFor(request, timeoutMs)
      if (status != 0) throw new IllegalStateException(s"URMA completion status=$status")
      val rawOperationBytes = transport.capabilities().maxRawOperationBytes
      completedChunks.addAndGet((bytes.toLong + rawOperationBytes - 1L) / rawOperationBytes)
    } catch {
      case NonFatal(e) => failedChunks.incrementAndGet(); throw e
    }
  }

  private def validateDescriptor(descriptor: Descriptor, peer: PeerIdentity, blockId: String): Unit = {
    try UBBlockTransferService.validateDescriptorIdentity(descriptor, peer, blockId)
    catch { case e: IllegalStateException =>
      generationErrors.incrementAndGet()
      throw e
    }
  }

  private def acceptLoop(): Unit = while (!closed) {
    try {
      val socket = server.accept()
      workers.execute(() => serve(socket))
    } catch {
      case _: java.net.SocketException if closed =>
      case NonFatal(e) => if (!closed) logWarning("UB control accept failed", e)
    }
  }

  private def serve(socket: Socket): Unit = {
    var ownerKey: String = null
    activeControlConnections.incrementAndGet()
    try {
      socket.setSoTimeout(timeoutMs)
      val channel = new ControlChannel(socket)
      val peer = channel.receiveRequest() match {
        case Hello(remoteNode, remoteEpoch, remoteEndpoint) =>
          // Complete the TCP HELLO handshake before importing/binding the remote
          // jetty.  Both peers call ensureConnected after receiving this reply;
          // doing it before the reply deadlocks the first cross-process control
          // exchange because each provider waits for the other peer to bind.
          channel.sendResponse(Hello(nodeId, epoch, endpoint.clone()))
          ensureConnected(remoteEndpoint)
          PeerIdentity(remoteNode, remoteEpoch)
        case other => throw new IllegalStateException(s"control connection must start with HELLO, got $other")
      }
      ownerKey = s"${peer.key}#${UUID.randomUUID()}"
      var keepServing = true
      while (keepServing) channel.receiveRequest() match {
        case Capabilities => channel.sendResponse(Capabilities)
        case Acquire(blockId) =>
          arena.acquire(blockId, ownerKey) match {
            case Some(d) =>
              trace(s"SCACHE_UB_CONTROL_ACQUIRE requestId=${channel.currentRequestId} producerNodeId=$nodeId " +
                s"producerEpoch=$epoch consumer=${peer.key} blockId=$blockId arenaId=${d.arenaId} " +
                s"offset=${d.offset} length=${d.length} token=${d.token} generation=${d.generation} " +
                s"crc=${d.crc} leaseId=${d.leaseId} leaseOwner=$ownerKey leaseTimeoutMs=$leaseTimeoutMs terminal=GRANT")
              channel.sendResponse(Grant(d))
            case None => channel.sendResponse(NotFound)
          }
        case Release(lease, generation) =>
          arena.release(lease, generation, ownerKey) match {
            case Released | AlreadyReleased =>
              trace(s"SCACHE_UB_CONTROL_RELEASE requestId=${channel.currentRequestId} producerNodeId=$nodeId " +
                s"consumer=${peer.key} leaseId=$lease generation=$generation leaseOwner=$ownerKey terminal=ACK")
              channel.sendResponse(Ack)
            case LeaseRejected(message) => leaseErrors.incrementAndGet(); channel.sendResponse(ErrorResponse(message))
          }
        case Prepare(blockId, length, expectedCrc, flags, replication, className) =>
          if (length < 0 || length > arena.capacity) channel.sendResponse(ErrorResponse("invalid PREPARE length"))
          else channel.sendResponse(Grant(arena.prepare(blockId, length, expectedCrc, flags, replication, ownerKey)))
        case Commit(lease, generation) =>
          try {
            val committed = arena.commit(lease, generation, ownerKey)
            val stored = dataManager.putBlockData(BlockId(committed.blockId),
              new NioManagedBuffer(ByteBuffer.wrap(committed.bytes)), committed.level, ClassTag.Any)
            if (!stored) throw new IllegalStateException(s"target refused uploaded block ${committed.blockId}")
            channel.sendResponse(Ack)
          } catch { case NonFatal(e) => channel.sendResponse(ErrorResponse(s"COMMIT failed: ${e.getMessage}")) }
        case Abort(lease, generation) => arena.abort(lease, generation, ownerKey); channel.sendResponse(Ack)
        case other => channel.sendResponse(ErrorResponse(s"unsupported control message $other")); keepServing = false
      }
    } catch {
      case _: EOFException =>
      case NonFatal(e) => if (!closed) logWarning("UB control request failed", e)
    } finally {
      if (ownerKey != null && arena != null) arena.releaseOwner(ownerKey)
      try socket.close() catch { case NonFatal(_) => }
      activeControlConnections.decrementAndGet()
    }
  }

  private def withControl[T](host: String, remotePort: Int)(body: (ControlChannel, PeerIdentity) => T): T = {
    val socket = new Socket(host, remotePort)
    socket.setSoTimeout(timeoutMs)
    try {
      val channel = new ControlChannel(socket)
      channel.exchange(Hello(nodeId, epoch, endpoint.clone())) match {
        case Hello(remoteNode, remoteEpoch, remoteEndpoint) =>
          ensureConnected(remoteEndpoint)
          val peer = PeerIdentity(remoteNode, remoteEpoch)
          channel.exchange(Capabilities) match {
            case Capabilities => body(channel, peer)
            case ErrorResponse(message) => throw new IllegalStateException(message)
            case other => throw new IllegalStateException(s"CAPABILITIES failed: $other")
          }
        case ErrorResponse(message) => throw new IllegalStateException(message)
        case other => throw new IllegalStateException(s"HELLO failed: $other")
      }
    } finally socket.close()
  }

  private def ensureConnected(remoteBytes: Array[Byte]): Unit = connectLock.synchronized {
    if (connectedPeer == null) {
      transport.connect(remoteBytes)
      connectedPeer = remoteBytes.clone()
    } else if (!java.util.Arrays.equals(connectedPeer, remoteBytes)) {
      transport.connect(remoteBytes)
      connectedPeer = remoteBytes.clone()
    }
  }

  private def requireReady(): Unit = {
    if (closed || transport == null || arena == null) throw new IllegalStateException("UB transport is not ready")
  }

  private def summary(message: => String): Unit = {
    if (summaryDiagnostics) logInfo(message)
  }

  private def trace(message: => String): Unit = {
    if (traceDiagnostics) {
      val index = traceEvents.getAndIncrement()
      if (index < traceEventLimit) {
        logInfo(message)
      } else if (index == traceEventLimit) {
        logInfo(s"SCACHE_UB_TRACE_SUPPRESSED limit=$traceEventLimit")
      }
    }
  }

  override def close(): Unit = {
    if (closed) return
    summary(s"UB BlockTransferService closing node=$nodeId reads=${readRequests.get()} " +
      s"writes=${writeRequests.get()} completedChunks=${completedChunks.get()} " +
      s"failedChunks=${failedChunks.get()} checksumErrors=${checksumErrors.get()} " +
      s"leaseErrors=${leaseErrors.get()} generationErrors=${generationErrors.get()}")
    closed = true
    if (server != null) try server.close() catch { case NonFatal(_) => }
    if (acceptThread != null) try acceptThread.join(1000L) catch { case _: InterruptedException => Thread.currentThread.interrupt() }
    if (arena != null) try arena.close(unpublishTimeoutMs) catch { case NonFatal(e) => logWarning("UB arena close failed", e) }
    workers.shutdownNow()
    if (transport != null) try transport.close() catch { case NonFatal(e) => logWarning("UB transport close failed", e) }
  }

  private[scache] def urmaMetrics: Map[String, Long] = {
    val native = if (transport == null || transport.isClosed) None else Some(transport.metrics())
    def backend(metric: UbTransportMetrics, name: String): Long =
      Option(metric.backendCounters.get(name)).map(_.longValue()).getOrElse(0L)
    Map(
      "urma.blockReadRequests" -> readRequests.get(), "urma.blockWriteRequests" -> writeRequests.get(),
      "urma.completedChunks" -> completedChunks.get(), "urma.failedChunks" -> failedChunks.get(),
      "urma.checksumErrors" -> checksumErrors.get(), "urma.leaseErrors" -> leaseErrors.get(),
      "urma.generationErrors" -> generationErrors.get(), "urma.tcpControlBytes" -> controlBytes.get(),
      "urma.sharedPoolPrepares" -> sharedPoolPrepares.get(),
      "urma.sharedPoolCommits" -> sharedPoolCommits.get(),
      "urma.sharedPoolAborts" -> sharedPoolAborts.get(),
      "urma.sharedPoolImports" -> sharedPoolImports.get(),
      "urma.sharedPoolDestinationReads" -> sharedPoolDestinationReads.get(),
      "urma.sharedPoolDestinationBytes" -> sharedPoolDestinationBytes.get(),
      "urma.sharedPoolDestinationIdentityMatches" -> sharedPoolDestinationIdentityMatches.get(),
      "urma.sharedPoolDestinationIdentityMismatches" -> sharedPoolDestinationIdentityMismatches.get(),
      "urma.sharedPoolConsumerReservations" -> sharedPoolConsumerReservations.get(),
      "urma.sharedPoolMultiConsumerReservations" -> sharedPoolMultiConsumerReservations.get(),
      "urma.sharedPoolDeferredReleases" -> sharedPoolDeferredReleases.get(),
      "urma.sharedPoolChecksumEnabled" -> (if (sharedPoolChecksumEnabled) 1L else 0L),
      "urma.tcpPayloadBytes" -> payloadBytes.get(), "urma.nettyPayloadBytes" -> 0L,
      "urma.fallbacks" -> 0L, "urma.arenaCount" -> (if (arena == null) 0L else arena.arenaCount.toLong),
      "urma.nodeEpoch" -> epoch,
      "urma.lastNativeAggregateRequestId" -> lastNativeRequestId.get(),
      "urma.activeControlConnections" -> activeControlConnections.get(),
      "urma.arenaFreeBytes" -> (if (arena == null) 0L else arena.freeBytes),
      "urma.arenaPublishedBlocks" -> (if (arena == null) 0L else arena.publishedBlocks),
      "urma.activeLeases" -> (if (arena == null) 0L else arena.activeLeases),
      "urma.activeScratch" -> (if (arena == null) 0L else arena.activeScratch),
      "urma.submittedChunks" -> native.map(backend(_, "submittedRequests")).getOrElse(0L),
      "urma.providerTerminalChunks" -> native.map(backend(_, "providerDrainedRequests")).getOrElse(0L),
      "urma.apiTerminalChunks" -> native.map(m => backend(m, "completedRequests") +
        backend(m, "failedRequests") + backend(m, "timedOutRequests")).getOrElse(0L),
      "urma.submittedBytes" -> native.map(_.submittedBytes).getOrElse(0L),
      "urma.completedBytes" -> native.map(_.completedBytes).getOrElse(0L),
      "urma.transportSubmittedOperations" -> native.map(_.submittedOperations).getOrElse(0L),
      "urma.transportCompletedOperations" -> native.map(_.completedOperations).getOrElse(0L),
      "urma.transportFailedOperations" -> native.map(_.failedOperations).getOrElse(0L),
      "urma.transportTimedOutOperations" -> native.map(_.timedOutOperations).getOrElse(0L),
      "urma.transportInflightOperations" -> native.map(_.inflightOperations).getOrElse(0L),
      "urma.carrierBytes" -> native.map(backend(_, "carrierBytes")).getOrElse(0L),
      "urma.carrierLateOperations" -> native.map(_.carrierLateOperations).getOrElse(0L),
      "urma.controlPayloadBytes" -> native.map(backend(_, "controlPayloadBytes")).getOrElse(0L),
      "urma.activeMappings" -> native.map(backend(_, "activeMappings")).getOrElse(0L),
      "urma.activeTokens" -> native.map(backend(_, "registeredTokens")).getOrElse(0L),
      "urma.activeSlots" -> native.map(backend(_, "activeSlots")).getOrElse(0L),
      "urma.providerOutstanding" -> native.map(backend(_, "providerOutstandingRequests")).getOrElse(0L),
      "urma.nativeInflight" -> native.map(backend(_, "inflightRequests")).getOrElse(0L),
      "urma.registeredRegions" -> native.map(_.registeredRegions).getOrElse(0L),
      "urma.activeImports" -> native.map(backend(_, "activeImports")).getOrElse(0L),
      "urma.activeTransports" -> native.map(backend(_, "activeTransports")).getOrElse(0L))
  }

  private def crc(buffer: ByteBuffer, length: Int): Long = UBBlockTransferService.crc(buffer, length)

  private final class ControlChannel(socket: Socket) {
    private val in = new DataInputStream(new BufferedInputStream(socket.getInputStream))
    private val out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream))
    private val receivedRequestIds = scala.collection.mutable.HashSet.empty[Long]
    private var lastReceivedRequestId = 0L
    private var lastExchangeRequestId = 0L
    def currentRequestId: Long = lastReceivedRequestId
    def lastExchangeId: Long = lastExchangeRequestId
    private def send(message: Message, requestId: Long): Unit = {
      val payload = encode(message)
      out.writeByte(message.kind); out.writeInt(ControlProtocolVersion); out.writeLong(requestId)
      out.writeInt(payload.length); out.write(payload); out.flush()
      controlBytes.addAndGet(ControlHeaderBytes + payload.length)
    }
    private def receive(): (Message, Long) = {
      val kind = in.readUnsignedByte(); val version = in.readInt(); val requestId = in.readLong(); val length = in.readInt()
      validateControlHeader(version, requestId, length, receivedRequestIds)
      receivedRequestIds += requestId
      val payload = new Array[Byte](length); in.readFully(payload); controlBytes.addAndGet(ControlHeaderBytes + length)
      (decode(kind, payload), requestId)
    }
    def receiveRequest(): Message = { val (message, id) = receive(); lastReceivedRequestId = id; message }
    def sendResponse(message: Message): Unit = {
      if (lastReceivedRequestId <= 0) throw new IllegalStateException("response has no request correlation id")
      send(message, lastReceivedRequestId)
    }
    def exchange(message: Message): Message = {
      val requestId = nextControlRequestId.getAndIncrement()
      lastExchangeRequestId = requestId
      send(message, requestId)
      val (response, correlationId) = receive()
      if (correlationId != requestId) throw new IllegalStateException(
        s"control correlation mismatch request=$requestId response=$correlationId")
      response
    }
  }
}

private[ub] object UBBlockTransferService {
  private val ControlProtocolVersion = 1
  private val ControlHeaderBytes = 17L // opcode + protocol version + request/correlation id + length
  private val MaxControlFrame = 64 * 1024
  private[ub] def validateControlHeader(
      version: Int,
      requestId: Long,
      length: Int,
      seen: scala.collection.Set[Long]): Unit = {
    if (version != ControlProtocolVersion) throw new IllegalArgumentException(s"unsupported control protocol $version")
    if (requestId <= 0 || seen.contains(requestId)) throw new IllegalArgumentException(s"invalid or duplicate control request id $requestId")
    if (length < 0 || length > MaxControlFrame) throw new IllegalArgumentException(s"invalid control frame length $length")
  }
  private[ub] def validateDescriptorIdentity(descriptor: Descriptor, peer: PeerIdentity, blockId: String): Unit = {
    if (descriptor.protocolVersion != ControlProtocolVersion || descriptor.nodeId != peer.nodeId ||
        descriptor.epoch != peer.epoch || descriptor.blockId != blockId) {
      throw new IllegalStateException(
        s"stale or mismatched UB descriptor block=${descriptor.blockId} producer=${descriptor.nodeId}/${descriptor.epoch} " +
          s"expected=$blockId/${peer.nodeId}/${peer.epoch}")
    }
    if (descriptor.offset < 0 || descriptor.length < 0 || descriptor.baseAddress <= 0 ||
        descriptor.remoteAddress != descriptor.baseAddress + descriptor.offset || descriptor.token < 0) {
      throw new IllegalStateException(s"invalid UB descriptor bounds for $blockId")
    }
  }
  private sealed trait Message { def kind: Int }
  private final case class PeerIdentity(nodeId: String, epoch: Long) {
    require(nodeId.nonEmpty, "peer nodeId must not be empty")
    def key: String = s"$nodeId/$epoch"
  }
  private final case class Hello(node: String, epoch: Long, endpoint: Array[Byte]) extends Message { val kind = 1 }
  private final case class Acquire(blockId: String) extends Message { val kind = 2 }
  private final case class Grant(descriptor: Descriptor) extends Message { val kind = 3 }
  private case object NotFound extends Message { val kind = 4 }
  private final case class Release(lease: String, generation: Long) extends Message { val kind = 5 }
  private case object Ack extends Message { val kind = 6 }
  private final case class ErrorResponse(message: String) extends Message { val kind = 7 }
  private final case class Prepare(blockId: String, length: Int, crc: Long, flags: Int, replication: Int, className: String) extends Message { val kind = 8 }
  private final case class Commit(lease: String, generation: Long) extends Message { val kind = 9 }
  private final case class Abort(lease: String, generation: Long) extends Message { val kind = 10 }
  private case object Stale extends Message { val kind = 11 }
  private case object Capabilities extends Message { val kind = 12 }

  private[ub] final case class Descriptor(protocolVersion: Int, nodeId: String, epoch: Long, arenaId: String,
      baseAddress: Long, offset: Int, remoteAddress: Long, length: Int, token: Int,
      segmentGeneration: Long, generation: Long, crc: Long, leaseId: String, blockId: String) {
    // generation is the lease/block generation; segmentGeneration is the
    // provider MR generation required by native-vdev import.
    def remote: RemoteBuffer = new RemoteBuffer(remoteAddress, length.toLong, token, segmentGeneration)
  }
  /** Local shared-memory bridge descriptor; remote peers never consume its path. */
  private[scache] final case class PoolSlice(path: String, offset: Long, length: Int, generation: Long)
  private[ub] final case class Committed(blockId: String, bytes: Array[Byte], level: StorageLevel)
  private[ub] sealed trait ReleaseResult
  private[ub] case object Released extends ReleaseResult
  private[ub] case object AlreadyReleased extends ReleaseResult
  private[ub] final case class LeaseRejected(message: String) extends ReleaseResult

  private def putString(out: DataOutputStream, value: String): Unit = {
    val raw = value.getBytes(StandardCharsets.UTF_8)
    if (raw.length > 8192) throw new IllegalArgumentException("control string too long")
    out.writeInt(raw.length); out.write(raw)
  }
  private def getString(in: DataInputStream): String = {
    val length = in.readInt(); if (length < 0 || length > 8192) throw new IllegalArgumentException("invalid control string")
    val raw = new Array[Byte](length); in.readFully(raw); new String(raw, StandardCharsets.UTF_8)
  }
  private def putDescriptor(out: DataOutputStream, d: Descriptor): Unit = {
    out.writeInt(d.protocolVersion); putString(out, d.nodeId); out.writeLong(d.epoch); putString(out, d.arenaId)
    out.writeLong(d.baseAddress); out.writeInt(d.offset); out.writeLong(d.remoteAddress); out.writeInt(d.length)
    out.writeInt(d.token); out.writeLong(d.segmentGeneration); out.writeLong(d.generation); out.writeLong(d.crc)
    putString(out, d.leaseId); putString(out, d.blockId)
  }
  private def getDescriptor(in: DataInputStream): Descriptor = {
    val version = in.readInt(); if (version != 1) throw new IllegalArgumentException(s"unsupported descriptor protocol $version")
    Descriptor(version, getString(in), in.readLong(), getString(in), in.readLong(), in.readInt(), in.readLong(),
      in.readInt(), in.readInt(), in.readLong(), in.readLong(), in.readLong(), getString(in), getString(in))
  }
  private def encode(message: Message): Array[Byte] = {
    val raw = new java.io.ByteArrayOutputStream(); val out = new DataOutputStream(raw)
    message match {
      case Hello(node, epoch, endpoint) => putString(out, node); out.writeLong(epoch); out.writeInt(endpoint.length); out.write(endpoint)
      case Acquire(id) => putString(out, id)
      case Grant(d) => putDescriptor(out, d)
      case Release(lease, generation) => putString(out, lease); out.writeLong(generation)
      case ErrorResponse(message) => putString(out, message)
      case Prepare(id, length, crc, flags, replication, className) => putString(out, id); out.writeInt(length); out.writeLong(crc); out.writeInt(flags); out.writeInt(replication); putString(out, className)
      case Commit(lease, generation) => putString(out, lease); out.writeLong(generation)
      case Abort(lease, generation) => putString(out, lease); out.writeLong(generation)
      case NotFound | Ack | Stale | Capabilities =>
    }
    out.flush(); raw.toByteArray
  }
  private def decode(kind: Int, raw: Array[Byte]): Message = {
    val in = new DataInputStream(new java.io.ByteArrayInputStream(raw))
    val message: Message = kind match {
      case 1 => val node = getString(in); val epoch = in.readLong(); val n = in.readInt(); if (n <= 0 || n > MaxControlFrame) throw new IllegalArgumentException("invalid endpoint size"); val endpoint = new Array[Byte](n); in.readFully(endpoint); Hello(node, epoch, endpoint)
      case 2 => Acquire(getString(in))
      case 3 => Grant(getDescriptor(in))
      case 4 => NotFound
      case 5 => Release(getString(in), in.readLong())
      case 6 => Ack
      case 7 => ErrorResponse(getString(in))
      case 8 => Prepare(getString(in), in.readInt(), in.readLong(), in.readInt(), in.readInt(), getString(in))
      case 9 => Commit(getString(in), in.readLong())
      case 10 => Abort(getString(in), in.readLong())
      case 11 => Stale
      case 12 => Capabilities
      case _ => throw new IllegalArgumentException(s"unknown control message $kind")
    }
    if (in.available() != 0) throw new IllegalArgumentException("trailing control bytes")
    message
  }

  private[ub] def runProtocolContracts(): Seq[(String, Boolean)] = {
    val d = Descriptor(1, "node", 7L, "arena", 0x1000L, 16, 0x1010L, 64, 3, 42L, 9L, 123L,
      "lease", "scache_contract_1_1_0_0")
    val messages: Seq[(String, Message)] = Seq(
      "hello" -> Hello("node", 7L, new Array[Byte](64)), "capabilities" -> Capabilities,
      "acquire" -> Acquire(d.blockId), "acquired" -> Grant(d), "not_found" -> NotFound,
      "release" -> Release(d.leaseId, d.generation), "released" -> Ack,
      "error" -> ErrorResponse("typed"),
      "prepare" -> Prepare(d.blockId, d.length, d.crc, 4, 1, "java.lang.Object"),
      "commit" -> Commit(d.leaseId, d.generation), "abort" -> Abort(d.leaseId, d.generation),
      "stale" -> Stale)
    val results = scala.collection.mutable.ArrayBuffer.empty[(String, Boolean)]
    messages.foreach { case (name, message) =>
      val raw = encode(message)
      results += name -> java.util.Arrays.equals(raw, encode(decode(message.kind, raw)))
    }
    def rejected(name: String)(body: => Unit): Unit = {
      var ok = false; try body catch { case _: IllegalArgumentException | _: IllegalStateException => ok = true }
      results += name -> ok
    }
    rejected("unknown_opcode") { decode(255, Array.emptyByteArray) }
    rejected("trailing_bytes") { decode(Ack.kind, Array[Byte](1)) }
    rejected("protocol_version") { validateControlHeader(2, 1L, 0, Set.empty) }
    rejected("zero_request_id") { validateControlHeader(1, 0L, 0, Set.empty) }
    rejected("duplicate_request_id") { validateControlHeader(1, 3L, 0, Set(3L)) }
    rejected("negative_frame") { validateControlHeader(1, 4L, -1, Set.empty) }
    rejected("oversize_frame") { validateControlHeader(1, 5L, MaxControlFrame + 1, Set.empty) }
    results += "descriptor_epoch_match" -> {
      validateDescriptorIdentity(d, PeerIdentity("node", 7L), d.blockId); true
    }
    rejected("stale_producer_epoch") {
      validateDescriptorIdentity(d, PeerIdentity("node", 8L), d.blockId)
    }
    rejected("descriptor_block_mismatch") {
      validateDescriptorIdentity(d, PeerIdentity("node", 7L), d.blockId + "-new")
    }
    results.toSeq
  }

  private[ub] def runArenaContracts(transport: UbTransport): Seq[(String, Boolean)] = {
    val arena = new RegisteredArena(transport, 4096, "contract-node", 1L)
    val results = scala.collection.mutable.ArrayBuffer.empty[(String, Boolean)]
    def check(name: String)(body: => Boolean): Unit = {
      var ok = false; try ok = body catch { case _: Throwable => ok = false }
      results += name -> ok
    }
    def rejected(name: String)(body: => Unit): Unit = {
      var ok = false; try body catch { case _: IllegalArgumentException | _: IllegalStateException => ok = true }
      results += name -> ok
    }
    try {
      check("arena_initial_free") { arena.freeBytes == 4096L }
      arena.publish("zero", Array.emptyByteArray)
      check("zero_publish") { arena.stateOf("zero") == "Published" && arena.freeBytes == 4095L }
      val zero = arena.acquire("zero").get
      check("lease_acquire") { arena.activeLeases == 1 }
      rejected("unpublish_wait") { arena.unpublish("zero", 0) }
      check("unpublish_rollback") { arena.stateOf("zero") == "Published" }
      check("stale_generation") { !arena.release(zero.leaseId, zero.generation + 1) }
      check("lease_release") { arena.release(zero.leaseId, zero.generation) && arena.activeLeases == 0 }
      check("double_release") { !arena.release(zero.leaseId, zero.generation) }
      arena.unpublish("zero", 10)
      check("zero_free") { arena.stateOf("zero") == "FREE" && arena.freeBytes == 4096L }
      arena.publish("a", Array.fill[Byte](64)(7)); val a = arena.acquire("a").get
      check("descriptor_fields") { a.offset == 0 && a.length == 64 && a.remoteAddress == a.baseAddress && a.crc != 0 }
      rejected("duplicate_publish") { arena.publish("a", Array[Byte](1)) }
      arena.release(a.leaseId, a.generation); arena.unpublish("a", 10)
      arena.publish("b", Array.fill[Byte](64)(9)); val b = arena.acquire("b").get
      check("generation_reuse") { b.offset == a.offset && b.generation > a.generation }
      check("old_lease_rejected") { !arena.release(a.leaseId, a.generation) }
      arena.release(b.leaseId, b.generation); arena.unpublish("b", 10)
      rejected("negative_allocation") { arena.allocateScratch(-1) }
      rejected("arena_exhaustion") { arena.allocateScratch(4097) }
      val prepared = arena.prepare("prepared_zero", 0, 0L, 4, 1)
      check("prepare_writing") { arena.stateOf("prepared_zero") == "Writing" }
      val committed = arena.commit(prepared.leaseId, prepared.generation)
      check("zero_commit") { committed.bytes.isEmpty && arena.stateOf("prepared_zero") == "FREE" }
      val aborted = arena.prepare("aborted", 8, 0L, 4, 1); arena.abort(aborted.leaseId, aborted.generation)
      check("abort_free") { arena.stateOf("aborted") == "FREE" && arena.activeLeases == 0 }
      arena.publish("close_active", Array[Byte](1)); val active = arena.acquire("close_active").get
      rejected("close_active_lease") { arena.close(1) }
      check("release_during_close") { arena.release(active.leaseId, active.generation) }
      arena.unpublish("close_active", 10); arena.close(10)
      check("double_close") { arena.close(10); true }
    } finally arena.close()
    val pool = new RegisteredArenaPool(transport, 128, 2, "pool-node", 2L, 60000)
    try {
      pool.publish("pool-a", Array.fill[Byte](96)(1))
      pool.publish("pool-b", Array.fill[Byte](96)(2))
      val pa = pool.acquire("pool-a", "peer-a").get
      val pb = pool.acquire("pool-b", "peer-a").get
      check("multi_arena_selection") { pa.arenaId != pb.arenaId && pool.registeredRegions == 2 }
      check("lease_owner_rejected") {
        pool.release(pa.leaseId, pa.generation, "peer-b").isInstanceOf[LeaseRejected]
      }
      check("lease_owner_release") { pool.release(pa.leaseId, pa.generation, "peer-a") == Released }
      check("lease_idempotent_release") { pool.release(pa.leaseId, pa.generation, "peer-a") == AlreadyReleased }
      pool.release(pb.leaseId, pb.generation, "peer-a")
      pool.unpublish("pool-a", 10); pool.unpublish("pool-b", 10)
      check("multi_arena_recovered") { pool.freeBytes == 256L && pool.activeLeases == 0 }
    } finally pool.close(1000)
    val expiry = new RegisteredArena(transport, 128, "expiry-node", 3L, 1)
    try {
      expiry.publish("expiry", Array[Byte](1)); val lease = expiry.acquire("expiry", "peer-expiry").get
      Thread.sleep(3L)
      check("expired_lease_rejected") {
        expiry.releaseDetailed(lease.leaseId, lease.generation, "peer-expiry") == LeaseRejected("expired lease")
      }
      expiry.unpublish("expiry", 10)
    } finally expiry.close(1000)
    results.toSeq
  }

  private def crc(buffer: ByteBuffer, length: Int): Long = {
    val check = new CRC32(); val copy = buffer.duplicate(); copy.position(0); copy.limit(length)
    while (copy.hasRemaining) check.update(copy.get() & 0xff)
    check.getValue
  }

  private sealed trait ArenaState
  private case object Allocated extends ArenaState
  private case object Writing extends ArenaState
  private case object Published extends ArenaState
  private case object Unpublishing extends ArenaState
  private case object Failed extends ArenaState

  /** A bounded set of stable registrations. Blocks and scratch buffers select an arena by space. */
  private[ub] final class RegisteredArenaPool(
      transport: UbTransport,
      val bytesPerArena: Int,
      val arenaCount: Int,
      nodeId: String,
      epoch: Long,
      leaseTimeoutMs: Int,
      sharedDirectory: Option[String] = None,
      forceSharedCommit: Boolean = false,
      checksumEnabled: Boolean = true) {
    require(arenaCount > 0, "arenaCount must be positive")
    private val arenas: Vector[RegisteredArena] = Vector.tabulate[RegisteredArena](arenaCount) { index =>
      new RegisteredArena(
        transport, bytesPerArena, nodeId, epoch, leaseTimeoutMs,
        sharedDirectory.map(dir => sharedArenaFile(dir, nodeId, index)),
        forceSharedCommit, checksumEnabled)
    }
    val capacity: Int = bytesPerArena
    val totalCapacity: Long = bytesPerArena.toLong * arenaCount
    final class PoolScratch(val owner: RegisteredArena, val scratch: RegisteredArena#Scratch) {
      def buffer: ByteBuffer = scratch.buffer
      def free(): Unit = owner.freeScratch(scratch.asInstanceOf[owner.Scratch])
    }
    private def exhaustion(operation: String): IllegalStateException =
      new IllegalStateException(s"UB arena pool exhausted during $operation totalCapacity=$totalCapacity arenas=$arenaCount")
    def publish(blockId: String, bytes: Array[Byte]): Unit = synchronized {
      if (arenas.exists(_.containsBlock(blockId))) throw new IllegalStateException(s"duplicate published block $blockId")
      val target = arenas.find(_.canAllocate(bytes.length)).getOrElse(throw exhaustion("publish"))
      target.publish(blockId, bytes)
    }
    def prepareShared(blockId: String, length: Int, ownerPeer: String = "spark-shared"): PoolSlice = synchronized {
      if (sharedDirectory.isEmpty) throw new IllegalStateException("UB shared arena is disabled")
      // Spark retries a map task with the same logical ShuffleBlockId after an earlier attempt
      // committed the slice but failed later in the task.  Reclaim that exact stale slice before
      // reserving its replacement; the common non-retry path is still one containsBlock lookup.
      arenas.find(_.containsBlock(blockId)).foreach(
        _.discardForRetry(blockId, ownerPeer, leaseTimeoutMs))
      val target: RegisteredArena = arenas.find(_.canAllocate(length))
        .getOrElse(throw exhaustion("shared prepare"))
      target.prepareShared(blockId, length, ownerPeer)
    }
    def commitShared(
        blockId: String,
        path: String,
        offset: Long,
        length: Int,
        ownerPeer: String = "spark-shared",
        expectedCrc: Long = -1L): Boolean =
      arenas.find(_.containsBlock(blockId)).exists(
        _.commitShared(blockId, path, offset, length, ownerPeer, expectedCrc))
    def abortShared(blockId: String, path: String, offset: Long, length: Int,
        ownerPeer: String = "spark-shared"): Unit =
      arenas.find(_.containsBlock(blockId)).foreach(
        _.abortShared(blockId, path, offset, length, ownerPeer))
    def localShared(blockId: String): Option[PoolSlice] =
      arenas.iterator.flatMap(_.localShared(blockId)).take(1).toSeq.headOption
    def sharedBuffer(blockId: String): ByteBuffer =
      arenas.find(_.containsBlock(blockId)).map(_.sharedBuffer(blockId))
        .getOrElse(throw new IllegalStateException(s"unknown UB shared block $blockId"))
    def unpublish(blockId: String, timeoutMs: Int): Unit =
      arenas.find(_.containsBlock(blockId)).foreach(_.unpublish(blockId, timeoutMs))
    def acquire(blockId: String, ownerPeer: String): Option[Descriptor] =
      arenas.iterator.flatMap(_.acquire(blockId, ownerPeer)).take(1).toSeq.headOption
    def release(lease: String, generation: Long, ownerPeer: String): ReleaseResult =
      arenas.find(_.knowsLease(lease)).map(_.releaseDetailed(lease, generation, ownerPeer))
        .getOrElse(LeaseRejected("unknown lease"))
    def prepare(blockId: String, length: Int, expectedCrc: Long, flags: Int,
        replication: Int, ownerPeer: String): Descriptor = synchronized {
      if (arenas.exists(_.containsBlock(blockId))) throw new IllegalStateException(s"block $blockId already exists")
      val target = arenas.find(_.canAllocate(length)).getOrElse(throw exhaustion("prepare"))
      target.prepare(blockId, length, expectedCrc, flags, replication, ownerPeer)
    }
    def commit(lease: String, generation: Long, ownerPeer: String): Committed =
      arenas.find(_.knowsLease(lease)).map(_.commit(lease, generation, ownerPeer))
        .getOrElse(throw new IllegalStateException("unknown prepared lease"))
    def abort(lease: String, generation: Long, ownerPeer: String): Unit =
      arenas.find(_.knowsLease(lease)).foreach(_.abort(lease, generation, ownerPeer))
    def releaseOwner(ownerPeer: String): Unit = arenas.foreach(_.releaseOwner(ownerPeer))
    def allocateScratch(length: Int): PoolScratch = synchronized {
      val target = arenas.find(_.canAllocate(length)).getOrElse(throw exhaustion("scratch"))
      val scratch = target.allocateScratch(length)
      new PoolScratch(target, scratch)
    }
    def freeScratch(scratch: PoolScratch): Unit = scratch.free()
    def publishedBlocks: Long = arenas.map(_.publishedBlocks).sum
    def activeLeases: Long = arenas.map(_.activeLeases.toLong).sum
    def activeScratch: Long = arenas.map(_.activeScratch.toLong).sum
    def freeBytes: Long = arenas.map(_.freeBytes).sum
    def registeredRegions: Long = arenas.count(_.isRegistered).toLong
    def close(timeoutMs: Int): Unit = {
      var failure: Throwable = null
      arenas.foreach { current =>
        try current.close(timeoutMs)
        catch { case NonFatal(e) => if (failure == null) failure = e else failure.addSuppressed(e) }
      }
      if (failure != null) throw failure
    }
  }

  private def sharedArenaFile(directory: String, nodeId: String, index: Int): String = {
    val safeNode = nodeId.replaceAll("[^A-Za-z0-9_.-]", "_")
    Paths.get(directory, s"$safeNode-arena-$index.pool").toString
  }

  /** One stable registration; individual descriptors are offset windows in it. */
  private[ub] final class RegisteredArena(
      transport: UbTransport,
      val capacity: Int,
      nodeId: String,
      epoch: Long,
      leaseTimeoutMs: Int = 60000,
      val sharedPath: Option[String] = None,
      forceSharedCommit: Boolean = false,
      checksumEnabled: Boolean = true) {
    require(capacity > 0, "spark.urma.arenaBytes must be positive")
    private val arenaId = UUID.randomUUID().toString
    private val memory: ByteBuffer = sharedPath match {
      case Some(path) =>
        val file = Paths.get(path)
        val parent = file.getParent
        if (parent != null) Files.createDirectories(parent)
        val channel = FileChannel.open(
          file,
          StandardOpenOption.READ,
          StandardOpenOption.WRITE,
          StandardOpenOption.CREATE)
        try {
          if (channel.size() < capacity.toLong) {
            channel.position(capacity.toLong - 1L)
            channel.write(ByteBuffer.wrap(Array[Byte](0)))
          }
          channel.map(MapMode.READ_WRITE, 0L, capacity.toLong)
        } finally channel.close()
      case None => ByteBuffer.allocateDirect(capacity)
    }
    private val registration = transport.registerBuffer(memory)
    final case class Slice(offset: Int, length: Int)
    private final case class Entry(blockId: String, slice: Slice, generation: Long, var expectedCrc: Long,
        flags: Int, replication: Int, var state: ArenaState, var leases: Int,
        var committedLength: Int = -1)
    private final case class Lease(leaseId: String, entry: Entry, acquiredAt: Long, deadline: Long,
        ownerPeer: String, var state: String)
    private val free = new java.util.TreeMap[Integer, Integer]()
    private val entries = scala.collection.mutable.HashMap.empty[String, Entry]
    private val leases = scala.collection.mutable.HashMap.empty[String, Lease]
    private val releasedLeases = scala.collection.mutable.LinkedHashMap.empty[String, (Long, String)]
    private var nextGeneration = 1L
    private var scratchCount = 0
    private var closing = false
    private var registrationClosed = false
    free.put(0, capacity)

    def publish(blockId: String, bytes: Array[Byte]): Unit = synchronized {
      if (closing) throw new IllegalStateException("UB arena is closing")
      if (entries.contains(blockId)) throw new IllegalStateException(s"duplicate published block $blockId")
      val slice = allocate(bytes.length); val target = view(slice); target.put(bytes); target.position(0)
      val entry = Entry(blockId, slice, generation(),
        if (checksumEnabled) crc(target, bytes.length) else -1L, 0, 1, Allocated, 0, bytes.length)
      entry.state = Published
      entries.put(blockId, entry)
    }
    def acquire(blockId: String, ownerPeer: String = "local-contract"): Option[Descriptor] = synchronized {
      if (closing) return None
      entries.get(blockId).filter(_.state == Published).map { entry =>
        val leaseId = UUID.randomUUID().toString
        val now = System.nanoTime()
        val lease = Lease(leaseId, entry, now, now + leaseTimeoutMs.toLong * 1000000L, ownerPeer, "ACQUIRED")
        entry.leases += 1; leases.put(leaseId, lease)
        descriptor(entry, leaseId)
      }
    }
    def release(lease: String, generation: Long): Boolean = synchronized {
      releaseDetailed(lease, generation, "local-contract") match {
        case Released => true
        case AlreadyReleased | _: LeaseRejected => false
      }
    }
    def releaseDetailed(leaseId: String, generation: Long, ownerPeer: String): ReleaseResult = synchronized {
      leases.get(leaseId) match {
        case Some(lease) if lease.entry.generation != generation => LeaseRejected("stale lease generation")
        case Some(lease) if lease.ownerPeer != ownerPeer => LeaseRejected("lease owner mismatch")
        case Some(lease) if System.nanoTime() > lease.deadline =>
          leases.remove(leaseId); lease.state = "EXPIRED"; lease.entry.leases -= 1; rememberReleased(lease); notifyAll()
          LeaseRejected("expired lease")
        case Some(lease) =>
          leases.remove(leaseId); lease.state = "RELEASED"; lease.entry.leases -= 1; rememberReleased(lease); notifyAll(); Released
        case None if releasedLeases.get(leaseId).contains((generation, ownerPeer)) => AlreadyReleased
        case None => LeaseRejected("unknown lease")
      }
    }
    def prepare(blockId: String, length: Int, expectedCrc: Long, flags: Int, replication: Int,
        ownerPeer: String = "local-contract"): Descriptor = synchronized {
      if (closing) throw new IllegalStateException("UB arena is closing")
      if (entries.contains(blockId)) throw new IllegalStateException(s"block $blockId already exists")
      val entry = Entry(blockId, allocate(length), generation(), expectedCrc, flags, replication, Writing, 1, length)
      val leaseId = UUID.randomUUID().toString; val now = System.nanoTime()
      val lease = Lease(leaseId, entry, now, now + leaseTimeoutMs.toLong * 1000000L, ownerPeer, "WRITING")
      entries.put(blockId, entry); leases.put(leaseId, lease); descriptor(entry, leaseId)
    }

    /** Reserve an arena slice for the Spark executor-side mmap writer. */
    def prepareShared(blockId: String, length: Int, ownerPeer: String = "spark-shared"): PoolSlice = synchronized {
      if (sharedPath.isEmpty) throw new IllegalStateException("UB shared arena is disabled")
      val descriptor = prepare(blockId, length, -1L, 0, 1, ownerPeer)
      PoolSlice(sharedPath.get, descriptor.offset, descriptor.length, descriptor.generation)
    }

    /** Return a duplicate view of the mapped slice; no bytes are copied. */
    def sharedBuffer(blockId: String): ByteBuffer = synchronized {
      val entry = entries.getOrElse(blockId,
        throw new IllegalStateException(s"unknown UB shared block $blockId"))
      if (entry.state != Writing && entry.state != Published) {
        throw new IllegalStateException(s"UB shared block $blockId is not readable in state ${entry.state}")
      }
      view(entry.slice)
    }

    def localShared(blockId: String): Option[PoolSlice] = synchronized {
      if (sharedPath.isEmpty) None
      else entries.get(blockId).filter(_.state == Published).map { entry =>
        PoolSlice(sharedPath.get, entry.slice.offset, entry.committedLength, entry.generation)
      }
    }

    /**
     * Transition a Spark-filled slice from WRITING to PUBLISHED. The CRC is computed in-place and
     * the mapped bytes remain in the registered MR; unlike the control-plane COMMIT this method
     * never materializes an Array[Byte] or calls BlockManager.putBlockData.
     */
    def commitShared(
        blockId: String,
        path: String,
        offset: Long,
        length: Int,
        ownerPeer: String = "spark-shared",
        expectedCrc: Long = -1L): Boolean = synchronized {
      val entry = entries.getOrElse(blockId,
        throw new IllegalStateException(s"unknown UB shared block $blockId"))
      if (sharedPath.isEmpty || sharedPath.get != path || entry.state != Writing ||
          entry.slice.offset.toLong != offset || length < 0 || length > entry.slice.length) {
        throw new IllegalStateException(s"UB shared commit bounds/state mismatch block=$blockId")
      }
      val lease = leases.getOrElse(entryLease(entry, ownerPeer),
        throw new IllegalStateException(s"unknown UB shared writer lease block=$blockId"))
      if (lease.ownerPeer != ownerPeer || lease.entry.generation != entry.generation ||
          System.nanoTime() > lease.deadline) {
        throw new IllegalStateException(s"UB shared writer lease is stale block=$blockId")
      }
      java.lang.invoke.VarHandle.fullFence()
      if (forceSharedCommit) memory match {
        case mapped: java.nio.MappedByteBuffer => mapped.force()
        case _ =>
      }
      val actual = if (checksumEnabled) crc(view(entry.slice), length) else -1L
      if (checksumEnabled && expectedCrc >= 0L && expectedCrc != actual) {
        entry.state = Failed
        freeEntry(entry)
        throw new IllegalStateException(
          s"UB shared CRC mismatch block=$blockId expected=$expectedCrc actual=$actual")
      }
      entry.expectedCrc = actual
      entry.committedLength = length
      entry.state = Published
      leases.remove(lease.leaseId)
      lease.state = "PUBLISHED"
      rememberReleased(lease)
      entry.leases -= 1
      notifyAll()
      true
    }

    def abortShared(
        blockId: String,
        path: String,
        offset: Long,
        length: Int,
        ownerPeer: String = "spark-shared"): Unit = synchronized {
      entries.get(blockId).foreach { entry =>
        if (sharedPath.contains(path) && entry.slice.offset.toLong == offset &&
            entry.slice.length == length && entry.state == Writing) {
          abort(entryLease(entry, ownerPeer), entry.generation, ownerPeer)
        }
      }
    }

    /**
     * Reclaim a slice left by a failed Spark task attempt.  A block id is logical (it does not
     * include Spark's task-attempt number), so a retry must be able to reuse it.  Never revoke an
     * in-flight remote read: an active lease makes the replacement fail clearly and preserves the
     * source bytes until the consumer releases them.
     */
    private[ub] def discardForRetry(
        blockId: String, ownerPeer: String, timeoutMs: Int): Unit = synchronized {
      entries.get(blockId).foreach { entry =>
        entry.state match {
          case Published | Unpublishing =>
            entry.state = Unpublishing
            val until = if (timeoutMs <= 0) 0L
            else System.nanoTime() + timeoutMs.toLong * 1000000L
            while (entry.leases > 0 && timeoutMs > 0 && System.nanoTime() < until) {
              wait(math.max(1L, (until - System.nanoTime()) / 1000000L))
            }
            if (entry.leases > 0) {
              entry.state = Published
              throw new IllegalStateException(
                s"cannot replace UB shared block $blockId while ${entry.leases} remote leases are active")
            }
            freeEntry(entry)
          case Writing =>
            val owned = leases.values.filter(lease =>
              (lease.entry eq entry) && lease.ownerPeer == ownerPeer).toVector
            owned.foreach { lease =>
              leases.remove(lease.leaseId)
              lease.state = "RETRY_REPLACED"
              rememberReleased(lease)
              entry.leases -= 1
            }
            if (entry.leases > 0) {
              throw new IllegalStateException(
                s"cannot replace UB shared block $blockId with non-owner leases active")
            }
            entry.state = Failed
            freeEntry(entry)
          case Failed | Allocated =>
            if (entry.leases == 0) freeEntry(entry)
            else throw new IllegalStateException(
              s"cannot replace UB shared block $blockId while leases are active")
        }
      }
    }

    def commit(leaseId: String, generation: Long, ownerPeer: String = "local-contract"): Committed = synchronized {
      val lease = leases.getOrElse(leaseId, throw new IllegalStateException("unknown prepared lease"))
      val entry = lease.entry
      if (entry.generation != generation || entry.state != Writing) throw new IllegalStateException("stale prepared lease")
      if (lease.ownerPeer != ownerPeer) throw new IllegalStateException("prepared lease owner mismatch")
      if (System.nanoTime() > lease.deadline) throw new IllegalStateException("prepared lease expired")
      leases.remove(leaseId); lease.state = "COMMITTED"; rememberReleased(lease)
      val actual = if (checksumEnabled) crc(view(entry.slice), entry.slice.length) else -1L
      if (checksumEnabled && actual != entry.expectedCrc) { entry.state = Failed; freeEntry(entry); throw new IllegalStateException(s"prepared CRC mismatch expected=${entry.expectedCrc} actual=$actual") }
      val bytes = new Array[Byte](entry.slice.length); val source = view(entry.slice); source.get(bytes)
      freeEntry(entry)
      Committed(entry.blockId, bytes, StorageLevel((entry.flags & 8) != 0, (entry.flags & 4) != 0,
        (entry.flags & 2) != 0, (entry.flags & 1) != 0, entry.replication))
    }
    def abort(leaseId: String, generation: Long, ownerPeer: String = "local-contract"): Unit = synchronized {
      leases.get(leaseId).foreach { lease =>
        val entry = lease.entry
        if (entry.generation == generation && entry.state == Writing && lease.ownerPeer == ownerPeer) {
          leases.remove(leaseId); lease.state = "ABORTED"; rememberReleased(lease); entry.state = Failed; freeEntry(entry)
        }
      }
    }
    def releaseOwner(ownerPeer: String): Unit = synchronized {
      leases.values.filter(_.ownerPeer == ownerPeer).toVector.foreach { lease =>
        leases.remove(lease.leaseId)
        lease.state = "OWNER_CLOSED"
        rememberReleased(lease)
        lease.entry.leases -= 1
        if (lease.entry.state == Writing) { lease.entry.state = Failed; freeEntry(lease.entry) }
      }
      notifyAll()
    }
    def unpublish(blockId: String, timeoutMs: Int): Unit = synchronized {
      entries.get(blockId).foreach { entry =>
        if (entry.state != Published) throw new IllegalStateException(s"block $blockId is not published")
        entry.state = Unpublishing
        val until = if (timeoutMs <= 0) 0L else System.nanoTime() + timeoutMs.toLong * 1000000L
        while (entry.leases > 0 && timeoutMs > 0 && System.nanoTime() < until) wait(math.max(1L, (until - System.nanoTime()) / 1000000L))
        if (entry.leases > 0) { entry.state = Published; throw new IllegalStateException(s"timed out waiting for UB leases of $blockId") }
        freeEntry(entry)
      }
    }
    def allocateScratch(length: Int): Scratch = synchronized {
      if (closing) throw new IllegalStateException("UB arena is closing")
      val slice = allocate(length)
      scratchCount += 1
      Scratch(slice, view(slice))
    }
    final case class Scratch(slice: Slice, buffer: ByteBuffer)
    def freeScratch(scratch: Scratch): Unit = synchronized {
      releaseSlice(scratch.slice); scratchCount -= 1; notifyAll()
    }
    def publishedBlocks: Long = synchronized(entries.values.count(_.state == Published).toLong)
    private[ub] def stateOf(blockId: String): String = synchronized(entries.get(blockId).map(_.state.toString).getOrElse("FREE"))
    private[ub] def activeLeases: Int = synchronized(leases.size)
    private[ub] def activeScratch: Int = synchronized(scratchCount)
    private[ub] def freeBytes: Long = synchronized(free.values().toArray.map(_.asInstanceOf[Integer].toLong).sum)
    private[ub] def containsBlock(blockId: String): Boolean = synchronized(entries.contains(blockId))
    private[ub] def canAllocate(length: Int): Boolean = synchronized {
      if (length < 0) false else {
        val needed = math.max(1, length)
        free.values().toArray.exists(_.asInstanceOf[Integer] >= needed)
      }
    }
    private[ub] def knowsLease(leaseId: String): Boolean = synchronized(leases.contains(leaseId) || releasedLeases.contains(leaseId))
    private[ub] def isRegistered: Boolean = synchronized(!registrationClosed)
    private[ub] def transportMetrics: UbTransportMetrics = transport.metrics()
    def close(timeoutMs: Int = 60000): Unit = synchronized {
      if (registrationClosed) return
      closing = true
      val until = System.nanoTime() + timeoutMs.toLong * 1000000L
      while ((leases.nonEmpty || scratchCount != 0) && System.nanoTime() < until) {
        wait(math.max(1L, (until - System.nanoTime()) / 1000000L))
      }
      if (leases.nonEmpty || scratchCount != 0) throw new IllegalStateException(
        s"timed out draining ${leases.size} UB leases and $scratchCount scratch slices")
      def providerOutstanding: Long = Option(transport.metrics().backendCounters
        .get("providerOutstandingRequests")).map(_.longValue()).getOrElse(0L)
      while (providerOutstanding != 0 && System.nanoTime() < until) {
        wait(math.max(1L, math.min(10L, (until - System.nanoTime()) / 1000000L)))
      }
      if (providerOutstanding != 0) throw new IllegalStateException(
        s"timed out draining $providerOutstanding provider requests")
      entries.values.foreach(entry => releaseSlice(entry.slice))
      entries.clear()
      transport.unregisterBuffer(registration)
      registrationClosed = true
    }

    private def descriptor(entry: Entry, lease: String) = Descriptor(1, nodeId, epoch, arenaId,
      registration.remoteAddress, entry.slice.offset, registration.remoteAddress + entry.slice.offset,
      if (entry.state == Published) entry.committedLength else entry.slice.length,
      registration.token.toInt, registration.generation, entry.generation,
      entry.expectedCrc, lease, entry.blockId)
    private def generation(): Long = { val value = nextGeneration; nextGeneration += 1; value }
    private def entryLease(entry: Entry, ownerPeer: String): String = {
      leases.values.find(lease => (lease.entry eq entry) && lease.ownerPeer == ownerPeer)
        .map(_.leaseId)
        .getOrElse(throw new IllegalStateException(
          s"no active UB lease for block=${entry.blockId} owner=$ownerPeer"))
    }
    private def view(slice: Slice): ByteBuffer = { val copy = memory.duplicate(); copy.position(slice.offset); copy.limit(slice.offset + slice.length); copy.slice() }
    private def allocate(length: Int): Slice = {
      if (length < 0) throw new IllegalArgumentException("negative arena allocation")
      val needed = math.max(1, length)
      val it = free.entrySet().iterator(); var selected: java.util.Map.Entry[Integer, Integer] = null
      while (it.hasNext && selected == null) { val candidate = it.next(); if (candidate.getValue >= needed) selected = candidate }
      if (selected == null) throw new IllegalStateException(s"UB arena exhausted requesting $length bytes")
      val offset = selected.getKey; val available = selected.getValue; free.remove(offset)
      if (available > needed) free.put(offset + needed, available - needed)
      Slice(offset, length)
    }
    private def releaseSlice(slice: Slice): Unit = {
      val actualLength = math.max(1, slice.length); var start = slice.offset; var size = actualLength
      val lower = free.floorEntry(start); if (lower != null && lower.getKey + lower.getValue == start) { start = lower.getKey; size += lower.getValue; free.remove(lower.getKey) }
      val higher = free.ceilingEntry(start); if (higher != null && start + size == higher.getKey) { size += higher.getValue; free.remove(higher.getKey) }
      free.put(start, size)
    }
    private def rememberReleased(lease: Lease): Unit = {
      releasedLeases.put(lease.leaseId, (lease.entry.generation, lease.ownerPeer))
      while (releasedLeases.size > 4096) releasedLeases.remove(releasedLeases.head._1)
    }
    private def freeEntry(entry: Entry): Unit = {
      entries.remove(entry.blockId)
      leases.retain { case (_, lease) => lease.entry ne entry }
      releaseSlice(entry.slice); notifyAll()
    }
  }
}
