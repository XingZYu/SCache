/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.scache.storage

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import org.scache.rpc.RpcEndpointRef
import org.scache.util.Utils

private[scache] object BlockManagerMessages {
  //////////////////////////////////////////////////////////////////////////////////
  // Messages from the master to slaves.
  //////////////////////////////////////////////////////////////////////////////////
  sealed trait ToBlockManagerSlave

  /** Result of a capability-aware map prefetch request. */
  case class PrefetchResult(
      correlationId: String,
      status: String,
      sourceBlockManagerId: BlockManagerId,
      targetBlockManagerId: BlockManagerId,
      blockCount: Int,
      submitted: Int,
      completed: Int,
      failed: Int,
      payloadBytes: Long,
      elapsedMs: Long,
      errorType: String = "",
      errorMessage: String = "") extends Serializable

  // Remove a block from the slaves that have it. This can only be used to remove
  // blocks that the master knows about.
  case class RemoveBlock(blockId: BlockId) extends ToBlockManagerSlave

  // Remove all blocks belonging to a specific RDD.
  case class RemoveRdd(rddId: Int) extends ToBlockManagerSlave

  // Remove all blocks belonging to a specific shuffle.
  case class RemoveShuffle(shuffleId: Int) extends ToBlockManagerSlave

  // Remove all blocks belonging to a specific broadcast.
  case class RemoveBroadcast(broadcastId: Long, removeFromDriver: Boolean = true)
    extends ToBlockManagerSlave

  case class StartMapFetch(
      blockManagerId: BlockManagerId,
      appName: String,
      jobId: Int,
      shuffleId: Int,
      mapId: Int,
      correlationId: String) extends ToBlockManagerSlave

  /**
   * Driver -> Executor message to trigger a thread dump.
   */
  case object TriggerThreadDump extends ToBlockManagerSlave

  //////////////////////////////////////////////////////////////////////////////////
  // Messages from slaves to the master.
  //////////////////////////////////////////////////////////////////////////////////
  sealed trait ToBlockManagerMaster

  /** Versioned data-node identity and remote-fetch capability. */
  case class BlockManagerCapability(
      protocolVersion: Int,
      nodeEpoch: String,
      clientId: String,
      blockManagerId: BlockManagerId,
      backend: String,
      networkEnabled: Boolean,
      remoteFetchSupported: Boolean,
      sharedCxlEnabled: Boolean,
      rpcHost: String,
      rpcPort: Int) extends Serializable

  case class RegisterBlockManager(
      blockManagerId: BlockManagerId,
      maxMemSize: Long,
      sender: RpcEndpointRef,
      capability: BlockManagerCapability)
    extends ToBlockManagerMaster

  case object GetBlockManagerCapabilities extends ToBlockManagerMaster

  case object GetPrefetchResults extends ToBlockManagerMaster

  case class UpdateBlockInfo(
      var blockManagerId: BlockManagerId,
      var blockId: BlockId,
      var storageLevel: StorageLevel,
      var memSize: Long,
      var diskSize: Long)
    extends ToBlockManagerMaster
    with Externalizable {

    def this() = this(null, null, null, 0, 0)  // For deserialization only

    override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
      blockManagerId.writeExternal(out)
      out.writeUTF(blockId.name)
      storageLevel.writeExternal(out)
      out.writeLong(memSize)
      out.writeLong(diskSize)
    }

    override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
      blockManagerId = BlockManagerId(in)
      blockId = BlockId(in.readUTF())
      storageLevel = StorageLevel(in)
      memSize = in.readLong()
      diskSize = in.readLong()
    }
  }

  case class GetLocations(blockId: BlockId) extends ToBlockManagerMaster

  case class GetLocationsMultipleBlockIds(blockIds: Array[BlockId]) extends ToBlockManagerMaster

  case class GetPeers(blockManagerId: BlockManagerId) extends ToBlockManagerMaster

  case class GetExecutorEndpointRef(executorId: String) extends ToBlockManagerMaster

  case class GetBlockManagerId(executorId: String) extends ToBlockManagerMaster

  case class RemoveExecutor(execId: String) extends ToBlockManagerMaster

  case object StopBlockManagerMaster extends ToBlockManagerMaster

  case object GetMemoryStatus extends ToBlockManagerMaster

  case object GetStorageStatus extends ToBlockManagerMaster

  case class GetBlockStatus(blockId: BlockId, askSlaves: Boolean = true)
    extends ToBlockManagerMaster

  case class GetMatchingBlockIds(filter: BlockId => Boolean, askSlaves: Boolean = true)
    extends ToBlockManagerMaster

  case class BlockManagerHeartbeat(blockManagerId: BlockManagerId) extends ToBlockManagerMaster

  case class HasCachedBlocks(executorId: String) extends ToBlockManagerMaster

  //////////////////////////////////////////////////////////////////////////////////
  // Shared CXL (fsdax) pool metadata (master-managed).
  //////////////////////////////////////////////////////////////////////////////////

  /** A location inside a shared, file-backed (fsdax) pool: (path, offset, length). */
  case class CxlBlockLocation(poolPath: String, offset: Long, length: Int) extends Serializable

  /**
   * Allocate a slice in the shared CXL pool for a block payload of `length` bytes.
   * @param domainId  Target CXL domain. If empty, the first registered domain is used.
   * @param length    Requested allocation size in bytes.
   */
  case class AllocateCxlBlock(domainId: String, length: Int) extends ToBlockManagerMaster

  /** Batch-allocate multiple slices in a single synchronized call. */
  case class AllocateCxlBlocks(domainId: String, lengths: Seq[Int]) extends ToBlockManagerMaster

  /** Allocate and track a not-yet-committed slice so cancellation can reclaim it. */
  case class ReserveCxlBlock(
      blockId: BlockId, domainId: String, length: Int) extends ToBlockManagerMaster

  /** Batch form of [[ReserveCxlBlock]]. */
  case class ReserveCxlBlocks(
      blockIds: Seq[BlockId], domainId: String, lengths: Seq[Int]) extends ToBlockManagerMaster

  /** Register a committed block as residing in the shared CXL pool. */
  case class RegisterCxlBlock(blockId: BlockId, location: CxlBlockLocation) extends ToBlockManagerMaster

  /** Lookup a block's shared CXL pool location, if any. */
  case class GetCxlBlock(blockId: BlockId) extends ToBlockManagerMaster

  /** Remove a block's shared CXL pool metadata and free the slice (best effort). */
  case class ReleaseCxlBlock(blockId: BlockId) extends ToBlockManagerMaster

  /** Release all shared-CXL blocks for one application shuffle. */
  case class ReleaseCxlAppShuffle(appName: String, shuffleId: Int) extends ToBlockManagerMaster

  /** Release all shared-CXL blocks for one application, including late registrations. */
  case class ReleaseCxlApplication(appName: String) extends ToBlockManagerMaster

  /** Register a CXL memory domain with its pool configuration and member hosts. */
  case class RegisterCxlDomain(domainId: String, poolPath: String,
      poolSize: Long, poolAlign: Int, memberHosts: Seq[String])
      extends ToBlockManagerMaster

  /** Query the topology of all registered CXL domains. */
  case object GetCxlDomainTopology extends ToBlockManagerMaster

  /** Query pool statistics for a specific CXL domain. */
  case class GetCxlPoolStats(domainId: String) extends ToBlockManagerMaster

}
