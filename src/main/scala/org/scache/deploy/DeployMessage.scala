package org.scache.deploy

import org.scache.rpc.RpcEndpointRef
import org.scache.storage.{BlockId, BlockManagerId}
import org.scache.util.ShuffleKey

/**
 * Created by frankfzw on 16-9-13.
 */
private[deploy] trait DeployMessage extends Serializable

private[deploy] object DeployMessages {

  sealed trait IpcLocation extends Serializable
  case class IpcFile(path: String) extends IpcLocation
  case class IpcPoolSlice(poolPath: String, offset: Long, length: Int) extends IpcLocation
  case class IpcBlock(size: Int, ipc: IpcLocation) extends Serializable

  sealed trait ToDeployMaster
  case class Heartbeat(id: String, worker: RpcEndpointRef) extends ToDeployMaster
  case class RegisterClient(hostname: String, port: Int, worker: RpcEndpointRef) extends ToDeployMaster
  // case class PutBlockMaster(scacheBlockId: BlockId, size: Int) extends ToDeployMaster
  // case class RegisterShuffleMaster(appName:String, jobId: Int, shuffleId: Int, numMapTask: Int, numReduceTask: Int) extends ToDeployMaster
  // case class RequestShuffleStatus(shuffleKey: ShuffleKey) extends ToDeployMaster
  case class MapEndToMaster(appName: String, jobId: Int, shuffleId: Int, mapId: Int) extends ToDeployMaster

  sealed trait FromDaemon
  case class PreparePutBlock(scacheBlockId: BlockId, size: Int) extends FromDaemon
  case class PutBlock(scacheBlockId: BlockId, size: Int, ipc: IpcLocation) extends FromDaemon
  case class GetBlock(scacheBlockId: BlockId) extends FromDaemon
  case class GetBlockIpc(scacheBlockId: BlockId) extends FromDaemon
  case class GetBlocksIpc(scacheBlockIds: Seq[BlockId]) extends FromDaemon
  case class RegisterShuffle(appName: String, jobId: Int, ids: Array[Int], numMaps: Array[Int], numReduces: Array[Int]) extends FromDaemon
  case class MapEnd(appName: String, jobId: Int, shuffleId: Int, mapId: Int) extends FromDaemon
  case class GetShuffleStatus(appName: String, jobId: Int, shuffleId: Int) extends FromDaemon

  sealed trait  ToDeployClient
  case class RegisterClientSuccess(clientId: Int) extends ToDeployClient
}
