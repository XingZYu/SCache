package org.scache.deploy

/**
 * Created by frankfzw on 16-8-4.
 */

import java.util.concurrent.ConcurrentHashMap

import org.scache.deploy.DeployMessages.{Heartbeat, MapEndToMaster, RegisterClient}
import org.scache.scheduler.LiveListenerBus
import org.scache.{MapOutputTracker, MapOutputTrackerMaster, MapOutputTrackerMasterEndpoint}
import org.scache.rpc._
import org.scache.storage._
import org.scache.util._

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

private[scache] class ScacheClientInfo(
    val id: Int,
    val host: String,
    val port: Int,
    val ref: RpcEndpointRef) extends Serializable

private class ScacheMaster(
    val rpcEnv: RpcEnv,
    val hostname: String,
    conf: ScacheConf,
    isDriver: Boolean = true,
    isLocal: Boolean) extends ThreadSafeRpcEndpoint with Logging {
  val clientIdToInfo: mutable.HashMap[Int, ScacheClientInfo] = new mutable.HashMap[Int, ScacheClientInfo]()
  val hostnameToClientId: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()


  conf.set("scache.master.port", rpcEnv.address.port.toString)

  val mapOutputTracker = new MapOutputTrackerMaster(conf, isLocal)
  mapOutputTracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
    new MapOutputTrackerMasterEndpoint(rpcEnv, mapOutputTracker.asInstanceOf[MapOutputTrackerMaster], conf))
  // add client list to mapOutputMaster
  mapOutputTracker.hostnameToClientId = hostnameToClientId
  logInfo("Registering " + MapOutputTracker.ENDPOINT_NAME)

  val blockManagerMasterEndpoint = rpcEnv.setupEndpoint(BlockManagerMaster.DRIVER_ENDPOINT_NAME,
    new BlockManagerMasterEndpoint(rpcEnv, isLocal, mapOutputTracker, conf))
  val blockManagerMaster = new BlockManagerMaster(blockManagerMasterEndpoint, conf, isDriver)
  private val futureExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("master-future", 128))
  // runTest()

  // meta data to track cluster
  // val shuffleOutputStatus = new ConcurrentHashMap[ShuffleKey, ShuffleStatus]()
  override def onStop(): Unit = {
    futureExecutionContext.shutdown()
  }

  override def receive: PartialFunction[Any, Unit] = {
    case Heartbeat(id, rpcRef) =>
      logInfo(s"Receive heartbeat from ${id}: ${rpcRef}")

    case _ =>
      logError("Empty message received !")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterClient(hostname, port, ref) =>
      context.reply(registerClient(hostname, port, ref))
    // case RegisterShuffleMaster(appName, jobId, shuffleId, numMapTask, numReduceTask) =>
    //   context.reply(registerShuffle(appName, jobId, shuffleId, numMapTask, numReduceTask))
    // case RequestShuffleStatus(shuffleKey) =>
    //   if (shuffleOutputStatus.containsKey(shuffleKey)) {
    //     context.reply(Some(shuffleOutputStatus.get(shuffleKey)))
    //   } else {
    //     context.reply(None)
    //   }
    case MapEndToMaster(appName, jobId, shuffleId, mapId) =>
      logInfo(s"Map task ${appName}_${jobId}_${shuffleId}_${mapId} finished on ${context.senderAddress.host}")
      // startMapFetch(context.senderAddress.host, appName, jobId, shuffleId, mapId)
    case _ =>
      logError("Empty message received !")
  }

  // def registerShuffle(appName: String, jobId: Int, shuffleId: Int, numMapTask: Int, numReduceTask: Int): Boolean = {
  //   val shuffleKey = ShuffleKey(appName, jobId, shuffleId)
  //   if (shuffleOutputStatus.containsKey(shuffleKey)) {
  //     logWarning(s"Shuffle: $shuffleKey has been registered again !")
  //     return false
  //   }
  //   val shuffleStatus = new ShuffleStatus(shuffleId, numMapTask, numReduceTask)

  //   // apply random reduce allocation
  //   val clientList = Random.shuffle(hostnameToClientId.keys.toList)
  //   val numRep = Math.min(conf.getInt("scache.shuffle.replication", 0), clientList.size)
  //   for (i <- 0 until numReduceTask) {
  //     val p = i % clientList.size
  //     val backups = (for (c <- clientList if c != clientList(p)) yield c)
  //     shuffleStatus.reduceArray(i) = new ReduceStatus(i, clientList(p), Random.shuffle(backups).toArray.slice(0, numRep))
  //   }
  //   shuffleOutputStatus.putIfAbsent(shuffleKey, shuffleStatus)
  //   logInfo(s"Register shuffle $appName:$jobId:$shuffleId with map:$numMapTask and reduce:$numReduceTask")

  //   true
  // }
  def registerClient(hostname: String, port: Int, rpcEndpointRef: RpcEndpointRef): Int = {
    if (conf.getBoolean("scache.driver.mode", true) && hostname.equals(this.hostname)) {
      return 0
    }
    if (hostnameToClientId.contains(hostname)) {
      logWarning(s"The client ${hostname}:${hostnameToClientId(hostname)} has been registered again")
      clientIdToInfo.remove(hostnameToClientId(hostname))
    }
    val clientId = ScacheMaster.CLIENT_ID_GENERATOR.next
    val info = new ScacheClientInfo(clientId, hostname, port, rpcEndpointRef)
    if (hostnameToClientId.contains(hostname)) {
      clientIdToInfo -= hostnameToClientId(hostname)
    }
    hostnameToClientId.update(hostname, clientId)
    clientIdToInfo.update(clientId, info)
    logInfo(s"Register client ${hostname} with id ${clientId} and rpc ref ${rpcEndpointRef}")
    return clientId
  }

  // def startMapFetch(host: String, appName: String, jobId: Int, shuffleId: Int, mapId: Int): Unit = {

  //   val shuffleStatus = mapOutputTracker.getShuffleStatuses(ShuffleKey(appName, jobId, shuffleId))
  //   if (shuffleStatus == null) {
  //     logError(s"Shuffle ${ShuffleKey(appName, jobId, shuffleId).toString()} is not registered")
  //     return
  //   }
  //   Future {
  //     val blockManagerId = hostnameToClientId.get(host) match {
  //       case Some(clientId) =>
  //         blockManagerMaster.getBlockManagerId(clientId.toString).get
  //       case None =>
  //         logError(s"Host $host is not registered")
  //         return
  //     }
  //     for (info <- clientIdToInfo.values) {
  //       logDebug(s"Start notify ${info.host} to fetch $jobId:$shuffleId:$mapId")
  //       info.ref.send(StartMapFetch(blockManagerId, appName, jobId, shuffleId, mapId))
  //     }

  //   }(futureExecutionContext)
  // }
}

object ScacheMaster extends Logging {
  private val CLIENT_ID_GENERATOR = new IdGenerator

  def main(args: Array[String]): Unit = {
    val hostName = Utils.findLocalInetAddress().getHostName
    System.setProperty("SCACHE_DAEMON", s"master-${hostName}")
    val conf = new ScacheConf()
    val SYSTEM_NAME = "scache.master"
    val arguments = new MasterArguments(args, conf)
    // conf.set("scache.app.id", "test")
    logInfo("Start Master")
    val rpcEnv = RpcEnv.create(SYSTEM_NAME, arguments.host, arguments.port, conf)
    val masterEndpoint = rpcEnv.setupEndpoint("ScacheMaster",
      new ScacheMaster(rpcEnv, arguments.host, conf, true, arguments.isLocal))
    rpcEnv.awaitTermination()
  //   logInfo(conf.getInt("scache.memory", 1).toString)
  //   logInfo(conf.getString("scache.master", "localhost").toString)
  //   logInfo(conf.getBoolean("scache.boolean", false).toString)
  }
}
