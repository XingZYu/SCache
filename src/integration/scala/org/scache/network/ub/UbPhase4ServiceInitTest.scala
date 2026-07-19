package org.scache.network.ub

import scala.reflect.ClassTag

import org.scache.network.BlockDataManager
import org.scache.network.buffer.ManagedBuffer
import org.scache.storage.{BlockId, StorageLevel}
import org.scache.util.ScacheConf

/** Minimal executable for strict UBBlockTransferService startup fault tests. */
private[scache] object UbPhase4ServiceInitTest {
  private object NoData extends BlockDataManager {
    override def getBlockData(blockId: BlockId): ManagedBuffer = throw new java.io.FileNotFoundException(blockId.toString)
    override def putBlockData(blockId: BlockId, data: ManagedBuffer, level: StorageLevel, classTag: ClassTag[_]): Boolean = false
    override def releaseLock(blockId: BlockId): Unit = ()
  }

  def main(args: Array[String]): Unit = {
    require(args.length == 3, "usage: <device> <listen|connect> <control-port>")
    val device = args(0); val role = args(1); val port = args(2).toInt
    val conf = new ScacheConf()
    conf.set("spark.urma.strict", "true", slient = true)
    conf.set("spark.urma.wireRole", role, slient = true)
    conf.set("spark.urma.wirePath", Option(System.getenv("OPENURMA_WIRE_PATH")).getOrElse(""), slient = true)
    conf.set(s"spark.urma.device.$role", device, slient = true)
    conf.set("spark.urma.controlPort", port.toString, slient = true)
    conf.set("spark.urma.arenaBytes", "1048576", slient = true)
    val service = new UBBlockTransferService(conf, "127.0.0.1", 1)
    try {
      service.init(NoData)
      println(s"UB_SERVICE_INIT=PASS device=$device role=$role port=${service.port}")
    } finally service.close()
  }
}
