package org.scache.network.ub

/** Executable phase-4 protocol and registered-arena contracts. */
private[scache] object UbPhase4ContractTest {
  def main(args: Array[String]): Unit = {
    val device = args.headOption.getOrElse("openurma0")
    val role = Option(System.getenv("OPENURMA_WIRE_ROLE")).getOrElse(
      throw new IllegalArgumentException("OPENURMA_WIRE_ROLE is required"))
    val requestedRole = args.lift(1).getOrElse(role)
    val protocol = UBBlockTransferService.runProtocolContracts()
    var transport: UbTransport = null
    val arena = try {
      transport = RealUbTransport.open(device, 32, 4096, true, requestedRole)
      UBBlockTransferService.runArenaContracts(transport)
    } finally if (transport != null) transport.close()
    val all = protocol.map { case (name, ok) => ("protocol", name, ok) } ++
      arena.map { case (name, ok) => ("arena", name, ok) }
    all.foreach { case (suite, name, ok) => println(s"CONTRACT suite=$suite case=$name result=${if (ok) "PASS" else "FAIL"}") }
    val failures = all.count(!_._3)
    println(s"SCACHE_TEST_CONTRACT_SUMMARY total=${all.size} failures=$failures")
    if (failures != 0) throw new IllegalStateException(s"$failures phase-4 contracts failed")
  }
}
