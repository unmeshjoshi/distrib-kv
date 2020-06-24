package org.dist.versionedkvstore

import org.dist.kvstore.{StorageService, TestUtils}
import org.dist.kvstore.network.{InetAddressAndPort, Networks}

object Node1 extends App {
  val localIp = new Networks().ipv4Address
  val seedIp = InetAddressAndPort(localIp, 8080)
  val clientListenAddress = InetAddressAndPort(localIp, TestUtils.choosePort())
  val seedNode = new StorageService(seedIp, clientListenAddress, seedIp)
  seedNode.start()
}
