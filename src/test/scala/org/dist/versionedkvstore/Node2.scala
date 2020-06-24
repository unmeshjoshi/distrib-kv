package org.dist.versionedkvstore

import org.dist.kvstore.{StorageService, TestUtils}
import org.dist.kvstore.network.{InetAddressAndPort, Networks}
import org.dist.versionedkvstore.Node1.localIp

object Node2 extends App {
  val localIp = new Networks().ipv4Address
  val seedIp = InetAddressAndPort(localIp, 8080)
  val clientAddress = InetAddressAndPort(localIp, TestUtils.choosePort())
  val storage = new StorageService(seedIp, clientAddress, InetAddressAndPort(localIp,8001))
  storage.start()
}
