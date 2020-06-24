package org.dist.versionedkvstore

import org.dist.kvstore.{StorageService, TestUtils}
import org.dist.kvstore.network.{InetAddressAndPort, Networks}

object Node3 extends App {

  val localIp = new Networks().ipv4Address
  val seedIp = InetAddressAndPort(localIp, 8080)
  val clientAddress = InetAddressAndPort(localIp, TestUtils.choosePort())
  val storage = new StorageService(seedIp, clientAddress, InetAddressAndPort(localIp, 8002))
  storage.start()

}
