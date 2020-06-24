package org.dist.kvstore.gossip

import org.dist.kvstore.network.{InetAddressAndPort, Networks}
import org.dist.kvstore.{StorageService, TestUtils}
import org.scalatest.FunSuite
import scala.jdk.CollectionConverters._

class GossipTest extends FunSuite {

  test("servers should detect each other with gossip") {
      val localIp = new Networks().ipv4Address
      val seedIp = InetAddressAndPort(localIp, 8080)
      val clientListenAddress = InetAddressAndPort(localIp, TestUtils.choosePort())
      val seedNode = new StorageService(seedIp, clientListenAddress, seedIp)
      seedNode.start()

      val storages = new java.util.ArrayList[StorageService]()
      val basePort = 8081
      val serverCount = 5
      for (i ← 1 to serverCount) {
        val clientAddress = InetAddressAndPort(localIp, TestUtils.choosePort())
        val storage = new StorageService(seedIp, clientAddress, InetAddressAndPort(localIp, basePort + i))
        storage.start()
        storages.add(storage)
      }
    TestUtils.waitUntilTrue(() ⇒ {
      //serverCount + 1 seedIp
      storages.asScala.toList.map(s ⇒ s.gossiper.endpointStateMap.size() == serverCount + 1).reduce(_ && _)
    }, "Waiting for all the endpoints to be available on all nodes", 30000)
  }
}
