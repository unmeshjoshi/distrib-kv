package org.dist.simplegossip

import java.util

import org.dist.kvstore.{EndPointState, InetAddressAndPort, RowMutationResponse}
import org.dist.kvstore.client.Client
import org.dist.queue.TestUtils
import org.dist.util.Networks
import org.scalatest.FunSuite

import scala.jdk.CollectionConverters._

class StorageServiceGossipTest extends FunSuite {
  test("should gossip state to all the nodes in the cluster") {
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
    }, "Waiting for all the endpoints to be available on all nodes", 15000)

    storages.asScala.foreach(s ⇒ {
      val states: util.Collection[EndPointState] = seedNode.gossiper.endpointStateMap.values()
      val tokens: Iterable[String] = states.asScala.map(ep => ep.getToken().value)
      assert(tokens.toList.contains(s.gossiper.token.toString()))
    })

    val client = new Client(clientListenAddress)
    val mutationResponses: Seq[RowMutationResponse] = client.put("table1", "key1", "value1")
    assert(mutationResponses.size == 2)
    assert(mutationResponses.map(m => m.success).toSet == Set(true))
  }
}
