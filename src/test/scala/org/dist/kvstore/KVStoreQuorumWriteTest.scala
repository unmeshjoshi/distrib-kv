package org.dist.kvstore

import java.util

import org.dist.kvstore.client.Client
import org.dist.kvstore.gossip.{ApplicationState, EndPointState}
import org.dist.kvstore.gossip.messages.{ReadMessageResponse, RowMutationResponse}
import org.dist.kvstore.network.{InetAddressAndPort, JsonSerDes, Networks}
import org.scalatest.FunSuite

import scala.jdk.CollectionConverters._

class KVStoreQuorumWriteTest extends FunSuite {
  test("should store key value on quorum") {
    val localIp = new Networks().ipv4Address
    val seedIp = InetAddressAndPort(localIp, 8080)
    val clientListenAddress = InetAddressAndPort(localIp, TestUtils.choosePort())
    val seedNode = new StorageService(seedIp, clientListenAddress, seedIp)
    seedNode.start()

    val storages = new java.util.ArrayList[StorageService]()
    val basePort = 8081
    val serverCount = 10
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
      val tokens = states.asScala.map(ep => ep.applicationStates.get(ApplicationState.TOKENS).value)
      assert(tokens.toList.contains(s.gossiper.token.toString()))
    })

    val client = new Client(clientListenAddress)
    val writeQuorum = client.put("table1", "key1", "value1")
    assert(writeQuorum.size == 3)
    assert(writeQuorum.map(m => JsonSerDes.deserialize(m.payloadJson, classOf[RowMutationResponse]).success).toSet == Set(true))

    val readQuorum = client.get("table1", "key1")
    assert(3 == readQuorum.size)
    assert(readQuorum.map(m => JsonSerDes.deserialize(m.payloadJson, classOf[ReadMessageResponse]).value.value) == List("value1", "value1", "value1"))

  }
}
