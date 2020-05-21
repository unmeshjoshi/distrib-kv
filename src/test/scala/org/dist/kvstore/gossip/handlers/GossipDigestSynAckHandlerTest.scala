package org.dist.kvstore.gossip.handlers

import java.util

import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState
import org.dist.kvstore.gossip
import org.dist.kvstore.gossip.builders.GossipSynAckMessageBuilder
import org.dist.kvstore.gossip.failuredetector.TimeoutBasedFailureDetector
import org.dist.kvstore.gossip.messages.{GossipDigest, GossipDigestAck2}
import org.dist.kvstore.gossip._
import org.dist.kvstore.network.{InetAddressAndPort, JsonSerDes}
import org.dist.util.Utils
import org.mockito.internal.runners.util.FailureDetector
import org.scalatest.FunSuite

import scala.jdk.CollectionConverters._


class GossipDigestSynAckHandlerTest extends FunSuite {

  test("should apply endpoint state in the message to gossiper and put endpoint state asked for in the ack2 message.") {
    val localEndpoint = InetAddressAndPort.create("10.10.10.10", 8000)
    val gossiper = new Gossiper(localEndpoint, localEndpoint, Utils.newToken(), new TokenMetadata)

    val server1Ep = InetAddressAndPort.create("10.10.10.10", 8001)
    val server2Ep = InetAddressAndPort.create("10.10.10.10", 8002)

    val ep1 = gossip.EndPointState(HeartBeatState(1, 1), Map(ApplicationState.TOKENS → VersionedValue("1001", 2)).asJava)
    gossiper.endpointStateMap.put(server1Ep, ep1)
    val ep2 = gossip.EndPointState(HeartBeatState(1, 3), Map(ApplicationState.TOKENS → VersionedValue("1002", 1)).asJava)
    gossiper.endpointStateMap.put(server2Ep, ep2)



    val stubMessageService = new StubMessagingService
    val handler = new GossipDigestSynAckHandler(gossiper, stubMessageService)

    val responseMap = new util.HashMap[InetAddressAndPort, EndPointState]
    val epInResponse = gossip.EndPointState(HeartBeatState(1, 4), Map(ApplicationState.TOKENS → VersionedValue("1002", 2)).asJava)
    responseMap.put(server2Ep, epInResponse)
    val digestsInResponse = List(GossipDigest(server1Ep, 1, 0))
    val message = new GossipSynAckMessageBuilder(server1Ep).build(digestsInResponse.asJava, responseMap)

    handler.handleMessage(message)

    val ack2Message = JsonSerDes.deserialize(stubMessageService.message.payloadJson, classOf[GossipDigestAck2])
    assert(ack2Message.epStateMap.get(server1Ep) == Some(ep1))

    assert(gossiper.endpointStateMap.get(server2Ep).heartBeatState.version == epInResponse.heartBeatState.version)
    assert(gossiper.endpointStateMap.get(server2Ep).applicationStates.get(ApplicationState.TOKENS) == VersionedValue("1002", 2))
  }

  test("should notify endpoints in the response to failure detector") {
    val localEndpoint = InetAddressAndPort.create("10.10.10.10", 8000)
    val failureDetector = new TimeoutBasedFailureDetector[InetAddressAndPort]

    val gossiper = new Gossiper(localEndpoint, localEndpoint, Utils.newToken(), new TokenMetadata, failureDetector)

    val server1Ep = InetAddressAndPort.create("10.10.10.10", 8001)
    val server2Ep = InetAddressAndPort.create("10.10.10.10", 8002)

    val ep1 = gossip.EndPointState(HeartBeatState(1, 1), Map(ApplicationState.TOKENS → VersionedValue("1001", 2)).asJava)
    gossiper.endpointStateMap.put(server1Ep, ep1)
    val ep2 = gossip.EndPointState(HeartBeatState(1, 3), Map(ApplicationState.TOKENS → VersionedValue("1002", 1)).asJava)
    gossiper.endpointStateMap.put(server2Ep, ep2)

    assert(failureDetector.serverStates.isEmpty)

    val stubMessageService = new StubMessagingService
    val handler = new GossipDigestSynAckHandler(gossiper, stubMessageService)

    val responseMap = new util.HashMap[InetAddressAndPort, EndPointState]
    val epInResponse = gossip.EndPointState(HeartBeatState(1, 4), Map(ApplicationState.TOKENS → VersionedValue("1002", 2)).asJava)
    responseMap.put(server2Ep, epInResponse)
    val digestsInResponse = List(GossipDigest(server1Ep, 1, 0))
    val message = new GossipSynAckMessageBuilder(server1Ep).build(digestsInResponse.asJava, responseMap)

    handler.handleMessage(message)

    assert(failureDetector.isAlive(server2Ep))

  }
}
