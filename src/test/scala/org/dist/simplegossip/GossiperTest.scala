package org.dist.simplegossip

import java.util

import org.dist.kvstore.{ApplicationState, EndPointState, GossipDigest, HeartBeatState, InetAddressAndPort, Stage, TokenMetadata, Verb, VersionedValue}
import org.dist.simplegossip.builders.{GossipDigestBuilder, GossipSynMessageBuilder}
import org.dist.util.Utils
import org.scalatest.FunSuite

import scala.jdk.CollectionConverters._

class GossiperTest extends FunSuite {
  test("should keep state for endpoints and initialize with own endpoint") {
    val listenAddress = InetAddressAndPort.create("127.0.0.1", 8000)
    val token = Utils.newToken()
    val gossiper = new Gossiper(listenAddress, listenAddress, token, new TokenMetadata())
    val epState = gossiper.endpointStateMap.get(listenAddress)
    assert(token.toString() == epState.applicationStates.get(ApplicationState.TOKENS).value)
  }

  test("should have HeartBeatState as part of endpoint state") {
    val listenAddress = InetAddressAndPort.create("127.0.0.1", 8000)
    val token = Utils.newToken()
    val gossiper = new Gossiper(listenAddress, listenAddress, token, new TokenMetadata())
    val epState = gossiper.endpointStateMap.get(listenAddress)
    assert(1 == epState.heartBeatState.version)
  }

  test("seed list should be empty for seed node itself") {
    val listenAddress = InetAddressAndPort.create("127.0.0.1", 8000)
    val token = Utils.newToken()
    val gossiper = new Gossiper(listenAddress, listenAddress, token, new TokenMetadata())
    assert(gossiper.seeds.isEmpty == true)
  }

  test("should initialize seed list without local endpoint") {
    val seed = InetAddressAndPort.create("127.0.0.1", 8000)
    val listenAddress = InetAddressAndPort.create("127.0.0.1", 8002)
    val token = Utils.newToken()
    val gossiper = new Gossiper(seed, listenAddress, token, new TokenMetadata())
    assert(gossiper.seeds == List(seed))
  }


  test("live endpoints and unreachables endpoints lists should be empty at initialization") {
    val seed = InetAddressAndPort.create("127.0.0.1", 8000)
    val localEndpoint = InetAddressAndPort.create("127.0.0.1", 8000)
    val gossiper = new Gossiper(seed, localEndpoint, Utils.newToken(), new TokenMetadata())
    assert(gossiper.liveEndpoints.isEmpty)
    assert(gossiper.unreachableEndpoints.isEmpty)
  }

  test("should make gossip digest builder from local and live endpoints") {
    val seed = InetAddressAndPort.create("127.0.0.1", 8000)
    val localEndpoint = InetAddressAndPort.create("127.0.0.1", 8000)
    val gossiper = new Gossiper(seed, localEndpoint, Utils.newToken(), new TokenMetadata())

    gossiper.liveEndpoints.add(InetAddressAndPort.create("127.0.0.1", 8001))
    gossiper.liveEndpoints.add(InetAddressAndPort.create("127.0.0.1", 8002))

    val digests = new GossipDigestBuilder(localEndpoint, gossiper.endpointStateMap, gossiper.liveEndpoints).makeRandomGossipDigest()
    assert(digests.size() == 3)
    assert(digests.asScala.map(digest => digest.endPoint).contains(InetAddressAndPort.create("127.0.0.1", 8000)))
    assert(digests.asScala.map(digest => digest.endPoint).contains(InetAddressAndPort.create("127.0.0.1", 8002)))
    assert(digests.asScala.map(digest => digest.endPoint).contains(InetAddressAndPort.create("127.0.0.1", 8001)))
  }

  test("should contain maximum version of the local and live endpoints in digest") {
    val seed = InetAddressAndPort.create("127.0.0.1", 8000)
    val localEndpoint = InetAddressAndPort.create("127.0.0.1", 8000)
    val gossiper = new Gossiper(seed, localEndpoint, Utils.newToken(), new TokenMetadata())


    //For node1, heartbeat is at version 3 and tokens at version 1.
    val ep = EndPointState(HeartBeatState(1, 3), Map(ApplicationState.TOKENS → VersionedValue("1001", 1)).asJava)
    val node1 = InetAddressAndPort.create("127.0.0.1", 8001)
    gossiper.endpointStateMap.put(node1, ep)

    gossiper.liveEndpoints.add(node1)
    val node2 = InetAddressAndPort.create("127.0.0.1", 8002)
    gossiper.liveEndpoints.add(node2)
    //we do not have anything for node2 in endpoint state.

    val digests = new GossipDigestBuilder(localEndpoint, gossiper.endpointStateMap, gossiper.liveEndpoints).makeRandomGossipDigest()

    assert(3 == digests.size())

    val node1Digest = digests.asScala.filter(digest => digest.endPoint == node1)
    assert(node1Digest(0).maxVersion == 3)

    val localDigest = digests.asScala.filter(digest => digest.endPoint == localEndpoint)
    assert(localDigest(0).maxVersion == 2)

    val node2Digest = digests.asScala.filter(digest => digest.endPoint == node2)
    assert(node2Digest(0).maxVersion == 0)
  }

  test("should construct GossipSynMessage with Gossip digests") {
    val localEndpoint = InetAddressAndPort.create("127.0.0.1", 8000)
    val digests = List(GossipDigest(localEndpoint, 0, 2))
    val message = new GossipSynMessageBuilder("cluster1", localEndpoint).build(digests.asJava)
    assert(message.header.from == localEndpoint)
    assert(message.header.messageType == Stage.GOSSIP)
    assert(message.header.verb == Verb.GOSSIP_DIGEST_SYN)
  }


  test("should request all information from the endpoint if the state does not exist locally") {
    val seed = InetAddressAndPort.create("127.0.0.1", 8000)
    val localEndpoint = InetAddressAndPort.create("127.0.0.1", 8000)
    val gossiper = new Gossiper(seed, localEndpoint, Utils.newToken(), new TokenMetadata())


    val deltaGossipDigest = new util.ArrayList[GossipDigest]()
    val deltaEndPointStates = new util.HashMap[InetAddressAndPort, EndPointState]()
    val digest1 = GossipDigest(InetAddressAndPort.create("10.10.10.10", 8000), 1, 1)
    val digest2 = GossipDigest(InetAddressAndPort.create("10.10.10.10", 8001), 1, 1)
    gossiper.examineGossiper(util.Arrays.asList(digest1, digest2), deltaGossipDigest, deltaEndPointStates)

    assert(deltaGossipDigest.size() == 2)
    assert(deltaGossipDigest.get(0) == GossipDigest(InetAddressAndPort.create("10.10.10.10", 8000), 1, 0))
    assert(deltaGossipDigest.get(1) == GossipDigest(InetAddressAndPort.create("10.10.10.10", 8001), 1, 0))
  }

  test("should request all for the versions which are missing locally") {
    val seed = InetAddressAndPort.create("127.0.0.1", 8000)
    val localEndpoint = InetAddressAndPort.create("127.0.0.1", 8000)
    val gossiper = new Gossiper(seed, localEndpoint, Utils.newToken(), new TokenMetadata())
    val server1Ep = InetAddressAndPort.create("10.10.10.10", 8000)
    val server2Ep = InetAddressAndPort.create("10.10.10.10", 8001)

    val ep1 = EndPointState(HeartBeatState(1, 1), Map(ApplicationState.TOKENS → VersionedValue("1001", 2)).asJava)
    gossiper.endpointStateMap.put(server1Ep, ep1)
    val ep2 = EndPointState(HeartBeatState(1, 3), Map(ApplicationState.TOKENS → VersionedValue("1001", 1)).asJava)
    gossiper.endpointStateMap.put(server2Ep, ep2)

    val deltaGossipDigest = new util.ArrayList[GossipDigest]()
    val deltaEndPointStates = new util.HashMap[InetAddressAndPort, EndPointState]()
    val digest1 = GossipDigest(server1Ep, 1, 4)
    val digest2 = GossipDigest(server2Ep, 1, 5)
    gossiper.examineGossiper(util.Arrays.asList(digest1, digest2), deltaGossipDigest, deltaEndPointStates)

    assert(deltaGossipDigest.size() == 2)
    assert(deltaGossipDigest.get(0) == GossipDigest(server1Ep, 1, 2))
    assert(deltaGossipDigest.get(1) == GossipDigest(server2Ep, 1, 3))
  }


  test("should not send endPointStates which are missing in the remote digests") {
    val seed = InetAddressAndPort.create("127.0.0.1", 8000)
    val localEndpoint = InetAddressAndPort.create("127.0.0.1", 8000)
    val gossiper = new Gossiper(seed, localEndpoint, Utils.newToken(), new TokenMetadata())
    val server1Ep = InetAddressAndPort.create("10.10.10.10", 8000)
    val server2Ep = InetAddressAndPort.create("10.10.10.10", 8001)

    val ep1 = EndPointState(HeartBeatState(1, 1), Map(ApplicationState.TOKENS → VersionedValue("1001", 2)).asJava)
    gossiper.endpointStateMap.put(server1Ep, ep1)
    val ep2 = EndPointState(HeartBeatState(1, 3), Map(ApplicationState.TOKENS → VersionedValue("1001", 1)).asJava)
    gossiper.endpointStateMap.put(server2Ep, ep2)

    val deltaGossipDigest = new util.ArrayList[GossipDigest]()
    val deltaEndPointStates = new util.HashMap[InetAddressAndPort, EndPointState]()
    val digest1 = GossipDigest(server1Ep, 1, 4)
    gossiper.examineGossiper(util.Arrays.asList(digest1), deltaGossipDigest, deltaEndPointStates)

    assert(deltaEndPointStates.size() == 0)
  }

  test("should send endPointStates with higher version than in the digest") {
    val seed = InetAddressAndPort.create("127.0.0.1", 8000)
    val localEndpoint = InetAddressAndPort.create("127.0.0.1", 8000)
    val gossiper = new Gossiper(seed, localEndpoint, Utils.newToken(), new TokenMetadata())
    val server1Ep = InetAddressAndPort.create("10.10.10.10", 8000)
    val server2Ep = InetAddressAndPort.create("10.10.10.10", 8001)

    val ep1 = EndPointState(HeartBeatState(1, 1), Map(ApplicationState.TOKENS → VersionedValue("1001", 2)).asJava)
    gossiper.endpointStateMap.put(server1Ep, ep1)
    val ep2 = EndPointState(HeartBeatState(1, 3), Map(ApplicationState.TOKENS → VersionedValue("1001", 1)).asJava)
    gossiper.endpointStateMap.put(server2Ep, ep2)

    val deltaGossipDigest = new util.ArrayList[GossipDigest]()
    val deltaEndPointStates = new util.HashMap[InetAddressAndPort, EndPointState]()
    val digest1 = GossipDigest(server1Ep, 1, 4)
    val digest2 = GossipDigest(server2Ep, 1, 1)
    gossiper.examineGossiper(util.Arrays.asList(digest1,digest2), deltaGossipDigest, deltaEndPointStates)

    assert(deltaEndPointStates.size() == 1)
    assert(deltaEndPointStates.get(server2Ep).heartBeatState == HeartBeatState(1, 3))
    assert(deltaEndPointStates.get(server2Ep).applicationStates.isEmpty == true)
  }

  test("should not send any endPointStates if the version in the digest matches local version") {
    val seed = InetAddressAndPort.create("127.0.0.1", 8000)
    val localEndpoint = InetAddressAndPort.create("127.0.0.1", 8000)
    val gossiper = new Gossiper(seed, localEndpoint, Utils.newToken(), new TokenMetadata())
    val server1Ep = InetAddressAndPort.create("10.10.10.10", 8000)
    val server2Ep = InetAddressAndPort.create("10.10.10.10", 8001)

    val ep1 = EndPointState(HeartBeatState(1, 1), Map(ApplicationState.TOKENS → VersionedValue("1001", 2)).asJava)
    gossiper.endpointStateMap.put(server1Ep, ep1)
    val ep2 = EndPointState(HeartBeatState(1, 3), Map(ApplicationState.TOKENS → VersionedValue("1001", 1)).asJava)
    gossiper.endpointStateMap.put(server2Ep, ep2)

    val deltaGossipDigest = new util.ArrayList[GossipDigest]()
    val deltaEndPointStates = new util.HashMap[InetAddressAndPort, EndPointState]()
    val digest1 = GossipDigest(server1Ep, 1, 4)
    val digest2 = GossipDigest(server2Ep, 1, 3)
    gossiper.examineGossiper(util.Arrays.asList(digest1,digest2), deltaGossipDigest, deltaEndPointStates)

    assert(deltaEndPointStates.size() == 0)
  }

}
