package org.dist.kvstore.gossip.handlers

import java.util

import org.dist.kvstore.gossip.builders.GossipAck2MessageBuilder
import org.dist.kvstore.gossip.{EndPointState, Gossiper}
import org.dist.kvstore.gossip.messages.GossipDigestAck
import org.dist.kvstore.network.{InetAddressAndPort, JsonSerDes, Message, MessagingService}

import scala.jdk.CollectionConverters._

class GossipDigestSynAckHandler(gossiper: Gossiper, messagingService: MessagingService) {
  def handleMessage(synAckMessage: Message): Unit = {
    val gossipDigestSynAck: GossipDigestAck = JsonSerDes.deserialize(synAckMessage.payloadJson.getBytes, classOf[GossipDigestAck])
    val epStateMap: Map[InetAddressAndPort, EndPointState] = gossipDigestSynAck.epStateMap
    if (epStateMap.size > 0) {
      gossiper.notifyFailureDetector(epStateMap.asJava)
      gossiper.applyStateLocally(epStateMap)
    }

    /* Get the state required to send to this gossipee - construct GossipDigestAck2Message */
    val deltaEpStateMap = new util.HashMap[InetAddressAndPort, EndPointState]
    for (gDigest <- gossipDigestSynAck.digestList) {
      val addr = gDigest.endPoint
      val localEpStatePtr = gossiper.getStateForVersionBiggerThan(addr, gDigest.maxVersion)
      if (localEpStatePtr != null) deltaEpStateMap.put(addr, localEpStatePtr)
    }

    val ack2Message = new GossipAck2MessageBuilder(gossiper.localEndPoint).build(deltaEpStateMap)
    messagingService.sendTcpOneWay(ack2Message, synAckMessage.header.from)
  }
}
