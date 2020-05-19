package org.dist.kvstore.handlers

import java.util

import org.dist.kvstore.builders.GossipAck2MessageBuilder
import org.dist.kvstore.messages.GossipDigestAck
import org.dist.kvstore.{EndPointState, Gossiper, InetAddressAndPort, JsonSerDes, Message, MessagingService}


class GossipDigestSynAckHandler(gossiper: Gossiper, messagingService: MessagingService) {
  def handleMessage(synAckMessage: Message): Unit = {
    val gossipDigestSynAck: GossipDigestAck = JsonSerDes.deserialize(synAckMessage.payloadJson.getBytes, classOf[GossipDigestAck])
    val epStateMap: Map[InetAddressAndPort, EndPointState] = gossipDigestSynAck.epStateMap
    if (epStateMap.size > 0) {
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
