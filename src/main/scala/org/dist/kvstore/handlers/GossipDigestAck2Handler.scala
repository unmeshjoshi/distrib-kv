package org.dist.kvstore.handlers

import org.dist.kvstore.messages.GossipDigestAck2
import org.dist.kvstore.{Gossiper, JsonSerDes, Message, MessagingService}

class GossipDigestAck2Handler(gossiper: Gossiper, messagingService: MessagingService) {
  def handleMessage(ack2Message: Message): Unit = {
    val gossipDigestAck2 = JsonSerDes.deserialize(ack2Message.payloadJson.getBytes, classOf[GossipDigestAck2])
    val epStateMap = gossipDigestAck2.epStateMap
    gossiper.applyStateLocally(epStateMap)
  }
}

