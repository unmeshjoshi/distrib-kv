package org.dist.kvstore.gossip.handlers

import org.dist.kvstore.gossip.Gossiper
import org.dist.kvstore.gossip.messages.GossipDigestAck2
import org.dist.kvstore.network.{JsonSerDes, Message, MessagingService}

import scala.jdk.CollectionConverters._
class GossipDigestAck2Handler(gossiper: Gossiper, messagingService: MessagingService) {
  def handleMessage(ack2Message: Message): Unit = {
    val gossipDigestAck2 = JsonSerDes.deserialize(ack2Message.payloadJson.getBytes, classOf[GossipDigestAck2])
    val epStateMap = gossipDigestAck2.epStateMap

    gossiper.notifyFailureDetector(epStateMap.asJava)
    gossiper.applyStateLocally(epStateMap)
  }
}

