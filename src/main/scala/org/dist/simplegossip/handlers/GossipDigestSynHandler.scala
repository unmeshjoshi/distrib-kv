package org.dist.simplegossip.handlers

import java.util

import org.dist.kvstore.{EndPointState, GossipDigest, GossipDigestSyn, InetAddressAndPort, JsonSerDes, Message}
import org.dist.simplegossip.{Gossiper, MessagingService}

class GossipDigestSynHandler(gossiper: Gossiper, messagingService: MessagingService) {
  def handleMessage(synMessage: Message): Unit = {
    val gossipDigestSyn = JsonSerDes.deserialize(synMessage.payloadJson.getBytes, classOf[GossipDigestSyn])

    val deltaGossipDigest = new util.ArrayList[GossipDigest]()
    val deltaEndPointStates = new util.HashMap[InetAddressAndPort, EndPointState]()
    gossiper.examineGossiper(gossipDigestSyn.gDigests, deltaGossipDigest, deltaEndPointStates)

    val synAckMessage = gossiper.makeGossipDigestAckMessage(deltaGossipDigest, deltaEndPointStates)
    messagingService.sendTcpOneWay(synAckMessage, synMessage.header.from)
  }
}

