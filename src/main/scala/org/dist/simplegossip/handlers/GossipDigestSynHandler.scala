package org.dist.simplegossip.handlers

import java.util

import org.dist.kvstore.{EndPointState, InetAddressAndPort}
import org.dist.simplegossip.Gossiper
import org.dist.simplegossip.builders.GossipSynAckMessageBuilder
import org.dist.simplegossip.messages.{GossipDigest, GossipDigestSyn}

class GossipDigestSynHandler(gossiper: Gossiper) {
  def handleMessage(gossipDigestSyn: GossipDigestSyn) = {
    val deltaGossipDigest = new util.ArrayList[GossipDigest]()
    val deltaEndPointStates = new util.HashMap[InetAddressAndPort, EndPointState]()
    gossiper.examineGossiper(gossipDigestSyn.gDigests, deltaGossipDigest, deltaEndPointStates)
    new GossipSynAckMessageBuilder(gossiper.localEndPoint).build(deltaGossipDigest, deltaEndPointStates)
  }
}

