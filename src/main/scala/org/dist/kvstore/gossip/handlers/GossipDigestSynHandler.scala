package org.dist.kvstore.gossip.handlers

import java.util

import org.dist.kvstore.gossip.builders.GossipSynAckMessageBuilder
import org.dist.kvstore.gossip.{EndPointState, Gossiper}
import org.dist.kvstore.gossip.messages.{GossipDigest, GossipDigestSyn}
import org.dist.kvstore.network.InetAddressAndPort

class GossipDigestSynHandler(gossiper: Gossiper) {
  def handleMessage(gossipDigestSyn: GossipDigestSyn) = {
    val deltaGossipDigest = new util.ArrayList[GossipDigest]()
    val deltaEndPointStates = new util.HashMap[InetAddressAndPort, EndPointState]()
    gossiper.examineGossiper(gossipDigestSyn.gDigests, deltaGossipDigest, deltaEndPointStates)
    new GossipSynAckMessageBuilder(gossiper.localEndPoint).build(deltaGossipDigest, deltaEndPointStates)
  }
}

