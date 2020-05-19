package org.dist.kvstore.handlers

import java.util

import org.dist.kvstore.{EndPointState, Gossiper, InetAddressAndPort}
import org.dist.kvstore.builders.GossipSynAckMessageBuilder
import org.dist.kvstore.messages.{GossipDigest, GossipDigestSyn}

class GossipDigestSynHandler(gossiper: Gossiper) {
  def handleMessage(gossipDigestSyn: GossipDigestSyn) = {
    val deltaGossipDigest = new util.ArrayList[GossipDigest]()
    val deltaEndPointStates = new util.HashMap[InetAddressAndPort, EndPointState]()
    gossiper.examineGossiper(gossipDigestSyn.gDigests, deltaGossipDigest, deltaEndPointStates)
    new GossipSynAckMessageBuilder(gossiper.localEndPoint).build(deltaGossipDigest, deltaEndPointStates)
  }
}

