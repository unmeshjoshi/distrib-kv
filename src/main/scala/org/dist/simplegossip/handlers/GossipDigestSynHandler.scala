package org.dist.simplegossip.handlers

import java.util

import org.dist.kvstore.{EndPointState, GossipDigest, GossipDigestSyn, InetAddressAndPort}
import org.dist.simplegossip.Gossiper

class GossipDigestSynHandler(gossiper: Gossiper) {
  def handleMessage(gossipDigestSyn: GossipDigestSyn) = {
    val deltaGossipDigest = new util.ArrayList[GossipDigest]()
    val deltaEndPointStates = new util.HashMap[InetAddressAndPort, EndPointState]()
    gossiper.examineGossiper(gossipDigestSyn.gDigests, deltaGossipDigest, deltaEndPointStates)
    gossiper.makeGossipDigestAckMessage(deltaGossipDigest, deltaEndPointStates)
  }
}

