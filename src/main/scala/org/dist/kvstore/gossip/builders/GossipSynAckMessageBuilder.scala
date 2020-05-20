package org.dist.kvstore.gossip.builders

import java.util

import org.dist.kvstore.gossip.EndPointState
import org.dist.kvstore.Stage
import org.dist.kvstore.gossip.messages.{GossipDigest, GossipDigestAck}
import org.dist.kvstore.network.{Header, InetAddressAndPort, JsonSerDes, Message, Verb}

import scala.jdk.CollectionConverters._

class GossipSynAckMessageBuilder(localEndPoint:InetAddressAndPort) {

  def build(deltaGossipDigest: util.List[GossipDigest], deltaEndPointStates: util.Map[InetAddressAndPort, EndPointState]) = {
    val map = deltaEndPointStates.asScala.toMap
    val gossipDigestAck = GossipDigestAck(deltaGossipDigest.asScala.toList, map)
    val header = Header(localEndPoint, Stage.GOSSIP, Verb.GOSSIP_DIGEST_ACK)
    Message(header, JsonSerDes.serialize(gossipDigestAck))
  }
}

