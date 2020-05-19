package org.dist.kvstore.builders

import java.util

import org.dist.kvstore.{EndPointState, Header, InetAddressAndPort, JsonSerDes, Message, Stage, Verb}
import org.dist.kvstore.messages.{GossipDigest, GossipDigestAck}

import scala.jdk.CollectionConverters._

class GossipSynAckMessageBuilder(localEndPoint:InetAddressAndPort) {

  def build(deltaGossipDigest: util.ArrayList[GossipDigest], deltaEndPointStates: util.Map[InetAddressAndPort, EndPointState]) = {
    val map = deltaEndPointStates.asScala.toMap
    val gossipDigestAck = GossipDigestAck(deltaGossipDigest.asScala.toList, map)
    val header = Header(localEndPoint, Stage.GOSSIP, Verb.GOSSIP_DIGEST_ACK)
    Message(header, JsonSerDes.serialize(gossipDigestAck))
  }
}

