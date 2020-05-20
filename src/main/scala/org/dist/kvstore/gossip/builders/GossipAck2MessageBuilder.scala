package org.dist.kvstore.gossip.builders

import java.util

import org.dist.kvstore.gossip.EndPointState
import org.dist.kvstore.Stage
import org.dist.kvstore.gossip.messages.GossipDigestAck2
import org.dist.kvstore.network.{Header, InetAddressAndPort, JsonSerDes, Message, Verb}

import scala.jdk.CollectionConverters._

class GossipAck2MessageBuilder(localEndPoint:InetAddressAndPort) {

  def build(deltaEndPointStates: util.Map[InetAddressAndPort, EndPointState]) = {
    val gossipDigestAck2 = GossipDigestAck2(deltaEndPointStates.asScala.toMap)
    val header = Header(localEndPoint, Stage.GOSSIP, Verb.GOSSIP_DIGEST_ACK2)
    Message(header, JsonSerDes.serialize(gossipDigestAck2))
  }
}
