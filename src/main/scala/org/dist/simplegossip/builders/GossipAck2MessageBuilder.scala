package org.dist.simplegossip.builders

import java.util

import org.dist.kvstore.{EndPointState, Header, InetAddressAndPort, JsonSerDes, Message, Stage, Verb}
import org.dist.simplegossip.messages.GossipDigestAck2

import scala.jdk.CollectionConverters._

class GossipAck2MessageBuilder(localEndPoint:InetAddressAndPort) {

  def build(deltaEndPointStates: util.Map[InetAddressAndPort, EndPointState]) = {
    val gossipDigestAck2 = GossipDigestAck2(deltaEndPointStates.asScala.toMap)
    val header = Header(localEndPoint, Stage.GOSSIP, Verb.GOSSIP_DIGEST_ACK2)
    Message(header, JsonSerDes.serialize(gossipDigestAck2))
  }
}
