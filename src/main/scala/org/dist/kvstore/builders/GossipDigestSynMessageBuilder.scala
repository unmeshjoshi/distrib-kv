package org.dist.kvstore.builders

import java.util

import org.dist.kvstore.{Header, InetAddressAndPort, JsonSerDes, Message, Stage, Verb}
import org.dist.kvstore.messages.{GossipDigest, GossipDigestSyn}

class GossipSynMessageBuilder(clusterName:String, localEndPoint:InetAddressAndPort) {
  def build(gDigests: util.List[GossipDigest]) = {
    val gDigestMessage = GossipDigestSyn(clusterName, gDigests)
    val header = Header(localEndPoint, Stage.GOSSIP, Verb.GOSSIP_DIGEST_SYN)
    Message(header, JsonSerDes.serialize(gDigestMessage))
  }
}
