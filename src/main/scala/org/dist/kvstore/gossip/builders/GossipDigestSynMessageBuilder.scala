package org.dist.kvstore.gossip.builders

import java.util

import org.dist.kvstore.Stage
import org.dist.kvstore.gossip.messages.{GossipDigest, GossipDigestSyn}
import org.dist.kvstore.network.{Header, InetAddressAndPort, JsonSerDes, Message, Verb}

class GossipSynMessageBuilder(clusterName:String, localEndPoint:InetAddressAndPort) {
  def build(gDigests: util.List[GossipDigest]) = {
    val gDigestMessage = GossipDigestSyn(clusterName, gDigests)
    val header = Header(localEndPoint, Stage.GOSSIP, Verb.GOSSIP_DIGEST_SYN)
    Message(header, JsonSerDes.serialize(gDigestMessage))
  }
}
