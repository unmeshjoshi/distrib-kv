package org.dist.simplegossip.builders

import java.util

import org.dist.kvstore.{GossipDigest, GossipDigestSyn, Header, InetAddressAndPort, JsonSerDes, Message, Stage, Verb}

class GossipSynMessageBuilder(clusterName:String, localEndPoint:InetAddressAndPort) {
  def build(gDigests: util.List[GossipDigest]) = {
    val gDigestMessage = GossipDigestSyn(clusterName, gDigests)
    val header = Header(localEndPoint, Stage.GOSSIP, Verb.GOSSIP_DIGEST_SYN)
    Message(header, JsonSerDes.serialize(gDigestMessage))
  }
}
