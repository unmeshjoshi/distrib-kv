package org.dist.kvstore.gossip.messages

import org.dist.kvstore.gossip.EndPointState
import org.dist.kvstore.network.InetAddressAndPort

case class GossipDigestAck(val digestList: List[GossipDigest],
                           val epStateMap: Map[InetAddressAndPort, EndPointState])
