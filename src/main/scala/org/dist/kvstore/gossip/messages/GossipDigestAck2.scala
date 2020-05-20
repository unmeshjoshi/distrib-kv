package org.dist.kvstore.gossip.messages

import org.dist.kvstore.gossip.EndPointState
import org.dist.kvstore.network.InetAddressAndPort

case class GossipDigestAck2(val epStateMap: Map[InetAddressAndPort, EndPointState])