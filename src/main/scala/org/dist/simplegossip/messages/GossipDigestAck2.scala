package org.dist.simplegossip.messages

import org.dist.kvstore.{EndPointState, InetAddressAndPort}

case class GossipDigestAck2(val epStateMap: Map[InetAddressAndPort, EndPointState])