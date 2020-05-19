package org.dist.simplegossip.messages

import org.dist.kvstore.InetAddressAndPort

/**
 * Contains information about a specified list of Endpoints and the largest version
 * of the state they have generated as known by the local endpoint.
 */
case class GossipDigest(endPoint: InetAddressAndPort, generation: Int, maxVersion: Int) {}
