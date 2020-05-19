package org.dist.simplegossip.messages

import java.util

case class GossipDigestSyn(clusterName: String, gDigests: util.List[GossipDigest])
