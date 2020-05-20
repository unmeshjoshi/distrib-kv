package org.dist.kvstore.gossip.messages

import java.util

case class GossipDigestSyn(clusterName: String, gDigests: util.List[GossipDigest])
