package org.dist.kvstore.messages

import java.util

case class GossipDigestSyn(clusterName: String, gDigests: util.List[GossipDigest])
