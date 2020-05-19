package org.dist.simplegossip.builders

import java.util
import java.util.{Collections, Random}

import scala.jdk.CollectionConverters._
import org.dist.kvstore.{EndPointState, InetAddressAndPort}
import org.dist.simplegossip.messages
import org.dist.simplegossip.messages.GossipDigest
import org.dist.util.Logging


class GossipDigestBuilder(localEndPoint: InetAddressAndPort,
                          endpointStatemap: util.Map[InetAddressAndPort, EndPointState],
                          liveEndpoints: util.List[InetAddressAndPort] = new util.ArrayList[InetAddressAndPort]) extends Logging {
    private val random: Random = new Random
  /**
   * No locking required since it is called from a method that already
   * has acquired a lock. The gossip digest is built based on randomization
   * rather than just looping through the collection of live endpoints.
   *
   */
  def makeRandomGossipDigest() = {
    val digests = new util.HashSet[GossipDigest]()
    /* Add the local endpoint state */
    var epState = endpointStatemap.get(localEndPoint)
    var generation = epState.heartBeatState.generation
    var maxVersion = epState.getMaxEndPointStateVersion
    val localDigest = new GossipDigest(localEndPoint, generation, maxVersion)

    digests.add(localDigest)

    val endpoints = new util.ArrayList[InetAddressAndPort](liveEndpoints)
    Collections.shuffle(endpoints, random)

    for (liveEndPoint <- endpoints.asScala) {
      epState = endpointStatemap.get(liveEndPoint)
      if (epState != null) {
        generation = epState.heartBeatState.generation
        maxVersion = epState.getMaxEndPointStateVersion
        digests.add(messages.GossipDigest(liveEndPoint, generation, maxVersion))
      }
      else digests.add(messages.GossipDigest(liveEndPoint, 0, 0)) //we do not have any version of any value for this endpoint.
    }

    log(digests)

    digests.asScala.toList.asJava
  }

  private def log(gDigests: util.Set[GossipDigest]) = {
    /* FOR DEBUG ONLY - remove later */ val sb = new StringBuilder
    for (gDigest <- gDigests.asScala) {
      sb.append(gDigest)
      sb.append(" ")
    }
    trace("Gossip Digests are : " + sb.toString)
  }
}
