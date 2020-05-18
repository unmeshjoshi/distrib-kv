package org.dist.simplegossip

import java.math.BigInteger
import java.util
import java.util.concurrent.{ConcurrentHashMap, ScheduledThreadPoolExecutor, TimeUnit}
import java.util.{Collections, Random}

import org.dist.kvstore.{ApplicationState, EndPointState, GossipDigest, GossipDigestSyn, Header, HeartBeatState, InetAddressAndPort, JsonSerDes, Message, Stage, TokenMetadata, Verb, VersionGenerator, VersionedValue}
import org.dist.util.Logging

import scala.jdk.CollectionConverters._

class Gossiper(val seed:InetAddressAndPort,
               val localEndPoint:InetAddressAndPort,
               val token:BigInteger,
               val tokenMetadata:TokenMetadata,
               val executor: ScheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1)) extends Logging {
  private[simplegossip] val seeds = if (seed == localEndPoint) List() else List(seed)
  val versionGenerator = new VersionGenerator()

  def initializeLocalEndpointState() = {
    var localState = endpointStateMap.get(localEndPoint)
    if (localState == null) {
      val hbState = HeartBeatState(0, versionGenerator.incrementAndGetVersion)
      localState = EndPointState(hbState, Collections.emptyMap())
      endpointStateMap.put(localEndPoint, localState)
    }
    addApplicationState(ApplicationState.TOKENS, token.toString)
  }


  def addApplicationState(state: ApplicationState, value: String) = this.synchronized {
    val localEndpointState = endpointStateMap.get(localEndPoint)
    val newState = localEndpointState.addApplicationState(state, VersionedValue(value, versionGenerator.incrementAndGetVersion))
    endpointStateMap.put(localEndPoint, newState)
  }

  def makeGossipDigestAck2Message(deltaEpStateMap: util.HashMap[InetAddressAndPort, EndPointState]) = {
    val gossipDigestAck2 = GossipDigestAck2(deltaEpStateMap.asScala.toMap)
    val header = Header(localEndPoint, Stage.GOSSIP, Verb.GOSSIP_DIGEST_ACK2)
    Message(header, JsonSerDes.serialize(gossipDigestAck2))
  }

  def handleNewJoin(ep: InetAddressAndPort, endPointState: EndPointState) = {
    this.liveEndpoints.add(ep)
    this.endpointStateMap.put(ep, endPointState)
    val versionedValue: VersionedValue = endPointState.applicationStates.get(ApplicationState.TOKENS)
    tokenMetadata.update(new BigInteger(versionedValue.value), ep)
    info(s"${ep} joined ${localEndPoint} ")
  }

  def applyStateLocally(epStateMap: Map[InetAddressAndPort, EndPointState]) = {
    val endPoints = epStateMap.keySet
    for(ep ← endPoints) {
      val remoteToken = epStateMap(ep)
      val token = this.endpointStateMap.get(ep)
      if (token == null) {
        handleNewJoin(ep, remoteToken)
      }
    }
  }


  def makeGossipDigestAckMessage(deltaGossipDigest: util.ArrayList[GossipDigest], deltaEndPointStates: util.HashMap[InetAddressAndPort, EndPointState]) = {
   val digestAck = GossipDigestAck(deltaGossipDigest.asScala.toList,deltaEndPointStates.asScala.toMap)
    val header = Header(localEndPoint, Stage.GOSSIP, Verb.GOSSIP_DIGEST_ACK)
    Message(header, JsonSerDes.serialize(digestAck))
  }

  def examineGossiper(digestList: util.List[GossipDigest], deltaGossipDigest: util.ArrayList[GossipDigest], deltaEndPointStates: util.HashMap[InetAddressAndPort, EndPointState]) = {
    for (gDigest <- digestList.asScala) {
      val endpointState: EndPointState = endpointStateMap.get(gDigest.endPoint)
      if (endpointState != null) {
        sendAll(gDigest, deltaEndPointStates)
      } else {
        requestAll(gDigest, deltaGossipDigest)
      }
    }
  }

  /* Request all the state for the endpoint in the gDigest */
  private def requestAll(gDigest: GossipDigest, deltaGossipDigestList: util.List[GossipDigest]): Unit = {
    /* We are here since we have no data for this endpoint locally so request everthing. */
    deltaGossipDigestList.add(new GossipDigest(gDigest.endPoint, 1, 0))
  }

  /* Send all the data with version greater than maxRemoteVersion */
  private def sendAll(gDigest: GossipDigest, deltaEpStateMap: util.Map[InetAddressAndPort, EndPointState]): Unit = {
    val localEpStatePtr = getStateFor(gDigest.endPoint)
    if (localEpStatePtr != null) deltaEpStateMap.put(gDigest.endPoint, localEpStatePtr)
  }


  private val random: Random = new Random
  val liveEndpoints: util.List[InetAddressAndPort] = new util.ArrayList[InetAddressAndPort]
  val unreachableEndpoints: util.List[InetAddressAndPort] = new util.ArrayList[InetAddressAndPort]

  private val intervalMillis = 1000
  //simple state
  val endpointStateMap = new ConcurrentHashMap[InetAddressAndPort, EndPointState]

  initializeLocalEndpointState()

  def start() = {
    executor.scheduleAtFixedRate(new GossipTask, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS)
  }

  var messagingService:MessagingService = _
  def setMessageService(messagingService:MessagingService): Unit = {
    this.messagingService = messagingService
  }
  def getStateFor(addr: InetAddressAndPort) = {
    endpointStateMap.get(addr)
  }


  class GossipTask extends Runnable {

    @Override
    def run() = {
      val randomDigest = makeRandomGossipDigest()

      val gossipDigestSynMessage = makeGossipDigestSynMessage(randomDigest)

      val sentToSeedNode = doGossipToLiveMember(gossipDigestSynMessage)
      if (!sentToSeedNode) {
        doGossipToSeed(gossipDigestSynMessage)
      }
    }

    def makeGossipDigestSynMessage(gDigests: util.List[GossipDigest]) = {
      val gDigestMessage = new GossipDigestSyn("TestCluster", gDigests)
      val header = Header(localEndPoint, Stage.GOSSIP, Verb.GOSSIP_DIGEST_SYN)
      Message(header, JsonSerDes.serialize(gDigestMessage))
    }


    private def doGossipToSeed(message: Message): Unit = {
      val size = seeds.size
      if (size > 0) {
        if (size == 1 && seeds.contains(localEndPoint)) return
        if (liveEndpoints.size == 0) sendGossip(message, seeds.asJava)
        else {
          /* Gossip with the seed with some probability. */
          val probability = seeds.size / (liveEndpoints.size + unreachableEndpoints.size)
          val randDbl = random.nextDouble
          if (randDbl <= probability) sendGossip(message, seeds.asJava)
        }
      }
    }

    private def doGossipToLiveMember(message: Message): Boolean = {
      val size = liveEndpoints.size
      if (size == 0) return false
      // return sendGossipToLiveNode(message);
      /* Use this for a cluster size >= 30 */ sendGossip(message, liveEndpoints)
    }

    //@return true if the chosen endpoint is also a seed.
    private def sendGossip(message: Message, epSet: util.List[InetAddressAndPort]) = {
      val size = epSet.size
      /* Generate a random number from 0 -> size */
      val liveEndPoints = new util.ArrayList[InetAddressAndPort](epSet)
      val index = if (size == 1) 0
      else random.nextInt(size)
      val to = liveEndPoints.get(index)

      info(s"Sending gossip message from ${localEndPoint} to ${to}")

      messagingService.sendTcpOneWay(message, to)
      seed == to
    }


    def localDigest() = {
      var epState = endpointStateMap.get(localEndPoint)
      GossipDigest(localEndPoint, 1, 1)
    }

    def makeRandomGossipDigest() = {
      //FIXME Figure out why duplicates getting added here
      val digests = new util.HashSet[GossipDigest]()
      /* Add the local endpoint state */

      digests.add(localDigest())

      val endpoints = new util.ArrayList[InetAddressAndPort](liveEndpoints)
      Collections.shuffle(endpoints, random)

      for (liveEndPoint <- endpoints.asScala) {
        val epState = endpointStateMap.get(liveEndPoint)
        if (epState != null) {
          digests.add(GossipDigest(liveEndPoint, 1, epState.getMaxEndPointStateVersion()))
        }
      }
      digests.asScala.toList.asJava
    }
  }
}
