package org.dist.kvstore.gossip

import java.math.BigInteger
import java.util
import java.util.concurrent.{ConcurrentHashMap, ScheduledThreadPoolExecutor, TimeUnit}
import java.util.{Collections, Random}

import org.dist.kvstore.gossip.builders.{GossipDigestBuilder, GossipSynMessageBuilder}
import org.dist.kvstore.gossip.failuredetector.PhiChiAccrualFailureDetector
import org.dist.kvstore.gossip.messages.GossipDigest
import org.dist.kvstore._
import org.dist.kvstore.network.{InetAddressAndPort, Message, MessagingService}
import org.dist.util.Logging

import scala.jdk.CollectionConverters._
import scala.util.control.Breaks

class Gossiper(val seed: InetAddressAndPort,
               val localEndPoint: InetAddressAndPort,
               val token: BigInteger,
               val tokenMetadata: TokenMetadata,
               val executor: ScheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1)) extends Logging {
  private[kvstore] val seeds = if (seed == localEndPoint) List() else List(seed)
  val versionGenerator = new VersionGenerator()
  val fd = new PhiChiAccrualFailureDetector[InetAddressAndPort]()

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

  def handleNewJoin(ep: InetAddressAndPort, endPointState: EndPointState) = {
    markAlive(ep)
    this.endpointStateMap.put(ep, endPointState)
    val versionedValue: VersionedValue = endPointState.applicationStates.get(ApplicationState.TOKENS)
    tokenMetadata.update(new BigInteger(versionedValue.value), ep)
    info(s"${ep} joined ${localEndPoint} ")
  }


  private def markAlive(ep: InetAddressAndPort) = {
    this.liveEndpoints.add(ep)
    this.unreachableEndpoints.remove(ep)
    val state = endpointStateMap.get(ep)
    if (state != null) {
      endpointStateMap.put(ep, state.markAlive())
    }
  }

  private def markDead(ep: InetAddressAndPort) = {
    this.liveEndpoints.remove(ep)
    this.unreachableEndpoints.add(ep)
    val state = endpointStateMap.get(ep)
    endpointStateMap.put(ep, state.markUnreachable())
  }

  def notifyFailureDetector(remoteEpStateMap: util.Map[InetAddressAndPort, EndPointState]): Unit = {
    val endpoints = remoteEpStateMap.keySet
    for (endpoint <- endpoints.asScala) {
      val remoteEndPointState = remoteEpStateMap.get(endpoint)
      val localEndPointState = endpointStateMap.get(endpoint)
      /*
                   * If the local endpoint state exists then report to the FD only
                   * if the versions workout.
                  */
      if (localEndPointState != null) {
        val localGeneration = localEndPointState.heartBeatState.generation
        val remoteGeneration = remoteEndPointState.heartBeatState.generation
        if (remoteGeneration > localGeneration) {
          info("Reporting " + endpoint + " to the FD.")
          fd.heartBeatReceived(endpoint)
          //          continue //todo: continue is not supported

        }
        if (remoteGeneration == localGeneration) {
          val localVersion = localEndPointState.getMaxEndPointStateVersion()
          val remoteVersion = remoteEndPointState.heartBeatState.version
          if (remoteVersion > localVersion) {
            info("Reporting " + endpoint + " to the FD.")
            fd.heartBeatReceived(endpoint)
          }
        }
      }
    }
  }
  def applyStateLocally(epStateMap: Map[InetAddressAndPort, EndPointState]) = {
    val endPoints = epStateMap.keySet
    for (ep ← endPoints) {
      val remoteEndpointState = epStateMap(ep)
      val localEndpointState = this.endpointStateMap.get(ep)
      if (localEndpointState == null) {
        handleNewJoin(ep, remoteEndpointState)

      } else { //we already have some state, apply the higher versions sent by remote.
        val localAppStates = applyHeartBeatStateLocally(ep, localEndpointState, remoteEndpointState)
        val remoteAppStates = remoteEndpointState.applicationStates
        val updatedStates = remoteAppStates.asScala.filter(keyValue => {
          val key = keyValue._1
          val remoteValue = keyValue._2
          val localValue = localAppStates.applicationStates.get(key)
          localValue == null || remoteValue.version > localValue.version
        })

        val updatedEnpointStates = localAppStates.addApplicationStates(updatedStates.asJava)
        endpointStateMap.put(ep, updatedEnpointStates)
      }
    }
  }

  private def applyHeartBeatStateLocally(addr: InetAddressAndPort, localState: EndPointState, remoteState: EndPointState): EndPointState = {
    val localHbState = localState.heartBeatState
    val remoteHbState = remoteState.heartBeatState
    if (remoteHbState.version > localHbState.version) {
      val oldVersion = localHbState.version

      debug("Updating heartbeat state version to " + localState.heartBeatState.version + " from " + oldVersion + " for " + addr + " ...")
      return localState.copy(remoteHbState)
    }

    localState
  }


  def examineGossiper(digestList: util.List[GossipDigest], deltaGossipDigest: util.ArrayList[GossipDigest], deltaEndPointStates: util.HashMap[InetAddressAndPort, EndPointState]) = {
    for (gDigest <- digestList.asScala) {
      Breaks.breakable {
        val endpointState: EndPointState = endpointStateMap.get(gDigest.endPoint)
        if (endpointState != null) {
          val maxRemoteVersion = gDigest.maxVersion
          val maxLocalVersion = endpointState.getMaxEndPointStateVersion()
          if (maxRemoteVersion == maxLocalVersion) {
            Breaks.break()
          }

          if (maxRemoteVersion < maxLocalVersion) {
            info(s"MaxVersion ${maxRemoteVersion} is less than ${maxLocalVersion} for ${gDigest.endPoint}. Sending local state with higher versions to remote")

            sendAll(gDigest, deltaEndPointStates, maxRemoteVersion)

          } else if (maxRemoteVersion > maxLocalVersion) {

            info(s"MaxVersion ${maxRemoteVersion} is greater than ${maxLocalVersion} for ${gDigest.endPoint}. Asking for it")
            deltaGossipDigest.add(new GossipDigest(gDigest.endPoint, 1, maxLocalVersion))
          }

        } else {
          requestAll(gDigest, deltaGossipDigest)
        }
      }
    }
  }

  def getStateForVersionBiggerThan(forEndpoint: InetAddressAndPort, version: Int) = {
    val epState = endpointStateMap.get(forEndpoint)
    var reqdEndPointState: EndPointState = null
    if (epState != null) {
      /*
                  * Here we try to include the Heart Beat state only if it is
                  * greater than the version passed in. It might happen that
                  * the heart beat version maybe lesser than the version passed
                  * in and some application state has a version that is greater
                  * than the version passed in. In this case we also send the old
                  * heart beat and throw it away on the receiver if it is redundant.
                  */
      val localHbVersion = epState.heartBeatState.version
      if (localHbVersion > version) reqdEndPointState = EndPointState(epState.heartBeatState)
      val appStateMap = epState.applicationStates
      /* Accumulate all application states whose versions are greater than "version" variable */ val keys = appStateMap.keySet
      for (key <- keys.asScala) {
        val versionValue = appStateMap.get(key)
        if (versionValue.version > version) {
          if (reqdEndPointState == null) reqdEndPointState = EndPointState(epState.heartBeatState)
          reqdEndPointState = reqdEndPointState.addApplicationState(key, versionValue)
        }
      }
    }
    reqdEndPointState
  }

  /* Request all the state for the endpoint in the gDigest */
  private def requestAll(gDigest: GossipDigest, deltaGossipDigestList: util.List[GossipDigest]): Unit = {
    /* We are here since we have no data for this endpoint locally so request everthing. */
    deltaGossipDigestList.add(new GossipDigest(gDigest.endPoint, 1, 0))
  }

  /* Send all the data with version greater than maxRemoteVersion */
  private def sendAll(gDigest: GossipDigest, deltaEpStateMap: util.Map[InetAddressAndPort, EndPointState], maxRemoteVersion: Int = 0): Unit = {
    val localEpStatePtr = getStateForVersionBiggerThan(gDigest.endPoint, maxRemoteVersion)
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

  var messagingService: MessagingService = _

  def setMessageService(messagingService: MessagingService): Unit = {
    this.messagingService = messagingService
  }

  def getStateFor(addr: InetAddressAndPort) = {
    endpointStateMap.get(addr)
  }


  class GossipTask extends Runnable {



    @Override
    def run() = {
      val randomDigest = new GossipDigestBuilder(localEndPoint, endpointStateMap, liveEndpoints).makeRandomGossipDigest()

      val gossipDigestSynMessage = new GossipSynMessageBuilder("cluster", localEndPoint).build(randomDigest)

      val sentToSeedNode = doGossipToLiveMember(gossipDigestSynMessage)
      if (!sentToSeedNode) {
        doGossipToSeed(gossipDigestSynMessage)
      }

      //todo mark servers as live or dead.
      fd.heartBeatCheck() //interpret results
      fd.unreachableServers.keys.foreach(ep => markDead(ep))

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
  }
}
