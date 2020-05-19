package org.dist.kvstore.failuredetector.heartbeat

import org.dist.kvstore.InetAddressAndPort
import org.dist.kvstore.failuredetector.Client

case class Peer(id:Int, address:InetAddressAndPort)

case class PeerProxy(peerInfo: Peer, client: Client = null, var matchIndex: Long = 0, heartbeatSender: PeerProxy ⇒ Unit) {

  def heartbeatSenderWrapper() = {
    heartbeatSender(this)
  }

  val heartBeat = new HeartBeatScheduler(heartbeatSenderWrapper)

  def start(): Unit = {
    heartBeat.start()
  }

  def stop() = {
    heartBeat.cancel()
  }
}
