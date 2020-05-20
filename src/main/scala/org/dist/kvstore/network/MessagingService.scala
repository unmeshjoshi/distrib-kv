package org.dist.kvstore.network

import java.net.{InetSocketAddress, ServerSocket, Socket}
import java.util

import org.dist.kvstore.gossip.Gossiper
import org.dist.kvstore.gossip.handlers.{GossipDigestAck2Handler, GossipDigestSynAckHandler, GossipDigestSynHandler, RowMutationHandler}
import org.dist.kvstore.gossip.messages.GossipDigestSyn
import org.dist.kvstore.StorageService
import org.slf4j.LoggerFactory






class MessagingService(val gossiper: Gossiper, storageService: StorageService) {

  gossiper.setMessageService(this)

  val callbackMap = new util.HashMap[String, MessageResponseHandler]()

  def init(): Unit = {
  }

  def listen(localEp: InetAddressAndPort): Unit = {
    assert(gossiper != null)
    new TcpListener(localEp, gossiper, storageService, this).start()
  }

  def sendRR(message: Message, to: List[InetAddressAndPort], messageResponseHandler: MessageResponseHandler): Unit = {
    callbackMap.put(message.header.id, messageResponseHandler)
    to.foreach(address => sendTcpOneWay(message, address))
  }

  def sendTcpOneWay(message: Message, to: InetAddressAndPort) = {
    val clientSocket = new Socket(to.address, to.port)
    new SocketIO[Message](clientSocket, classOf[Message]).write(message)
  }

  def sendUdpOneWay(message: Message, to: InetAddressAndPort) = {
    //for control messages like gossip use udp.
  }
}

