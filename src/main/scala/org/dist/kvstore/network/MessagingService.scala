package org.dist.kvstore.network

import java.net.Socket
import java.util

import org.dist.kvstore.StorageService
import org.dist.kvstore.gossip.Gossiper
import org.dist.util.Logging


trait MessagingService extends Logging {

  val callbackMap = new util.concurrent.ConcurrentHashMap[String, MessageResponseHandler]()

  def getHandler(id: String): MessageResponseHandler = callbackMap.get(id)

  def removeHandlerFor(id: String): Unit = {
    trace(s"Removing handler for ${id}")
    callbackMap.remove(id)
  }

  def sendWithCallback(message: Message, to: List[InetAddressAndPort], messageResponseHandler: MessageResponseHandler): Unit = {
    callbackMap.put(message.header.id, messageResponseHandler)
    to.foreach(address => sendTcpOneWay(message, address))
  }

  def sendTcpOneWay(message: Message, to: InetAddressAndPort)
  def sendUdpOneWay(message: Message, to: InetAddressAndPort)
}

class MessagingServiceImpl(val gossiper: Gossiper, storageService: StorageService) extends MessagingService {

  gossiper.setMessageService(this)

  def init(): Unit = {
  }

  def listen(localEp: InetAddressAndPort): Unit = {
    assert(gossiper != null)
    new TcpListener(localEp, gossiper, storageService, this).start()
  }

  def sendTcpOneWay(message: Message, to: InetAddressAndPort) = {
    val clientSocket = new Socket(to.address, to.port)
    new SocketIO[Message](clientSocket, classOf[Message]).write(message)
  }

  def sendUdpOneWay(message: Message, to: InetAddressAndPort) = {
    //for control messages like gossip use udp.
  }

}

