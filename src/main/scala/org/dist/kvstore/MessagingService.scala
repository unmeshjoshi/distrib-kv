package org.dist.kvstore

import java.net.{InetSocketAddress, ServerSocket, Socket}
import java.util

import org.dist.kvstore.handlers.{GossipDigestAck2Handler, GossipDigestSynAckHandler, GossipDigestSynHandler, RowMutationHandler}
import org.dist.kvstore.messages.{GossipDigest, GossipDigestSyn}
import org.dist.util.SocketIO
import org.slf4j.LoggerFactory

class TcpListener(localEp: InetAddressAndPort, gossiper: Gossiper, storageService: StorageService, messagingService: MessagingService) extends Thread {
  private val logger = LoggerFactory.getLogger(classOf[TcpListener])

  override def run(): Unit = {
    val serverSocket = new ServerSocket()
    serverSocket.bind(new InetSocketAddress(localEp.address, localEp.port))

    logger.info(s"Listening on ${localEp}")

    while (true) {
      val socket = serverSocket.accept()
      val message = new SocketIO[Message](socket, classOf[Message]).read()
      logger.debug(s"Got message ${message}")

      if (message.header.verb == Verb.GOSSIP_DIGEST_SYN) {
        val gossipDigestSyn = JsonSerDes.deserialize(message.payloadJson.getBytes, classOf[GossipDigestSyn])
        val synAckMessage = new GossipDigestSynHandler(gossiper).handleMessage(gossipDigestSyn)
        messagingService.sendTcpOneWay(synAckMessage, message.header.from)

      } else if (message.header.verb == Verb.GOSSIP_DIGEST_ACK) {
        new GossipDigestSynAckHandler(gossiper, messagingService).handleMessage(message)

      } else if (message.header.verb == Verb.GOSSIP_DIGEST_ACK2) {
        new GossipDigestAck2Handler(gossiper, messagingService).handleMessage(message)

      } else if(message.header.verb == Verb.RESPONSE) {

        val handler = messagingService.callbackMap.get(message.header.id)
        if (handler != null) handler.response(message)

      } else if (message.header.verb == Verb.ROW_MUTATION) {
        new RowMutationHandler(localEp, storageService, messagingService).handleMessage(message)
      }
    }
  }
}


trait MessageResponseHandler {
  def response(msg: Message): Unit
}

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

