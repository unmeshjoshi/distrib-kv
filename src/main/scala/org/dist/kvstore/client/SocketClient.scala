package org.dist.kvstore.client

import java.net.Socket

import org.dist.kvstore.network.{InetAddressAndPort, Message, SocketIO}

class SocketClient {
  def sendReceiveTcp(message: Message, to: InetAddressAndPort) = {
    val clientSocket = new Socket(to.address, to.port)
    val responseMessage = new SocketIO[Message](clientSocket, classOf[Message]).requestResponse(message)
    responseMessage
  }
}
