package org.dist.kvstore.gossip.handlers

import java.util

import org.dist.kvstore.network.{InetAddressAndPort, Message, MessagingService}

class StubMessagingService extends MessagingService {
  var message:Message = _
  var toAddress:util.List[InetAddressAndPort] = new util.ArrayList[InetAddressAndPort]()

  override def sendUdpOneWay(message: Message, to: InetAddressAndPort): Unit = {
    this.message = message
    this.toAddress.add(to)
  }

  override def sendTcpOneWay(message: Message, to: InetAddressAndPort): Unit = {
    this.message = message
    this.toAddress.add(to)
  }
}
