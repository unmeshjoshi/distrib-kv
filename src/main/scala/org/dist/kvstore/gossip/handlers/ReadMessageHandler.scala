package org.dist.kvstore.gossip.handlers

import org.dist.kvstore.gossip.messages.{ReadMessage, ReadMessageResponse, RowMutation, RowMutationResponse}
import org.dist.kvstore.network._
import org.dist.kvstore.{Stage, StorageService}

class ReadMessageHandler(localEp: InetAddressAndPort, storageService: StorageService, messagingService: MessagingService) {
  def handleMessage(rowMutationMessage: Message) = {
    val readMessage = JsonSerDes.deserialize(rowMutationMessage.payloadJson.getBytes, classOf[ReadMessage])
    val value = storageService.read(readMessage)
    val response = ReadMessageResponse(value)
    val responseMessage = Message(Header(localEp, Stage.RESPONSE_STAGE, Verb.RESPONSE, rowMutationMessage.header.id), JsonSerDes.serialize(response))
    messagingService.sendTcpOneWay(responseMessage, rowMutationMessage.header.from)
  }
}
