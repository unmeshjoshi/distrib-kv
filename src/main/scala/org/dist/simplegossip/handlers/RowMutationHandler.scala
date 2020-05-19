package org.dist.simplegossip.handlers

import org.dist.kvstore.{Header, InetAddressAndPort, JsonSerDes, Message, RowMutation, RowMutationResponse, Stage, Verb}
import org.dist.simplegossip.{MessagingService, StorageService}

class RowMutationHandler(localEp: InetAddressAndPort, storageService: StorageService, messagingService: MessagingService) {
  def handleMessage(rowMutationMessage: Message) = {
    val rowMutation = JsonSerDes.deserialize(rowMutationMessage.payloadJson.getBytes, classOf[RowMutation])
    val success = storageService.apply(rowMutation)
    val response = RowMutationResponse(1, rowMutation.key, success)
    val responseMessage = Message(Header(localEp, Stage.RESPONSE_STAGE, Verb.RESPONSE, rowMutationMessage.header.id), JsonSerDes.serialize(response))
    messagingService.sendTcpOneWay(responseMessage, rowMutationMessage.header.from)
  }
}
