package org.dist.kvstore.gossip.handlers

import org.dist.kvstore.gossip.messages.{RowMutation, RowMutationResponse}
import org.dist.kvstore.network.{Header, InetAddressAndPort, JsonSerDes, Message, MessagingService, Verb}
import org.dist.kvstore.{Stage, StorageService}

class RowMutationHandler(localEp: InetAddressAndPort, storageService: StorageService, messagingService: MessagingService) {
  def handleMessage(rowMutationMessage: Message) = {
    val rowMutation = JsonSerDes.deserialize(rowMutationMessage.payloadJson.getBytes, classOf[RowMutation])
    val success = storageService.apply(rowMutation)
    val response = RowMutationResponse(1, rowMutation.key, success)
    val responseMessage = Message(Header(localEp, Stage.RESPONSE_STAGE, Verb.RESPONSE, rowMutationMessage.header.id), JsonSerDes.serialize(response))
    messagingService.sendTcpOneWay(responseMessage, rowMutationMessage.header.from)
  }
}
