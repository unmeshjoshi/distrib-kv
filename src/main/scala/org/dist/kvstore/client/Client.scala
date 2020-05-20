package org.dist.kvstore.client

import org.dist.kvstore.gossip.messages.{QuorumResponse, RowMutation}
import org.dist.kvstore.network.{Header, InetAddressAndPort, JsonSerDes, Message, Networks, Verb}
import org.dist.kvstore.Stage

class Client(bootstrapServer: InetAddressAndPort) {
  private val socketClient = new SocketClient
  def put(table: String, key: String, value: String) = {
    val mutation = RowMutation(table, key, value)

    val header = Header(InetAddressAndPort(new Networks().ipv4Address, 8000)
      , Stage.MUTATION, Verb.ROW_MUTATION)
    val message = Message(header, JsonSerDes.serialize(mutation))
    val responseMessage: Message = socketClient.sendReceiveTcp(message, bootstrapServer)
    val responses: QuorumResponse = JsonSerDes.deserialize(responseMessage.payloadJson.getBytes, classOf[QuorumResponse])
    responses.values
  }
}
