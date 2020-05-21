package org.dist.kvstore.client

import org.apache.zookeeper.server.quorum.ReadOnlyRequestProcessor
import org.dist.kvstore.gossip.messages.{QuorumResponse, ReadMessage, RowMutation, RowMutationResponse}
import org.dist.kvstore.network.{Header, InetAddressAndPort, JsonSerDes, Message, Networks, Verb}
import org.dist.kvstore.{Stage, Value}

class Client(bootstrapServer: InetAddressAndPort) {
  def get(table: String, key: String) =  {
    val mutation = ReadMessage(table, key)

    val header = Header(InetAddressAndPort(new Networks().ipv4Address, 8000)
      , Stage.READ, Verb.GET_CF)
    val message = Message(header, JsonSerDes.serialize(mutation))
    val responseMessage: Message = socketClient.sendReceiveTcp(message, bootstrapServer)
    val responses: QuorumResponse = JsonSerDes.deserialize(responseMessage.payloadJson.getBytes, classOf[QuorumResponse])
    responses.values
  }

  private val socketClient = new SocketClient
  def put(table: String, key: String, value: String) = {
    val mutation = RowMutation(table, key, Value(value))

    val header = Header(InetAddressAndPort(new Networks().ipv4Address, 8000)
      , Stage.MUTATION, Verb.ROW_MUTATION)
    val message = Message(header, JsonSerDes.serialize(mutation))
    val responseMessage: Message = socketClient.sendReceiveTcp(message, bootstrapServer)
    val responses: QuorumResponse = JsonSerDes.deserialize(responseMessage.payloadJson.getBytes, classOf[QuorumResponse])
    responses.values
  }
}
