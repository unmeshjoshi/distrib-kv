package org.dist.kvstore.gossip.messages

import org.dist.kvstore.Value
import org.dist.kvstore.network.Message

case class ReadMessage(table:String, key:String)

case class RowMutation(table:String, key:String, value:Value)

case class RowMutationMessage(correlationId:Int, rowMutation:RowMutation)

case class QurorumResponse(messages:List[Message])

case class RowMutationResponse(correlationId:Int, key:String, success:Boolean)
case class ReadMessageResponse(value:Value)

case class QuorumResponse(values:List[Message])