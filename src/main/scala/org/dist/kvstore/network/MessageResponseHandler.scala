package org.dist.kvstore.network

trait MessageResponseHandler {
  def response(msg: Message): Unit
}
