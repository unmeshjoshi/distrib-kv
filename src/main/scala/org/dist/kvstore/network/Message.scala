package org.dist.kvstore.network

import java.util.concurrent.atomic.AtomicInteger

import org.dist.util.GuidGenerator

object Message {
  private val messageId = new AtomicInteger(0)

  def nextId = {
    GuidGenerator.guid
  }
}

case class Message(header:Header, payloadJson:String)
