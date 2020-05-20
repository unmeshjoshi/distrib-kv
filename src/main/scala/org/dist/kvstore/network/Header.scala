package org.dist.kvstore.network

import org.dist.kvstore.Stage

case class Header(val from: InetAddressAndPort, messageType: Stage, verb: Verb, val id:String = Message.nextId)
