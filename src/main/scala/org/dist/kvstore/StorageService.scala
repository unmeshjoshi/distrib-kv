package org.dist.kvstore

import java.math.BigInteger
import java.util

import org.apache.log4j.Logger
import org.dist.kvstore.gossip.{Gossiper, TokenMetadata, VersionedValue}
import org.dist.kvstore.gossip.messages.{ReadMessage, RowMutation}
import org.dist.kvstore.network.{InetAddressAndPort, MessagingService, MessagingServiceImpl}
import org.dist.util.{FBUtilities, GuidGenerator}

case class Value(value:String, timestamp:Long = System.currentTimeMillis())
case class Table(name:String, kv:util.Map[String, Value]) {
  def get(key: String): Value = kv.get(key)

  def put(key:String, value:Value) = kv.put(key, value)
}

class StorageService(seed:InetAddressAndPort, clientListenAddress:InetAddressAndPort, val localEndPoint:InetAddressAndPort) {

  private val logger = Logger.getLogger(classOf[StorageService])

  val tables = new util.HashMap[String, Table]()

  val token = newToken()
  val tokenMetadata = new TokenMetadata()
  val gossiper = new Gossiper(seed, localEndPoint, token, tokenMetadata)
  val messagingService = new MessagingServiceImpl(gossiper, this)
  val storageProxy = new StorageProxy(clientListenAddress, this, messagingService)

  def start() = {
    messagingService.listen(localEndPoint)
    gossiper.start()
    tokenMetadata.update(token, localEndPoint)
    storageProxy.start()
  }

  def newToken() = {
    val guid = GuidGenerator.guid
    var token = FBUtilities.hash(guid)
    if (token.signum == -1) token = token.multiply(BigInteger.valueOf(-1L))
    token
  }

  val ReplicationFactor = 5
  def getNStorageEndPointMap(key: String) = {
    val token: BigInteger = FBUtilities.hash(key)
    new RackUnawareStrategy(tokenMetadata).getStorageEndPoints(token, tokenMetadata.cloneTokenEndPointMap)
  }

  def apply(rowMutation: RowMutation) = {
    var table = tables.get(rowMutation.table)
    if (table == null) {
      table = new Table(rowMutation.table, new util.HashMap[String, Value]())
      tables.put(rowMutation.table, table)
    }
    table.put(rowMutation.key, rowMutation.value)
    true
  }

  def read(readMessage:ReadMessage): Value = {
    val table = tables.get(readMessage.table)
    table.get(readMessage.key)
  }

  def stop() = {
    messagingService.stop()
    gossiper.stop()
    storageProxy.stop()
  }
}
