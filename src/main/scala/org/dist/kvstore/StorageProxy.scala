package org.dist.kvstore

import java.net.{InetSocketAddress, ServerSocket}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import org.dist.kvstore.gossip.messages.{QuorumResponse, ReadMessageResponse, RowMutation, RowMutationResponse}
import org.dist.kvstore.network.{Header, InetAddressAndPort, JsonSerDes, Message, MessageResponseHandler, MessagingService, SocketIO, TcpListener, Verb}
import org.dist.util.Utils
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._


class StorageProxy(clientRequestIp: InetAddressAndPort, storageService: StorageService, messagingService:MessagingService) {



  private val listner = new TcpClientRequestListner(clientRequestIp, storageService, messagingService)

  def start(): Unit = {
      listner.start()
  }

  def stop() = {
    listner.shutdown()
  }
}


class TcpClientRequestListner(localEp: InetAddressAndPort, storageService:StorageService, messagingService:MessagingService) extends Thread {
  private val logger = LoggerFactory.getLogger(classOf[TcpListener])

  val serverSocket = new ServerSocket()
  @volatile var isRunning = false


  override def run(): Unit = {
    isRunning = true

    serverSocket.bind(new InetSocketAddress(localEp.address, localEp.port))
    println(s"Listening for client connections on ${localEp}")
    while (true) {
      val socket = serverSocket.accept()
      val socketIO = new SocketIO[Message](socket, classOf[Message], 5000)
      val message = socketIO.readHandleRespond { message =>

        logger.debug(s"Got client message ${message}")

        if(message.header.verb == Verb.ROW_MUTATION) {
          val response = new RowMutationHandler(storageService).handleMessage(message)
          new Message(message.header, JsonSerDes.serialize(QuorumResponse(response)))

        } else if(message.header.verb == Verb.GET_CF) {
          val response = new ReadMessageHandler(storageService).handleMessage(message)
          new Message(message.header, JsonSerDes.serialize(QuorumResponse(response)))

        } else {
          ""
        }
      }
    }
  }

  def shutdown(): Unit = {
    Utils.swallow(serverSocket.close())
    isRunning = false
  }

  class RowMutationHandler(storageService: StorageService) {
    def handleMessage(rowMutationMessage: Message) = {
      val rowMutation = JsonSerDes.deserialize(rowMutationMessage.payloadJson.getBytes, classOf[RowMutation])
      val serversHostingKey = storageService.getNStorageEndPointMap(rowMutation.key)
      val quorumResponseHandler = new QuorumResponseHandler(serversHostingKey.size, new WriteResponseResolver())
      val header = Header(storageService.localEndPoint, Stage.MUTATION, Verb.ROW_MUTATION)
      val message = Message(header, rowMutationMessage.payloadJson)
      messagingService.sendWithCallback(message, serversHostingKey.toList, quorumResponseHandler)
      quorumResponseHandler.get()
    }
  }

  class ReadMessageHandler(storageService: StorageService) {
    def handleMessage(readMessage: Message) = {
      val rowMutation = JsonSerDes.deserialize(readMessage.payloadJson.getBytes, classOf[RowMutation])
      val serversHostingKey = storageService.getNStorageEndPointMap(rowMutation.key)
      val quorumResponseHandler = new QuorumResponseHandler(serversHostingKey.size, new WriteResponseResolver())
      val header = Header(storageService.localEndPoint, Stage.READ, Verb.GET_CF)
      val message = Message(header, readMessage.payloadJson)

      messagingService.sendWithCallback(message, serversHostingKey.toList, quorumResponseHandler)
      quorumResponseHandler.get()
    }
  }

  trait ResponseResolver {
    def resolve(messages:List[Message]):List[Message]
    def isDataPresent(message:List[Message]):Boolean
  }

  class WriteResponseResolver extends ResponseResolver {
    override def resolve(messages: List[Message]): List[Message] = {
      messages
    }

    override def isDataPresent(message: List[Message]): Boolean = true
  }

  class QuorumResponseHandler(responseCount:Int, resolver:ResponseResolver) extends MessageResponseHandler {
    private val lock = new ReentrantLock
    private val condition = lock.newCondition()
    private val responses = new java.util.ArrayList[Message]()
    private val done = new AtomicBoolean(false)
    override def response(message: Message): Unit = {
      lock.lock()
      try {
        val majority = (responseCount >> 1) + 1
        if (!done.get) {
          responses.add(message)
          logger.info(s"QuorumResponseHandler got message ${message}")
          if (responses.size >= majority && resolver.isDataPresent(responses.asScala.toList)) {
            done.set(true)
            condition.signal()
          }
        }
      } finally {
         lock.unlock()
      }
    }

    def get():List[Message] = {
      val startTime = System.currentTimeMillis
      lock.lock()
      try {
        var bVal = true
        try {
          if (!done.get) bVal = condition.await(5000, TimeUnit.MILLISECONDS)
        }
        catch {
          case ex: InterruptedException =>
            logger.debug(ex.getMessage)
        }

      } finally {
        lock.unlock()
        responses.forEach( m => messagingService.removeHandlerFor(m.header.id))
      }
      resolver.resolve(responses.asScala.toList)
    }
  }
}
