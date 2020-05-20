package org.dist.kvstore.gossip.failuredetector

import java.net.{InetSocketAddress, ServerSocket}
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean

import org.dist.kvstore.network.{InetAddressAndPort, RequestOrResponse, SocketIO}
import org.dist.util.{Logging, Utils}


class SingularUpdateQueue(handler:RequestOrResponse => RequestOrResponse) extends Thread {
  val workQueue = new ArrayBlockingQueue[(RequestOrResponse, SocketIO[RequestOrResponse])](100)
  @volatile var running = true

  def shutdown() = {
    running = false
  }

  def submitRequest(req: RequestOrResponse, socketIo:SocketIO[RequestOrResponse]): Unit = {
    workQueue.add((req, socketIo))
  }

  override def run(): Unit = {
    while (running) {
      val (request, socketIo) = workQueue.take()
      val response = handler(request)
      socketIo.write(response)
    }
  }

}


class TcpListener(localEp: InetAddressAndPort, handler: RequestOrResponse ⇒ RequestOrResponse) extends Thread with Logging {
  val isRunning = new AtomicBoolean(true)
  var serverSocket: ServerSocket = null

  def shudown() = {
    Utils.swallow(serverSocket.close())
  }

  val workQueue = new SingularUpdateQueue(handler)

  workQueue.start()

  override def run(): Unit = {
    Utils.swallow({
      serverSocket = new ServerSocket()
      serverSocket.bind(new InetSocketAddress(localEp.address, localEp.port))
      info(s"Listening on ${localEp}")
      while (true) {
        val socket = serverSocket.accept()
        val socketIo = new SocketIO(socket, classOf[RequestOrResponse])
        socketIo.readHandleWithSocket((request, socket) ⇒ { //Dont close socket after read. It will be closed after write
          workQueue.submitRequest(request, socketIo)
        })
      }
    }
    )
  }
}