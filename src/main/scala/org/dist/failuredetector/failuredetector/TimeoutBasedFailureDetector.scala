package org.dist.failuredetector.failuredetector

import java.util
import java.util.concurrent

import org.dist.failuredetector.failuredetector.heartbeat.HeartBeatScheduler
import org.dist.util.Logging

import scala.jdk.CollectionConverters._

abstract class FailureDetector[T] extends Logging {
  object ServerState extends Enumeration {
    type ServerState = Value
    val UP, DOWN = Value
  }
  val serverStates = new util.HashMap[T, ServerState.Value]
  def isAlive(serverId: T): Boolean = {
    serverStates.get(serverId) == ServerState.UP
  }
  val timeoutChecker = new HeartBeatScheduler(heartBeatCheck)

  def markUp(serverId:T) = {
    info(s"Marking ${serverId} as Up")
    serverStates.put(serverId, ServerState.UP)
  }
  def markDown(serverId:T) = {
    info(s"Marking ${serverId} as Down")
    serverStates.put(serverId, ServerState.DOWN)
  }


  def start(): Unit = {
    timeoutChecker.startWithRandomInterval()
  }

  def stop() = timeoutChecker.cancel()

  def heartBeatCheck()
  def heartBeatReceived(serverId:T)
}

class TimeoutBasedFailureDetector[T] extends FailureDetector[T] {
  val serverHeartBeatReceived = new util.HashMap[T, Long]
  val timeOutNanos = concurrent.TimeUnit.MILLISECONDS.toNanos(100)

  override def heartBeatReceived(serverId: T): Unit = {
    this.synchronized {
      val currentTime: Long = System.nanoTime()
      info(s"Heartbeat received from ${serverId} at ${currentTime}")
      serverHeartBeatReceived.put(serverId, currentTime)
      serverStates.put(serverId, ServerState.UP)
    }
  }

  override def heartBeatCheck() = {
    this.synchronized {
      val currentTime: Long = System.nanoTime()
      val keys = serverHeartBeatReceived.keySet()
      for (key ← keys.asScala) {
        val lastReceivedNanos = serverHeartBeatReceived.get(key)
        val timeSinceLastHeartBeat = currentTime - lastReceivedNanos
        info(s"Time since last heartbeat from ${key} is ${timeSinceLastHeartBeat}")
        if (timeSinceLastHeartBeat >= timeOutNanos) {
          markDown(key)
        }
      }
    }
  }
}
