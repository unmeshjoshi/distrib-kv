package org.dist.simplegossip

import java.math.BigInteger

import org.dist.kvstore.{ApplicationState, InetAddressAndPort, TokenMetadata}
import org.dist.util.Utils
import org.scalatest.FunSuite

class GossiperTest extends FunSuite {
  test("should keep state for endpoints and initialize with own endpoint") {
    val listenAddress = InetAddressAndPort.create("127.0.0.1", 8000)
    val token = Utils.newToken()
    val gossiper = new Gossiper(listenAddress, listenAddress, token, new TokenMetadata())
    val epState = gossiper.endpointStateMap.get(listenAddress)
    val localToken = epState.applicationStates.get(ApplicationState.TOKENS).value
    assert(token == new BigInteger(localToken))
  }

  test("should have HeartBeatState as part of endpoint state") {

  }
}
