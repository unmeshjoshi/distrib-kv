package org.dist.kvstore

import java.math.BigInteger
import java.util
import java.util.Collections

import org.dist.util.{FBUtilities, Utils}
import org.scalatest.FunSuite

class Server {
  val map = new util.HashMap[String, String]
  def put(key:String, value:String) = map.put(key, value)
}


class RackUnawareStrategyTest extends FunSuite {
  test("should pick up token for given key") {
    val tokenMap = new util.HashMap[BigInteger, Server]()
    tokenMap.put(Utils.newToken(), new Server()) //InetAddressAndPort
    tokenMap.put(Utils.newToken(), new Server())
    tokenMap.put(Utils.newToken(), new Server())

    val tokens
    = new util.ArrayList[BigInteger](tokenMap.keySet())

    Collections.sort(tokens)

    val key = Utils.newToken()

    var i = Collections.binarySearch(tokens, key)
    var keyIndex =
    if (i < 0) {
      (i + 1) * (-1)
    } else i

    keyIndex = if (keyIndex >= tokens.size()) 0 else keyIndex

    assert(keyIndex <= tokens.size())
    val nextReplica = keyIndex + 1
    val thirdReplica = nextReplica + 1

    tokenMap.get(keyIndex).put(key.toString(), "value") // send tcp request to server
    tokenMap.get(nextReplica).put(key.toString(), "value") // send tcp request to server
    tokenMap.get(thirdReplica).put(key.toString(), "value") // send tcp request to server

  }

}
