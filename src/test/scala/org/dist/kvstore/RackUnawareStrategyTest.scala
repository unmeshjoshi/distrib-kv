package org.dist.kvstore

import java.util

import org.dist.kvstore.locator.RackUnawareStrategy
import org.scalatest.FunSuite

class RackUnawareStrategyTest extends FunSuite {

  test("testGetStorageEndPoints") {
    val tokenMetadata = new TokenMetadata()
    tokenMetadata.update(newToken(), InetAddressAndPort.create("10.10.10.10", 8000))
    tokenMetadata.update(newToken(), InetAddressAndPort.create("10.10.10.11", 8000))
    tokenMetadata.update(newToken(), InetAddressAndPort.create("10.10.10.12", 8000))
    tokenMetadata.update(newToken(), InetAddressAndPort.create("10.10.10.13", 8000))
    tokenMetadata.update(newToken(), InetAddressAndPort.create("10.10.10.14", 8000))
    val rackUnawareStrategy = new RackUnawareStrategy(tokenMetadata)

    assert(2 == getEndPoints(rackUnawareStrategy, "5"))
  }


  test("Should find index for given integer in list of integers") {
    val ints:util.List[_ <: Comparable[Int]] = util.Arrays.asList(1, 10, 20).asInstanceOf[util.List[_ <: Comparable[Int]]]
    assert(-4 == util.Collections.binarySearch(ints, 21))
    assert(-2 == util.Collections.binarySearch(ints, 3))
    assert(-3 == util.Collections.binarySearch(ints, 15))
    assert(-1 == util.Collections.binarySearch(ints, 0))
  }

  private def newToken() = {
    FBUtilities.hash(GuidGenerator.guid)
  }

  private def getEndPoints(rackUnawareStrategy: RackUnawareStrategy, key: String) = {
    rackUnawareStrategy.getStorageEndPoints(FBUtilities.hash(key))
  }
}
