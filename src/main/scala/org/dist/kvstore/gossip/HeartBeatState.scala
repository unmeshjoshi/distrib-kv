package org.dist.kvstore.gossip

//generation is incremented every time a server restarts, so that version is always a pair of 'generation, versionNbr'
//Default generation to zero, for the cases where we do not care about restarts.
case class HeartBeatState(generation:Int = 0, version:Int) {
  def updateVersion(version:Int) = {
    HeartBeatState(generation, version)
  }
}
