package swim

import org.dist.kvstore.network.InetAddressAndPort

case class Node(name:String, addr:InetAddressAndPort, state:NodeState)
