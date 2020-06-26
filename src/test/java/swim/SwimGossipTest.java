package swim;

import org.dist.kvstore.TestUtils;
import org.dist.kvstore.network.InetAddressAndPort;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SwimGossipTest {

    @Test
    public void shouldInitializeNodeListWithSelf() {
        InetAddressAndPort listenAddress = InetAddressAndPort.create("127.0.0.1", 8000);
        SwimGossip gossiper = new SwimGossip( listenAddress, "node1", listenAddress);
        Map<String, Node> nodeMap = gossiper.getNodeMap();
        Node node = nodeMap.get("node1");
        assertEquals(node.state(), NodeState.StateAlive);
    }

    @Test
    public void shouldJoinThroughSeedNode() throws IOException, InterruptedException {
        InetAddressAndPort listenAddress = InetAddressAndPort.create("127.0.0.1", 8001);
        InetAddressAndPort seedAddress = InetAddressAndPort.create("127.0.0.1", 8000);
        SwimGossip seed = new SwimGossip( seedAddress, "node1", seedAddress);
        seed.start();
        TestUtils.waitUntilTrue(()-> {
            return seed.tcpListener.serverSocket() != null && seed.tcpListener.serverSocket().isBound();
        }, () -> "waiting for seed node to be up", 5000, 100);


        SwimGossip node1 = new SwimGossip(seedAddress, "node2", listenAddress);
        node1.start();
        node1.join();

        assertNotNull(node1.getNodeMap().get("node2"));

        TestUtils.waitUntilTrue(()-> {
            return seed.nodeMap.keySet().size() == 2 && node1.nodeMap.keySet().size() == 2;
        }, () -> "waiting for all nodes to know about each other", 5000, 100);

        seed.stop();
        node1.stop();
    }

    @Test
    public void shouldBroadcastAliveMessageWhenNewNodeJoins() throws IOException, InterruptedException {
        InetAddressAndPort seedAddress = InetAddressAndPort.create("127.0.0.1", 8000);
        SwimGossip seed = new SwimGossip( seedAddress, "seed", seedAddress);
        seed.start();
        TestUtils.waitUntilTrue(()-> {
            return seed.tcpListener.serverSocket() != null && seed.tcpListener.serverSocket().isBound();
        }, () -> "waiting for seed node to be up", 5000, 100);


        SwimGossip node1 = new SwimGossip(seedAddress, "node1", InetAddressAndPort.create("127.0.0.1", 8001));
        node1.start();
        node1.join();

        assertNotNull(node1.getNodeMap().get("node1"));

        TestUtils.waitUntilTrue(()-> {
            return seed.nodeMap.keySet().size() == 2 && node1.nodeMap.keySet().size() == 2;
        }, () -> "waiting for all nodes to know about each other", 5000, 100);


        SwimGossip node2 = new SwimGossip(seedAddress, "node2", InetAddressAndPort.create("127.0.0.1", 8002));
        node2.start();
        node2.join();

        TestUtils.waitUntilTrue(()-> {
            return seed.nodeMap.keySet().size() == 3 && node1.nodeMap.keySet().size() == 3 && node2.nodeMap.keySet().size() == 3;
        }, () -> "waiting for all nodes to know about each other", 15000, 100);

        node1.stop();

        TestUtils.waitUntilTrue(()-> {
            return seed.nodeMap.get("node1").state() == NodeState.DEAD && node2.nodeMap.get("node1").state() == NodeState.DEAD;
        }, () -> "waiting for node1 to be detected as dead", 2000, 100);
    }

}