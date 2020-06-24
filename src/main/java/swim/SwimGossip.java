package swim;

import org.dist.kvstore.gossip.failuredetector.TcpListener;
import org.dist.kvstore.network.InetAddressAndPort;
import org.dist.kvstore.network.JsonSerDes;
import org.dist.kvstore.network.RequestOrResponse;
import org.dist.kvstore.network.SocketIO;

import java.io.IOException;
import java.net.Socket;
import java.util.*;

enum SwimGossipRequestType {
    PushPull(1);

    int id;

    SwimGossipRequestType(int id) {
        this.id = id;
    }
}

public class SwimGossip {
    private final InetAddressAndPort seed;
    private final InetAddressAndPort listenAddress;
    final TcpListener tcpListener;
    Map<String, Node> nodeMap = new HashMap<String, Node>();

    public SwimGossip(InetAddressAndPort seed, String name, InetAddressAndPort listenAddress) {
        this.seed = seed;
        this.listenAddress = listenAddress;
        nodeMap.put(name, new Node(name, listenAddress, NodeState.StateAlive, 1));
        this.tcpListener = new TcpListener(listenAddress, this::handle);
    }

    public void start() {
        tcpListener.start();
    }

    private RequestOrResponse handle(RequestOrResponse request) {
        if (request.requestId() == SwimGossipRequestType.PushPull.id) {
            PushPullMessage pushPul = JsonSerDes.deserialize(request.messageBodyJson(), PushPullMessage.class);
            PushPullMessage response = new PushPullMessage(this.getLocalState());

            this.mergeStates(pushPul.getNodeStates());

            return new RequestOrResponse((short) SwimGossipRequestType.PushPull.id, JsonSerDes.serialize(response), request.correlationId());

        }
        return new RequestOrResponse((short) 0, "", 0);
    }

    public Map<String, Node> getNodeMap() {
        return nodeMap;
    }

    public void join() throws IOException {
        pushPullNode(seed);
    }

    private void pushPullNode(InetAddressAndPort seed) throws IOException {
        List<PushNodeState> nodeStates = getLocalState();
        List<PushNodeState> remoteStates = sendReceiveState(new PushPullMessage(nodeStates), seed);
        mergeStates(remoteStates);
    }

    private void mergeStates(List<PushNodeState> remoteStates) {
        for (PushNodeState remoteState : remoteStates) {
            if (remoteState.state == NodeState.StateAlive) {
                aliveNode(remoteState);
            } else if (remoteState.state == NodeState.SUSPECTED) {
                suspectNode(remoteState);
            } else if (remoteState.state == NodeState.DEAD) {
                deadNode(remoteState);
            }
        }
    }

    void aliveNode(PushNodeState remoteNode) {
        Node localNode = nodeMap.get(remoteNode.getName());
        if (localNode == null) {
            nodeMap.put(remoteNode.getName(), new Node(remoteNode.getName(), remoteNode.getAddr(), remoteNode.getState(), remoteNode.getIncarnation()));
        } else {
            //check if address is different. Accept if node was dead or left.
        }

        //if node == self and incarnation < current incarnation return
        //if node != self and incarnation <= current incarnation return.. except if we need to update because node was restarted.


    }

    void deadNode(PushNodeState remoteNode) {
        Node localNode = nodeMap.get(remoteNode.getName());
        if (localNode == null) {
            return;
        }

        if (remoteNode.getIncarnation() < localNode.incarnation()) {
            return;
        }
        //delete suspicion timers
        if (localNode.state().equals(NodeState.DEAD)) {
            return;
        }

        if (remoteNode.getName().equals(localNode.name())) {
            //refute
        } else {
            //broadcast ?? why
        }

        Node node = new Node(remoteNode.getName(), remoteNode.getAddr(), remoteNode.getState(), remoteNode.getIncarnation());
        nodeMap.put(node.name(), node);
    }

    void suspectNode(PushNodeState remoteNode) {
        Node localNode = nodeMap.get(remoteNode.getName());
        if (localNode == null) {
            return;
        }

        if (remoteNode.getIncarnation() < localNode.incarnation()) {
            return;
        }

        if (localNode.state() != NodeState.StateAlive) {
            return;
        }

        if (remoteNode.getName().equals(localNode.name())) {
            //refute
        } else {
            //broadcast ?? why
        }

        Node node = new Node(remoteNode.getName(), remoteNode.getAddr(), remoteNode.getState(), remoteNode.getIncarnation());
        nodeMap.put(node.name(), node);

        //setup suspicion timer.

    }

    private List<PushNodeState> getLocalState() {
        List<PushNodeState> nodeStates = new ArrayList<PushNodeState>();
        Set<String> inetAddressAndPorts = nodeMap.keySet();
        for (String name : inetAddressAndPorts) {
            Node node = nodeMap.get(name);
            nodeStates.add(new PushNodeState(node.name(), node.addr(), new byte[0], node.incarnation(), node.state()));
        }
        return nodeStates;
    }

    private List<PushNodeState> sendReceiveState(PushPullMessage pushPullMessag, InetAddressAndPort to) throws IOException {
        Socket socket = new Socket(to.address(), to.port());
        RequestOrResponse request = new RequestOrResponse((short) SwimGossipRequestType.PushPull.id, JsonSerDes.serialize(pushPullMessag), 0);
        RequestOrResponse response = new SocketIO<RequestOrResponse>(socket, RequestOrResponse.class).requestResponse(request);
        PushPullMessage responseMessage = JsonSerDes.deserialize(response.messageBodyJson(), PushPullMessage.class);
        return responseMessage.nodeStates;
    }
}
