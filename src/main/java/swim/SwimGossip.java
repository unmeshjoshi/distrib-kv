package swim;

import org.apache.log4j.Logger;
import org.dist.kvstore.gossip.failuredetector.TcpListener;
import org.dist.kvstore.network.*;

import java.io.IOException;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

enum SwimGossipRequestType {
    PushPull(1),
    Ping(2),
    Alive(3),
    Dead(4);

    short id;

    SwimGossipRequestType(int id) {
        this.id = (short) id;
    }
}

class GossipTask implements Runnable {
    private static Logger logger = Logger.getLogger(GossipTask.class);
    private BlockingQueue<RequestOrResponse> broadcasts;
    private SwimGossip swimGossip;

    public GossipTask(BlockingQueue<RequestOrResponse> broadcasts, SwimGossip swimGossip) {
        this.broadcasts = broadcasts;
        this.swimGossip = swimGossip;
    }

    @Override
    public void run() {
        logger.info("Running Gossip Task");
        try {
            RequestOrResponse request = broadcasts.take();
            swimGossip.broadcast(request);
        } catch (InterruptedException e) {

        }
    }
}

class ProbeTask implements Runnable {
    private static Logger logger = Logger.getLogger(ProbeTask.class);

    AtomicInteger sequenceNumber = new AtomicInteger(0);
    private final SwimGossip swimGossip;
    int probeIndex;

    public ProbeTask(SwimGossip swimGossip) {
        this.swimGossip = swimGossip;
    }

    @Override
    public void run() {
        List<Node> aliveNodes = swimGossip.getAliveNodes();
        logger.info("alive nodes " + aliveNodes.size() + " in " + swimGossip.getName());
        if (aliveNodes.size() == 0) {
            return;
        }

        if (probeIndex == aliveNodes.size()) {
            probeIndex = 0;
        }

        Node node = aliveNodes.get(probeIndex);
        logger.info("Probing " + node + " from " + swimGossip.getName());
        try {
            RequestOrResponse pingRequest = new RequestOrResponse(SwimGossipRequestType.Ping.id, "", sequenceNumber.incrementAndGet());
            int probeTimeout = 500;
            RequestOrResponse requestOrResponse = swimGossip.sendMessage(node.addr(), pingRequest, probeTimeout);
            logger.info("Got  " + requestOrResponse.messageBodyJson() + " from " + node.name());
        } catch (IOException e) {
            //not implementing suspicion for now.
            Node deadNode = new Node(node.name(), node.addr(), NodeState.DEAD, node.incarnation());
            swimGossip.deadNode(deadNode);
        }
        probeIndex++;
    }
}

public class SwimGossip {
    private static Logger logger = Logger.getLogger(SwimGossip.class);

    private final InetAddressAndPort seed;
    private String name;
    private final InetAddressAndPort listenAddress;
    final TcpListener tcpListener;
    HashMap<String, Node> nodeMap = new HashMap<String, Node>();
    ScheduledThreadPoolExecutor gossipExecutor = new ScheduledThreadPoolExecutor(1);
    ScheduledFuture taskFuture;
    BlockingQueue<RequestOrResponse> broadcastMessages = new ArrayBlockingQueue<RequestOrResponse>(100);

    ScheduledThreadPoolExecutor probeExecutor = new ScheduledThreadPoolExecutor(1);
    ScheduledFuture probeTaskFuture;
    private AtomicInteger correlationId = new AtomicInteger();

    public SwimGossip(InetAddressAndPort seed, String name, InetAddressAndPort listenAddress) {
        this.seed = seed;
        this.name = name;
        this.listenAddress = listenAddress;
        nodeMap.put(name, new Node(name, listenAddress, NodeState.StateAlive, 1));
        this.tcpListener = new TcpListener(listenAddress, this::handle);
    }

    public void start() {
        tcpListener.start();
        int gossipIntervalMs = 200;
        int probeIntervalMs = 1000;

        taskFuture = gossipExecutor.scheduleAtFixedRate(new GossipTask(broadcastMessages, this), gossipIntervalMs, gossipIntervalMs, TimeUnit.MILLISECONDS);
        probeTaskFuture = probeExecutor.scheduleAtFixedRate(new ProbeTask(this), probeIntervalMs, probeIntervalMs, TimeUnit.MILLISECONDS);
    }

    private RequestOrResponse handle(RequestOrResponse request) {
        if (request.requestId() == SwimGossipRequestType.PushPull.id) {
            PushPullMessage pushPul = JsonSerDes.deserialize(request.messageBodyJson(), PushPullMessage.class);
            PushPullMessage response = new PushPullMessage(this.getLocalState());

            this.mergeStates(pushPul.getNodeStates());

            return new RequestOrResponse((short) SwimGossipRequestType.PushPull.id, JsonSerDes.serialize(response), request.correlationId());

        } else if (request.requestId() == SwimGossipRequestType.Alive.id) {
            AliveMessage aliveMessage = JsonSerDes.deserialize(request.messageBodyJson(), AliveMessage.class);
            aliveNode(aliveMessage.getNode());

        } else if (request.requestId() == SwimGossipRequestType.Dead.id) {
            DeadMessage deadMessage = JsonSerDes.deserialize(request.messageBodyJson(), DeadMessage.class);
            deadNode(deadMessage.getNode());
        } else if (request.requestId() == SwimGossipRequestType.Ping.id) {
            return new RequestOrResponse(SwimGossipRequestType.Ping.id, "ACK", 0);
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
                aliveNode(new Node(remoteState.getName(), remoteState.getAddr(), remoteState.getState(), remoteState.getIncarnation()));
            } else if (remoteState.state == NodeState.SUSPECTED) {
                suspectNode(remoteState);
            } else if (remoteState.state == NodeState.DEAD) {
                deadNode(new Node(remoteState.getName(), remoteState.getAddr(), remoteState.getState(), remoteState.getIncarnation()));
            }
        }
    }

    void aliveNode(Node node) {
        lock.writeLock().lock();
        try {
            Node localNode = nodeMap.get(node.name());
            if (localNode == null) {
                logger.info("Marking node " + node.name() + " alive in " + this.name);
                nodeMap.put(node.name(), new Node(node.name(), node.addr(), NodeState.StateAlive, node.incarnation()));
                //broadcast
                AliveMessage aliveMessage = new AliveMessage(node);
                broadcastMessages.add(new RequestOrResponse(SwimGossipRequestType.Alive.id, JsonSerDes.serialize(aliveMessage), correlationId.incrementAndGet()));
            } else {
                //check if address is different. Accept if node was dead or left.
            }

            //if node == self and incarnation < current incarnation return
            //if node != self and incarnation <= current incarnation return.. except if we need to update because node was restarted.

        } finally {
            lock.writeLock().unlock();
        }

    }

    void deadNode(Node node) {
        lock.writeLock().lock();
        try {
            Node localNode = nodeMap.get(node.name());
            if (localNode == null) {
                return;
            }

            if (node.incarnation() < localNode.incarnation()) {
                return;
            }
            //delete suspicion timers
            if (localNode.state().equals(NodeState.DEAD)) {
                return;
            }

            if (node.name().equals(localNode.name())) {
                //refute
            } else {
                Node newNodeState = new Node(node.name(), node.addr(), NodeState.DEAD, node.incarnation());
                nodeMap.put(node.name(), newNodeState);
                //broadcast
                DeadMessage deadMessage = new DeadMessage(newNodeState);
                RequestOrResponse message = new RequestOrResponse(SwimGossipRequestType.Dead.id, JsonSerDes.serialize(deadMessage), correlationId.incrementAndGet());
                broadcastMessages.add(message);

            }
            logger.info("Marking node " + node.name() + " dead in " + this.name);
            nodeMap.put(node.name(), node);
        } finally {
            lock.writeLock().unlock();
        }
    }

    void suspectNode(PushNodeState remoteNode) {
        lock.writeLock().lock();
        try {
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
        } finally {
            lock.writeLock().unlock();
        }
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
        RequestOrResponse request = new RequestOrResponse((short) SwimGossipRequestType.PushPull.id, JsonSerDes.serialize(pushPullMessag), 0);
        RequestOrResponse response = sendMessage(to, request, 5000);
        PushPullMessage responseMessage = JsonSerDes.deserialize(response.messageBodyJson(), PushPullMessage.class);
        return responseMessage.nodeStates;
    }

    RequestOrResponse sendMessage(InetAddressAndPort to, RequestOrResponse request, int timeout) throws IOException {
        Socket socket = new Socket(to.address(), to.port());
        return new SocketIO<RequestOrResponse>(socket, RequestOrResponse.class, timeout).requestResponse(request);
    }

    Random random = new Random();
    int fanOut = 3;
    public void broadcast(RequestOrResponse request) {

        List<Node> liveNodes = getAliveNodes();
        List<Node> kRandomNodes = new ArrayList<>();
        int randomNodeCount = liveNodes.size() < fanOut ? liveNodes.size():fanOut;
        for (int i = 0; i < randomNodeCount; i++) {
            int index = random.nextInt(liveNodes.size());
            kRandomNodes.add(liveNodes.get(index));
        }
        logger.info("broadcasting " + request + " to " + kRandomNodes);
        sendMessageOneWay(kRandomNodes, request);
    }

    ReadWriteLock lock = new ReentrantReadWriteLock();
    List<Node> getAliveNodes() {
        lock.readLock().lock();
        try {
            return nodeMap.values().stream().filter(node -> node.state() == NodeState.StateAlive && node.name() != this.name).collect(Collectors.toList());
        } finally {
            lock.readLock().unlock();
        }
    }

    void sendMessageOneWay(List<Node> kRandomNodes, RequestOrResponse request) {
        for (Node kRandomNode : kRandomNodes) {
            InetAddressAndPort to = kRandomNode.addr();
            try {
                Socket socket = new Socket(to.address(), to.port());
                new SocketIO<RequestOrResponse>(socket, RequestOrResponse.class, 5000).write(request);
            } catch (IOException e) {
                logger.error(e);
            }
        }

    }

    public void stop() {
        tcpListener.shudown();
        taskFuture.cancel(true);
        probeTaskFuture.cancel(true);
    }

    public String getName() {
        return this.name;
    }
}
