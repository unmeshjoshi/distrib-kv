package swim;

import org.dist.kvstore.network.InetAddressAndPort;

class PushNodeState {
    String name;
    InetAddressAndPort addr;
    byte[] meta;
    int incarnation;
    NodeState state;

    public PushNodeState(String name, InetAddressAndPort addr, byte[] meta, int incarnation, NodeState state) {
        this.name = name;
        this.addr = addr;
        this.meta = meta;
        this.incarnation = incarnation;
        this.state = state;
    }

    public String getName() {
        return name;
    }

    public InetAddressAndPort getAddr() {
        return addr;
    }

    public byte[] getMeta() {
        return meta;
    }

    public int getIncarnation() {
        return incarnation;
    }

    public NodeState getState() {
        return state;
    }
}
