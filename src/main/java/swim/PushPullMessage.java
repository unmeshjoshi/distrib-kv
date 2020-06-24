package swim;

import java.util.List;

public class PushPullMessage {
    List<PushNodeState> nodeStates;
    //For Jaxon
    private PushPullMessage(){}

    public PushPullMessage(List<PushNodeState> nodeStates) {
        this.nodeStates = nodeStates;
    }

    public List<PushNodeState> getNodeStates() {
        return nodeStates;
    }
}
