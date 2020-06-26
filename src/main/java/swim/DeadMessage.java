package swim;

public class DeadMessage {
    Node node;

    private DeadMessage() {
    }

    public DeadMessage(Node node) {
        this.node = node;
    }

    public Node getNode() {
        return node;
    }
}
