package swim;

public class AliveMessage {
    Node node;

    private AliveMessage() {
    }

    public AliveMessage(Node node) {
        this.node = node;
    }

    public Node getNode() {
        return node;
    }
}
