package blackboard;

public interface KEdge extends KEntity {
    KNode getSource ();
    KNode getTarget ();
    boolean isDirected ();
    KNode other (KNode node);
}
