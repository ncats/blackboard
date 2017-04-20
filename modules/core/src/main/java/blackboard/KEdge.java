package blackboard;

public interface KEdge extends KEntity {
    default long getSource () { return source().getId(); }
    default long getTarget () { return target().getId(); }
    KNode source ();
    KNode target ();
    boolean isDirected ();
    KNode other (KNode node);
}
