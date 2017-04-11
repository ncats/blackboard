package services;

public interface KEdge extends KBase {
    KNode getSource ();
    KNode getTarget ();
    boolean isDirected ();
    KNode other (KNode node);
}
