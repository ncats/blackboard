package services;

public interface KEdge extends KBase {
    KNode source ();
    KNode target ();
    boolean directed ();
    KNode other (KNode node);
}
