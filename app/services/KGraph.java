package services;

import java.util.Map;

/**
 * Knowledge graph
 */
public interface KGraph extends KBase {
    String name ();
    long nodeCount ();
    long edgeCount ();
    KNode[] nodes ();
    KEdge[] edges ();
    KNode node (long id);
    KEdge edge (long id);
    KNode createNode (Map<String, Object> properties);
    KEdge createEdge (long source, long target,
                      boolean directed, Map<String, Object> properties);
    Blackboard blackboard ();
}
