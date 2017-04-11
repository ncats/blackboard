package services;

import java.util.Map;

/**
 * Knowledge graph
 */
public interface KGraph extends KBase {
    String getName ();
    long getNodeCount ();
    long getEdgeCount ();
    KNode[] getNodes ();
    KEdge[] getEdges ();
    KNode node (long id);
    KEdge edge (long id);
    KNode createNode (Map<String, Object> properties);
    KEdge createEdge (long source, long target,
                      boolean directed, Map<String, Object> properties);
    Blackboard blackboard ();
}
