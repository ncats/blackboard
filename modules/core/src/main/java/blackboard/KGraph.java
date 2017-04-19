package blackboard;

import java.util.Map;

/**
 * Knowledge graph
 */
public interface KGraph extends KEntity {
    long getNodeCount ();
    long getEdgeCount ();
    KNode[] getNodes ();
    KEdge[] getEdges ();
    KNode node (long id);
    KEdge edge (long id);
    KNode createNode (Map<String, Object> properties);
    KNode createNodeIfAbsent (Map<String, Object> properties, String key);
    KEdge createEdge (long source, long target,
                      boolean directed, Map<String, Object> properties);
    Blackboard blackboard ();
}
