package blackboard;

import java.util.Map;
import java.util.function.Predicate;

/**
 * Knowledge graph
 */
public interface KGraph extends KEntity {
    long getNodeCount ();
    long getEdgeCount ();
    KNode[] getNodes ();
    KNode[] nodes (Predicate<KNode> predicate);
    KEdge[] getEdges ();
    KEdge[] edges (Predicate<KEdge> predicate);
    KNode node (long id);
    KEdge edge (long id);
    KNode createNode (Map<String, Object> properties);
    KNode createNodeIfAbsent (Map<String, Object> properties, String key);
    KEdge createEdge (KNode source, KNode target, String type,
                      Map<String, Object> properties);
    KEdge createEdgeIfAbsent (KNode source, KNode target, String type);
    KEdge createEdgeIfAbsent (KNode source, KNode target, String type,
                              Map<String, Object> properties,
                              String key);
    Blackboard blackboard ();
}
