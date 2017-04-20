package blackboard.neo4j;

import java.util.Map;
import java.util.stream.*;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import blackboard.KEdge;
import blackboard.KNode;

public class Neo4jKEdge extends Neo4jKEntity implements KEdge {
    protected final Neo4jKNode source, target;
    
    public Neo4jKEdge (Relationship edge,
                       Neo4jKNode source, Neo4jKNode target) {
        this (edge, source, target, null);
    }
    
    public Neo4jKEdge (Relationship edge, Neo4jKNode source, Neo4jKNode target,
                       Map<String, Object> properties) {
        super (edge, properties);
        this.source = source;
        this.target = target;
    }

    public KNode source () { return source; }
    public KNode target () { return target; }
    public KNode other (KNode node) {
        if (node == target) return source;
        if (node == source) return target;
        return null;
    }

    protected Relationship edge () { return (Relationship)entity; }
    
    public boolean isDirected () {
        try (Transaction tx = graphDb.beginTx()) {
            return (Boolean)edge().getProperty(DIRECTED_P, false);
        }
    }
}
