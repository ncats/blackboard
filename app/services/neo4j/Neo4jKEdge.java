package services.neo4j;

import java.util.stream.*;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import services.KEdge;
import services.KNode;

public class Neo4jKEdge extends Neo4jKBase implements KEdge {
    public final String DIRECTED_PROP = "_directed";
    
    protected final Relationship edge;
    protected final KNode source, target;
    protected final boolean directed;

    public Neo4jKEdge (Relationship edge) {
        super (edge);
        source = new Neo4jKNode (edge.getStartNode());
        target = new Neo4jKNode (edge.getEndNode());
        directed = (Boolean)edge.getProperty(DIRECTED_PROP, false);
        this.edge = edge;
    }

    public KNode getSource () { return source; }
    public KNode getTarget () { return target; }
    public KNode other (KNode node) {
        if (node == target) return source;
        if (node == source) return target;
        return null;
    }
    public boolean isDirected () { return directed; }
}
