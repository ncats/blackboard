package services.neo4j;

import java.util.stream.*;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import services.KNode;

public class Neo4jKNode extends Neo4jKBase implements KNode {
    protected final Node node;
    
    public Neo4jKNode (Node node) {
        super (node);
        this.node = node;
    }

    public int getDegree () {
        try (Transaction tx = graphDb.beginTx()) {
            return node.getDegree();
        }
    }

    public int getInDegree () {
        try (Transaction tx = graphDb.beginTx()) {
            return node.getDegree(Direction.INCOMING);
        }
    }

    public int getOutDegree () {
        try (Transaction tx = graphDb.beginTx()) {
            return node.getDegree(Direction.OUTGOING);
        }
    }

    public KNode[] neighbors () {
        try (Transaction tx = graphDb.beginTx()) {
            return StreamSupport
                .stream(node.getRelationships().spliterator(), false)
                .map(rel -> new Neo4jKNode (rel.getOtherNode(node)))
                .collect(Collectors.toList())
                .toArray(new KNode[0]);
        }
    }
}
