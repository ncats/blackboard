package blackboard.neo4j;

import java.util.Map;
import java.util.stream.*;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import blackboard.KNode;

public class Neo4jKNode extends Neo4jKEntity implements KNode {
    public Neo4jKNode (Node node) {
        this (node, null);
    }
        
    public Neo4jKNode (Node node, Map<String, Object> properties) {
        super (node, properties);
    }

    protected Node node () { return (Node)entity; }

    public int getDegree () {
        try (Transaction tx = graphDb.beginTx()) {
            return node().getDegree();
        }
    }

    public int getInDegree () {
        try (Transaction tx = graphDb.beginTx()) {
            return node().getDegree(Direction.INCOMING);
        }
    }

    public int getOutDegree () {
        try (Transaction tx = graphDb.beginTx()) {
            return node().getDegree(Direction.OUTGOING);
        }
    }

    public KNode[] neighbors () {
        try (Transaction tx = graphDb.beginTx()) {
            return StreamSupport
                .stream(node().getRelationships().spliterator(), false)
                .map(rel -> new Neo4jKNode (rel.getOtherNode(node())))
                .collect(Collectors.toList())
                .toArray(new KNode[0]);
        }
    }

    public void addTag (String... tags) {
        try (Transaction tx = graphDb.beginTx()) {
            Node n = node ();
            for (String t : tags)
                n.addLabel(Label.label(t));
            tx.success();
        }
    }
}
