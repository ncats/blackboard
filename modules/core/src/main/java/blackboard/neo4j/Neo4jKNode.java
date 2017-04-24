package blackboard.neo4j;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
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

    public KNode[] neighbors (String... tags) {
        try (Transaction tx = graphDb.beginTx()) {
            return StreamSupport
                .stream(node().getRelationships().spliterator(), false)
                .filter(rel -> {
                        if (tags == null || tags.length == 0)
                            return true;
                        for (String t : tags)
                            if (rel.getOtherNode(node())
                                .hasLabel(Label.label(t)))
                                return true;
                        return false;
                    })
                .map(rel -> new Neo4jKNode (rel.getOtherNode(node())))
                .collect(Collectors.toList())
                .toArray(new KNode[0]);
        }
    }

    public boolean hasNeighbors (String... tags) {
        KNode[] nodes = neighbors (tags);
        return nodes != null && nodes.length > 0;
    }

    public void addTag (String... tags) {
        try (Transaction tx = graphDb.beginTx()) {
            Node n = node ();
            for (String t : tags)
                n.addLabel(Label.label(t));
            tx.success();
        }
    }

    public String[] getTags () {
        List<String> tags = new ArrayList<>();
        try (Transaction tx = graphDb.beginTx()) {
            Node n = node ();
            for (Label l : n.getLabels())
                tags.add(l.name());
        }
        return tags.toArray(new String[0]);
    }

    public boolean hasTag (String tag) {
        try (Transaction tx = graphDb.beginTx()) {
            return node().hasLabel(Label.label(tag));
        }
    }
}
