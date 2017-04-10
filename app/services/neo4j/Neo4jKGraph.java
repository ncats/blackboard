package services.neo4j;

import java.util.*;
import org.neo4j.graphdb.*;
import play.Logger;

import services.KGraph;
import services.Blackboard;
import services.KNode;
import services.KEdge;

public class Neo4jKGraph extends Neo4jKBase implements KGraph {
    final Blackboard blackboard;
    final Label kgLabel;
    
    public Neo4jKGraph (Blackboard blackboard, Node node) {
        super (node);
        kgLabel = Label.label("KG:"+node.getId());
        this.blackboard = blackboard;
    }
    
    public long nodeCount () {
        try (Transaction tx = graphDb.beginTx()) {
            return graphDb.findNodes(kgLabel).stream().count();
        }
    }

    public long edgeCount () {
        try (Transaction tx = graphDb.beginTx();
             Result result = graphDb.execute("match(n:`"+kgLabel.name()
                                             +"`)-[e]-(m:`"+kgLabel.name()
                                             +"`) return count(e) as COUNT")) {
            if (result.hasNext()) {
                Number n = (Number)result.next().get("COUNT");
                return n.longValue();
            }
        }
        return -1l;
    }

    public KNode[] nodes () {
        return null;
    }

    public KEdge[] edges () {
        return null;
    }

    public KNode node (long id) {
        return null;
    }

    public KEdge edge (long id) {
        return null;
    }

    public KNode createNode (Map<String, Object> properties) {
        return null;
    }

    public KEdge createEdge (long source, long target, boolean directed,
                             Map<String, Object> properties) {
        return null;
    }
    
    public Blackboard blackboard () { return blackboard; }
}
