package blackboard.neo4j;

import java.util.*;
import java.lang.reflect.Array;
import java.util.stream.Collectors;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.*;
import play.Logger;

import blackboard.KGraph;
import blackboard.Blackboard;
import blackboard.KNode;
import blackboard.KEdge;

public class Neo4jKGraph extends Neo4jKEntity implements KGraph {
    final Blackboard blackboard;
    final Label kgLabel;
    final RelationshipType kgType;
    final Index<Node> nodeIndex;
    final Index<Relationship> edgeIndex;
    
    public Neo4jKGraph (Blackboard blackboard, Node node) {
        this (blackboard, node, null);
    }
    
    public Neo4jKGraph (Blackboard blackboard,
                        Node node, Map<String, Object> properties) {
        super (node, properties);
        kgLabel = Label.label("KG:"+node.getId());
        kgType = RelationshipType.withName("KG:"+node.getId());
        nodeIndex = graphDb.index().forNodes(kgLabel.name());
        edgeIndex = graphDb.index().forRelationships(kgLabel.name());
        this.blackboard = blackboard;
    }
    
    public long getNodeCount () {
        try (Transaction tx = graphDb.beginTx()) {
            return graphDb.findNodes(kgLabel).stream().count();
        }
    }

    public long getEdgeCount () {
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

    public KNode[] getNodes () {
        try (Transaction tx = graphDb.beginTx()) {
            List<Neo4jKNode> nodes = graphDb.findNodes(kgLabel).stream()
                .map(n -> new Neo4jKNode (n))
                .collect(Collectors.toList());
            return nodes.toArray(new KNode[0]);
        }
    }

    public KEdge[] getEdges () {
        try (Transaction tx = graphDb.beginTx()) {
            List<Neo4jKEdge> edges = graphDb.getAllRelationships().stream()
                .filter(rel -> rel.isType(kgType))
                .map(rel -> new Neo4jKEdge
                     (rel, new Neo4jKNode (rel.getStartNode()),
                      new Neo4jKNode (rel.getEndNode())))
                .collect(Collectors.toList());
            return edges.toArray(new KEdge[0]);
        }
    }

    public KNode node (long id) {
        try (Transaction tx = graphDb.beginTx()) {
            Node n = graphDb.getNodeById(id);
            return new Neo4jKNode (n);
        }
        catch (NotFoundException ex) {
            Logger.warn("Unknown node: "+id);
        }
        return null;
    }

    public KEdge edge (long id) {
        try (Transaction tx = graphDb.beginTx()) {
            Relationship rel = graphDb.getRelationshipById(id);
            return new Neo4jKEdge (rel, new Neo4jKNode (rel.getStartNode()),
                                   new Neo4jKNode (rel.getEndNode()));
        }
        catch (NotFoundException ex) {
            Logger.warn("Unknown edge: "+id);
        }
        return null;
    }

     <T extends PropertyContainer> void index
                (Index<T> index, T e, Map<String, Object> props) {
         if (props != null) {
             for (Map.Entry<String, Object> me : props.entrySet()) {
                 Object val = me.getValue();
                 if (val.getClass().isArray()) {
                     int len = Array.getLength(val);
                     for (int i = 0; i < len; ++i)
                         index.add(e, me.getKey(), Array.get(val, i));
                 }
                 else 
                     index.add(e, me.getKey(), val);
             }
         }
    }

    public KNode createNode (Map<String, Object> properties) {
        KNode node = null;
        try (Transaction tx = graphDb.beginTx()) {
            Node n = graphDb.createNode(kgLabel);
            index (nodeIndex, n, properties);       
            node = new Neo4jKNode (n, properties);
            tx.success();
        }
        return node;
    }

    public KNode createNodeIfAbsent
        (Map<String, Object> properties, String key) {
        KNode node = null;
        if (properties.containsKey(key)) {
            Object val = properties.get(key);
            if (val == null) {
            }
            else {
                if (val.getClass().isArray())
                    Logger.warn("Key \""+key+"\" has multiple values!");
            
                try (Transaction tx = graphDb.beginTx();
                     IndexHits<Node> hits = nodeIndex.get(key, val)) {
                    if (hits.hasNext()) {
                        Node n = hits.next();
                        node = new Neo4jKNode (n);
                    }
                }
            }
        }

        if (node == null)
            node = createNode (properties);

        return node;
    }

    public KEdge createEdge (KNode source, KNode target,
                             Map<String, Object> properties) {
        KEdge edge = null;
        try (Transaction tx = graphDb.beginTx()) {
            Neo4jKNode s = (Neo4jKNode)source;
            Neo4jKNode t = (Neo4jKNode)target;
            Relationship rel =
                s.node().createRelationshipTo(t.node(), kgType);
            index (edgeIndex, rel, properties);     
            edge = new Neo4jKEdge (rel, s, t, properties);
            tx.success();
        }
        return edge;
    }

    public KEdge createEdgeIfAbsent (KNode source, KNode target) {
        try (Transaction tx = graphDb.beginTx()) {
            Neo4jKNode s = (Neo4jKNode)source;
            Neo4jKNode t = (Neo4jKNode)target;
            for (Relationship rel : s.node().getRelationships()) {
                Node other = rel.getOtherNode(s.node());
                if (other.equals(t.node())) {
                    return new Neo4jKEdge (rel, s, t);
                }
            }
        }
        
        return createEdge (source, target, null);
    }
    
    public KEdge createEdgeIfAbsent (KNode source, KNode target,
                                     Map<String, Object> properties,
                                     String key) {
        if (key == null || !properties.containsKey(key)
            || null == properties.get(key)) {
            return createEdge (source, target, properties);
        }
        
        Object val = properties.get(key);
        if (val.getClass().isArray())
            Logger.warn("Key \""+key+"\" has multiple values!");
        
        KEdge edge = null;
        try (Transaction tx = graphDb.beginTx();
             IndexHits<Relationship> hits = edgeIndex.get(key, val)) {
            if (hits.hasNext()) {
                Relationship rel = hits.next();
                Neo4jKNode s = (Neo4jKNode)source;
                Neo4jKNode t = (Neo4jKNode)target;
                if (rel.getStartNode().equals(s.node())
                    && rel.getEndNode().equals(t.node()))
                    edge = new Neo4jKEdge (rel, s, t);
            }
        }

        if (edge == null)
            edge = createEdge (source, target, properties);
        
        return edge;
    }
    
    public Blackboard blackboard () { return blackboard; }
}
