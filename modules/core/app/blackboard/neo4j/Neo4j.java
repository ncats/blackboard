package blackboard.neo4j;

import java.io.File;
import java.util.Map;
import java.util.Date;
import java.util.HashMap;
import java.util.TreeMap;

import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.event.*;
import org.neo4j.graphdb.index.*;

import play.Logger;


/**
 * Neo4j base class 
 */
public class Neo4j extends TransactionEventHandler.Adapter {
    protected final GraphDatabaseService gdb;
    protected final File dbdir;
    protected final Map<String, String> indexConfig = new HashMap<>();
    protected final Label META_LABEL;
    protected final Long metanode;

    public Neo4j (File dbdir) {
        indexConfig.put(IndexManager.PROVIDER, "lucene");
        indexConfig.put("type", "fulltext");
        indexConfig.put("to_lower_case", "true");
        
        gdb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbdir)
            .newGraphDatabase();
        META_LABEL = Label.label(getDbName());

        // initialize database..
        try (Transaction tx = gdb.beginTx()) {
            try (ResourceIterator<Node> it = gdb.findNodes(META_LABEL)) {
                Node node;
                if (it.hasNext()) {
                    node = it.next();
                    Logger.debug("## database "+dbdir+" initialized..."
                                 +new Date ((Long)node.getProperty("created")));
                }
                else {
                    node = gdb.createNode(META_LABEL);
                    node.setProperty("created", System.currentTimeMillis());
                    Logger.debug("## database for "+getDbName ()
                                 +" initialized..."+dbdir);
                }
                metanode = node.getId();                
            }
            tx.success();
        }
        gdb.registerTransactionEventHandler(this);
        
        this.dbdir = dbdir;
    }

    @Override
    public void afterCommit (TransactionData data, Object state) {
    }

    /*
     * should override as appropriate
     */
    protected String getDbName () {
        return getClass().getName();
    }

    protected Node getMetaNode () {
        try (Transaction tx = gdb.beginTx()) {
            Node n = gdb.getNodeById(metanode);
            tx.success();
            return n;
        }
    }
    
    protected Index<Node> getNodeIndex (String name) {
        return gdb.index().forNodes(getDbName()+"."+name);
    }

    protected Index<Node> getTextIndex () {
        return gdb.index().forNodes(getDbName()+".text", indexConfig);
    }

    protected Index<Node> addTextIndex (Node node, String text) {
        Index<Node> index = getTextIndex ();
        if (text != null)
            index.add(node, "text", text);
        else
            index.remove(node, "text", text);
        return index;
    }
    
    public GraphDatabaseService getGraphDb () { return gdb; }
    public void shutdown () throws Exception {
        gdb.shutdown();
    }
    public Date getLastUpdate () {
        Long updated = null;
        try (Transaction tx = gdb.beginTx()) {
            Node node = gdb.getNodeById(metanode);
            if (node.hasProperty("updated"))
                updated = (Long)node.getProperty("updated");
            tx.success();
        }
        return updated != null ? new Date(updated) : null;
    }
    public File getDbFile () { return dbdir; }
    
    public static void setProperties
        (Entity entity, Map<String, Object> properties) {
        try (Transaction tx = entity.getGraphDatabase().beginTx()) {
            for (Map.Entry<String, Object> me : properties.entrySet())
                entity.setProperty(me.getKey(), me.getValue());
            tx.success();
        }
    }

    public static Map<String, Object> getProperties (Entity entity) {
        Map<String, Object> props = new TreeMap<>();    
        try (Transaction tx = entity.getGraphDatabase().beginTx()) {
            for (Map.Entry<String, Object> me :
                     entity.getAllProperties().entrySet()) {
                if (me.getKey().charAt(0) != '_')
                    props.put(me.getKey(), me.getValue());
            }
        }
        return props;
    }
}
