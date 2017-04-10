package services.neo4j;

import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import play.Logger;

import services.KBase;

public class Neo4jKBase implements KBase {
    public static final String NAME_PROP = "_name";
    public static final String TYPE_PROP = "_type";
    
    protected final GraphDatabaseService graphDb;
    protected final Entity entity;
    protected final String type;
    protected final String name;

    public Neo4jKBase (Entity entity) {
        graphDb = entity.getGraphDatabase();
        
        try (Transaction tx = graphDb.beginTx()) {
            name = (String)entity.getProperty(NAME_PROP, null);
            type = (String)entity.getProperty(TYPE_PROP, null);
        }
        
        this.entity = entity;
    }

    public long id () {
        try (Transaction tx = graphDb.beginTx()) {
            return entity.getId();
        }
    }

    public String name () { return name; }
    public String type () { return type; }

    public void set (String name, Object value) {
        try (Transaction tx = graphDb.beginTx()) {
            entity.setProperty(name, value);
            tx.success();
        }
    }

    public Object get (String name) {
        try (Transaction tx = graphDb.beginTx()) {
            return entity.getProperty(name);
        }
    }
}
