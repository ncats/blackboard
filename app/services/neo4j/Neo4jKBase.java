package services.neo4j;

import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import play.Logger;

import services.KBase;

public class Neo4jKBase implements KBase {
    protected final GraphDatabaseService graphDb;
    protected final Entity entity;

    public Neo4jKBase (Entity entity) {
        graphDb = entity.getGraphDatabase();
        this.entity = entity;
    }

    public long id () {
        try (Transaction tx = graphDb.beginTx()) {
            return entity.getId();
        }
    }

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
