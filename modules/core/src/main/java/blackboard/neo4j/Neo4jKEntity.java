package blackboard.neo4j;

import java.util.Map;
import java.util.TreeMap;
import java.util.Collection;

import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import play.Logger;

import blackboard.KEntity;

public class Neo4jKEntity implements KEntity {    
    protected final GraphDatabaseService graphDb;
    protected final Entity entity;
    protected final String type;
    protected final String name;

    public Neo4jKEntity (Entity entity) {
        this (entity, null);
    }
    
    public Neo4jKEntity (Entity entity, Map<String, Object> properties) {
        graphDb = entity.getGraphDatabase();
        name = (String)entity.getProperty(NAME_P, null);
        type = (String)entity.getProperty(TYPE_P, null);
        if (properties != null)
            Neo4j.setProperties(entity, properties);
        
        this.entity = entity;
    }

    public long getId () {
        try (Transaction tx = graphDb.beginTx()) {
            return entity.getId();
        }
    }

    public String getName () {
        try (Transaction tx = graphDb.beginTx()) {
            return (String) entity.getProperty(NAME_P, null);
        }
    }
    
    public String getType () {
        try (Transaction tx = graphDb.beginTx()) {
            return (String) entity.getProperty(TYPE_P, null);
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

    public Map<String, Object> getProperties () {
        return Neo4j.getProperties(entity);
    }
}
