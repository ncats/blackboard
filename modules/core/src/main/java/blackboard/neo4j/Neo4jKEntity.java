package blackboard.neo4j;

import java.util.Map;
import java.util.TreeMap;
import java.util.Collection;
import java.util.function.Supplier;

import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import play.Logger;

import blackboard.KEntity;

public class Neo4jKEntity implements KEntity {    
    protected final GraphDatabaseService graphDb;
    protected final Entity entity;

    public Neo4jKEntity (Entity entity) {
        this (entity, null);
    }
    
    public Neo4jKEntity (Entity entity, Map<String, Object> properties) {
        graphDb = entity.getGraphDatabase();
        if (properties != null)
            Neo4j.setProperties(entity, properties);
        
        this.entity = entity;
    }

    public long getId () {
        try (Transaction tx = graphDb.beginTx()) {
            return entity.getId();
        }
    }

    public long getCreated () {
        try (Transaction tx = graphDb.beginTx()) {
            return (Long)entity.getProperty(CREATED_P, 0l);
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

    public void put (String name, Object value) {
        try (Transaction tx = graphDb.beginTx()) {
            entity.setProperty(name, value);
            tx.success();
        }
    }

    public void putAll (Map<String, Object> properties) {
        try (Transaction tx = graphDb.beginTx()) {
            for (Map.Entry<String, Object> me : properties.entrySet())
                entity.setProperty(me.getKey(), me.getValue());
            tx.success();
        }
    }

    public void putIfAbsent (String name, Supplier supplier) {
        try (Transaction tx = graphDb.beginTx()) {
            if (!entity.hasProperty(name)) {
                entity.setProperty(name, supplier.get());
                tx.success();
            }
        }
    }

    public Object get (String name) {
        try (Transaction tx = graphDb.beginTx()) {
            return entity.getProperty(name, null);
        }
    }

    public Map<String, Object> getProperties () {
        return Neo4j.getProperties(entity);
    }
}
