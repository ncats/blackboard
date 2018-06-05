package blackboard.neo4j;

import java.util.Map;
import java.util.TreeMap;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import play.Logger;


/**
 * Neo4j utilities
 */
public class Neo4j {
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
