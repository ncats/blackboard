package services.neo4j;

import java.io.*;
import java.util.*;
import javax.inject.*;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.event.*;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexCreator;

import play.Logger;
import play.Configuration;
import play.cache.CacheApi;
import play.inject.ApplicationLifecycle;
import play.libs.F;

import services.Blackboard;
import services.KGraph;


@Singleton
public class Neo4jBlackboard implements Blackboard {
    static public final Label KGRAPH_LABEL = Label.label("KGraphNode");

    protected GraphDatabaseService graphDb;
    protected Configuration config;
    protected Set<String> types;

    @Inject
    public Neo4jBlackboard (Configuration config,
                            ApplicationLifecycle lifecycle) throws IOException {
        String param = config.getString("blackboard.base", ".");
        File base = new File (param);
        base.mkdirs();

        File dir = new File (base, "blackboard.db");
        dir.mkdirs();

        types = new TreeSet<>(config.getStringList
                              ("blackboard.types", new ArrayList<>()));
        
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dir)
            .setConfig(GraphDatabaseSettings.dump_configuration, "true")
            .newGraphDatabase();

        lifecycle.addStopHook(() -> {
                shutdown ();
                return F.Promise.pure(null);
            });
        this.config = config;   
    }

    protected void shutdown () {
        Logger.debug(getClass().getName()+": shutting down");
        graphDb.shutdown();
    }

    public KGraph getKGraph (long id) {
        KGraph kg = null;
        try (Transaction tx = graphDb.beginTx()) {
            Node node = graphDb.getNodeById(id);
            if (node.hasLabel(KGRAPH_LABEL)) {
                kg = new Neo4jKGraph (this, node);
                tx.success();
            }
        }
        catch (NotFoundException ex) {
            Logger.warn("Node "+id+" not found");
        }
        return kg;
    }

    public Iterator<KGraph> iterator () {
        try (Transaction tx = graphDb.beginTx()) {
            return graphDb.findNodes(KGRAPH_LABEL)
                .stream().map(n -> (KGraph)new Neo4jKGraph
                              (Neo4jBlackboard.this, n))
                .iterator();
        }
    }

    public long _getKGraphCount () {
        try (Transaction tx = graphDb.beginTx();
             Result result = graphDb.execute("match(n:`"+KGRAPH_LABEL.name()
                                             +"`) return count(n) as COUNT")) {
            if (result.hasNext()) {
                Map<String, Object> row = result.next();
                Number n = (Number)row.get("COUNT");
                return n.longValue();
            }
        }
        catch (QueryExecutionException ex) {
            Logger.error("Can't execute count query", ex);
        }
        return -1;
    }

    public long getKGraphCount () {
        try (Transaction tx = graphDb.beginTx()) {
            return graphDb.findNodes(KGRAPH_LABEL).stream().count();
        }
    }

    public KGraph createKGraph (String name, Map<String, Object> properties) {
        KGraph kg = null;
        try (Transaction tx = graphDb.beginTx()) {
            Node node = graphDb.createNode(KGRAPH_LABEL);
            node.setProperty(Neo4jKBase.NAME_PROP, name);
            // this should be an enum of some sort here
            node.setProperty(Neo4jKBase.TYPE_PROP, "kgraph");
            if (properties != null) {
                for (Map.Entry<String, Object> me : properties.entrySet())
                    node.setProperty(me.getKey(), me.getValue());
            }
            kg = new Neo4jKGraph (this, node);
            tx.success();
        }
        return kg;
    }

    public Collection<String> getTypes () {
        return types;
    }
}
