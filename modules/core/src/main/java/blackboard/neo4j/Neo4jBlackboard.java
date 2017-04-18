package blackboard.neo4j;

import java.io.*;
import java.util.*;
import javax.inject.*;
import java.util.stream.Collectors;

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
import play.inject.Injector;

import blackboard.Blackboard;
import blackboard.KGraph;
import static blackboard.KEntity.*;

@Singleton
public class Neo4jBlackboard implements Blackboard {
    static public final Label KGRAPH_LABEL = Label.label("KGraph");
    static public final Label KSOURCE_LABEL = Label.label("KSource");

    protected final GraphDatabaseService graphDb;
    protected final Configuration config;
    protected final Set<String> nodeTypes;
    protected final Set<String> edgeTypes;
    protected final Set<String> evidenceTypes;

    @Inject
    public Neo4jBlackboard (Configuration config,
                            ApplicationLifecycle lifecycle) throws IOException {
        String param = config.getString("blackboard.base", ".");
        File base = new File (param);
        base.mkdirs();

        File dir = new File (base, "blackboard.db");
        dir.mkdirs();

        nodeTypes = new TreeSet<>(config.getStringList
                                  ("blackboard.node.type", new ArrayList<>())); 
        edgeTypes = new TreeSet<>(config.getStringList
                                  ("blackboard.edge.type", new ArrayList<>()));
        evidenceTypes = new TreeSet<>(config.getStringList
                                      ("blackboard.evidence.type",
                                       new ArrayList<>()));
        
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dir)
            .setConfig(GraphDatabaseSettings.dump_configuration, "true")
            .newGraphDatabase();

        lifecycle.addStopHook(() -> {
                shutdown ();
                return F.Promise.pure(null);
            });
        this.config = config;
        
        initBlackboard ();
    }

    protected void initBlackboard () {
        long created;
        try (Transaction tx = graphDb.beginTx()) {
            Node meta = graphDb.getNodeById(0l);
            created = (Long)meta.getProperty("created", 0l);
        }
        catch (NotFoundException ex) {
            created = System.currentTimeMillis();
            try (Transaction tx = graphDb.beginTx()) {
                Node meta = graphDb.createNode(Label.label("Blackboard"));
                meta.setProperty("created", created);
                tx.success();
            }
        }
        Logger.debug("## Blackboard initialized; created = "
                     +new java.util.Date(created));
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
            List<KGraph> kgraphs = graphDb.findNodes(KGRAPH_LABEL)
                .stream().map(n -> (KGraph)new Neo4jKGraph
                              (Neo4jBlackboard.this, n))
                .collect(Collectors.toList());
            return kgraphs.iterator();
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

    protected Node createNode (Label label, String type,
                               Map<String, Object> properties) {
        Node node = graphDb.createNode(label);
        node.setProperty(TYPE_P, type);
        if (properties.containsKey(NAME_P))
            node.setProperty(NAME_P, properties.get(NAME_P));
        return node;
    }
    
    public KGraph createKGraph (Map<String, Object> properties) {
        Neo4jKGraph kg = null;
        try (Transaction tx = graphDb.beginTx()) {
            /*
             * every knowledge graph has a meta-node of type=kgraph and
             * label=KGRAPH_LABEL
             */
            Node node = createNode (KGRAPH_LABEL, "kgraph", properties);
            kg = new Neo4jKGraph (this, node);
            kg.createNode(properties);
            tx.success();
        }
        return kg;
    }
    
    public Collection<String> getNodeTypes () { return nodeTypes; }
    public Collection<String> getEdgeTypes () { return edgeTypes; }
    public Collection<String> getEvidenceTypes () { return evidenceTypes; }
}
