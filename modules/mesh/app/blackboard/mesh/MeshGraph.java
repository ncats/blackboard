package blackboard.mesh;

import java.io.*;
import java.util.*;
import java.lang.reflect.Array;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.event.*;
import org.neo4j.graphdb.index.*;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexCreator;

public class MeshGraph implements AutoCloseable {
    static final Logger logger = Logger.getLogger(MeshGraph.class.getName());
    static final String INDEX_LABEL = "MSH"; // MeSH source

    final GraphDatabaseService gdb;
    final File dbdir;

    class MeshNodeImpl implements MeshNode {
        final Node node;
        MeshNodeImpl (Node node) {
            
            this.node = node;
        }
    }

    public MeshGraph (File dbdir) throws IOException {
        gdb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbdir)
            .setConfig(GraphDatabaseSettings.read_only, "true")
            .newGraphDatabase();

        logger.info("## "+dbdir+" mesh database initialized...");
        this.dbdir = dbdir;
    }

    Index<Node> nodeIndex () {
        return gdb.index().forNodes(INDEX_LABEL);
    }

    public MeshNode getNode (String ui) {
        MeshNode node = null;
        try (Transaction tx = gdb.beginTx();
             IndexHits<Node> hits = nodeIndex().get("ui", ui)) {
            if (hits.hasNext()) {
                node = new MeshNodeImpl (hits.next());
            }
            tx.success();
        }
        return node;
    }

    public void close () throws Exception {
        logger.info("## shutting down MeshGraph instance...");
        gdb.shutdown();
    }
}
