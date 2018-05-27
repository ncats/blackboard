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

import static blackboard.mesh.MeshParser.*;

/*
 * sbt mesh/"run-main blackboard.mesh.BuildMeshGraph OUTDIR INDIR"
 */
public class BuildMeshGraph implements Mesh, AutoCloseable {
    static final Logger logger =
        Logger.getLogger(BuildMeshGraph.class.getName());

    static final String INDEX_LABEL = "MSH"; // MeSH source

    interface NodeInstr<T extends Entry> {
        void instrument (Index<Node> index, Node node, T entry);
    }

    GraphDatabaseService gdb;
    File indir;
    File outdir;

    public BuildMeshGraph (File outdir, File indir) throws IOException {
        this.indir = indir;
        this.outdir = outdir;

        outdir.mkdirs();
        logger.info("Initializing "+outdir+"...");
        
        gdb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(outdir)
            .setConfig(GraphDatabaseSettings.dump_configuration, "true")
            .newGraphDatabase();
        //gdb.registerTransactionEventHandler(this);
    }

    Index<Node> nodeIndex () {
        return gdb.index().forNodes(INDEX_LABEL);
    }

    public void build () throws Exception {
        for (File f : indir.listFiles()) {
            String fname = f.getName();
            if (fname.startsWith("pa")) {
                buildPharmacologicalAction (f);
            }
            else if (fname.startsWith("desc")) {
                buildDescriptor (f);
            }
            else if (fname.startsWith("supp")) {
                buildSupplementDescriptor (f);
            }
            else if (fname.startsWith("qual")) {
                buildQualifier (f);
            }
            else {
                logger.warning("Unknown file: "+f);
            }
        }

        try (Transaction tx = gdb.beginTx()) {
            gdb.schema().indexFor(Label.label(Entry.class.getSimpleName()))
                .on("ui").create();
            tx.success();
        }
    }

    void buildPharmacologicalAction (File file) {
        logger.info("### parsing pharmacological action file "+file+"...");
        try {
            MeshParser parser = new MeshParser
                (this::createNodePharmacologicalAction,
                 "PharmacologicalAction");
            parser.parse(file);
        }
        catch (Exception ex) {
            logger.log(Level.SEVERE, "Can't parse file: "+file, ex);
        }
    }

    void createNodePharmacologicalAction (Entry entry) {
        createNodeIfAbsent (entry, (index, node, e) -> {
                Label label = Label.label(PA);
                if (node.hasLabel(label))
                    return;
                
                node.addLabel(label);
                PharmacologicalAction pa = (PharmacologicalAction)e;
                for (Entry s : pa.substances) {
                    Node n = createNodeIfAbsent (s);
                    Relationship r = node.createRelationshipTo
                        (n, RelationshipType.withName("substance"));
                }
            });
    }

    void buildDescriptor (File file) {
        logger.info("### parsing descriptor file "+file+"...");
        try {
            MeshParser parser = new MeshParser
                (this::createNodeDescriptor, "DescriptorRecord");
            parser.parse(file);
        }
        catch (Exception ex) {
            logger.log(Level.SEVERE, "Can't parse file: "+file, ex);
        }
    }

    void createNodeDescriptor (Entry entry) {
        createNodeIfAbsent (entry, (index, node, e) -> {
                Label label = Label.label(DESC);
                if (node.hasLabel(label))
                    return;

                node.addLabel(label);
                Descriptor desc = (Descriptor)e;
                if (desc.annotation != null)
                    node.setProperty("annotation", desc.annotation);
                if (!desc.treeNumbers.isEmpty())
                    setProperty (index, node, "treeNumbers",
                                 desc.treeNumbers.toArray(new String[0]));
                for (Concept c : desc.concepts) {
                    Node nc = createNodeConcept (c);
                    Relationship rel = node.createRelationshipTo
                        (nc, RelationshipType.withName("concept"));
                    if (c.preferred)
                        rel.setProperty("preferred", true);
                }

                for (Qualifier q : desc.qualifiers) {
                    // create qualifier node placeholder
                    createNodeIfAbsent (q, (i, n, ent) -> {
                           Relationship rel = node.createRelationshipTo
                               (n, RelationshipType.withName("qualifier"));
                           if (q.preferred)
                               rel.setProperty("preferred", true);
                        });
                }

                /*
                for (Entry pa : desc.pharm) {
                    createNodeIfAbsent (pa, (i, n, ent) -> {
                            Relationship rel = node.createRelationshipTo
                                (n, RelationshipType.withName
                                 ("pharmacologicalaction"));
                        });
                }
                */
            });
    }

    Node createNodeConcept (final Concept c) {
        final Node node = createNodeIfAbsent (c, (index, n, e) -> {
                Label label = Label.label("Concept");
                if (n.hasLabel(label))
                    return;
                
                n.addLabel(label);
                // this can happen due to Relation
                if (!c.name.equals(n.getProperty("name"))) {
                    setProperty (index, n, "name", null); // remove index
                    setProperty (index, n, "name", c.name);
                }
                    
                if (c.casn1 != null)
                    setProperty (index, n, "casn1", c.casn1);
                if (c.regno != null)
                    setProperty (index, n, "regno", c.regno);
                if (c.note != null)
                    n.setProperty("note", c.note);
                if (!c.relatedRegno.isEmpty())
                    setProperty (index, n, "relatedRegno",
                                 c.relatedRegno.toArray(new String[0]));
                
                RelationshipType type = RelationshipType.withName("term");
                for (Term t : c.terms) {
                    Node nt = createNodeTerm (t);

                    boolean connected = false;
                    for (Relationship rel : n.getRelationships(type)) {
                        if (rel.getOtherNode(n).equals(nt)) {
                            connected = true;
                            break;
                        }
                    }

                    if (connected) {
                        // already connected to this term
                    }
                    else if (t.preferred) {
                        Relationship rel = n.createRelationshipTo(nt, type);
                        rel.setProperty("preferred", true);
                    }
                    else {
                        nt.createRelationshipTo(n, type);
                    }
                }
            });
        
        for (Relation r : c.relations) {
            createNodeIfAbsent (r, (index, n, e) -> {
                    Relationship rel = n.createRelationshipTo
                        (node, RelationshipType.withName("concept"));
                    rel.setProperty("name", r.name);
                });
        }
        
        return node;
    }

    Node createNodeTerm (final Term t) {
        return createNodeIfAbsent (t, (index, n, e) -> {
                Label label = Label.label("Term");
                if (n.hasLabel(label))
                    return;

                n.addLabel(label);
            });
    }

    void createNodeQualifier (Entry entry) {
        createNodeIfAbsent (entry, (index, node, e) -> {
                Label label = Label.label(QUAL);
                if (node.hasLabel(label))
                    return;

                node.addLabel(label);
                Qualifier q = (Qualifier)e;
                if (q.annotation != null)
                    node.setProperty("annotation", q.annotation);
                if (q.abbr != null)
                    setProperty (index, node, "abbr", q.abbr);
                if (!q.treeNumbers.isEmpty())
                    setProperty (index, node, "treeNumbers",
                                 q.treeNumbers.toArray(new String[0]));
                for (Concept c : q.concepts) {
                    Node nc = createNodeConcept (c);
                    Relationship rel = node.createRelationshipTo
                        (nc, RelationshipType.withName("concept"));
                    if (c.preferred)
                        rel.setProperty("preferred", true);                    
                }
            });
    }

    void buildSupplementDescriptor (File file) {
        logger.info("### parsing supplement descriptor file "+file+"...");
        try {
            MeshParser parser = new MeshParser
                (this::createNodeSupplementDescriptor, "SupplementalRecord");
            parser.parse(file);
        }
        catch (Exception ex) {
            logger.log(Level.SEVERE, "Can't parse file: "+file, ex);
        }        
    }

    void buildQualifier (File file) {
        logger.info("### parsing qualifier file "+file+"...");
        try {
            MeshParser parser = new MeshParser
                (this::createNodeQualifier, "QualifierRecord");
            parser.parse(file);
        }
        catch (Exception ex) {
            logger.log(Level.SEVERE, "Can't parse file: "+file, ex);
        }        
    }

    void createNodeSupplementDescriptor (Entry entry) {
        createNodeIfAbsent (entry, (index, node, e) -> {
                Label label = Label.label(SUPP);
                if (node.hasLabel(label))
                    return;

                node.addLabel(label);
                SupplementDescriptor supp = (SupplementDescriptor)entry;
                setProperty (index, node, "freq", supp.freq);
                if (!supp.sources.isEmpty())
                    node.setProperty("sources",
                                     supp.sources.toArray(new String[0]));
                
                RelationshipType type = RelationshipType.withName("mapped");
                for (Descriptor d : supp.mapped) {
                    Node a = createNodeIfAbsent (d);
                    createRelationshipIfAbsent (node, a, type);
                    
                    for (Qualifier q : d.qualifiers) {
                        Node n = createNodeIfAbsent (q);
                        createRelationshipIfAbsent (node, n, type);
                        createRelationshipIfAbsent (n, a, type);
                    }
                }

                type = RelationshipType.withName("indexed");
                for (Descriptor d : supp.indexed) {
                    Node a = createNodeIfAbsent (d);
                    createRelationshipIfAbsent (node, a, type);
                    
                    for (Qualifier q : d.qualifiers) {
                        Node n = createNodeIfAbsent (q);
                        createRelationshipIfAbsent (node, n, type);
                        createRelationshipIfAbsent (n, a, type);
                    }
                }

                for (Concept c : supp.concepts) {
                    Node nc = createNodeConcept (c);
                    Relationship rel = node.createRelationshipTo
                        (nc, RelationshipType.withName("concept"));
                    if (c.preferred)
                        rel.setProperty("preferred", true);
                }
            });
    }

    Relationship createRelationshipIfAbsent
        (Node from, Node to, RelationshipType type) {
        for (Relationship rel : from.getRelationships(type)) {
            if (to.equals(rel.getOtherNode(from)))
                return rel;
        }
        return from.createRelationshipTo(to, type);
    }

    <T extends PropertyContainer>
        void setProperty (Index<T> index, T e, String prop, Object value) {
        if (value != null) {
            e.setProperty(prop, value);
            if (value.getClass().isArray()) {
                int len = Array.getLength(value);
                for (int i = 0; i < len; ++i)
                    index.add(e, prop, Array.get(value, i));
            }
            else
                index.add(e, prop, value);
        }
        else {
            // remove this
            index.remove(e, prop);
            e.removeProperty(prop);
        }
    }

    Node createNodeIfAbsent (MeshParser.Entry entry) {
        return createNode (entry, true, null);
    }
    Node createNodeIfAbsent (MeshParser.Entry entry, NodeInstr instr) {
        return createNode (entry, true, instr);
    }
    Node createNode (MeshParser.Entry entry) {
        return createNode (entry, false, null);
    }
    Node createNode (MeshParser.Entry entry, NodeInstr instr) {
        return createNode (entry, false, instr);
    }
    
    Node createNode (MeshParser.Entry entry,
                     boolean check, NodeInstr nodeinstr) {
        String ui = entry.ui.charAt(0) == '*'
            ? entry.ui.substring(1) : entry.ui;
        
        try (Transaction tx = gdb.beginTx()) {
            Index<Node> index = nodeIndex ();
            
            Node n = null;
            if (check) {
                IndexHits<Node> hits = index.get("ui", ui);
                if (hits.hasNext())
                    n = hits.next();
            }

            if (n == null) {
                n = gdb.createNode();
                setProperty (index, n, "ui", ui);
                setProperty (index, n, "name", entry.name);
                if (entry.created != null)
                    setProperty (index, n, "created", entry.created.getTime());
                if (entry.revised != null)
                    setProperty (index, n, "revised", entry.revised.getTime());
                if (entry.established != null)
                    setProperty (index, n, "established",
                                 entry.established.getTime());
                if (nodeinstr != null)
                    nodeinstr.instrument(index, n, entry);
            }
            else if (nodeinstr != null)
                nodeinstr.instrument(index, n, entry);
            
            tx.success();
            return n;
        }
    }

    public void close () throws Exception {
        logger.info(getClass().getName()+": shutting down");
        gdb.shutdown();
    }
    
    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.out.println("Usage: "+BuildMeshGraph.class.getName()
                               +" OUTDIR INDIR");
            System.out.println("where OUTDIR is the output index directory "
                               +"and INDIR is a directory contains XML files");
            System.out.println
                ("downloaded from "
                 +"https://www.nlm.nih.gov/mesh/download_mesh.html");
            System.exit(1);
        }

        File indir = new File (argv[1]);
        if (!indir.exists()) {
            logger.log(Level.SEVERE,
                       "Input directory \""+argv[1]+"\" doesn't exist!");
            System.exit(1);
        }
        else if (!indir.isDirectory()) {
            logger.log(Level.SEVERE, indir+": not a directory!");
            System.exit(1);
        }
        
        File outdir = new File (argv[0]);
        try (BuildMeshGraph mesh = new BuildMeshGraph (outdir, indir)) {
            mesh.build();
        }
    }
}
