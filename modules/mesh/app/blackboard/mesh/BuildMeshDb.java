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


/*
 * sbt mesh/"run-main blackboard.mesh.BuildMeshDb OUTDIR INDIR"
 */
public class BuildMeshDb implements Mesh, AutoCloseable {
    static final Logger logger =
        Logger.getLogger(BuildMeshDb.class.getName());

    interface NodeInstr<T extends Entry> {
        void instrument (Index<Node> index, Node node, T entry);
    }

    GraphDatabaseService gdb;
    File indir;
    File outdir;
    AtomicInteger pacnt = new AtomicInteger ();
    AtomicInteger qualcnt = new AtomicInteger ();
    AtomicInteger desccnt = new AtomicInteger ();
    AtomicInteger suppcnt = new AtomicInteger ();
    Map<String, String> textIndexConfig;
    
    public BuildMeshDb (File outdir, File indir) throws IOException {
        this.indir = indir;
        this.outdir = outdir;

        outdir.mkdirs();
        logger.info("Initializing "+outdir+"...");
        
        gdb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(outdir)
            .setConfig(GraphDatabaseSettings.dump_configuration, "true")
            .newGraphDatabase();
        //gdb.registerTransactionEventHandler(this);
        textIndexConfig = new HashMap<>();
        textIndexConfig.put(IndexManager.PROVIDER, "lucene");
        textIndexConfig.put("type", "fulltext");
    }

    Index<Node> exactNodeIndex () {
        return gdb.index().forNodes(EXACT_INDEX);
    }

    Index<Node> textNodeIndex (Node n, String text) {
        Index<Node> index = gdb.index().forNodes(TEXT_INDEX, textIndexConfig);
        if (text != null)
            index.add(n, "text", text);
        else
            index.remove(n, "text", text);
        return index;
    }

    public void build () throws Exception {
        for (File f : indir.listFiles()) {
            String fname = f.getName();
            String sha = null;
            int count = 0;
            if (fname.startsWith("pa")) {
                sha = buildPharmacologicalAction (f);
                count = pacnt.get();
            }
            else if (fname.startsWith("desc")) {
                sha = buildDescriptor (f);
                count = desccnt.get();
            }
            else if (fname.startsWith("supp")) {
                sha = buildSupplementalDescriptor (f);
                count = suppcnt.get();
            }
            else if (fname.startsWith("qual")) {
                sha = buildQualifier (f);
                count = qualcnt.get();
            }
            else {
                logger.warning("Unknown file: "+f);
            }

            logger.info("## "+fname+": count="+count+" sha="+sha);
            if (sha != null) {
                try (Transaction tx = gdb.beginTx()) {
                    Node node = gdb.createNode
                        (Label.label(MeshDb.class.getName()));
                    node.setProperty("sha", sha);
                    node.setProperty("name", fname);
                    node.setProperty("count", count);
                    tx.success();
                }
            }
        }

        try (Transaction tx = gdb.beginTx()) {
            for (String l : new String[]{"Descriptor", "PharmacologicalAction",
                                         "SupplementalDescriptor", "Concept",
                                         "Qualifier", "Term"}) {
                gdb.schema().indexFor(Label.label(l))
                    .on("ui").create();
            }
            tx.success();
        }
    }

    String buildPharmacologicalAction (File file) {
        logger.info("### parsing pharmacological action file "+file+"...");
        String sha = null;
        try {
            MeshParser parser = new MeshParser
                (this::createNodePharmacologicalAction,
                 "PharmacologicalAction");
            sha = parser.parseFile(file);
        }
        catch (Exception ex) {
            logger.log(Level.SEVERE, "Can't parse file: "+file, ex);
        }
        return sha;
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
                
                pacnt.incrementAndGet();
            });
    }

    String buildDescriptor (File file) {
        logger.info("### parsing descriptor file "+file+"...");
        String sha = null;
        try {
            MeshParser parser = new MeshParser
                (this::createNodeDescriptor, "DescriptorRecord");
            sha = parser.parseFile(file);
        }
        catch (Exception ex) {
            logger.log(Level.SEVERE, "Can't parse file: "+file, ex);
        }
        return sha;
    }

    void createNodeDescriptor (Entry entry) {
        createNodeIfAbsent (entry, (index, node, e) -> {
                Label label = Label.label(DESC);
                if (node.hasLabel(label))
                    return;

                node.addLabel(label);
                Descriptor desc = (Descriptor)e;
                if (desc.annotation != null) {
                    node.setProperty("annotation", desc.annotation);
                    textNodeIndex (node, desc.annotation);
                }
                
                if (!desc.treeNumbers.isEmpty()) {
                    indexTreeNumbers (index, node,
                                      desc.treeNumbers.toArray(new String[0]));
                }
                
                for (Concept c : desc.concepts) {
                    Node nc = createNodeConcept (c);
                    Relationship rel = node.createRelationshipTo
                        (nc, RelationshipType.withName("concept"));
                    if (c.preferred != null)
                        rel.setProperty("preferred",  c.preferred);
                }

                List<String> qualifiers = new ArrayList<>();
                for (Qualifier q : desc.qualifiers) {
                    // create qualifier node placeholder
                    /*
                    createNodeIfAbsent (q, (i, n, ent) -> {
                           Relationship rel = node.createRelationshipTo
                               (n, RelationshipType.withName("qualifier"));
                           if (q.preferred)
                               rel.setProperty("preferred", true);
                        });
                    */
                    createNodeIfAbsent (q);
                    qualifiers.add(q.name);
                    if (q.preferred != null && q.preferred)
                        setProperty (index, node, "qual_preferred", q.name);
                }
                setProperty (index, node, "qualifiers",
                             qualifiers.toArray(new String[0]));
                
                /*
                for (Entry pa : desc.pharm) {
                    createNodeIfAbsent (pa, (i, n, ent) -> {
                            Relationship rel = node.createRelationshipTo
                                (n, RelationshipType.withName
                                 ("pharmacologicalaction"));
                        });
                }
                */
                
                desccnt.incrementAndGet();
            });
    }

    void indexTreeNumbers (Index<Node> index,
                           Node node, String... treeNumbers) {
        RelationshipType type = RelationshipType.withName("parent");
        for (String tr : treeNumbers) {
            // identify child nodes
            try (IndexHits<Node> hits = index.get("parent", tr)) {
                for (Node child : hits) {
                    Relationship rel = child.createRelationshipTo
                        (node, type);
                    rel.setProperty("name", tr);
                }
            }

            // now search for parent nodes
            String[] path = tr.split("\\.");
            if (path.length > 1) {
                StringBuilder p = new StringBuilder (path[0]);
                for (int i = 1; i < path.length-1; ++i) {
                    p.append("."+path[i]);
                }
                    
                String tn = p.toString();
                try (IndexHits<Node> hits = index.get("treeNumbers", tn)) {
                    for (Node parent : hits) {
                        Relationship rel = node.createRelationshipTo
                            (parent, type);
                        rel.setProperty("name", tn);
                    }
                }
                
                // index this node
                index (index, node, "parent", tn);
            }
        }
        setProperty (index, node, "treeNumbers", treeNumbers);        
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
                if (c.regno != null) {
                    setProperty (index, n, "regno", c.regno);
                    textNodeIndex (n, c.regno);
                }
                
                if (c.note != null) {
                    n.setProperty("note", c.note);
                    textNodeIndex (n, c.note);
                }
                
                if (!c.relatedRegno.isEmpty()) {
                    setProperty (index, n, "relatedRegno",
                                 c.relatedRegno.toArray(new String[0]));
                    for (String regno : c.relatedRegno)
                        textNodeIndex (n, regno);
                }
                
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
                    else if (t.preferred != null) {
                        Relationship rel = n.createRelationshipTo(nt, type);
                        rel.setProperty("preferred",  t.preferred);
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
                
                if (!q.treeNumbers.isEmpty()) {
                    indexTreeNumbers (index, node,
                                      q.treeNumbers.toArray(new String[0]));
                }

                for (Concept c : q.concepts) {
                    Node nc = createNodeConcept (c);
                    Relationship rel = node.createRelationshipTo
                        (nc, RelationshipType.withName("concept"));
                    if (c.preferred != null)
                        rel.setProperty("preferred", c.preferred);
                }
                
                qualcnt.incrementAndGet();
            });
    }

    String buildSupplementalDescriptor (File file) {
        logger.info("### parsing supplement descriptor file "+file+"...");
        String sha = null;
        try {
            MeshParser parser = new MeshParser
                (this::createNodeSupplementalDescriptor, "SupplementalRecord");
            sha = parser.parseFile(file);
        }
        catch (Exception ex) {
            logger.log(Level.SEVERE, "Can't parse file: "+file, ex);
        }
        return sha;
    }

    String buildQualifier (File file) {
        logger.info("### parsing qualifier file "+file+"...");
        String sha = null;
        try {
            MeshParser parser = new MeshParser
                (this::createNodeQualifier, "QualifierRecord");
            sha = parser.parseFile(file);
        }
        catch (Exception ex) {
            logger.log(Level.SEVERE, "Can't parse file: "+file, ex);
        }
        return sha;
    }

    void createNodeSupplementalDescriptor (Entry entry) {
        createNodeIfAbsent (entry, (index, node, e) -> {
                Label label = Label.label(SUPP);
                if (node.hasLabel(label))
                    return;

                node.addLabel(label);
                SupplementalDescriptor supp = (SupplementalDescriptor)entry;
                setProperty (index, node, "freq", supp.freq);
                if (supp.note != null) {
                    node.setProperty("note", supp.note);
                    textNodeIndex (node, supp.note);
                }
                
                if (!supp.sources.isEmpty())
                    node.setProperty("sources",
                                     supp.sources.toArray(new String[0]));
                
                RelationshipType type = RelationshipType.withName("mapped");
                for (Descriptor d : supp.mapped) {
                    Node a = createNodeIfAbsent (d);
                    Relationship rel = createRelationshipIfAbsent
                        (node, a, type);

                    if (!d.qualifiers.isEmpty()) {
                        List<String> qualifiers = new ArrayList<>();
                        for (Qualifier q : d.qualifiers) {
                            qualifiers.add(q.name);
                        }
                        rel.setProperty("qualifiers",
                                        qualifiers.toArray(new String[0]));
                    }
                }

                type = RelationshipType.withName("indexed");
                for (Descriptor d : supp.indexed) {
                    Node a = createNodeIfAbsent (d);
                    Relationship rel = createRelationshipIfAbsent
                        (node, a, type);

                    if (!d.qualifiers.isEmpty()) {
                        List<String> qualifiers = new ArrayList<>();
                        for (Qualifier q : d.qualifiers) {
                            qualifiers.add(q.name);
                        }
                        rel.setProperty("qualifiers",
                                        qualifiers.toArray(new String[0]));
                    }
                }

                for (Concept c : supp.concepts) {
                    Node nc = createNodeConcept (c);
                    Relationship rel = node.createRelationshipTo
                        (nc, RelationshipType.withName("concept"));
                    if (c.preferred != null)
                        rel.setProperty("preferred", c.preferred);
                }

                suppcnt.incrementAndGet();
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

    static <T extends PropertyContainer> void setProperty
        (Index<T> index, T e, String prop, Object value) {
        if (value != null) {
            e.setProperty(prop, value);
        }
        else {
            // remove this
            e.removeProperty(prop);
        }
        index (index, e, prop, value);
    }

    static <T extends PropertyContainer> void index
        (Index<T> index, T e, String name, Object value) {
        if (value != null) {
            if (value.getClass().isArray()) {
                int len = Array.getLength(value);
                for (int i = 0; i < len; ++i)
                    index.add(e, name, Array.get(value, i));
            }
            else
                index.add(e, name, value);
        }
        else {
            index.remove(e, name);
        }
    }

    Node createNodeIfAbsent (Entry entry) {
        return createNode (entry, true, null);
    }
    Node createNodeIfAbsent (Entry entry, NodeInstr instr) {
        return createNode (entry, true, instr);
    }
    Node createNode (Entry entry) {
        return createNode (entry, false, null);
    }
    Node createNode (Entry entry, NodeInstr instr) {
        return createNode (entry, false, instr);
    }
    
    Node createNode (Entry entry, boolean check, NodeInstr nodeinstr) {
        String ui = entry.ui.charAt(0) == '*'
            ? entry.ui.substring(1) : entry.ui;
        
        try (Transaction tx = gdb.beginTx()) {
            Index<Node> index = exactNodeIndex ();
            
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
                /*
                if (entry.established != null)
                    setProperty (index, n, "established",
                                 entry.established.getTime());
                */
                if (nodeinstr != null)
                    nodeinstr.instrument(index, n, entry);
                textNodeIndex (n, entry.name);
                textNodeIndex (n, ui);
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
            System.out.println("Usage: "+BuildMeshDb.class.getName()
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
        try (BuildMeshDb mesh = new BuildMeshDb (outdir, indir)) {
            mesh.build();
        }
    }
}
