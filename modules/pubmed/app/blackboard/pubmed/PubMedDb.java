package blackboard.pubmed;

import java.io.*;
import java.util.*;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.zip.GZIPInputStream;
import java.util.function.Consumer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import blackboard.mesh.Entry;
import blackboard.mesh.Descriptor;
import blackboard.mesh.Qualifier;
import blackboard.mesh.MeshDb;
import blackboard.mesh.MeshKSource;
import blackboard.mesh.Concept;
import blackboard.mesh.CommonDescriptor;

import blackboard.neo4j.Neo4j;

import com.google.inject.assistedinject.Assisted;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.*;

import play.Logger;
import play.libs.F;
import play.libs.ws.*;
import play.inject.ApplicationLifecycle;
import javax.inject.Inject;

public class PubMedDb extends Neo4j implements AutoCloseable {
    static final public RelationshipType MESH_RELTYPE =
        RelationshipType.withName("mesh");

    static final PubMedDoc POISON_DOC = new PubMedDoc ();
    
    class PubMedNodeFactory extends UniqueFactory.UniqueNodeFactory {
        int count;
        PubMedNodeFactory () {
            super (getDbIndex ());
        }

        @Override
        protected void initialize (Node node, Map<String, Object> props) {
            for (Map.Entry<String, Object> me : props.entrySet()) {
                node.setProperty(me.getKey(), me.getValue());
            }
            node.setProperty("created", System.currentTimeMillis());
            node.addLabel(Label.label("article"));
            ++count;
        }

        public int getCount () { return count; }
    }

    class MeshNodeFactory extends UniqueFactory.UniqueNodeFactory {
        int count;
        MeshNodeFactory () {
            super (getNodeIndex ("mesh"));
        }
        
        @Override
        protected void initialize (Node node, Map<String, Object> props) {
            for (Map.Entry<String, Object> me : props.entrySet()) {
                node.setProperty(me.getKey(), me.getValue());
            }
            node.setProperty("created", System.currentTimeMillis());
            node.addLabel(Label.label("mesh"));
            ++count;
        }

        public int getCount () { return count; }
    }

    class IndexTask implements Callable<Integer> {
        SimpleDateFormat sdf = new SimpleDateFormat ("yyyy.MM.dd");        
        IndexTask () {
        }

        public Integer call () throws Exception {
            Logger.debug(Thread.currentThread().getName()
                         +": index thread started...");
            int ndocs = 0;
            for (PubMedDoc d; (d = queue.take()) != POISON_DOC;) {
                Logger.debug(count.incrementAndGet()+" "+d.pmid+" "
                             +sdf.format(d.date)+" chem="+d.chemicals.size()
                             +" mh="+d.headings.size());
                add (d);
                ++ndocs;
            }
            Logger.debug(Thread.currentThread().getName()
                         +": thread processed "+ndocs+" documents!");
            return ndocs;
        }
    }

    WSClient wsclient;    
    final MeshDb mesh;
    final PubMedNodeFactory pmf;
    final MeshNodeFactory mnf;
    final AtomicInteger count = new AtomicInteger ();
    final BlockingQueue<PubMedDoc> queue =
        new ArrayBlockingQueue<PubMedDoc>(1000);

    @Inject
    public PubMedDb (WSClient wsclient, MeshKSource mesh,
                     ApplicationLifecycle lifecycle,
                     @Assisted File dbdir) {
        this (dbdir, mesh.getMeshDb());
        
        if (lifecycle != null) {
            lifecycle.addStopHook(() -> {
                    wsclient.close();
                    return CompletableFuture.completedFuture(null);
                });
        }
        this.wsclient = wsclient;
    }
    
    public PubMedDb (File dbdir, MeshDb mesh) {
        super (dbdir);
        
        try (Transaction tx = gdb.beginTx()) {
            pmf = new PubMedNodeFactory ();
            mnf = new MeshNodeFactory ();
            tx.success();
        }
        this.mesh = mesh;
    }

    @Override
    public void shutdown () throws Exception {
        Logger.debug("## shutting down PubMedDb instance "+dbdir+"...");
        super.shutdown();
        
        if (wsclient != null)
            wsclient.close();
    }
    
    public void close () throws Exception {
        try {
            shutdown ();
        }
        catch (Exception ex) {
            Logger.error("Can't shutdown database", ex);
        }
    }

    public Node add (PubMedDoc doc) {
        Node n = null;
        try (Transaction tx = gdb.beginTx()) {
            UniqueFactory.UniqueEntity<Node> uf = _add (doc);
            n = uf.entity();
            tx.success();
        }
        return n;
    }
    
    UniqueFactory.UniqueEntity<Node> _add (PubMedDoc d) {
        UniqueFactory.UniqueEntity<Node> uf = pmf
            .getOrCreateWithOutcome("pmid", d.pmid);
        if (!uf.wasCreated())
            return uf;
        
        Node node = uf.entity();
        node.setProperty("title", d.title);
        if (!d.abs.isEmpty())
            node.setProperty("abstract", d.abs.toArray(new String[0]));
        node.setProperty("journal", d.journal);
        node.setProperty("date", d.date.getTime());
        Calendar cal = Calendar.getInstance();
        cal.setTime(d.date);
        node.setProperty("year", cal.get(Calendar.YEAR));
        
        addTextIndex (node, d.title);
        for (String text : d.abs)
            addTextIndex (node, text);
        for (Entry e : d.chemicals)
            add (e, node, null);
        for (MeshHeading mh : d.headings)
            add (mh, node);
            
        return uf;
    }

    UniqueFactory.UniqueEntity<Node> add
        (Entry e, Node node, Boolean major, Entry... quals) {
         UniqueFactory.UniqueEntity<Node> uf =
            mnf.getOrCreateWithOutcome("ui", e.ui);
         
         Node n = uf.entity();        
        if (uf.wasCreated()) {
            // initialize...
            n.setProperty("name", e.name);
            addTextIndex (node, e.name);
            
            CommonDescriptor cd = (CommonDescriptor)e;
            for (Concept c : cd.getConcepts()) {
                if (c.regno != null)
                    addTextIndex (node, c.regno);
                for (String r : c.relatedRegno)
                    addTextIndex (node, r);
                addTextIndex (node, c.name);
            }

            List<String> pharm = new ArrayList<>();
            for (Entry a : cd.getPharmacologicalActions()) {
                //n.addLabel(Label.label(a.name));
                addTextIndex (node, a.name);
                pharm.add(a.name);
            }
            
            if (!pharm.isEmpty())
                n.setProperty("pharmacological", pharm.toArray(new String[0]));

            if (e instanceof Descriptor) {
                Descriptor desc = (Descriptor)e;
                List<String> paths = new ArrayList<>();
                for (String tr : desc.treeNumbers) {
                    String[] toks = tr.split("\\.");
                    StringBuilder path = new StringBuilder (toks[0]);
                    n.addLabel(Label.label(path.toString()));
                    for (int i = 1; i < toks.length; ++i) {
                        //n.addLabel(Label.label(path.toString()));
                        paths.add(path.toString());
                        path.append("."+toks[i]);
                        addTextIndex (node, path.toString());
                    }
                    paths.add(path.toString());
                    addTextIndex (node, path.toString());
                }
                
                if (!desc.treeNumbers.isEmpty()) {
                    n.setProperty("treeNumbers", desc.treeNumbers.toArray
                                  (new String[0]));
                }
            }
        }

        RelationshipType type = RelationshipType.withName
            (e.getClass().getSimpleName());
        for (Relationship rel : node.getRelationships(type)) {
            if (rel.getOtherNode(node).equals(n))
                return uf;
        }
        
        Relationship rel = node.createRelationshipTo(n, type);
        List<String> qualifiers = new ArrayList<>();
        for (Entry q : quals)
            qualifiers.add(((Qualifier)q).name);
        
        if (major != null)
            rel.setProperty("majorTopic", major);
        
        if (!qualifiers.isEmpty()) {
            rel.setProperty("qualifiers",
                            qualifiers.toArray(new String[0]));
        }
        
        return uf;
    }

    UniqueFactory.UniqueEntity<Node> add (MeshHeading mh, Node node) {
        return add (mh.descriptor, node, mh.majorTopic,
                    mh.qualifiers.toArray(new Entry[0]));
    }

    public int index (InputStream is) throws Exception {
        return index (is, 1);
    }
    
    public int index (InputStream is, int nthreads) throws Exception {
        ExecutorService es = Executors.newFixedThreadPool(nthreads);
        Future[] futures = new Future[nthreads];
        for (int i = 0; i < nthreads; ++i)
            futures[i] = es.submit(new IndexTask ());
        
        new PubMedSax(mesh, d -> {
                boolean cont = false;
                try {
                    //Logger.debug("queuing "+d.pmid+"...");
                    queue.put(d);
                    cont = true;
                }
                catch (Exception ex) {
                    Logger.error("Can't queue document", ex);
                }
                return cont;
        }).parse(is);
        
        for (Future f : futures)
            queue.put(POISON_DOC);
        es.shutdown();

        return count.get();
    }

    public int index (String fname) throws Exception {
        int count = 0;
        try {
            File file = new File (fname);
            Node meta = getMetaNode();
            RelationshipType ftype = RelationshipType.withName("file");
            try (Transaction tx = gdb.beginTx()) {
                for (Relationship rel : meta.getRelationships(ftype)) {
                    Node fn = rel.getOtherNode(meta);
                    if (file.getName().equals(fn.getProperty("name"))) {
                        count = (Integer)fn.getProperty("count");
                        break;
                    }
                }
                tx.success();
            }

            if (count == 0) {
                count = index (new GZIPInputStream
                               (new FileInputStream (file)));
                try (Transaction tx = gdb.beginTx()) {
                    Node fn = gdb.createNode(Label.label(file.getName()));
                    fn.setProperty("name", file.getName());
                    fn.setProperty("count", count);
                    meta.createRelationshipTo(fn, ftype);
                    tx.success();
                }
            }
        }
        catch (Exception ex) {
            Logger.error("Can't process file: "+fname, ex);
        }
        return count;
    }

    /*
     * sbt pubmed/'runMain blackboard.pubmed.PubMedDb$BuildIndex ...'
     */
    public static class BuildIndex {
        public static void main (String[] argv) throws Exception {
            if (argv.length < 3) {
                System.err.println
                    ("Usage: blackboard.pubmed.PubMedDb$BuildIndex "
                     +"DBDIR MESHDB FILES...");
                System.exit(1);
            }
            
            try (MeshDb mesh = new MeshDb (null, new File (argv[1]));
                 PubMedDb pdb = new PubMedDb (new File (argv[0]), mesh)) {
                for (int i = 2; i < argv.length; ++i)
                    pdb.index(argv[i]);
            }
        }
    }
}

