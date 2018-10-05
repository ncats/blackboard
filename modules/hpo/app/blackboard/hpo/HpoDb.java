package blackboard.hpo;

import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.regex.*;
import java.util.function.Function;
import java.lang.reflect.Array;
import java.security.MessageDigest;
import java.security.DigestInputStream;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.event.*;
import org.neo4j.graphdb.index.*;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.index.lucene.*;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanQuery;
import static org.apache.lucene.search.BooleanQuery.Builder;
import static org.apache.lucene.search.BooleanClause.Occur;

import javax.inject.Inject;

import play.libs.F;
import play.Logger;
import play.inject.ApplicationLifecycle;
import com.google.inject.assistedinject.Assisted;

import blackboard.KType;
import blackboard.KEntity;
import blackboard.neo4j.Neo4j;

public class HpoDb extends Neo4j implements AutoCloseable, KType {
    static final Label PHENOTYPE_LABEL = Label.label(PHENOTYPE_T);
    static final Label DISEASE_LABEL = Label.label(DISEASE_T);
    static final Label GENE_LABEL = Label.label(GENE_T);
    
    static final RelationshipType ISA_RELTYPE =
        RelationshipType.withName("IS_A");
    static final RelationshipType GENE_RELTYPE =
        RelationshipType.withName(GENE_T);
    
    final Map<String, Integer> files;
    final HpoUF hpoUF;
    final Pattern quoteRx = Pattern.compile("\"([^\"]+)");
    final Pattern idRx = Pattern.compile
        ("(pmid|PMID|HPO|hpo|ORCID):([0-9a-z\\-]+)");
    final MessageDigest digest;
    final Index<Node> index;

    static class HpoUF extends UniqueFactory.UniqueNodeFactory {
        HpoUF (Index<Node> index) {
            super (index);
        }

        @Override
        protected void initialize (Node created, Map<String, Object> props) {
            for (Map.Entry<String, Object> me : props.entrySet()) {
                created.setProperty(me.getKey(), me.getValue());
            }
            created.setProperty("created", System.currentTimeMillis());
        }
    }
    
    
    @Inject
    public HpoDb (ApplicationLifecycle lifecycle, @Assisted File dbdir) {
        super (dbdir);
        
        files = new TreeMap<>();
        try {
            digest = MessageDigest.getInstance("sha1");
        }
        catch (Exception ex) {
            throw new RuntimeException (ex);
        }
        
        Node meta = getMetaNode ();
        try (Transaction tx = gdb.beginTx()) {
            index = getDbIndex ();
            hpoUF = new HpoUF (index);
            
            for (Relationship rel : meta.getRelationships
                     (RelationshipType.withName("file"))) {
                Node n = rel.getOtherNode(meta);
                Logger.debug("## name="+n.getProperty("name")
                             +" count="+n.getProperty("count"));
                files.put((String)n.getProperty("name"),
                          (Integer)n.getProperty("count"));
            }
            tx.success();
        }

        if (files.isEmpty()) {
            Logger.debug("## "+dbdir+" has not been initialized!");
        }
        else {
            Logger.debug("## "+dbdir+": "+files);
        }
        
        if (lifecycle != null) {
            lifecycle.addStopHook(() -> {
                    shutdown ();
                    return CompletableFuture.completedFuture(null);
                });
        }
        Logger.debug("## "+dbdir+" HPO database initialized...");        
    }

    public void build (File indir) throws IOException {
        File obo = new File (indir, "hp.obo");
        if (!obo.exists()) {
            throw new IllegalArgumentException
                ("No OBO file \"hp.obo\" found in "+indir.getPath());
        }

        loadOBO (obo);
        
        for (File f : indir.listFiles()) {
            String name = f.getName();
            if (name.equals("phenotype_annotation_new.tab")) {
                loadAnnotations (f);
            }
            else if (name.equals
                     ("ALL_SOURCES_ALL_FREQUENCIES_genes_to_phenotype.txt")) {
            }
            else if (name.equals
                     ("ALL_SOURCES_ALL_FREQUENCIES_phenotype_to_genes.txt")) {
                loadGenes (f);
            }
            else { // ignore
            }
        }
    }

    static void getContent (String line, Map<String, Object> data) {
        int pos = line.indexOf(':');
        if (pos > 0) {
            String field = line.substring(0, pos);
            String value = line.substring(pos+1).trim();
            Object content = data.get(field);
            if (content == null) {
                data.put(field, value);
            }
            else if (content.getClass().isArray()) {
                int len = Array.getLength(content);
                String[] ary = new String[len+1];
                for (int i = 0; i < len; ++i)
                    ary[i] = (String) Array.get(content, i);
                ary[len] = value;
                data.put(field, ary);
            }
            else {
                String[] ary = new String[2];
                ary[0] = (String)content;
                ary[1] = value;
                data.put(field, ary);
            }
        }
    }

    String getDigest () {
        byte[] d = digest.digest();
        StringBuilder sha = new StringBuilder ();
        for (int i = 0; i < d.length; ++i) {
            sha.append(String.format("%1$02x", d[i] & 0xff));
        }
        return sha.toString();
    }

    protected int loadFile (File file,
                            Function<InputStream, Integer> loader)
        throws IOException {
        Integer count = files.get(file.getName());
        if (count == null || count == 0) {
            digest.reset();
            Logger.debug("## loading "+file+"...");
            try (DigestInputStream mds = new DigestInputStream
                 (new FileInputStream (file), digest)) {
                count = loader.apply(mds);
                if (count > 0) {
                    Node meta = getMetaNode ();
                    try (Transaction tx = gdb.beginTx()) {
                        Node node = gdb.createNode(Label.label(file.getName()));
                        node.setProperty("sha", getDigest ());
                        node.setProperty("name", file.getName());
                        node.setProperty("count", count);
                        node.createRelationshipTo
                            (meta, RelationshipType.withName("file"));
                        tx.success();
                    }
                    files.put(file.getName(), count);
                }
                Logger.debug("## "+file.getName()+"..."+count);
            }
        }
        
        return count;        
    }

    protected int loadAnnotations (File file) throws IOException {
        return loadFile (file, is -> {
                int count = 0;
                try {
                    count = loadAnnotations (is);
                }
                catch (IOException ex) {
                    Logger.error("Can't parse annotation: "+file, ex);
                }
                return count;
            });
    }
    
    protected int loadOBO (File file) throws IOException {
        return loadFile (file, is -> {
                int count = 0;
                try {
                    count = loadOBO (is);
                }
                catch (IOException ex) {
                    Logger.error("Can't parse OBO: "+file, ex);
                }
                return count;
            });
    }

    protected int loadGenes (File file) throws IOException {
        return loadFile (file, is -> {
                int count = 0;
                try {
                    count = loadGenes (is);
                }
                catch (IOException ex) {
                    Logger.error("Can't parse gene: "+file, ex);
                }
                return count;
            });
    }

    protected int loadAnnotations (InputStream is) throws IOException {
        int count = 0;
        try (BufferedReader br = new BufferedReader
             (new InputStreamReader (is))) {
            for (String line; (line = br.readLine()) != null; ) {
                Node node = createAnnotationNodeIfAbsent (line);
                if (node != null)
                    ++count;
                /*
                if (count > 1000)
                    break;
                */
            }
        }
        return count;
    }

    protected int loadGenes (InputStream is) throws IOException {
        int count = 0;
        try (BufferedReader br = new BufferedReader
             (new InputStreamReader (is))) {
            // #Format: HPO-ID<tab>HPO-Name<tab>Gene-ID<tab>Gene-Name
            br.readLine(); 
            for (String line; (line = br.readLine()) != null; ) {
                String[] toks = line.split("\t");
                try (Transaction tx = gdb.beginTx();
                     IndexHits<Node> hits = index.get("id", toks[0])) {
                    if (hits.hasNext()) {
                        Node node = hits.next();
                        UniqueFactory.UniqueEntity<Node> ent =
                            hpoUF.getOrCreateWithOutcome("gene", toks[3]);
                        Node gene = ent.entity();
                        if (ent.wasCreated()) {
                            gene.addLabel(GENE_LABEL);
                            gene.setProperty(KEntity.TYPE_P, GENE_T);
                        }
                        
                        for (Relationship rel : node.getRelationships()) {
                            if (gene.equals(rel.getOtherNode(node))) {
                                node = null;
                                break;
                            }
                        }

                        if (node != null) {
                            node.createRelationshipTo(gene, GENE_RELTYPE);
                            ++count;
                            
                            Logger.debug(count+": "+node.getId()
                                         +" <-> "+gene.getId()+" "+toks[3]);
                        }
                    }
                    tx.success();
                }
            }
        }
        return count;
    }
    
    protected int loadOBO (InputStream is) throws IOException {
        int count = 0;
        try (BufferedReader br = new BufferedReader
             (new InputStreamReader (is))) {
            boolean term = false;
            Map<String, Object> data = new TreeMap<>();
            for (String line; (line = br.readLine()) != null; ) {
                line = line.trim();
                if (line.startsWith("[Term]")) {
                    term = true;
                    data.clear();
                    ++count;
                }
                else if ("".equals(line)) {
                    if (term) {
                        Node node = createPhenotypeNodeIfAbsent (data);
                        Logger.debug(node.getId()+" "+data.get("id")
                                     +" "+data.get("name"));
                        term = false;
                    }
                }
                else if (term) {
                    getContent (line, data);
                }
            }
        }

        if (count > 0) {
            // create links
            try (Transaction tx = gdb.beginTx();
                 Result result = gdb.execute
                 ("match(n:`"+PHENOTYPE_T+"`) "
                  +"where exists(n.is_a) return n")) {
                while (result.hasNext()) {
                    Map<String, Object> row = result.next();
                    Node n = (Node) row.get("n");
                    Object isa = n.getProperty("is_a");
                    List<Relationship> rels = new ArrayList<>();
                    mapArray (isa, rels, id -> {
                            try (Transaction t = gdb.beginTx();
                                 IndexHits<Node> hits = index.get("id", id)) {
                                Relationship rel = null;
                                if (hits.hasNext()) {
                                    Node m = hits.next();
                                    rel = n.createRelationshipTo
                                        (m, ISA_RELTYPE);
                                }
                                t.success();
                                return rel;
                            }
                        });
                    
                    for (Relationship r : rels) {
                        Logger.debug
                            (n.getId()+" -> "+ r.getOtherNode(n).getId());
                    }
                }
                tx.success();
            }
        }
        
        return count;
    }

    <T> void mapArray (Object value, List<T> vals, Function<Object, T> f) {
        if (value.getClass().isArray()) {
            int len = Array.getLength(value);
            for (int i = 0; i < len; ++i)
                mapArray (Array.get(value, i), vals, f);
        }
        else {
            T x = f.apply(value);
            if (x != null && vals != null)
                vals.add(x);
        }
    }

    void getQuoteStrings (Object value, List<String> vals) {
        mapArray (value, vals, v -> {
                Matcher m = quoteRx.matcher((String)v);
                return m.find() ? m.group(1) : null;
            });
    }

    void getIsAStrings (Object value, List<String> vals) {
        mapArray (value, vals, v -> {
                String s = (String)v;
                int pos = s.indexOf('!');
                if (pos <= 0)
                    pos = s.length();
                return s.substring(0, pos).trim();
            });
    }

    Node createPhenotypeNodeIfAbsent (Map<String, Object> term) {
        String id = (String) term.get("id");
        try (Transaction tx = gdb.beginTx()) {
            UniqueFactory.UniqueEntity<Node> ent =
                hpoUF.getOrCreateWithOutcome("id", id);
            Node node = ent.entity();
            if (ent.wasCreated()) {
                node.addLabel(PHENOTYPE_LABEL);
                node.setProperty(KEntity.TYPE_P, PHENOTYPE_T);
                List<String> syns = new ArrayList<>();
                List<String> isa = new ArrayList<>();
                for (Map.Entry<String, Object> me : term.entrySet()) {
                    String key = me.getKey();
                    Object value = me.getValue();
                    
                    if ("synonym".equals(key)) {
                        getQuoteStrings (value, syns);
                    }
                    else if ("is_a".equals(key)) {
                        getIsAStrings (value, isa);
                    }
                    else {
                        if ("xref".equals(key)) {
                            mapArray (value, null, x -> {
                                    String s = (String)x;
                                    int pos = s.indexOf(':');
                                    if (pos > 0) {
                                        String name = s.substring(0, pos);
                                        String val = s.substring(pos+1).trim();
                                        switch (name) {
                                        case "SNOMEDCT_US":
                                        case "PMID":
                                        case "pmid":
                                            index.add(node, name.toUpperCase(),
                                                      Long.parseLong(val));
                                            break;
                                        default:
                                            index.add(node, name, val);
                                        }
                                    }
                                    return null;
                                });
                        }
                        else if ("alt_id".equals(key)) {
                            mapArray (value, null, x -> {
                                    index.add(node, "id", x);
                                    return null;
                                });
                        }
                        node.setProperty(key, value);
                    }
                    
                    mapArray (value, null, x -> {
                            Matcher m = idRx.matcher((String)x);
                            while (m.find()) {
                                String ns = m.group(1).toUpperCase();
                                String val = m.group(2);
                                if ("PMID".equals(ns)) {
                                    try {
                                        index.add(node,
                                                  ns, Long.parseLong(val));
                                    }
                                    catch (NumberFormatException ex) {
                                        Logger.warn
                                            ("Bogus PMID \""+val+"\": "+x);
                                    }
                                }
                                else {
                                    index.add(node, ns, val);
                                }
                                Logger.debug
                                    (node.getId()+": "+val+"["+ns+"]");
                            }
                            addTextIndex (node, (String)x);
                            
                            return null;
                        });
                } // foreach field
                
                if (!syns.isEmpty())
                    node.setProperty("synonym", syns.toArray(new String[0]));

                if (!isa.isEmpty())
                    node.setProperty("is_a", isa.toArray(new String[0]));
            }
                        
            tx.success();
            return node;
        }
    }

    // see format https://hpo.jax.org/app/help/annotations
    Node createAnnotationNodeIfAbsent (String line) {
        String[] toks = line.split("\t");
        Logger.debug(toks[2]+" ["+toks[5]+"] "+toks[4]+" "+toks[6]);
        try (Transaction tx = gdb.beginTx()) {
            String id = toks[0]+":"+toks[1];
            UniqueFactory.UniqueEntity<Node> ent =
                hpoUF.getOrCreateWithOutcome("id", id);
            Node node = ent.entity();
            if (ent.wasCreated()) {
                node.setProperty("name", toks[2]);
                node.addLabel(DISEASE_LABEL);
                node.addLabel(Label.label(toks[0]));
                node.setProperty(KEntity.TYPE_P, DISEASE_T);
            }

            try (IndexHits<Node> hits = index.get("id", toks[4])) {
                if (hits.hasNext()) {
                    Node p = hits.next();
                    RelationshipType type = RelationshipType.withName(toks[6]);
                    for (Relationship rel : p.getRelationships(type)) {
                        if (node.equals(rel.getOtherNode(p))) {
                            node = null;
                            break;
                        }
                    }

                    if (node != null) {
                        Relationship rel = p.createRelationshipTo(node, type);
                        rel.setProperty("xref", toks[5]);
                        addTextIndex (node, line);
                    }
                }
            }

            tx.success();
            return node;
        }
    }
    
    public void close () throws Exception {
        shutdown ();
    }

    public static class BuildIndex {
        // sbt hpo/'run-main blackboard.hpo.HpoDb$BuildIndex OUTDIR INDIR'
        public static void main (String[] argv) throws Exception {
            if (argv.length < 2) {
                System.err.println("Usage: "+BuildIndex.class.getName()
                                   +" OUTDIR INDIR");
                System.err.println
                    ("where OUTDIR is the output index directory "
                     +"and INDIR is a directory contains HPO\n"
                     +"OBO and annotation files "
                     +"downloaded from "
                     +"https://hpo.jax.org/app/download/ontology and\n"
                     +"https://hpo.jax.org/app/download/annotation, "
                     +"respectively.");
                System.exit(1);
            }

            File db = new File (argv[0]);
            try (HpoDb hpo = new HpoDb (null, db)) {
                hpo.build(new File (argv[1]));
            }
        }
    }
}
