package blackboard.mesh;

import java.io.*;
import java.util.*;
import java.lang.reflect.Array;
import java.util.function.Consumer;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CompletableFuture;

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
import play.cache.SyncCacheApi;

import com.google.inject.assistedinject.Assisted;

import blackboard.neo4j.Neo4j;

public class MeshDb extends Neo4j implements Mesh, AutoCloseable {
    final Map<String, Integer> files;
    final SyncCacheApi cache;

    @Inject
    public MeshDb (ApplicationLifecycle lifecycle,
                   @Assisted File dbdir, SyncCacheApi cache) {
        super (dbdir);

        files = new TreeMap<>();
        
        Node meta = getMetaNode ();
        try (Transaction tx = gdb.beginTx()) {
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
            throw new RuntimeException
                ("Not a valid MeSH database: "+dbdir);
        }
        
        if (lifecycle != null) {
            lifecycle.addStopHook(() -> {
                    shutdown ();
                    return CompletableFuture.completedFuture(null);
                });
        }
        
        this.cache = cache;
        Logger.debug("## "+dbdir+" mesh database initialized...");
    }

    public MeshDb (File dbdir) {
        this (null, dbdir, null);
    }

    public void close () throws Exception {
        shutdown ();
    }

    Index<Node> nodeIndex () {
        return getNodeIndex (EXACT_INDEX);
    }

    Entry instrument (Entry entry, Node node) {
        entry.ui = (String) node.getProperty("ui");
        entry.name = (String) node.getProperty("name");
        if (node.hasProperty("created"))
            entry.created = new Date ((Long)node.getProperty("created"));
        if (node.hasProperty("revised"))
            entry.revised = new Date ((Long)node.getProperty("revised"));
        if (node.hasProperty("established"))
            entry.established = new Date
                ((Long)node.getProperty("established"));
        return entry;
    }

    Qualifier instrument (Qualifier qual, Node node) {
        instrument ((Entry)qual, node);
        if (node.hasProperty("treeNumbers")) {
            String[] treeNumbers = (String[])node.getProperty("treeNumbers");
            for (String tr : treeNumbers)
                qual.treeNumbers.add(tr);
        }
        for (Relationship rel : node.getRelationships(CONCEPT_RELTYPE)) {
            Node n = rel.getOtherNode(node);
            qual.concepts.add(instrument (new Concept (), n));
        }
        return qual;
    }

    Concept instrument (Concept concept, Node node) {
        instrument ((Entry)concept, node);
        if (node.hasProperty("casn1"))
            concept.casn1 = (String)node.getProperty("casn1");
        if (node.hasProperty("regno"))
            concept.regno = (String)node.getProperty("regno");
        if (node.hasProperty("note"))
            concept.note = (String)node.getProperty("note");
        if (node.hasProperty("relatedRegno")) {
            String[] related = (String[])node.getProperty("relatedRegno");
            for (String r : related)
                concept.relatedRegno.add(r);
        }
        
        for (Relationship rel : node.getRelationships
                 (CONCEPT_RELTYPE, Direction.INCOMING)) {
            Node n = rel.getOtherNode(node);
            if (n.hasLabel(CONCEPT_LABEL)) {
                Relation r = new Relation ((String)n.getProperty("ui"),
                                           (String)n.getProperty("name"));
                r.scope = (String)rel.getProperty("name");
                concept.relations.add(r);
            }
        }
        
        for (Relationship rel : node.getRelationships(TERM_RELTYPE)) {
            Node n = rel.getOtherNode(node);
            Term term = instrument (new Term (), n);
            if (rel.hasProperty("preferred"))
                term.preferred = (Boolean)rel.getProperty("preferred");
            concept.terms.add(term);
        }
            
        return concept;
    }

    Term instrument (Term term, Node node) {
        instrument ((Entry)term, node);
        return term;
    }

    Qualifier[] getQualifiers (String... qualifiers) {
        List<Qualifier> quals = new ArrayList<>();
        if (qualifiers != null) {
            for (String qual : qualifiers) {
                try (IndexHits<Node> hits = nodeIndex().get("name", qual)) {
                    while (hits.hasNext()) {
                        Node qn = hits.next();
                        if (qn.hasLabel(QUAL_LABEL)) {
                            quals.add(instrument (new Qualifier (), qn));
                        }
                    }
                }
            }
        }
        return quals.toArray(new Qualifier[0]);
    }

    Concept[] getConcepts (Node node) {
        List<Concept> concepts = new ArrayList<>();
        for (Relationship rel : node.getRelationships(CONCEPT_RELTYPE)) {
            Node n = rel.getOtherNode(node);
            Concept concept = instrument (new Concept (), n);
            if (rel.hasProperty("preferred"))
                concept.preferred = (Boolean)rel.getProperty("preferred");
            concepts.add(concept);
        }
        return concepts.toArray(new Concept[0]);
    }

    Entry[] getPharmacologicalActions (Node node) {
        List<Entry> pharm = new ArrayList<>();
        for (Relationship rel : node.getRelationships
                 (SUBSTANCE_RELTYPE, Direction.INCOMING)) {
            Node n = rel.getOtherNode(node);
            pharm.add(instrument (new Entry (), n));
        }
        return pharm.toArray(new Entry[0]);
    }

    Descriptor instrument (Descriptor desc, Node node) {
        instrument ((Entry)desc, node);
        if (node.hasLabel(PA_LABEL)) {
            // all substance relationships
            for (Relationship rel : node.getRelationships(SUBSTANCE_RELTYPE)) {
                Node n = rel.getOtherNode(node);
                desc.pharm.add(new Entry ((String)n.getProperty("ui"),
                                          (String)n.getProperty("name")));
            }
        }

        if (node.hasProperty("qualifiers")) {
            Qualifier[] qualifiers = getQualifiers
                ((String[]) node.getProperty("qualifiers"));
            for (Qualifier qual : qualifiers)
                desc.qualifiers.add(qual);
        }

        for (Concept c : getConcepts (node))
            desc.concepts.add(c);

        if (node.hasProperty("treeNumbers")) {
            String[] treeNumbers = (String[]) node.getProperty("treeNumbers");
            for (String tr : treeNumbers)
                desc.treeNumbers.add(tr);
        }

        for (Entry entry : getPharmacologicalActions (node))
            desc.pharm.add(entry);
        
        return desc;
    }

    Descriptor[] getDescriptors
        (Node node, Iterable<Relationship> relationships) {
        List<Descriptor> desc = new ArrayList<>();
        for (Relationship rel : relationships) {
            Node n = rel.getOtherNode(node);
            Descriptor d = instrument (new Descriptor (), n);
            if (rel.hasProperty("qualifiers")) {
                String[] quals = (String[])rel.getProperty("qualifiers");
                for (Qualifier q : getQualifiers (quals)) {
                    d.qualifiers.add(q);
                }
            }
            desc.add(d);
        }
        return desc.toArray(new Descriptor[0]);
    }

    SupplementalDescriptor instrument
        (SupplementalDescriptor supp, Node node) {
        instrument ((Entry)supp, node);
        if (node.hasProperty("note"))
            supp.note = (String)node.getProperty("note");
        
        if (node.hasProperty("sources")) {
            String[] sources = (String[])node.getProperty("sources");
            for (String s : sources)
                supp.sources.add(s);
        }

        for (Descriptor d : getDescriptors
                 (node, node.getRelationships(MAPPED_RELTYPE))) {
            supp.mapped.add(d);
        }

        for (Descriptor d : getDescriptors
                 (node, node.getRelationships(INDEXED_RELTYPE))) {
            supp.indexed.add(d);
        }
        
        for (Concept c : getConcepts (node))
            supp.concepts.add(c);

        for (Entry e : getPharmacologicalActions (node))
            supp.pharm.add(e);
        
        return supp;
    }
    

    Entry toEntry (Node node) {
        Entry entry = null;
        if (node.hasLabel(DESC_LABEL)) {
            entry = instrument (new Descriptor (), node);
        }
        else if (node.hasLabel(SUPP_LABEL)) {
            entry = instrument (new SupplementalDescriptor (), node);
        }
        else if (node.hasLabel(QUAL_LABEL)) {
            entry = instrument (new Qualifier (), node);
        }
        else if (node.hasLabel(CONCEPT_LABEL)) {
            entry = instrument (new Concept (), node);
        }
        else if (node.hasLabel(TERM_LABEL)) {
            entry = instrument (new Term (), node);
        }
        else {
            Logger.warn("Node "+node.getId()+" is not a MeSH node!");
        }
        
        return entry;
    }

    Node getNode (String ui) {
        Node node = null;
        try (Transaction tx = gdb.beginTx();
             IndexHits<Node> hits = nodeIndex().get("ui", ui)) {
            if (hits.hasNext()) {
                node = hits.next();
            }
            tx.success();
        }
        return node;
    }
    
    Node getNode (Entry e) {
        return getNode (e.ui);
    }

    public synchronized void traverse (MeshVisitor visitor) {
        try (Transaction tx = gdb.beginTx();
             ResourceIterator<Node> nodes = gdb.findNodes(DESC_LABEL)) {
            while (nodes.hasNext()) {
                Node n = nodes.next();
                Entry e = toEntry (n);
                if (!visitor.visit
                    (e.ui, e.name,
                     ((Descriptor)e).treeNumbers.toArray(new String[0]))) {
                    break;
                }
            }
            tx.success();
        }
    }

    public Map<String, Integer> getSummary () { return files; }
    
    public Entry getEntry (String ui) {
        return cache.getOrElseUpdate("mesh/entry/"+ui, new Callable<Entry>() {
                public Entry call () {
                    Entry entry = null;
                    try (Transaction tx = gdb.beginTx()) {
                        Node node = getNode (ui);
                        if (node != null)
                            entry = toEntry (node);
                        tx.success();
                    }
                    return entry;
                }
            });
    }

    public Entry getEntryByName (String name) {
        return cache.getOrElseUpdate
            ("mesh/entry/"+name, new Callable<Entry>() {
                    public Entry call () {
                        Entry entry = null;
                        try (Transaction tx = gdb.beginTx();
                             IndexHits<Node> hits =
                             nodeIndex().get("name", name)) {
                            if (hits.hasNext()) {
                                entry = toEntry (hits.next());
                            }
                            tx.success();
                        }
                        return entry;
                    }
                });
    }

    public List<Descriptor> getDescriptorsByTreeNumber (String trNo) {
        return cache.getOrElseUpdate
            ("mesh/tree/"+trNo, new Callable<List<Descriptor>> () {
                    public List<Descriptor> call () {
                        List<Descriptor> descriptors = new ArrayList<>();
                        try (Transaction tx = gdb.beginTx()) {
                            Index<Node> index = nodeIndex ();
                            char end = trNo.charAt(trNo.length()-1);
                            if (end == '.') {
                                String tr = trNo.substring(0, trNo.length()-1);
                                try (IndexHits<Node> hits =
                                     index.get("parent", tr)) {
                                    for (Node n : hits) {
                                        Entry e = toEntry (n);
                                        descriptors.add((Descriptor)e);
                                    }
                                }
                            }
                            else {
                                try (IndexHits<Node> hits =
                                     index.get("treeNumbers", trNo)) {
                                    for (Node n : hits) {
                                        Entry e = toEntry (n);
                                        descriptors.add((Descriptor)e);
                                    }
                                }
                            }
                            tx.success();
                        }
                        return descriptors;
                    }
                });
    }

    public List<Entry> getParents (Entry e) {
        return getParents (e.ui);
    }
    
    public List<Entry> getParents (String ui) {
        return cache.getOrElseUpdate
            ("mesh/parent/"+ui, new Callable<List<Entry>> () {
                    public List<Entry> call () {
                        Node node = getNode (ui);
                        if (node != null) {
                            List<Entry> parents = new ArrayList<>();
                            try (Transaction tx = gdb.beginTx()) {
                                Set<Long> seen = new HashSet<>();
                                for (Relationship rel : node.getRelationships
                                         (PARENT_RELTYPE, Direction.OUTGOING)) {
                                    Node n = rel.getOtherNode(node);
                                    if (!seen.contains(n.getId())) {
                                        parents.add(toEntry (n));
                                        seen.add(n.getId());
                                    }
                                }
                                tx.success();
                            }
                            return parents;
                        }
                        return null;
                    }
                });
    }

    /*
     * can either be Descriptor or SupplementalDescriptor
     */
    public synchronized CommonDescriptor getDescriptor (Entry e) {
        start: do {
            if (e instanceof Descriptor
                || e instanceof SupplementalDescriptor) {
                return (CommonDescriptor)e;
            }
            
            List<Entry> entries = getContext (e.ui, 0, 10);
            for (Entry ctx : entries) {
                Logger.debug(e.getClass()+"[ui="+e.ui+"] => "+ctx.getClass()
                             +"[ui="+ctx.ui+"]");
                if (!e.getClass().equals(ctx.getClass())) {
                    e = ctx;
                    continue start;
                }
            }
            
            return null;
        }
        while (true);
    }

    public synchronized List<Entry> getContext (String ui, int skip, int top) {
        List<Entry> entries = new ArrayList<>();
        Node node = getNode (ui);
        if (node != null) {
            try (Transaction tx = gdb.beginTx()) {
                if (node.hasLabel(TERM_LABEL)) {
                    // return concept
                    Relationship rel = node.getSingleRelationship
                        (TERM_RELTYPE, Direction.BOTH);
                    entries.add(toEntry (rel.getOtherNode(node)));
                }
                else if (node.hasLabel(CONCEPT_LABEL)) {
                    for (Relationship rel :
                             node.getRelationships(CONCEPT_RELTYPE))
                        entries.add(toEntry (rel.getOtherNode(node)));
                }
                else if (node.hasLabel(QUAL_LABEL)) {
                    Map<String, Object> params = new HashMap<>();
                    params.put("q", node.getProperty("name"));
                    try (Result result = gdb.execute
                         ("match(n) where $q in n.qualifiers "
                          +"return n order by id(n)", params)) {
                        int cnt = 0, total = skip+top;
                        while (result.hasNext() && cnt < total) {
                            Map<String, Object> row = result.next();
                            if (cnt < skip) {
                            }
                            else {
                                Node n = (Node)row.get("n");
                                entries.add(toEntry (n));
                            }
                            ++cnt;
                        }
                    }
                }
                else if (node.hasLabel(SUPP_LABEL)
                         || node.hasLabel(DESC_LABEL)) {
                    entries.add(toEntry (node));
                }
                else {
                    Logger.warn("Unknown node: "+ui);
                }
                tx.success();
            }
        }
        return entries;
    }

    public List<Entry> search (final String q, final int top,
                               final String... label) {
        return cache.getOrElseUpdate
            ("mesh/search/"+q+"/"+top, new Callable<List<Entry>>() {
                    public List<Entry> call () {
                        return _search (q, top, label);
                    }
                });
    }
    
    public List<Entry> _search (String q, int top, String... label) {
        if (q.charAt(0) == '"') {
        }
        else {
            StringBuilder sb = new StringBuilder ();
            for (int i = 0; i < q.length(); ++i) {
                char ch = q.charAt(i);
                switch (ch) {
                case '+': case '-': case '&': case '|': case '!':
                case '(': case ')': case '{': case '}': case '[':
                case ']': case '^': case '~': case '*': case '?':
                case ':': case '\\': case '/': case '"':
                    sb.append("\\");
                    break;
                }
                sb.append(ch);
            }
            q = sb.toString();
        }
        
        List<Entry> matches = new ArrayList<>();
        try (Transaction tx = gdb.beginTx()) {
            Index<Node> index = getTextIndex ();
            Set<Label> labels = new HashSet<>();
            if (label != null) {
                for (String l : label) {
                    labels.add(Label.label(l));
                }
            }
            
            try (IndexHits<Node> hits = index.query
                 ("text", new QueryContext(q).top(top).sortByScore())) {
                while (hits.hasNext()) {
                    Node n = hits.next();
                    if (labels.isEmpty()) {
                        Entry e = toEntry (n);
                        e.score = hits.currentScore();
                        matches.add(e);
                    }
                    else {
                        for (Label l : labels) {
                            if (n.hasLabel(l)) {
                                Entry e = toEntry (n);
                                e.score = hits.currentScore();
                                matches.add(e);
                                break;
                            }
                        }
                    }
                }
            }
            catch (Exception ex) {
                Logger.error("Query ("+q+") failed!", ex);
            }
            tx.success();
            Logger.debug("Query: ("+q+") => "+matches.size()+" match(es)!");
        }
        
        return matches;
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length == 0) {
            System.err.println("Usage: MeshDb MESHDB [TREENUMBERS...]");
            System.exit(1);
        }

        try (MeshDb mesh = new MeshDb (null, new File (argv[0]), null)) {
            if (argv.length == 1) {
                mesh.traverse((ui, name, tr) -> {
                        System.out.print(ui+"\t"+name);
                        if (tr.length > 0) {
                            System.out.print("\t"+tr[0]);
                            for (int i = 1; i < tr.length; ++i)
                                System.out.print(" "+tr[i]);
                        }
                        System.out.println();
                        return true;
                    });
            }
            else {
                for (int i = 1; i < argv.length; ++i) {
                    System.out.println("****** ["+argv[i]+"] ******");
                    for (Descriptor d :
                             mesh.getDescriptorsByTreeNumber(argv[i])) {
                        System.out.println(d.ui+"\t"+d.name+"\t"+d.treeNumbers);
                    }
                }
            }
        }
    }
}
