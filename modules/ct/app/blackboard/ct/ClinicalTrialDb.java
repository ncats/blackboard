package blackboard.ct;

import java.io.*;
import java.util.*;
import java.util.function.Function;
import java.lang.reflect.Array;
import java.util.function.Consumer;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CompletableFuture;
import java.util.regex.*;

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

import javax.inject.Inject;
import com.fasterxml.jackson.databind.JsonNode;

import play.Logger;
import play.libs.Json;
import play.libs.ws.*;
import play.inject.ApplicationLifecycle;

import org.w3c.dom.Document;
import org.xml.sax.InputSource;
//import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import blackboard.mesh.MeshKSource;
import blackboard.mesh.MeshDb;
import blackboard.mesh.Mesh;
import blackboard.mesh.Entry;
import blackboard.mesh.Concept;
import blackboard.mesh.Term;
import blackboard.mesh.Descriptor;
import blackboard.mesh.SupplementalDescriptor;
import blackboard.umls.UMLSKSource;
import blackboard.umls.MatchedConcept;

import blackboard.KType;
import blackboard.KEntity;
import com.google.inject.assistedinject.Assisted;

public class ClinicalTrialDb implements KType {
    static final String ALL_CONDITIONS =
        "https://clinicaltrials.gov/ct2/search/browse?brwse=cond_alpha_all";
    static final String DOWNLOAD_FIELDS =
        "https://clinicaltrials.gov/ct2/results/download_fields";

    static final Label CONDITION_LABEL = Label.label("condition");
    static final Label INTERVENTION_LABEL = Label.label("intervention");
    static final Label META_LABEL =
        Label.label(ClinicalTrialDb.class.getName());
    static final Label UMLS_LABEL = Label.label("umls");
    static final Label CLINICALTRIAL_LABEL = Label.label("clinicaltrial");

    static final RelationshipType MESH_RELTYPE =
        RelationshipType.withName("mesh");
    static final RelationshipType UMLS_RELTYPE =
        RelationshipType.withName("umls");
    static final RelationshipType RESOLVE_RELTYPE =
        RelationshipType.withName("resolve");
    static final RelationshipType CLINICALTRIAL_RELTYPE =
        RelationshipType.withName("clinicaltrial");
    
    final GraphDatabaseService gdb;
    final File dbdir;
    
    final Map<String, String> indexConfig = new HashMap<>();
    final Pattern cntregex;
    
    final WSClient wsclient;
    final MeshKSource mesh;
    final UMLSKSource umls;

    UniqueFactory<Node> condUF;
    UniqueFactory<Node> meshUF;
    UniqueFactory<Node> umlsUF;
    UniqueFactory<Node> studyUF;

    final ReentrantLock lock = new ReentrantLock ();

    @Inject
    public ClinicalTrialDb (WSClient wsclient, MeshKSource mesh,
                            UMLSKSource umls, ApplicationLifecycle lifecycle,
                            @Assisted File dbdir) {
        indexConfig.put("type", "fulltext");
        indexConfig.put("to_lower_case", "true");
        cntregex = Pattern.compile("\"([^\"]+)");

        gdb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbdir)
            .newGraphDatabase();

        this.wsclient = wsclient;
        this.mesh = mesh;
        this.umls = umls;
        this.dbdir = dbdir;
        
        init ();
        lifecycle.addStopHook(() -> {
                shutdown ();
                return CompletableFuture.completedFuture(null);
            });        
    }

    void init ()  {
        try (Transaction tx = gdb.beginTx()) {
            condUF = new UniqueFactory.UniqueNodeFactory
                (getNodeIndex ("condition")) {
                    @Override
                    protected void initialize
                        (Node created, Map<String, Object> props) {
                        created.addLabel(CONDITION_LABEL);
                        created.setProperty(KEntity.TYPE_P, DISEASE_T);
                        created.setProperty("name", props.get("name"));
                        addTextIndex (created, (String)props.get("name"));
                    }
                };
            
            meshUF = new UniqueFactory.UniqueNodeFactory
                (getNodeIndex ("mesh")) {
                    @Override
                    protected void initialize
                        (Node created, Map<String, Object> props) {
                        created.setProperty(KEntity.TYPE_P, MESH_T);
                        created.setProperty("ui", props.get("ui"));
                        addTextIndex (created, (String)props.get("ui"));
                    }
                };
            
            umlsUF = new UniqueFactory.UniqueNodeFactory
                (getNodeIndex ("umls")) {
                    @Override
                    protected void initialize
                        (Node created, Map<String, Object> props) {
                        created.addLabel(UMLS_LABEL);
                        created.setProperty(KEntity.TYPE_P, CONCEPT_T);
                        created.setProperty("ui", props.get("ui"));
                        addTextIndex (created, (String)props.get("ui"));
                    }
                };

            studyUF = new UniqueFactory.UniqueNodeFactory
                (getNodeIndex ("clinicaltrial")) {
                    @Override protected void initialize
                        (Node created, Map<String, Object> props) {
                        created.addLabel(CLINICALTRIAL_LABEL);
                        for (Map.Entry<String, Object> me : props.entrySet())
                            created.setProperty(me.getKey(), me.getValue());
                    }
                };
            
            try (ResourceIterator<Node> it = gdb.findNodes(META_LABEL)) {
                if (it.hasNext()) {
                    Node node = it.next();
                    Logger.debug("## "+dbdir
                                 +" ClinicalTrial database initialized..."
                                 +new Date ((Long)node.getProperty("created")));
                }
                else {
                    Node node = gdb.createNode(META_LABEL);
                    node.setProperty("created", System.currentTimeMillis());
                    Logger.debug("## "+dbdir
                                 +" ClinicalTrial database hasn't "
                                 +"been initialized...");
                }
            }
            
            tx.success();
        }
    }        

    Index<Node> getNodeIndex (String name) {
        return gdb.index().forNodes(ClinicalTrialDb.class.getName()+"."+name);
    }

    void addTextIndex (Node node, String text) {
        Index<Node> index = gdb.index().forNodes
            (ClinicalTrialDb.class.getName()+".text", indexConfig);
        if (text != null)
            index.add(node, "text", text);
        else
            index.remove(node, "text", text);
    }
        
    
    public void shutdown () throws Exception {
        Logger.debug("## shutting down ClinicalTrialDb instance "+dbdir+"...");
        gdb.shutdown();
        wsclient.close();
    }
    
    public void build (int skip, int top) throws Exception {
        /*
        int count = 0;
        for (Condition cond : getConditionsFromCt (skip, top)) {
            try (Transaction tx = gdb.beginTx()) {
                Node node = createNodeIfAbsent (cond);
                ++count;
                Logger.info("Node "+count+"/"+node.getId()+": "+cond.name);
                tx.success();
            }
        }
        */
        
        try (Transaction tx = gdb.beginTx()) {
            for (String c : new String[]{
                    //"Chromosome 5q Deletion",
                    //"5q- syndrome",
                    "Amyloidosis AA",
                    "Amyloidosis, Familial"
                }) {
                createNodeIfAbsent (new Condition (c));
                tx.success();
            }
        }
    }

    public List<Condition> getConditions (int skip, int top) throws Exception {
        List<Condition> conditions = new ArrayList<>();
        try (Transaction tx = gdb.beginTx()) {
            Map<String, Object> params = new TreeMap<>();
            params.put("skip", skip);
            params.put("top", top);
            Result results = gdb.execute
                ("match(n:`condition`) return n skip $skip limit $top", params);
            while (results.hasNext()) {
                Map<String, Object> row = results.next();
                Node n = (Node) row.get("n");
                Condition cond = new Condition ((String)n.getProperty("name"));
                if (n.hasProperty("count"))
                    cond.count = (Integer)n.getProperty("count");
                
                if (n.hasProperty("rare")) {
                    cond.rare = (Boolean)n.getProperty("rare");
                }

                for (Relationship rel :
                         n.getRelationships(MESH_RELTYPE, UMLS_RELTYPE)) {
                    Node xn = rel.getOtherNode(n);
                    Condition.Entry entry = new Condition.Entry
                        ((String)xn.getProperty("ui"),
                         (String)xn.getProperty("name"));
                    if (rel.isType(MESH_RELTYPE)) {
                        cond.mesh.add(entry);
                    }
                    else { // UMLS
                        cond.umls.add(entry);
                    }
                }
                conditions.add(cond);
            }
            results.close();
            tx.success();
        }
        return conditions;
    }
    
    List<Condition> getAllConditionsFromCt () throws Exception {
        return getConditions (0, 0);
    }
    
    List<Condition> getConditionsFromCt (int skip, int top) throws Exception {
        WSRequest req = wsclient.url(ALL_CONDITIONS)
            .setFollowRedirects(true);
        Logger.debug("+++ retrieving all conditions..."+req.getUrl());
        
        List<Condition> conditions = new ArrayList<>();
        WSResponse res = req.get().toCompletableFuture().get();

        Logger.debug("+++ parsing..."+res.getUri());
        if (200 != res.getStatus()) {
            Logger.error(res.getUri()+" returns status "+res.getStatus());
            return conditions;
        }
        
        BufferedReader br = new BufferedReader
            (new InputStreamReader (res.getBodyAsStream()));
        
        int count = 0, total = skip+top;
        for (String line; (line = br.readLine()) != null;) {
            int start = line.indexOf('[');
            if (start > 0) {
                int end = line.indexOf(']', start+1);
                if (end > start) {
                    line = line.substring(start+1, end);
                    start = line.indexOf("\">");
                    if (start > 0) {
                        end = line.indexOf("</a>", start);
                        if (end > start) {
                            String name = line.substring(start+2, end);
                            Matcher m = cntregex.matcher
                                (line.substring(end+5));
                            if (m.find()) {
                                if (++count > skip) {
                                    int cnt = Integer.parseInt
                                        (m.group(1).trim()
                                         .replaceAll(",",""));
                                    Condition cond = new Condition (name);
                                    cond.count = cnt;
                                    
                                    conditions.add(cond);
                                    if (count >= total && total > 0)
                                        break;
                                }
                            }
                        }
                    }
                }
            }
        }
        
        return conditions;
    }

    UniqueFactory.UniqueEntity<Node> getOrCreateNode (Entry entry) {
        try (Transaction tx = gdb.beginTx()) {
            UniqueFactory.UniqueEntity<Node> ent =
                meshUF.getOrCreateWithOutcome("ui", entry.ui);
            Node n = ent.entity();
            if (ent.wasCreated()) {
                n.setProperty("ui", entry.ui);
                n.setProperty("name", entry.name);
                n.setProperty(KEntity.TYPE_P, MESH_T);
                if (entry instanceof Descriptor) {
                    Descriptor desc = (Descriptor)entry;
                    if (desc.annotation != null) {
                        n.setProperty("annotation", desc.annotation);
                        addTextIndex (n, desc.annotation);
                    }
                    
                    if (!desc.treeNumbers.isEmpty()) {
                        n.setProperty
                            ("treeNumbers",
                             desc.treeNumbers.toArray(new String[0]));
                        for (String tr : desc.treeNumbers)
                            addTextIndex (n, tr);
                    }
                    n.addLabel(Mesh.DESC_LABEL);
                }
                else if (entry instanceof SupplementalDescriptor) {
                    SupplementalDescriptor supp = (SupplementalDescriptor)entry;
                    if (supp.note != null) {
                        n.setProperty("note", supp.note);
                        addTextIndex (n, supp.note);
                    }
                    n.addLabel(Mesh.SUPP_LABEL);
                }
                
                addTextIndex (n, entry.ui);
                addTextIndex (n, entry.name);
            }
            tx.success();

            return ent;
        }
    }
    
    UniqueFactory.UniqueEntity<Node> getOrCreateNode (Node node, Entry entry) {
        // from MeSH's concept to Descriptor or SupplementalDescriptor
        List<Entry> entries = mesh.getMeshDb().getContext(entry.ui, 0, 10);
        try (Transaction tx = gdb.beginTx()) {
            UniqueFactory.UniqueEntity<Node> ent = null;            
            if (!entries.isEmpty()) {
                Entry e = null;
                for (int i = 0; i < entries.size(); ++i) {
                    if (entries.get(i) instanceof Descriptor
                        || entries.get(i) instanceof SupplementalDescriptor) {
                        e = entries.get(i);
                        break;
                    }
                }
                
                if (e == null) {
                    Logger.warn
                        ("No Descriptor or SupplementalDescriptor context "
                         +"for "+entry.ui+"!");
                    e = entries.get(0);
                }
                
                ent = getOrCreateNode (e);
                if (ent.wasCreated()) {
                    Relationship rel =
                        node.createRelationshipTo(ent.entity(), MESH_RELTYPE);
                    rel.setProperty("ui", entry.ui);
                    rel.setProperty("name", entry.name);
                    if (entry.score != null)
                        rel.setProperty("score", entry.score);
                }
            }
            tx.success();
            
            return ent;
        }
    }
       
    void resolveMesh (Node node, Condition cond) {
        List<Entry> results = mesh.getMeshDb().search("\""+cond.name+"\"", 10);
        Entry matched = null;
        for (Entry r : results) {
            if (r instanceof Term) {
                List<Entry> entries = mesh.getMeshDb().getContext(r.ui, 0, 10);
                for (Entry e : entries) {
                    if (e instanceof Concept) {
                        matched = e;
                        matched.score = r.score;
                        break;
                    }
                }
                
                if (matched != null)
                    break;
            }
            else if (r instanceof Concept) {
                matched = r;
                break;
            }
        }
        
        if (matched != null) {
            Node n = getOrCreateNode(node, matched).entity();
            cond.mesh.add(new Condition.Entry(matched.ui, matched.name));
            Logger.debug("MeSH node "+n.getId()
                         +": \""+cond.name+"\" => "+matched.ui
                         +" \""+matched.name+"\"");
        }
    }

    UniqueFactory.UniqueEntity<Node> getOrCreateNode
        (blackboard.umls.Concept concept) {
        try (Transaction tx = gdb.beginTx()) {
            UniqueFactory.UniqueEntity<Node> ent =
                umlsUF.getOrCreateWithOutcome("ui", concept.cui);
            Node un = ent.entity();
            if (ent.wasCreated()) {
                un.setProperty("name", concept.name);
                for (blackboard.umls.SemanticType semtype
                         : concept.semanticTypes) {
                    un.addLabel(Label.label(semtype.name));
                }
                
                for (blackboard.umls.Definition def : concept.definitions) {
                    un.setProperty("definition/"+def.source, def.description);
                    addTextIndex (un, def.description);
                }
            }
            tx.success();
            return ent;
        }
    }
    
    UniqueFactory.UniqueEntity<Node> getOrCreateNode
        (Node node, blackboard.umls.MatchedConcept mc) {
        try (Transaction tx = gdb.beginTx()) {
            UniqueFactory.UniqueEntity<Node> ent = getOrCreateNode (mc.concept);
            if (ent.wasCreated()) {
                Relationship rel = node.createRelationshipTo
                    (ent.entity(), UMLS_RELTYPE);
                if (mc.score != null) {
                    rel.setProperty("score", mc.score);
                    rel.setProperty("cui", mc.cui);
                    rel.setProperty("name", mc.name);
                }
            }
            else {
                Node n = ent.entity();
                for (Relationship r : n.getRelationships(UMLS_RELTYPE)) {
                    if (r.getOtherNode(n).equals(node)) {
                        n = null;
                        break;
                    }
                }
                
                if (n != null) {
                    Relationship rel = node.createRelationshipTo
                        (n, UMLS_RELTYPE);
                    if (mc.score != null) {
                        rel.setProperty("score", mc.score);
                        rel.setProperty("cui", mc.cui);
                        rel.setProperty("name", mc.name);
                    }
                }
            }
            tx.success();
            return ent;
        }
    }

    void resolveUMLS (Node node, Condition cond) {
        try {
            List<MatchedConcept> results = umls.findConcepts(cond.name);
            for (blackboard.umls.MatchedConcept mc : results) {
                Condition.Entry e = new Condition.Entry
                    (mc.concept.cui, mc.concept.name);
                Node n = getOrCreateNode(node, mc).entity();
                Logger.debug
                    ("UMLS node "+n.getId()+": \""+cond.name+"\" => "
                     +e.ui+" \""+e.name+"\" score="+mc.score);
                cond.umls.add(e);
            }
            
            for (Condition.Entry mesh : cond.mesh) {
                // now see if this mesh is an atom of this umls concept
                blackboard.umls.Concept concept =
                    umls.getConcept("scui", mesh.ui);
                Logger.debug("MeSH "+mesh.ui+" => UMLS "
                             +(concept != null
                               ?(concept.cui+" "+concept.name):""));
                if (concept != null) {
                    Node un = getOrCreateNode(concept).entity();
                    for (Relationship rel :
                             node.getRelationships(MESH_RELTYPE)) {
                        if (rel != null && mesh.ui.equals
                            (rel.getProperty("ui"))) {
                            Logger.debug("umls "+un.getId()+" <=> mesh "
                                         +rel.getOtherNode(node).getId());
                            Relationship r = rel.getOtherNode(node)
                                .createRelationshipTo
                                (un, RelationshipType.withName("atom"));
                            r.setProperty("scui", mesh.ui);
                        }
                    }

                    Index<Node> index = getNodeIndex ("umls");
                    // now check consistency with any existing umls concepts
                    for (Condition.Entry e : cond.umls) {
                        if (!concept.cui.equals(e.ui)) {                        
                            try (IndexHits<Node> hits = index.get("ui", e.ui)) {
                                Node n = hits.getSingle();
                                for (blackboard.umls.Relation r
                                         : concept.findRelations(e.ui)) {
                                    RelationshipType rt =
                                        RelationshipType.withName(r.type);
                                    Relationship rel =
                                        un.createRelationshipTo(n, rt);
                                    rel.setProperty("rui", r.rui);
                                    if (r.attr != null)
                                        rel.setProperty("value", r.attr);
                                    if (r.source != null)
                                        rel.setProperty("source", r.source);
                                }
                            }
                        }
                    }
                }
                else {
                    Logger.error("Can't retrieve UMLS source for MSH "
                                 +mesh.ui);
                }
            }
        }
        catch (Exception ex) {
            Logger.error("Can't match UMLS for condition \""
                         +cond.name+"\"", ex);
        }
    }

    Node createNodeIfAbsent (Condition cond) {
        try (Transaction tx = gdb.beginTx()) {
            UniqueFactory.UniqueEntity<Node> ent =
                condUF.getOrCreateWithOutcome("name", cond.name);
            Node node = ent.entity();
            if (ent.wasCreated()) {
                node.setProperty("name", cond.name);
                if (cond.count != null)
                    node.setProperty("count", cond.count);
                addTextIndex (node, cond.name);
            }
            
            resolveMesh (node, cond);
            resolveUMLS (node, cond);
            
            tx.success();
            return node;
        }
    }

    static Object setProperty (Node node, String prop, Element elm) {
        return setProperty (node, prop, elm, prop, null);
    }
    
    static Object setProperty (Node node, String prop,
                                Element elm, String tag) {
        return setProperty (node, prop, elm, tag, null);
    }
    
    static Object setProperty (Node node, String prop,
                               Element elm, String tag,
                               Function<String, Object> func) {
        NodeList nodelist = elm.getElementsByTagName(tag);
        int len = nodelist.getLength();
        Object value = null;
        if (len > 0) {
            if (len == 1) {
                String v = ((Element)nodelist.item(0)).getTextContent();
                node.setProperty
                    (prop, value = func != null ? func.apply(v): v);
            }
            else {
                String[] values = new String[len];
                for (int i = 0; i < len; ++i) {
                    values[i] = ((Element)nodelist.item(i)).getTextContent();
                }
                node.setProperty(prop, values);
                value = values;
            }
        }
        return value;
    }

    void resolveStudyConditions (Node study, Object value) {
        Index<Node> index = getNodeIndex ("condition");        
        if (value.getClass().isArray()) {
            int len = Array.getLength(value);
            for (int i = 0; i < len; ++i) {
                resolveStudyConditions (study, Array.get(value, i));
            }
        }
        else {
            try (IndexHits<Node> hits = index.get("name", value)) {
                Node n = hits.getSingle();
                if (n != null) {
                    for (Relationship rel :
                             study.getRelationships(CLINICALTRIAL_RELTYPE)) {
                        if (rel.hasProperty("value")
                            && value.equals(rel.getProperty("value")))
                            return;
                    }
                    
                    Relationship rel = study.createRelationshipTo
                        (n, CLINICALTRIAL_RELTYPE);
                    rel.setProperty("value", value);
                }
            }
        }
    }

    Node createStudyIfAbsent (Element study) {
        Node node = null;
        NodeList nodelist = study.getElementsByTagName("nct_id");
        if (nodelist.getLength() == 0) {
        }
        else {
            try (Transaction tx = gdb.beginTx()) {
                UniqueFactory.UniqueEntity<Node> ent =
                    studyUF.getOrCreateWithOutcome
                    ("nct_id", ((Element)nodelist.item(0)).getTextContent());
                
                node = ent.entity();
                if (ent.wasCreated()) {
                    String title = (String)setProperty (node, "title", study);
                    addTextIndex (node, title);
                    setProperty (node, "status", study);
                    setProperty (node, "study_results", study);
                    
                    Object value = setProperty (node, "condition", study);
                    if (value != null) {
                        resolveStudyConditions (node, value);
                    }
                    
                    setProperty (node, "phase", study);
                    setProperty (node, "enrollment", study,
                                 "enrollment", c -> Integer.parseInt(c));
                    setProperty (node, "study_types", study);
                    setProperty (node, "intervention", study);
                }
                tx.success();
            }
        }
        
        return node;
    }

    int fetchClinicalTrials (Node node, int top) throws Exception {
        return fetchClinicalTrials (node, top, 1);
    }
    
    int fetchClinicalTrials (Node node, int top, int chunk) throws Exception {
        String term = (String) node.getProperty("name");
        WSResponse res = wsclient.url(DOWNLOAD_FIELDS)
            .setQueryParameter("cond", "\""+term+"\"")
            .setQueryParameter("down_fmt", "xml")
            .setQueryParameter("down_flds", "all")
            .setQueryParameter("down_count", String.valueOf(top))
            .setQueryParameter("down_chunk", String.valueOf(chunk))
            .get().toCompletableFuture().get();
        
        if (200 != res.getStatus()) {
            Logger.error(res.getUri()+" return status "+res.getStatus());
            return 0;
        }

        Document doc = res.asXml();
        int count = Integer.parseInt
            (doc.getDocumentElement()
             .getAttributes().getNamedItem("count").getNodeValue());
                                                        
        NodeList studies = doc.getElementsByTagName("study");
        int ns = studies.getLength();
        Logger.debug("## fetching "+res.getUri()+"..."+(ns*chunk)
                     +"/"+count);
        
        for (int i = 0; i < ns; ++i) {
            Element study = (Element)studies.item(i);
            Node n = createStudyIfAbsent (study);
            Logger.debug(String.format("%1$5d: ", i+1)
                         +"++ study created: "+n.getProperty("nct_id")
                         +" "+n.getProperty("title"));
        }
        
        return ns;
    }

    public void mapAllConditions () throws Exception {
        try (Transaction tx = gdb.beginTx();
             ResourceIterator<Node> it = gdb.findNodes(CONDITION_LABEL)) {
            while (it.hasNext()) {
                Node cond = it.next();
                int n = mapCondition (cond);
                Logger.debug(n+" clinical trial(s) mapped for \""
                             +cond.getProperty("name")+"\"...");
                for (Relationship rel :
                         cond.getRelationships(CLINICALTRIAL_RELTYPE)) {
                    Node node = rel.getOtherNode(cond);
                    Logger.debug("++ "+node.getProperty("nct_id")+" "
                                 +node.getProperty("name"));
                }
            }
            tx.success();
        }
    }

    int mapCondition (Node node) throws Exception {
        return mapCondition (node, 500);
    }
    
    int mapCondition (Node node, int top) throws Exception {
        int chunk = 0, n = 0;
        do {
            int c = fetchClinicalTrials (node, top, ++chunk);
            n += c;
            if (c < top)
                break;
        }
        while (true);
        
        return n;
    }
    
    public int mapCondition (String name) throws Exception {
        try (Transaction tx = gdb.beginTx()) {
            int n = 0;
            try (IndexHits<Node> hits = getNodeIndex("condition")
                 .get("name", name)) {
                Node node = hits.getSingle();
                if (node != null) {
                    n = mapCondition (node);
                }
                Logger.debug(">>> "+n+" clinical trials mapped for \""+
                             name+"\" <<<");
            }
            tx.success();
            
            return n;
        }
    }

    public void resolve (String name) throws Exception {
        try (Transaction tx = gdb.beginTx();
             IndexHits<Node> hits = getNodeIndex("condition")
             .get("name", name)) {
            Node node = hits.getSingle();
            tx.success();
        }
    }
}
