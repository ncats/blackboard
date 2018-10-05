package blackboard.ct;

import java.io.*;
import java.util.*;
import java.util.zip.*;
import java.util.function.Function;
import java.util.function.Consumer;
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
import blackboard.mesh.Concept;
import blackboard.mesh.Term;
import blackboard.mesh.Descriptor;
import blackboard.mesh.SupplementalDescriptor;

import blackboard.umls.UMLSKSource;
import blackboard.umls.MatchedConcept;

import blackboard.KType;
import blackboard.KEntity;
import blackboard.neo4j.Neo4j;
import com.google.inject.assistedinject.Assisted;

public class ClinicalTrialDb extends Neo4j implements KType {
    static final String ALL_CONDITIONS =
        "https://clinicaltrials.gov/ct2/search/browse?brwse=cond_alpha_all";
    static final String DOWNLOAD_FIELDS =
        "https://clinicaltrials.gov/ct2/results/download_fields";

    static final Label CONDITION_LABEL = Label.label(CONDITION_T);
    static final Label INTERVENTION_LABEL = Label.label(INTERVENTION_T);
    static final Label UMLS_LABEL = Label.label("umls");
    static final Label CLINICALTRIAL_LABEL = Label.label(CLINICALTRIAL_T);
    static final Label CONCEPT_LABEL = Label.label(CONCEPT_T);

    static final RelationshipType ATOM_RELTYPE =
        RelationshipType.withName("atom");
    static final RelationshipType MESH_RELTYPE =
        RelationshipType.withName("mesh");
    static final RelationshipType UMLS_RELTYPE =
        RelationshipType.withName("umls");
    static final RelationshipType RESOLVE_RELTYPE =
        RelationshipType.withName("resolve");
    static final RelationshipType CLINICALTRIAL_RELTYPE =
        RelationshipType.withName(CLINICALTRIAL_T);
    static final RelationshipType CONDITION_RELTYPE =
        RelationshipType.withName(CONDITION_T);
    static final RelationshipType INTERVENTION_RELTYPE =
        RelationshipType.withName(INTERVENTION_T);
    
    final Pattern cntregex;
    final WSClient wsclient;
    final MeshKSource mesh;
    final UMLSKSource umls;
    final Map<String, EntityRepo> repo;

    class EntityRepo extends UniqueFactory.UniqueNodeFactory {
        final String name;
        final String type;

        EntityRepo (String name, String type) {
            super (getNodeIndex (name));
            this.name = name;
            this.type = type;
        }

        @Override
        protected void initialize (Node created, Map<String, Object> props) {
            created.setProperty("created", System.currentTimeMillis());
            created.addLabel(Label.label(type));
            for (Map.Entry<String, Object> me : props.entrySet()) {
                created.setProperty(me.getKey(), me.getValue());
            }
            created.setProperty(KEntity.TYPE_P, type);
        }

        IndexHits<Node> get (String name, Object value) {
            return getNodeIndex(this.name).get(name, value);
        }
    }

    class MeshEntityRepo extends EntityRepo {
        MeshEntityRepo () {
            super ("mesh", MESH_T);
        }
    }

    class UMLSEntityRepo extends EntityRepo {
        UMLSEntityRepo () {
            super ("umls", CONCEPT_T);
        }
        @Override
        protected void initialize (Node created, Map<String, Object> props) {
            super.initialize(created, props);
            addTextIndex (created, (String)props.get("ui"));
        }
    }

    class ConditionEntityRepo extends EntityRepo {
        ConditionEntityRepo () {
            super ("condition", CONDITION_T);
        }
        @Override
        protected void initialize (Node created, Map<String, Object> props) {
            super.initialize(created, props);
            String name = (String)props.get("name");
            if (name != null) {
                addTextIndex (created, name);
                resolve (created, new Condition (name));
            }
            else {
                Logger.warn("Condition node "+created.getId()+" has no name!");
            }
        }
    }

    class InterventionEntityRepo extends EntityRepo {
        final Map<String, String> uniis = new HashMap<>();
        
        InterventionEntityRepo () {
            super ("intervention", INTERVENTION_T);
            try {
                uniis.putAll(loadUniiToNames ());
            }
            catch (Exception ex) {
                Logger.error("Unable to load UNII mappings!", ex);
            }
        }

        @Override
        protected void initialize (Node created, Map<String, Object> props) {
            super.initialize(created, props);
            String unii = (String)props.get("unii");
            if (unii != null) {
                addTextIndex (created, unii);
                // get other information about this unii from gsrs
                instrument (created, unii);
            }
        }

        void instrument (Node node, String unii) {
            String name = uniis.get(unii);
            if (name != null) {
                int why = name.indexOf(", LICENSE HOLDER UNSPECIFIED");
                if (why > 0)
                    name = name.substring(0, why);
                node.setProperty("name", name);
                addTextIndex (node, name);
                
                // now map to umls & mesh using name
                resolve (node, new Intervention (unii, name));
            }
        }
        
        void instrumentOld (Node node, String unii) throws Exception {
            WSRequest req = wsclient.url
                ("https://drugs.ncats.io/api/v1/substances")
                .setQueryParameter("filter", "approvalID='"+unii+"'");
            WSResponse res = req.get().toCompletableFuture().get();
            
            Logger.debug(req.getUrl()+"..."+res.getStatus());
            if (res.getStatus() == 200) {
                JsonNode n = res.asJson();
                if ((n = n.get("content")) != null) {
                    if (n.size() > 0) {
                        String name = n.get(0).get("_name").asText();
                        int why = name.indexOf(", LICENSE HOLDER UNSPECIFIED");
                        if (why > 0)
                            name = name.substring(0, why);
                        node.setProperty("name", name);
                        addTextIndex (node, name);
                        
                        // now map to umls & mesh using name
                        resolve (node, new Intervention (unii, name));
                    }
                }
                else {
                    Logger.warn(unii+" has no content!");
                }
            }
            else {
                Logger.error(req.getUrl()+" return status "+res.getStatus());
            }
        }
    }

    class StudyEntityRepo extends EntityRepo {
        StudyEntityRepo () {
            super ("study", CLINICALTRIAL_T);
        }
        @Override
        protected void initialize (Node created, Map<String, Object> props) {
            super.initialize(created, props);
            addTextIndex (created, (String)props.get("nct_id"));
        }        
    }

    @Inject
    public ClinicalTrialDb (WSClient wsclient, MeshKSource mesh,
                            UMLSKSource umls, ApplicationLifecycle lifecycle,
                            @Assisted File dbdir) {
        super (dbdir);
        cntregex = Pattern.compile("\"([^\"]+)");
        
        this.wsclient = wsclient;
        this.mesh = mesh;
        this.umls = umls;

        repo = new TreeMap<>();
        try (Transaction tx = gdb.beginTx()) {
            for (EntityRepo er : new EntityRepo[]{
                    new MeshEntityRepo (),
                    new UMLSEntityRepo (),
                    new ConditionEntityRepo (),
                    new InterventionEntityRepo (),
                    new StudyEntityRepo ()
                }) {
                repo.put(er.name, er);
            }
            tx.success();
        }

        lifecycle.addStopHook(() -> {
                shutdown ();
                return CompletableFuture.completedFuture(null);
            });        
    }

    @Override
    public void shutdown () throws Exception {
        Logger.debug("## shutting down ClinicalTrialDb instance "+dbdir+"...");
        super.shutdown();
        wsclient.close();
    }

    Map<String, String> loadUniiToNames () throws Exception {
        WSRequest req = wsclient
            .url("https://fdasis.nlm.nih.gov/srs/download/srs/UNIIs.zip")
            .setFollowRedirects(true);
        WSResponse res = req.get().toCompletableFuture().get();

        Map<String, String> lut = new HashMap<>();
        if (200 != res.getStatus()) {
            Logger.warn(req.getUrl()+" returns status "+res.getStatus());
        }
        else {
            try (ZipInputStream zis =
                 new ZipInputStream (res.getBodyAsStream())) {
                for (ZipEntry ze; (ze = zis.getNextEntry()) != null; ) {
                    if (ze.getName().startsWith("UNII Names")) {
                        BufferedReader br = new BufferedReader
                            (new InputStreamReader (zis));
                        String line = br.readLine(); // skip header
                        while ((line = br.readLine()) != null) {
                            String[] toks = line.split("\t");
                            if (toks.length == 4) {
                                String unii = toks[2].trim();
                                if (!lut.containsKey(unii)) {
                                    lut.put(unii, toks[3].trim());
                                    //Logger.debug(unii+" "+toks[3]);
                                }
                            }
                        }
                    }
                }
                Logger.debug("Loaded "+lut.size()+" UNIIs!");
            }
        }
        
        return lut;
    }
    
    void test (int skip, int top) throws Exception {
        int count = 0;
        /*
        for (Condition cond : getConditionsFromCt (skip, top)) {
            try (Transaction tx = gdb.beginTx()) {
                UniqueFactory.UniqueEntity<Node> uf = createNodeIfAbsent (cond);
                Node node = uf.entity();
                ++count;
                Logger.info("Node "+count+"/"+node.getId()+": "+cond.name);
                tx.success();
            }
        }
        */

        try (Transaction tx = gdb.beginTx()) {
            for (String c : new String[]{
                    "Chromosome 5q Deletion",
                    "5q- syndrome",
                    "Amyloidosis AA",
                    "Amyloidosis, Familial",
                    "Hereditary Hemorrhagic Telangiectasia"
                }) {
                createNodeIfAbsent (new Condition (c));
            }
            tx.success();            
        }
    }

    Condition toCondition (Node n) {
        Condition cond = new Condition ((String)n.getProperty("name"));
        if (n.hasProperty("count"))
            cond.count = (Integer)n.getProperty("count");
        
        if (n.hasProperty("rare")) {
            cond.rare = (Boolean)n.getProperty("rare");
        }
        
        for (Relationship rel :
                 n.getRelationships(MESH_RELTYPE, UMLS_RELTYPE)) {
            Node xn = rel.getOtherNode(n);
            Entry entry = new Entry((String)xn.getProperty("ui"),
                                    (String)xn.getProperty("name"));
            if (rel.isType(MESH_RELTYPE)) {
                cond.mesh.add(entry);
            }
            else { // UMLS
                cond.umls.add(entry);
            }
        }
        return cond;
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
                conditions.add(toCondition (n));
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

        Logger.debug("+++ parsing..."+req.getUrl());
        if (200 != res.getStatus()) {
            Logger.error(req.getUrl()+" returns status "+res.getStatus());
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

    UniqueFactory.UniqueEntity<Node> getOrCreateNode
        (blackboard.mesh.CommonDescriptor entry) {
        try (Transaction tx = gdb.beginTx()) {
            UniqueFactory.UniqueEntity<Node> ent =
                repo.get("mesh").getOrCreateWithOutcome("ui", entry.getUI());
            Node n = ent.entity();
            if (ent.wasCreated()) {
                n.setProperty("ui", entry.getUI());
                n.setProperty("name", entry.getName());
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

                    blackboard.mesh.Concept concept =
                        desc.getPreferredConcept();
                    if (concept != null) {
                        List<String> regno = new ArrayList<>();
                        if (concept.regno != null) {
                            regno.add(concept.regno);
                            addTextIndex (n, concept.regno);
                        }
                        
                        for (String r : concept.relatedRegno) {
                            regno.add(r);
                            addTextIndex (n, r);
                        }
                        
                        if (!regno.isEmpty()) {
                            n.setProperty
                                ("regno", regno.toArray(new String[0]));
                        }
                    }
                }
                else if (entry instanceof SupplementalDescriptor) {
                    SupplementalDescriptor supp = (SupplementalDescriptor)entry;
                    if (supp.note != null) {
                        n.setProperty("note", supp.note);
                        addTextIndex (n, supp.note);
                    }
                    n.addLabel(Mesh.SUPP_LABEL);
                }
                
                addTextIndex (n, entry.getUI());
                addTextIndex (n, entry.getName());
            }
            tx.success();

            return ent;
        }
    }
    
    List<Entry> resolveMesh (Node node, String term) {
        List<blackboard.mesh.Entry> results =
            mesh.getMeshDb().search("\""+term+"\"", 5);
        List<blackboard.mesh.CommonDescriptor> matches = new ArrayList<>();
        Map<String, blackboard.mesh.Entry> scores = new HashMap<>();
        for (blackboard.mesh.Entry r : results) {
            blackboard.mesh.CommonDescriptor desc =
                mesh.getMeshDb().getDescriptor(r);
            if (desc != null && !scores.containsKey(desc.getUI())) {
                matches.add(desc);
                scores.put(desc.getUI(), r);
            }
        }

        List<Entry> entries = new ArrayList<>();
        if (!matches.isEmpty()) {
            for (blackboard.mesh.CommonDescriptor desc: matches) {
                UniqueFactory.UniqueEntity<Node> uf = getOrCreateNode (desc);
                if (uf.wasCreated()) {
                    Logger.debug("MeSH node "+uf.entity().getId()
                                 +": \""+term+"\" => "+desc.getUI()
                                 +" \""+desc.getName()+"\"");
                }
                
                Relationship rel = node.createRelationshipTo
                    (uf.entity(), MESH_RELTYPE);
                blackboard.mesh.Entry e = scores.get(desc.getUI());
                if (e.score != null)
                    rel.setProperty("score", e.score);

                blackboard.mesh.Concept concept =
                    desc.getPreferredConcept();
                if (concept != null) {
                    rel.setProperty("ui", concept.ui);
                    rel.setProperty("name", concept.name);
                    entries.add(new Entry (concept.ui, concept.name));
                }
                else {
                    Logger.debug("MeSH descriptor "+desc.getUI()
                                 +" has no preferred concept!");
                }
            }
        }

        return entries;
    }

    void resolveMesh (Node node, EntryMapping mapping) {
        List<Entry> entries = resolveMesh (node, mapping.name);
        mapping.mesh.addAll(entries);
    }

    UniqueFactory.UniqueEntity<Node> getOrCreateNode
        (blackboard.umls.Concept concept) {
        try (Transaction tx = gdb.beginTx()) {
            UniqueFactory.UniqueEntity<Node> ent =
                repo.get("umls").getOrCreateWithOutcome("ui", concept.cui);
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

    Relationship createRelationshipIfAbsent
        (Node from, Node to, RelationshipType type, String name, Object value) {
        try (Transaction tx = gdb.beginTx()) {
            Relationship rel = null;
            for (Relationship r : from.getRelationships(type)) {
                if (to.equals(r.getOtherNode(from))
                    && value.equals(r.getProperty(name))) {
                    rel = r;
                    break;
                }
            }

            if (rel == null) {
                rel = from.createRelationshipTo(to, type);
                rel.setProperty(name, value);
            }
            tx.success();
            
            return rel;
        }
    }
    
    List<Entry> resolveUMLS (Node node, String term) {
        List<Entry> entries = new ArrayList<>();
        try {
            List<MatchedConcept> results = umls.findConcepts(term, 0, 5);
            for (blackboard.umls.MatchedConcept mc : results) {
                Entry entry = new Entry (mc.concept.cui, mc.concept.name);
                
                Node n = getOrCreateNode(node, mc).entity();
                Logger.debug
                    ("UMLS node "+n.getId()+": \""+term+"\" => "
                     +entry.ui+" \""+entry.name+"\" score="+mc.score);
                
                entries.add(entry);
            }
        }
        catch (Exception ex) {
            Logger.error("Can't match UMLS for term \""+term+"\"", ex);
        }
        
        return entries;
    }

    void resolveUMLS (Node node, EntryMapping mapping) {
        List<Entry> entries = resolveUMLS (node, mapping.name);
        mapping.umls.addAll(entries);
        
        for (Entry mesh : mapping.mesh) {
            // now see if this mesh is an atom of this umls concept
            try {
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
                            createRelationshipIfAbsent
                                (rel.getOtherNode(node), un, ATOM_RELTYPE,
                                 "scui", mesh.ui);
                        }
                    }
                    
                    EntityRepo index = repo.get("umls");
                    // now check consistency with any existing umls concepts
                    for (Entry e : mapping.umls) {
                        if (!concept.cui.equals(e.ui)) {                        
                            try (IndexHits<Node> hits = index.get("ui", e.ui)) {
                                Node n = hits.getSingle();
                                for (blackboard.umls.Relation r
                                         : concept.findRelations(e.ui)) {
                                    RelationshipType rt =
                                        RelationshipType.withName(r.type);
                                    Relationship rel =
                                        n.createRelationshipTo(un, rt);
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
            catch (Exception ex) {
                Logger.error("Can't retrieve UMLS concept for scui="
                             +mesh.ui, ex);
            }
        }
    }

    UniqueFactory.UniqueEntity<Node> createNodeIfAbsent (Condition cond) {
        try (Transaction tx = gdb.beginTx()) {
            UniqueFactory.UniqueEntity<Node> ent =
                repo.get("condition").getOrCreateWithOutcome("name", cond.name);
            Node node = ent.entity();
            if (ent.wasCreated()) {
                if (cond.count != null)
                    node.setProperty("count", cond.count);
            }
                        
            tx.success();
            return ent;
        }
    }

    void resolve (Node node, EntryMapping mapping) {
        resolveMesh (node, mapping);
        resolveUMLS (node, mapping);
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
        EntityRepo index = repo.get("condition");
        if (value.getClass().isArray()) {
            int len = Array.getLength(value);
            for (int i = 0; i < len; ++i) {
                resolveStudyConditions (study, Array.get(value, i));
            }
        }
        else {
            UniqueFactory.UniqueEntity<Node> cond =
                createNodeIfAbsent (new Condition ((String)value));
            if (cond.wasCreated()) {
            }
            else {
                // make sure it's not already connected
                for (Relationship rel
                         : study.getRelationships(CONDITION_RELTYPE)) {
                    if (rel.getOtherNode(study).equals(cond.entity()))
                        return;
                }
            }
            
            Relationship rel = study.createRelationshipTo
                (cond.entity(), CONDITION_RELTYPE);
            rel.setProperty("value", value);
        }
    }

    UniqueFactory.UniqueEntity<Node>
        resolveInterventionMesh (Node node, String name) throws Exception {
        EntityRepo index = repo.get("intervention");
        // resolve the intervention name to mesh concepts
        List<blackboard.mesh.Entry>  entries
            = mesh.getMeshDb().search(name, 10);
        if (!entries.isEmpty()) {
            blackboard.mesh.Entry e = entries.get(0);
            if (e.score > 1.f) {
                blackboard.mesh.CommonDescriptor desc =
                    mesh.getMeshDb().getDescriptor(e);
                
                Set<String> syns = new TreeSet<>();
                Set<String> code = new TreeSet<>();
                blackboard.mesh.Concept concept = null;                
                for (blackboard.mesh.Concept c : desc.getConcepts()) {
                    syns.add(c.name);
                    if (c.regno != null)
                        code.add(c.regno);
                    if (c.preferred)
                        concept = c;
                }
                
                if (concept != null) {
                    Logger.debug("Intervention \""+name+"\" maps to "
                                 +e.getClass()+" "+e.ui+" \""+e.name+"\" "
                                 +e.score+" "+" => "+desc.getUI()+" \""
                                 +desc.getName()+"\" "
                                 +concept.regno+" syns="+syns
                                 +" code="+code);
                }
                else {
                    Logger.warn("Descriptor "+desc.getUI()
                                +" has no preferred concept; this cannot be!!!");
                }
            }
            else {
                Logger.warn("Intervention \""+name+"\" matched to "
                            +e.getClass()+" "+e.ui+" \""+e.name+"\" "
                            +e.score+" is too low!");
            }
            
            /*
            UniqueFactory.UniqueEntity<Node> uf = concept.regno != null
            ? index.getOrCreateWithOutcome("unii", concept.regno)
            : index.getOrCreateWithOutcome("scui", concept.ui);
            
            if (uf.wasCreated()) {

                    Relationship rel = node.createRelationshipTo
                        (uf.entity(), MESH_RELTYPE);
                    if (e.score != null)
                        rel.setProperty("score", e.score);
                    rel.setProperty("ui", e.ui);
                    rel.setProperty("name", e.name);
                }
            }
            */
        }
        else {
            Logger.debug("Intervention \""+name+"\" doesn't map to a MeSH "
                         +"conept!");
            // try umls here
        }
        
        return null;
    }
    
    UniqueFactory.UniqueEntity<Node> createStudyIfAbsent
        (Element study) throws Exception {
        NodeList nodelist = study.getElementsByTagName("nct_id");
        if (nodelist.getLength() == 0) {
            Logger.warn("Study contains no element nct_id!");
            return null;
        }

        UniqueFactory.UniqueEntity<Node> ent = null;        
        try (Transaction tx = gdb.beginTx()) {
            ent = repo.get("study").getOrCreateWithOutcome
                ("nct_id", ((Element)nodelist.item(0)).getTextContent());
            
            Node node = ent.entity();
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
                value = setProperty (node, "intervention", study);
            }
            tx.success();
        }
        
        return ent;
    }

    int fetchClinicalTrials
        (int top, int chunk,
         Consumer<UniqueFactory.UniqueEntity<Node>> consumer)
        throws Exception {
        
        //String term = (String) node.getProperty("name");
        WSRequest req = wsclient.url(DOWNLOAD_FIELDS)
            //.setQueryParameter("cond", "\""+term+"\"")
            .setQueryParameter("down_fmt", "xml")
            .setQueryParameter("down_flds", "all")
            .setQueryParameter("down_count", String.valueOf(top))
            .setQueryParameter("down_chunk", String.valueOf(chunk));
        WSResponse res = req.get().toCompletableFuture().get();
        
        if (200 != res.getStatus()) {
            Logger.warn(req.getUrl()+" return status "+res.getStatus());
            return 0;
        }

        Document doc = res.asXml();
        int count = Integer.parseInt
            (doc.getDocumentElement()
             .getAttributes().getNamedItem("count").getNodeValue());
                                                        
        NodeList studies = doc.getElementsByTagName("study");
        int ns = studies.getLength();
        Logger.debug("## fetching "+req.getUrl()+"..."+(ns*chunk)
                     +"/"+count);

        for (int i = 0; i < ns; ++i) {
            Element study = (Element)studies.item(i);
            NodeList nl = study.getElementsByTagName("study_types");
            if (nl != null && nl.getLength() > 0) {
                // only consider interventional studies for now!
                Element type = (Element)nl.item(0);
                String val = type.getTextContent();
                
                if ("Interventional".equalsIgnoreCase(val)) {
                    UniqueFactory.UniqueEntity<Node> n =
                        createStudyIfAbsent (study);
                    if (n.wasCreated()) {
                        Logger.debug(String.format("%1$5d: ", top*(chunk-1)+i+1)
                                     +"++ study created: "
                                     +n.entity().getProperty("nct_id")
                                     +" "+n.entity().getProperty("title"));
                    }
                    
                    if (consumer != null)
                        consumer.accept(n);
                }
                else {
                    Logger.debug("Skipping "
                                 +((Element)study.getElementsByTagName
                                   ("nct_id").item(0)).getTextContent()
                                 +" because it's "+val);
                }
            }
        }
        
        return ns;
    }

    public Long getCount (String label) {
        Long count = null;
        try (Transaction tx = gdb.beginTx();
             Result result = gdb.execute
             ("match(n:`"+label+"`) return count(n) as cnt")) {
            if (result.hasNext()) {
                Map<String, Object> row = result.next();
                count = (Long) row.get("cnt");
            }
            tx.success();
        }
        return count;
    }

    public Long getClinicalTrialCount () {
        return getCount (CLINICALTRIAL_LABEL.name());
    }
    public Long getConditionCount () {
        return getCount (CONDITION_LABEL.name());
    }
    public Long getInterventionCount () {
        return getCount (INTERVENTION_LABEL.name());
    }

    int fetchClinicalTrials
        (Consumer<UniqueFactory.UniqueEntity<Node>> consumer)
        throws Exception {
        return fetchClinicalTrials (0, consumer);
    }
    
    int fetchClinicalTrials
        (int max, Consumer<UniqueFactory.UniqueEntity<Node>> consumer)
        throws Exception {
        int chunk = 0, n = 0, bucket = 100;
        do {
            int c = fetchClinicalTrials (bucket, ++chunk, consumer);
            n += c;
            if (c < bucket || (max > 0 && n >= max))
                break;
        }
        while (true);
        
        return n;
    }
    
    public Condition getCondition (String name) {
        Condition cond = null;
        try (Transaction tx = gdb.beginTx();
             IndexHits<Node> hits = repo.get("condition").get("name", name)) {
            Node n = hits.getSingle();
            if (n != null) {
                cond = toCondition (n);
            }
            tx.success();
            return cond;
        }
    }

    public int mapInterventions (InputStream is) throws IOException {
        BufferedReader br = new BufferedReader (new InputStreamReader (is));
        int mapped = 0;
        
        String header = br.readLine();
        // should look something like this:
        // NCT_ID  DATE    UNII    CONDITION       STATUS
        Logger.debug("Parsing intervention stream: "+header);
        if (header.split("\t").length != 5) {
            Logger.warn("Expecting there are to be 5 columns!");
            return mapped;
        }

        EntityRepo study = repo.get("study");
        EntityRepo intervention = repo.get("intervention");
        for (String line; (line = br.readLine()) != null; ) {
            String[] toks = line.split("\t");
            if (toks.length < 2)
                continue;
            
            String unii = toks[2];
            if (!"FAKE_FOR_PLACEBO".equals(unii)) {
                String nctid = toks[0];
                try (Transaction tx = gdb.beginTx();
                     IndexHits<Node> hits = study.get("nct_id", nctid)) {
                    Node ct = hits.getSingle();
                    if (ct != null) {
                        UniqueFactory.UniqueEntity<Node> uf =
                            intervention.getOrCreateWithOutcome("unii", unii);
                        for (Relationship rel
                                 : ct.getRelationships(INTERVENTION_RELTYPE)) {
                            if (unii.equals(rel.getProperty("value"))) {
                                unii = null;
                                break;
                            }
                        }
                        
                        if (unii != null) {
                            Relationship rel = ct.createRelationshipTo
                                (uf.entity(), INTERVENTION_RELTYPE);
                            rel.setProperty("value", unii);
                            
                            Node n = uf.entity();
                            Logger.debug(toks[0]+" -> "+unii
                                         +" "+(n.hasProperty("name")
                                               ? n.getProperty("name") : ""));
                            ++mapped;
                        }
                    }
                    else {
                        //Logger.warn("Hmm.. you don't seem to have "+nctid);
                    }
                    tx.success();
                }
            }
        }
        
        return mapped;
    }

    public void initialize (int max, InputStream is) throws Exception {
        try (Transaction tx = gdb.beginTx()) {
            /*
            UniqueFactory.UniqueEntity<Node> uf =
                repo.get("intervention")
                .getOrCreateWithOutcome("unii", "GEB06NHM23");
            */
            
            int n = fetchClinicalTrials (max, null);
            Logger.debug(">>>> "+n+" clinical trials fetched!");
            n = mapInterventions (is);
            Logger.debug(">>>> "+n+" interventions mapped!");
            Node node = gdb.getNodeById(metanode);
            node.setProperty("updated", System.currentTimeMillis());

            tx.success();
        }
    }

    List<Condition> getStudyConditions (Node n) {
        List<Condition> conditions = new ArrayList<>();        
        for (Relationship rel : n.getRelationships(CONDITION_RELTYPE)) {
            Node cn = rel.getOtherNode(n);
            Condition cond = new Condition ((String)cn.getProperty("name"));
            
            // now get all umls mapping with null score (exact match)
            Set<Node> umls = new HashSet<>();
            for (Relationship r : cn.getRelationships(UMLS_RELTYPE)) {
                Node u = r.getOtherNode(cn);
                if (!r.hasProperty("score")) {
                    if (umls.add(u)) {
                        cond.umls.add(new Entry
                                      ((String)u.getProperty("ui"),
                                       (String)u.getProperty("name")));
                        /*
                        for (Relationship ro : u.getRelationships()) {
                            Node o = ro.getOtherNode(u);
                            if (o.hasLabel(CONCEPT_LABEL) && umls.add(o)) {
                                cond.umls.add(new Entry
                                              ((String)o.getProperty("ui"),
                                               (String)o.getProperty("name")));
                            }
                        }
                        */
                    }
                }
            }
            
            // now check to see if there's a mesh entry
            for (Node u : umls) {
                for (Relationship r : u.getRelationships(ATOM_RELTYPE)) {
                    if (r.hasProperty("scui")) { // mesh concept
                        Node m = r.getOtherNode(u);
                        cond.mesh.add(new Entry
                                      ((String)m.getProperty("ui"),
                                       (String)m.getProperty("name")));
                    }
                }
            }
            
            conditions.add(cond);
        }
        
        return conditions;
    }

    List<Intervention> getStudyInterventions (Node n) {
        List<Intervention> interventions = new ArrayList<>();
        for (Relationship rel : n.getRelationships(INTERVENTION_RELTYPE)) {
            Node in = rel.getOtherNode(n);
            Intervention inv = new Intervention
                ((String)in.getProperty("unii"),
                 (String)in.getProperty("name"));
            Set<Node> umls = new HashSet<>();
            for (Relationship r : in.getRelationships(UMLS_RELTYPE)) {
                if (!r.hasProperty("score")) {
                    Node u = r.getOtherNode(in);
                    if (umls.add(u)) {
                        inv.umls.add(new Entry ((String)u.getProperty("ui"),
                                                (String)u.getProperty("name")));
                        // also add other umls nodes directly related to
                        //  this one
                        for (Relationship ro : u.getRelationships()) {
                            Node o = ro.getOtherNode(u);
                            if (o.hasLabel(CONCEPT_LABEL) && umls.add(o)) {
                                inv.umls.add
                                    (new Entry
                                     ((String)o.getProperty("ui"),
                                      (String)o.getProperty("name")));
                            }
                        }
                    }
                }
            }

            for (Relationship r : in.getRelationships(MESH_RELTYPE)) {
                Node m = r.getOtherNode(in);
                Relationship mr = m.getSingleRelationship
                    (ATOM_RELTYPE, Direction.OUTGOING);
                
                boolean add = false;
                if (mr != null && umls.contains(mr.getOtherNode(m))) {
                    add = true;
                }
                else if (m.hasProperty("regno")) {
                    String[] regno = (String[])m.getProperty("regno");
                    for (String s : regno) {
                        if (s.equals(inv.unii)) {
                            add = true;
                            break;
                        }
                    }
                }
                
                if (add) {
                    inv.mesh.add(new Entry ((String)m.getProperty("ui"),
                                            (String)m.getProperty("name")));
                }
            }
            interventions.add(inv);
        }
        return interventions;
    }
    
    ClinicalTrial toStudy (Node n) {
        if (!n.hasLabel(CLINICALTRIAL_LABEL)) 
            throw new IllegalArgumentException ("Not a clinical trial node");
        
        ClinicalTrial ct = new ClinicalTrial ((String)n.getProperty("nct_id"));
        ct.title = (String)n.getProperty("title");
        ct.status = (String)n.getProperty("status");
        Object phase = n.getProperty("phase");
        if (phase.getClass().isArray()) {
            int len = Array.getLength(phase);
            for (int i = 0; i < len; ++i)
                ct.phase.add((String)Array.get(phase, i));
        }
        else {
            ct.phase.add((String)phase);
        }
        ct.enrollment = (Integer)n.getProperty("enrollment");
        ct.conditions.addAll(getStudyConditions (n));
        ct.interventions.addAll(getStudyInterventions (n));
        
        return ct;
    }

    public ClinicalTrial getStudy (String nctId) {
        ClinicalTrial ct = null;
        try (Transaction tx = gdb.beginTx();
             IndexHits<Node> hits =
             getNodeIndex("study").get("nct_id", nctId)) {
            Node n = hits.getSingle();
            if (n != null) {
                ct = toStudy (n);
            }
            tx.success();
        }
        return ct;
    }

    public List<ClinicalTrial> findStudiesForConcept
        (String ui, String concept, int skip, int top) {
        Map<String, Object> params = new HashMap<>();
        params.put("ui", ui);
        params.put("skip", skip);
        params.put("top", skip+top);
        List<ClinicalTrial> studies = new ArrayList<>();
        try (Transaction tx = gdb.beginTx();
             Result results = gdb.execute
             ("match(n:clinicaltrial)-[]-()-[:`"+concept+"`]-(m{ui:$ui}) "
              +"return n order by n.nct_id skip $skip limit $top", params)) {
            while (results.hasNext()) {
                Map<String, Object> row = results.next();
                Node n = (Node) row.get("n");
                studies.add(toStudy (n));
            }
            tx.success();
        }
        return studies;
    }
}
