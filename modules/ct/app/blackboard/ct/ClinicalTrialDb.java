package blackboard.ct;

import java.io.*;
import java.util.*;
import java.lang.reflect.Array;
import java.util.function.Consumer;
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
import blackboard.umls.UMLSKSource;

import com.google.inject.assistedinject.Assisted;

public class ClinicalTrialDb {
    static final String ALL_CONDITIONS =
        "https://clinicaltrials.gov/ct2/search/browse?brwse=cond_alpha_all";

    static final Label CONDITION_LABEL = Label.label("condition");
    static final Label INTERVENTION_LABEL = Label.label("intervention");
    static final Label META_LABEL =
        Label.label(ClinicalTrialDb.class.getName());
    static final Label UMLS_LABEL = Label.label("umls");

    static final RelationshipType MESH_RELTYPE =
        RelationshipType.withName("mesh");
    static final RelationshipType UMLS_RELTYPE =
        RelationshipType.withName("umls");
    
    final GraphDatabaseService gdb;
    final File dbdir;
    
    final Map<String, String> indexConfig = new HashMap<>();
    final Pattern cntregex;
    
    final WSClient wsclient;
    final MeshKSource mesh;
    final UMLSKSource umls;

    final UniqueFactory<Node> condUF;
    final UniqueFactory<Node> meshUF;
    final UniqueFactory<Node> umlsUF;

    @Inject
    public ClinicalTrialDb (WSClient wsclient, MeshKSource mesh,
                            UMLSKSource umls, ApplicationLifecycle lifecycle,
                            @Assisted File dbdir) {
        indexConfig.put("type", "fulltext");
        indexConfig.put("to_lower_case", "true");
        cntregex = Pattern.compile("\"([^\"]+)");

        gdb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbdir)
            //.setConfig(GraphDatabaseSettings.read_only, "true")
            .newGraphDatabase();

        try (Transaction tx = gdb.beginTx()) {
            condUF = new UniqueFactory.UniqueNodeFactory
                (getNodeIndex ("condition")) {
                    @Override
                    protected void initialize
                        (Node created, Map<String, Object> props) {
                        created.addLabel(CONDITION_LABEL);
                        created.setProperty("name", props.get("name"));
                        addTextIndex (created, (String)props.get("name"));
                    }
                };
            
            meshUF = new UniqueFactory.UniqueNodeFactory
                (getNodeIndex ("mesh")) {
                    @Override
                    protected void initialize
                        (Node created, Map<String, Object> props) {
                        created.addLabel(Mesh.DESC_LABEL);
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
                        created.setProperty("ui", props.get("ui"));
                        addTextIndex (created, (String)props.get("ui"));
                    }
                };
            tx.success();
        }
        
        try (Transaction tx = gdb.beginTx();
             ResourceIterator<Node> it = gdb.findNodes(META_LABEL)) {
            if (it.hasNext()) {
                Logger.debug("## "+dbdir
                             +" ClinicalTrial database initialized...");
            }
            else {
                Logger.debug("## "+dbdir
                             +" ClinicalTrial database hasn't "
                             +"been initialized...");
            }
            tx.success();
        }

        lifecycle.addStopHook(() -> {
                shutdown ();
                return CompletableFuture.completedFuture(null);
            });

        this.wsclient = wsclient;
        this.mesh = mesh;
        this.umls = umls;
        this.dbdir = dbdir;
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
        for (Condition cond : getConditions (skip, top)) {
            try (Transaction tx = gdb.beginTx()) {
                Node node = createNodeIfAbsent (cond);
                tx.success();
            }
        }
    }

    public List<Condition> getAllConditions () throws Exception {
        return getConditions (0, 0);
    }
    
    public List<Condition> getConditions (int skip, int top) throws Exception {
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

    Node createMeshNode (Node node, Entry entry) {
        List<Entry> entries = mesh.getMeshDb().getContext(entry.ui, 0, 10);
        Node meshnode = null;
        for (Entry e : entries) {
            if (e instanceof Descriptor) {
                UniqueFactory.UniqueEntity<Node> ent =
                    meshUF.getOrCreateWithOutcome("ui", e.ui);
                Node n = ent.entity();
                if (ent.wasCreated()) {
                    n.addLabel(Mesh.DESC_LABEL);
                    n.setProperty("ui", e.ui);
                    n.setProperty("name", e.name);
                    addTextIndex (node, e.ui);
                    addTextIndex (node, e.name);
                    
                    Relationship rel =
                        node.createRelationshipTo(n, MESH_RELTYPE);
                    rel.setProperty("ui", entry.ui);
                    rel.setProperty("name", entry.name);
                }
                meshnode = n;
                
                break;
            }
        }
        addTextIndex (node, entry.ui);
        addTextIndex (node, entry.name);
        
        return meshnode;
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
            createMeshNode (node, matched);
            cond.mesh = new Condition.Entry(matched.ui, matched.name);
            Logger.debug("\""+cond.name+"\" => "+matched.ui
                         +" \""+matched.name+"\"");
        }
    }

    Node createUMLSNode (Node node, JsonNode json) {
        UniqueFactory.UniqueEntity<Node> ent =
            umlsUF.getOrCreateWithOutcome("ui", json.get("ui").asText());
        Node umlsnode = ent.entity();
        if (ent.wasCreated()) {
            umlsnode.setProperty("name", json.get("name").asText());
            Relationship rel =
                node.createRelationshipTo(umlsnode, UMLS_RELTYPE);
        }
        return umlsnode;
    }

    void resolveUMLS (Node node, Condition cond) {
        try {
            JsonNode json = umls.getSearch(cond.name, 0, 10);
            if (json != null) {
                json = json.get(0);
                if (json.hasNonNull("ui") && json.hasNonNull("name")) {
                    cond.umls = new Condition.Entry
                        (json.get("ui").asText(),
                         json.get("name").asText());
                    if ("NONE".equals(cond.umls.ui))
                        cond.umls = null;
                    else {
                        Logger.debug
                            ("\""+cond.name+"\" => "+cond.umls.ui
                             +" \""+cond.umls.name+"\"");
                        createUMLSNode (node, json);
                    }
                }
            }
            else if (cond.mesh != null) {
                json = umls.getSource("MSH", cond.mesh.ui, "atoms/preferred");
                if (json != null && json.hasNonNull("concept")) {
                    String url = json.get("concept").asText();
                    int pos = url.lastIndexOf('/');
                    if (pos > 0) {
                        String ui = url.substring(pos+1);
                        json = umls.getCui(ui);
                        if (json != null) {
                            cond.umls = new Condition.Entry
                                (json.get("ui").asText(),
                                 json.get("name").asText());
                            createUMLSNode (node, json);
                        }
                    }
                }
                else {
                    Logger.error("Can't retrieve UMLS source for MSH "
                                 +cond.mesh.ui);
                }
            }
            else {
                Logger.error("Can't retrieve UMLS search result for \""
                             +cond.name+"\"");
            }
        }
        catch (Exception ex) {
            Logger.error("Can't match UMLS for condition "+cond.name, ex);
        }
    }

    Node createNodeIfAbsent (Condition cond) {
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
        
        return node;
    }
}
