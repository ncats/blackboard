package blackboard.pharos;

import java.util.*;
import java.util.function.BiConsumer;
import java.net.URLEncoder;
import java.util.concurrent.*;

import javax.inject.Inject;
import javax.inject.Named;

import play.Logger;
import play.Configuration;
import play.libs.ws.*;
import play.inject.ApplicationLifecycle;
import play.libs.F;
import akka.actor.ActorSystem;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import blackboard.*;
import static blackboard.KEntity.*;

public class PharosKSource implements KSource {
    interface Resolver {
        void resolve (JsonNode json, KNode kn, KGraph kg);
    }
    
    private final ActorSystem actorSystem;
    private final WSClient wsclient;
    private final KSourceProvider ksp;
    
    @Inject
    public PharosKSource (ActorSystem actorSystem, WSClient wsclient,
                          @Named("pharos") KSourceProvider ksp,
                          ApplicationLifecycle lifecycle) {
        this.actorSystem = actorSystem;
        this.wsclient = wsclient;
        this.ksp = ksp;

        lifecycle.addStopHook(() -> {
                wsclient.close();
                return F.Promise.pure(null);
            });
        
        Logger.debug("$"+ksp.getId()+": "+ksp.getName()
                     +" initialized; provider is "+ksp);
    }

    public void execute (KGraph kgraph, KNode... nodes) {
        Logger.debug("$"+ksp.getId()
                     +": executing on KGraph "+kgraph.getId()
                     +" \""+kgraph.getName()+"\"");
        
        if (nodes == null || nodes.length == 0)
            nodes = kgraph.getNodes();

        for (KNode kn : nodes) {
            switch (kn.getType()) {
            case "query":
                seedQuery ((String)kn.get("term"), kn, kgraph);
                break;
                
            case "disease":
                seedDisease (kn, kgraph);
                break;
                
            case "protein":
                seedTarget (kn, kgraph);
                break;
                
            case "drug":
                seedLigand (kn, kgraph);
                break;          
            }
        }
    }
    
    void seedQuery (String term, KNode kn, KGraph kg) {
        Logger.debug(">>> seedQuery \""+term+"\"");
        try {
            Map<String, String> q = new HashMap<>();
            q.put("q", "\""+term+"\"");
            q.put("facet","IDG Development Level/Tclin");
            
            resolve (ksp.getUri()+"/targets/search",
                     q, kn, kg, this::resolveTargets);
            
            resolve (ksp.getUri()+"/ligands/search",
                     q, kn, kg, this::resolveLigands);
            
            resolve (ksp.getUri()+"/diseases/search",
                     q, kn, kg, this::resolveDiseases);
        }
        catch (Exception ex) {
            Logger.error("Unable to utf encode query \""+term+"\"", ex);
        }
    }

    void seedTarget (KNode kn, KGraph kg) {
        Logger.debug(">>> seedTarget \""+kn.getName()+"\"");
        String uri = (String) kn.get(URI_P);
        if (uri != null && uri.startsWith(ksp.getUri())) {
            // argh.. should update the pharos api to allow list for filter
            resolve (uri+"/links(kind=ix.idg.models.Ligand)", null,
                     kn, kg, this::resolveLinks);
            resolve (uri+"/links(kind=ix.idg.models.Disease)", null,
                     kn, kg, this::resolveLinks);
        }
        else if (kn.getName() != null) {
            Map<String, String> query = new HashMap<>();
            query.put("filter", "name='"+kn.getName()+"'");
            resolve (ksp.getUri()+"/targets", query, 
                     kn, kg, this::resolveTargets);
        }
    }

    void seedLigand (KNode kn, KGraph kg) {
        Logger.debug(">>> seedLigand \""+kn.getName()+"\"");
        String uri = (String) kn.get(URI_P);
        if (uri != null && uri.startsWith(ksp.getUri())) {
            resolve (uri+"/links(kind=ix.idg.models.Target)", null,
                     kn, kg, this::resolveLinks);
            resolve (uri+"/links(kind=ix.idg.models.Disease)", null,
                     kn, kg, this::resolveLinks);
        }
        else if (kn.getName() != null) {
            Map<String, String> query = new HashMap<>();
            query.put("filter", "name='"+kn.getName()+"'");
            resolve (ksp.getUri()+"/ligands", query,
                     kn, kg, this::resolveLigands);
        }
    }

    void seedDisease (KNode kn, KGraph kg) {
        Logger.debug(">>> seedDisease \""+kn.getName()+"\"");
        String uri = (String) kn.get(URI_P);
        if (uri != null && uri.startsWith(ksp.getUri())) {      
            resolve (uri+"/links(kind=ix.idg.models.Target)", null,
                     kn, kg, this::resolveLinks);
            resolve (uri+"/links(kind=ix.idg.models.Ligand)", null,
                     kn, kg, this::resolveLinks);
        }
        else if (kn.getName() != null) {
            Map<String, String> query = new HashMap<>();
            query.put("filter", "name='"+kn.getName()+"'");
            resolve (ksp.getUri()+"/diseases", query,
                     kn, kg, this::resolveDiseases);
        }
    }

    void resolve (String url, Map<String, String> params,
                  KNode kn, KGraph kg, Resolver resolver) {
        WSRequest req = wsclient.url(url).setFollowRedirects(true);
        if (params != null) {
            Logger.debug(url);
            for (Map.Entry<String, String> me : params.entrySet()) {
                //Logger.debug(".."+me.getKey()+": "+me.getValue());
                req = req.setQueryParameter(me.getKey(), me.getValue());
            }
        }
        //Logger.debug("+++ resolving..."+req.getUrl());
        
        try {   
            WSResponse res = req.get().toCompletableFuture().get();
            JsonNode json = res.asJson();
            resolver.resolve(json, kn, kg);
        }
        catch (Exception ex) {
            Logger.error("Can't resolve url: "+url, ex);
        }
    }
    
    void resolve (String entity, JsonNode json, KNode kn, KGraph kg,
                  BiConsumer<JsonNode, Map<String, Object>> consumer) {
        String uri = null;
        if (json.hasNonNull("uri"))
            uri = json.get("uri").asText();
        
        JsonNode content = json.get("content"); 
        for (int i = 0; i < content.size(); ++i) {
            JsonNode jn = content.get(i);
            long id = jn.get("id").asLong();
            String name = jn.get("name").asText();
            Map<String, Object> props = new TreeMap<>();
            props.put(URI_P, ksp.getUri()+"/"+entity+"("+id+")");
            props.put(NAME_P, name);
            consumer.accept(jn, props);
            
            KNode node = kg.createNodeIfAbsent(props, URI_P);
            if (node.getId() != kn.getId()) {
                node.addTag("KS:"+ksp.getId());
                props.clear();
                props.put("value", uri);
                kg.createEdgeIfAbsent(kn, node, "resolve", props, null);
                Logger.debug(node.getId()+"..."+name);
            }
        }
        Logger.debug("uri: "+json.get(URI_P).asText()+"..."+content.size());    
    }

    void resolveTargets (JsonNode json, KNode kn, KGraph kg) {
        resolve ("targets", json, kn, kg, (jn, props) -> {
                props.put(TYPE_P, "protein");       
                props.put("family", jn.get("idgFamily").asText());
                String[] syns = retrieveSynonyms
                    ((String)props.get(URI_P), "(label=UniProt*)");
                if (syns.length > 0)
                    props.put(SYNONYMS_P, syns);
            });
    }

    void resolveLigands (JsonNode json, KNode kn, KGraph kg) {
        resolve ("ligands", json, kn, kg, (jn, props) -> {
                props.put(TYPE_P, "drug");
                String[] syns = retrieveSynonyms
                    ((String)props.get(URI_P), null);
                if (syns.length > 0)
                    props.put(SYNONYMS_P, syns);
            });
    }

    void resolveDiseases (JsonNode json, KNode kn, KGraph kg) {
        resolve ("diseases", json, kn, kg, (jn, props) -> {
                props.put(TYPE_P, "disease");
                String[] syns = retrieveSynonyms
                    ((String)props.get(URI_P), null);
                if (syns.length > 0)
                    props.put(SYNONYMS_P, syns);                
            });
    }

    void resolveLinks (JsonNode json, KNode kn, KGraph kg) {
        if (json.isArray()) {
            for (int i = 0; i < json.size(); ++i) {
                resolveLinks (json.get(i), kn, kg); // recurse.. 
            }
        }
        else if (json.hasNonNull("kind")) {
            String kind = json.get("kind").asText();
            switch (kind) {
            case "ix.idg.models.Ligand":
                resolveLigandLink (json, kn, kg);
                break;
                
            case "ix.idg.models.Target":
                resolveTargetLink (json, kn, kg);
                break;
                
            case "ix.idg.models.Disease":
                resolveDiseaseLink (json, kn, kg);
                break;
            }
        }
    }

    void resolveLigandLink (JsonNode node, KNode kn, KGraph kg) {
        long id = node.get("refid").asLong();
        JsonNode pn = node.get("properties");
        
        String name = null, moa = null, href = null;
        for (int j = 0; j < pn.size(); ++j) {
            JsonNode n = pn.get(j);
            // we only care if this ligand has a known pharmalogical action
            if ("Pharmalogical Action".equals(n.get("label").asText())) {
                moa = n.get("term").asText();
                if (n.hasNonNull("href"))
                    href = n.get("href").asText();
            }
            else if ("IDG Ligand".equals(n.get("label").asText())) {
                name = n.get("term").asText();
            }
        }

        // disease -> ligand can have empty properties, so we just
        // make
        if (moa != null || pn.size() == 0) {
            Map<String, Object> props = new TreeMap<>();
            props.put(TYPE_P, "drug");
            String uri = ksp.getUri()+"/ligands("+id+")";
            props.put(URI_P, uri);
            if (name != null)
                props.put(NAME_P, name);
            
            // create this node if it isn't on the graph already
            KNode xn = kg.createNodeIfAbsent(props, URI_P);
            if (xn.getId() != kn.getId()) {
                xn.addTag("KS:"+ksp.getId());
                if (name == null)
                    xn.putIfAbsent(NAME_P, () -> {
                            return retrieveJsonValue (uri+"/$name");
                        });
                xn.putIfAbsent(SYNONYMS_P, () -> {
                        return retrieveSynonyms (uri, null);
                    });
                
                // now link it
                if (href != null) {
                    props.clear();
                    props.put("href", href);
                }
                kg.createEdgeIfAbsent(xn, kn, moa != null
                                      ? moa.toLowerCase() : "assertion",
                                      props, null);
                Logger.debug(xn.getId()+":"+xn.getName()
                             + " <-> "+kn.getId()+":"+kn.getName());
            }
        }
    }

    void resolveDiseaseLink (JsonNode node, KNode kn, KGraph kg) {
        long id = node.get("refid").asLong();
        JsonNode pn = node.get("properties");

        String ds = null, disease = null;
        for (int i = 0; i < pn.size(); ++i) {
            JsonNode n = pn.get(i);
            switch (n.get("label").asText()) {
            case "Data Source":
                {
                    String term = n.get("term").asText();
                    // only do these data source for now
                    /*if ("DisGeNET".equals(term))
                        ds = term;
                        else*/
                    if ("DrugCentral Indication".equals(term))
                        ds = "indication";
                }
                break;
                
            case "IDG Disease":
                disease = n.get("term").asText();
                break;
            }
        }

        if (ds != null) {
            Map<String, Object> props = new TreeMap<>();
            props.put(TYPE_P, "disease");
            String uri = ksp.getUri()+"/diseases("+id+")";
            props.put(URI_P, uri);
            props.put(NAME_P, disease);

            KNode xn = kg.createNodeIfAbsent(props, URI_P);
            if (xn.getId() != kn.getId()) {
                xn.addTag("KS:"+ksp.getId());
                xn.putIfAbsent(SYNONYMS_P, () -> {
                        return retrieveSynonyms (uri, null);                    
                    });
                
                KEdge ke = kg.createEdgeIfAbsent(kn, xn, ds);
                Logger.debug(kn.getId()+":"+kn.getName()
                             + " <-> "+xn.getId()+":"+xn.getName());
            }
        }
    }

    void resolveTargetLink (JsonNode node, KNode kn, KGraph kg) {
        long id = node.get("refid").asLong();
        JsonNode pn = node.get("properties");

        String moa = null, name = null, ds = null, tdl = null; 
        for (int i = 0; i < pn.size(); ++i) {
            JsonNode n = pn.get(i);
            switch (n.get("label").asText()) {
            case "Pharmalogical Action":
                moa = n.get("term").asText();
                break;
                
            // unfortunately this isn't a target name.. sigh
            case "IDG Target":
                name = n.get("term").asText();
                break;

            case "Data Source":
                ds = n.get("term").asText();
                break;

            case "IDG Development Level":
                if ("Tclin".equals(n.get("term").asText()))
                    tdl = "Tclin";
                break;
            }
        }

        if (moa != null || tdl != null) {
            Map<String, Object> props = new TreeMap<>();
            props.put(TYPE_P, "protein");
            String uri = ksp.getUri()+"/targets("+id+")";
            props.put(URI_P, uri);
            
            KNode xn = kg.createNodeIfAbsent(props, URI_P);
            if (xn.getId() != kn.getId()) {
                xn.addTag("KS:"+ksp.getId());
                xn.putIfAbsent(NAME_P, () -> {
                        return retrieveJsonValue (uri+"/$name");
                    });
                xn.putIfAbsent(SYNONYMS_P, () -> {
                        return retrieveSynonyms (uri, null);
                    });
                
                KEdge ke = kg.createEdgeIfAbsent
                    (kn, xn, moa != null ? moa.toLowerCase() : "assertion");
                if (ds != null) {
                    final String source = ds;
                    ke.putIfAbsent("source", () -> {
                            return source;
                        });
                }
                Logger.debug(kn.getId()+":"+kn.getName()
                             + " <-> "+xn.getId()+":"+xn.getName());
            }
        }
    }

    String[] retrieveSynonyms (String url, String filter) {
        List<String> syns = new ArrayList<>();
        // only retrieve UniProt related synonyms
        WSRequest req = wsclient.url(url+"/synonyms"
                                     +(filter != null ? filter:""));
        try {
            WSResponse res = req.get().toCompletableFuture().get();
            JsonNode json = res.asJson();
            for (int i = 0; i < json.size(); ++i) {
                String s = json.get(i).get("term").asText();
                if (syns.indexOf(s) < 0) // terrible
                    syns.add(s);
            }
        }
        catch (Exception ex) {
            Logger.error("Can't get synonyms for "+url, ex);
        }
        //Logger.debug(url+" => "+syns.size()+" synonyms!");
        
        return syns.toArray(new String[0]);
    }

    String retrieveJsonValue (String url) {
        WSRequest req = wsclient.url(url);
        try {
            WSResponse res = req.get().toCompletableFuture().get();
            return res.getBody();
        }
        catch (Exception ex) {
            Logger.error("Can't get Json value for "+url, ex);
        }
        return null;
    }
}
