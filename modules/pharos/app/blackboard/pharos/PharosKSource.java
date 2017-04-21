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

    class PharosResolver {
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
        
        Logger.debug("$$ "+getClass().getName()
                     +" initialized; provider is "+ksp);
    }

    public void execute (KGraph kgraph) {
        Logger.debug(getClass().getName()
                     +": executing on KGraph "+kgraph.getId()
                     +" \""+kgraph.getName()+"\"");

        for (KNode kn : kgraph.getNodes()) {
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

        resolve (ksp.getUri()+"/targets/search?q="+term
                 +"&facet=IDG%20Development%20Level/Tclin",
                 kn, kg, this::resolveTargets);

        resolve (ksp.getUri()+"/ligands/search?q="+term
                 +"&facet=IDG%20Development%20Level/Tclin",
                 kn, kg, this::resolveLigands);
        
        resolve (ksp.getUri()+"/diseases/search?q="+term
                 +"&facet=IDG%20Development%20Level/Tclin",
                 kn, kg, this::resolveDiseases);
    }

    void seedTarget (KNode kn, KGraph kg) {
        Logger.debug(">>> seedTarget \""+kn.getName()+"\"");
        // argh.. should update the pharos api to allow list for filter
        resolve (kn.get("uri")+"/links(kind=ix.idg.models.Ligand)",
                 kn, kg, this::resolveLinks);
        resolve (kn.get("uri")+"/links(kind=ix.idg.models.Disease)",
                 kn, kg, this::resolveLinks);
    }

    void seedLigand (KNode kn, KGraph kg) {
        Logger.debug(">>> seedLigand \""+kn.getName()+"\"");
        resolve (kn.get("uri")+"/links(kind=ix.idg.models.Target)",
                 kn, kg, this::resolveLinks);
        resolve (kn.get("uri")+"/links(kind=ix.idg.models.Disease)",
                 kn, kg, this::resolveLinks);
    }

    void seedDisease (KNode kn, KGraph kg) {
        Logger.debug(">>> seedDisease \""+kn.getName()+"\"");
        resolve (kn.get("uri")+"/links(kind=ix.idg.models.Target)",
                 kn, kg, this::resolveLinks);
        resolve (kn.get("uri")+"/links(kind=ix.idg.models.Ligand)",
                 kn, kg, this::resolveLinks);
    }

    void resolve (String url, KNode kn, KGraph kg, Resolver resolver) {
        Logger.debug("+++ resolving..."+url);
        WSRequest req = wsclient.url(url);
        req.get().thenAccept(res -> {
                try {
                    JsonNode json = res.asJson();
                    resolver.resolve(json, kn, kg);
                }
                catch (Exception ex) {
                    Logger.error("Can't resolve url: "+url, ex);
                }
            });
    }
    
    void resolve (String entity, JsonNode json, KNode kn, KGraph kg,
                  BiConsumer<JsonNode, Map<String, Object>> consumer) {
        JsonNode content = json.get("content");
        for (int i = 0; i < content.size(); ++i) {
            JsonNode jn = content.get(i);
            long id = jn.get("id").asLong();
            String name = jn.get("name").asText();
            String uri = ksp.getUri()+"/"+entity+"("+id+")";
            
            Map<String, Object> props = new HashMap<>();
            props.put("uri", uri);
            props.put("name", name);
            consumer.accept(jn, props);
            
            KNode node = kg.createNodeIfAbsent(props, "uri");
            kg.createEdgeIfAbsent(kn, node, "assertion");
            Logger.debug(node.getId()+"..."+name);
        }
        Logger.debug("uri: "+json.get("uri").asText()+"..."+content.size());    
    }

    void resolveTargets (JsonNode json, KNode kn, KGraph kg) {
        resolve ("targets", json, kn, kg, (jn, props) -> {
                props.put(TYPE_P, "protein");       
                props.put("family", jn.get("idgFamily").asText());
                String[] syns = retrieveSynonyms
                    ((String)props.get("uri"), "(label=UniProt*)");
                if (syns.length > 0)
                    props.put("synonyms", syns);
            });
    }

    void resolveLigands (JsonNode json, KNode kn, KGraph kg) {
        resolve ("ligands", json, kn, kg, (jn, props) -> {
                props.put(TYPE_P, "drug");
                String[] syns = retrieveSynonyms
                    ((String)props.get("uri"), null);
                if (syns.length > 0)
                    props.put("synonyms", syns);
            });
    }

    void resolveDiseases (JsonNode json, KNode kn, KGraph kg) {
        resolve ("diseases", json, kn, kg, (jn, props) -> {
                props.put(TYPE_P, "disease");
                String[] syns = retrieveSynonyms
                    ((String)props.get("uri"), null);
                if (syns.length > 0)
                    props.put("synonyms", syns);                
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
            Map<String, Object> props = new HashMap<>();
            props.put(TYPE_P, "drug");
            String uri = ksp.getUri()+"/ligands("+id+")";
            props.put("uri", uri);
            if (name != null)
                props.put(NAME_P, name);
            
            // create this node if it isn't on the graph already
            KNode xn = kg.createNodeIfAbsent(props, "uri");
            if (name == null)
                xn.putIfAbsent(NAME_P, () -> {
                        return retrieveJsonValue (uri+"/$name");
                    });
            xn.putIfAbsent("synonyms", () -> {
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
            Map<String, Object> props = new HashMap<>();
            props.put(TYPE_P, "disease");
            String uri = ksp.getUri()+"/diseases("+id+")";
            props.put("uri", uri);
            props.put(NAME_P, disease);

            KNode xn = kg.createNodeIfAbsent(props, "uri");
            xn.putIfAbsent("synonyms", () -> {
                    return retrieveSynonyms (uri, null);                    
                });

            KEdge ke = kg.createEdgeIfAbsent(kn, xn, ds);
            Logger.debug(kn.getId()+":"+kn.getName()
                         + " <-> "+xn.getId()+":"+xn.getName());
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
            Map<String, Object> props = new HashMap<>();
            props.put(TYPE_P, "protein");
            String uri = ksp.getUri()+"/targets("+id+")";
            props.put("uri", uri);
            
            KNode xn = kg.createNodeIfAbsent(props, "uri");
            xn.putIfAbsent(NAME_P, () -> {
                    return retrieveJsonValue (uri+"/$name");
                });
            xn.putIfAbsent("synonyms", () -> {
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
