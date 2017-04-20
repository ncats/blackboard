package blackboard.pharos;

import java.util.*;
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

public class PharosKSource implements KSource {
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
                resolveQuery ((String)kn.get("term"), kn, kgraph);
                break;
                
            case "disease":
                break;
                
            case "protein":
                break;
            case "drug":
                break;
            }
        }
    }

    void resolveQuery (String term, KNode kn, KGraph kg) {
        Logger.debug("query \""+term+"\"");

        // first try target
        WSRequest req = wsclient.url
            (ksp.getUri()+"/targets/search?q="+term
             +"&facet=IDG%20Development%20Level/Tclin");
        
        req.get().thenAccept(res -> {
                try {
                    JsonNode json = res.asJson();
                    resolveTargets (json, kn, kg);
                }
                catch (Exception ex) {
                    Logger.error("Can't resolve targets for term \""
                                 +term+"\"", ex);
                }
            });

        req = wsclient.url
            (ksp.getUri()+"/ligands/search?q="+term
             +"&facet=IDG%20Development%20Level/Tclin");
        req.get().thenAccept(res -> {
                try {
                    JsonNode json = res.asJson();
                    resolveLigands (json, kn, kg);
                }
                catch (Exception ex) {
                    Logger.error("Can't resolve ligands for term \""
                                 +term+"\"", ex);
                }
            });
    }

    void resolveTargets (JsonNode json, KNode kn, KGraph kg) {
        Logger.debug("uri: "+json.get("uri").asText());
        JsonNode content = json.get("content");
        for (int i = 0; i < content.size(); ++i) {
            long id = content.get(i).get("id").asLong();
            String name = content.get(i).get("name").asText();
            String uri = ksp.getUri()+"/targets("+id+")";
            
            Map<String, Object> props = new HashMap<>();
            props.put("uri", uri);
            props.put("type", "protein");
            props.put("name", name);
            props.put("family", content.get(i).get("idgFamily").asText());
            String[] syns = retrieveSynonyms (uri, "(label=UniProt*)");
            if (syns.length > 0)
                props.put("synonyms", syns);
            KNode node = kg.createNodeIfAbsent(props, "uri");
            kg.createEdgeIfAbsent(kn, node);
            Logger.debug(node.getId()+"..."+name);
        }
        Logger.debug(content.size()+" targets!");
    }

    void resolveLigands (JsonNode json, KNode kn, KGraph kg) {
        Logger.debug("uri: "+json.get("uri").asText());
        JsonNode content = json.get("content");
        for (int i = 0; i < content.size(); ++i) {
            long id = content.get(i).get("id").asLong();
            String name = content.get(i).get("name").asText();
            String uri = ksp.getUri()+"/ligands("+id+")";
            
            Map<String, Object> props = new HashMap<>();
            props.put("uri", uri);
            props.put("type", "drug");
            props.put("name", name);
            String[] syns = retrieveSynonyms (uri, null);
            if (syns.length > 0)
                props.put("synonyms", syns);
            KNode node = kg.createNodeIfAbsent(props, "uri");
            kg.createEdgeIfAbsent(kn, node);
            Logger.debug(node.getId()+"..."+name);          
        }
        Logger.debug(content.size()+" ligands!");
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
}
