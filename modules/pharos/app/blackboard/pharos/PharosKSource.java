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
        try {
            WSRequest req = wsclient.url
                (ksp.getUri()+"/search?q="+term
                 +"&facet=ix.Class/ix.idg.models.Target"
                 +"&facet=IDG%20Development%20Level/Tclin");
            
            req.get().thenAccept(res -> {
                    JsonNode json = res.asJson();
                    resolveTargets (json);
                });
        }
        catch (Exception ex) {
            Logger.error("Unable to execute request", ex);
        }
    }

    void resolveTargets (JsonNode json) {
        Logger.debug("uri: "+json.get("uri").asText());
        JsonNode content = json.get("content");
        for (int i = 0; i < content.size(); ++i) {
            Logger.debug("..."+content.get(i).get("name").asText());
        }
        Logger.debug(content.size()+" targets!");
    }
}
