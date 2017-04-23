package blackboard.biothings;

import java.util.*;
import java.util.function.BiConsumer;
import java.net.URLEncoder;
import java.util.concurrent.*;

import javax.inject.Inject;
import javax.inject.Named;

import play.Logger;
import play.Configuration;
import play.libs.ws.*;
import play.libs.Json;
import play.inject.ApplicationLifecycle;
import play.libs.F;
import akka.actor.ActorSystem;

import com.fasterxml.jackson.databind.JsonNode;

import blackboard.*;
import static blackboard.KEntity.*;

public class DrugKSource implements KSource {
    private final WSClient wsclient;
    private final KSourceProvider ksp;

    @Inject
    public DrugKSource (WSClient wsclient,
                        @Named("biothings") KSourceProvider ksp,
                        ApplicationLifecycle lifecycle) {
        this.wsclient = wsclient;
        this.ksp = ksp;

        lifecycle.addStopHook(() -> {
                wsclient.close();
                return F.Promise.pure(null);
            });
        
        Logger.debug("$"+ksp.getId()+": "+ksp.getName()
                     +" initialized; provider is "+ksp.getImplClass());
    }

    public void execute (KGraph kgraph) {
        Logger.debug("$"+ksp.getId()
                     +": executing on KGraph "+kgraph.getId()
                     +" \""+kgraph.getName()+"\"");
        KNode[] drugs = kgraph.nodes(n -> "drug".equals(n.getType()));
        for (KNode kn : drugs) {
            seedDrug (kn, kgraph);
        }
        Logger.debug("$"+ksp.getId()+": "
                     +drugs.length+" drug nodes processed!");
    }

    void seedDrug (KNode kn, KGraph kg) {
        String url = ksp.getUri()+"/query?q=drugbank.name:"+kn.getName();
        Logger.debug("+++ resolving..."+url);
        WSRequest req = wsclient.url(url);
        req.get().thenAccept(res -> {
                try {
                    JsonNode json = res.asJson().get("hits");
                    for (int i = 0; i < json.size(); ++i) {
                        JsonNode node = json.get(i);
                        for (Iterator<Map.Entry<String, JsonNode>> it
                                 = node.fields(); it.hasNext(); ) {
                            Map.Entry<String, JsonNode> me = it.next();
                            JsonNode n = me.getValue();
                            switch (me.getKey()) {
                            case "aeolus":
                                aeolus (n, kn, kg);
                                break;
                            case "chebi":
                                chebi (n, kn, kg);
                                break;
                            case "chembl":
                                chembl (n, kn, kg);
                                break;
                            case "drugbank":
                                drugbank (n, kn, kg);
                                break;
                            case "unii":
                                unii (n, kn, kg);
                                break;
                            }
                        }
                    }
                    //Logger.debug(Json.stringify(json));
                }
                catch (Exception ex) {
                    Logger.error("Can't resolve url: "+url, ex);
                }
            });
    }

    void aeolus (JsonNode json, KNode kn, KGraph kg) {
    }

    void chebi (JsonNode json, KNode kn, KGraph kg) {
    }

    void chembl (JsonNode json, KNode kn, KGraph kg) {
    }

    void drugbank (JsonNode json, KNode kn, KGraph kg) {
    }

    void unii (JsonNode json, KNode kn, KGraph kg) {
        Map<String, Object> props = new HashMap<>();
        props.put(TYPE_P, "drug");
        String uri = ksp.getUri()+"/drug/"+json.get("unii").asText();
        props.put("uri", uri);
        props.put(NAME_P, json.get("preferred_term").asText());
        KNode xn = kg.createNodeIfAbsent(props, "uri");
        // tag this node with this knowledge source id  
        xn.addTag(ksp.getId()); 
        kg.createEdgeIfAbsent(kn, xn, "resolve");
        Logger.debug(xn.getId()+":"+xn.getName()
                     + " <-> "+kn.getId()+":"+kn.getName());
    }
}
