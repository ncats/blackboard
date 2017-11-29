package blackboard.beacons;

import java.util.*;
import java.util.function.BiConsumer;
import java.net.URLEncoder;
import java.util.concurrent.*;

import javax.inject.Inject;
import javax.inject.Named;

import akka.actor.ActorSystem;
import play.Logger;
import play.libs.ws.*;
import play.libs.Json;
import play.inject.ApplicationLifecycle;
import play.libs.F;

import com.fasterxml.jackson.databind.JsonNode;

import blackboard.*;
import static blackboard.KEntity.*;

public class BeaconKSource implements KSource {


    interface Resolver {
        void resolve (JsonNode json, KNode kn, KGraph kg);
    }
    private static final Map<String, String> SEMANTIC_GROUPS;
    static
    {
        SEMANTIC_GROUPS = new HashMap<String, String>();
        SEMANTIC_GROUPS.put("CHEM", "drug");
        SEMANTIC_GROUPS.put("DISO", "disease");
        SEMANTIC_GROUPS.put("GENE","gene");
    }

    protected final KSourceProvider ksp;    
    @Inject protected WSClient wsclient;
    
    @Inject
    public BeaconKSource (ActorSystem actorSystem, WSClient wsclient,
                          @Named("beacons") KSourceProvider ksp,
                          ApplicationLifecycle lifecycle) {
        this.ksp = ksp;
        Logger.debug("$"+ksp.getId()+": "+ksp.getName()
                     +" initialized; provider is "+ksp.getImplClass());
    }

    @Inject
    protected void setLifecycle (ApplicationLifecycle lifecycle) {
        lifecycle.addStopHook(() -> {
                wsclient.close();
                return F.Promise.pure(null);
            });
        Logger.debug("lifecycle hook registered!");
    }
    
    protected void seedQuery (String term, KNode kn, KGraph kg) {
        String url = ksp.getUri()+"/concepts";
        try {
            Map<String, String> q = new HashMap<>();
            q.put("keywords",term);
            resolve(ksp.getUri()+"/concepts",q,kn,kg,this::resolveQuery);
        }
        catch (Exception ex) {
            Logger.error("Can't resolve url: "+url, ex);
        }
    }

    void instrument (String entity, JsonNode json, KNode kn, KGraph kg,
                     BiConsumer<JsonNode, Map<String, Object>> consumer) {
        String uri = null;
        if (json.hasNonNull("uri"))
            uri = json.get("uri").asText();
        if(json.get("id")!=null)
        {
            String id = json.get("id").asText();
            String name = json.get("name").asText();
            Map<String, Object> props = new TreeMap<>();
            props.put(URI_P, ksp.getUri()+"/concepts/"+id);
            props.put(NAME_P, name);
            consumer.accept(json, props);

            KNode node = kg.createNodeIfAbsent(props, URI_P);
            if (node.getId() != kn.getId()) {
                node.addTag("KS:"+ksp.getId());
                props.clear();
                props.put("value", uri);
                kg.createEdgeIfAbsent(kn, node, "resolve");
                Logger.debug(node.getId()+"..."+name);
            }
        }

    }

    public void execute (KGraph kgraph, KNode... nodes) {
        Logger.debug("$"+ksp.getId()
                     +": executing on KGraph "+kgraph.getId()
                     +" \""+kgraph.getName()+"\"");
        if (nodes == null || nodes.length == 0)
            nodes = kgraph.getNodes();

        for (KNode kn : nodes) {
            if(kn.get("term")!=null)
            {
                seedQuery ((String)kn.get("term"), kn, kgraph);
            }
            else
            {
                seedQuery((String)kn.get("name"),kn,kgraph);
            }
//            switch (kn.getType()) {
//            case "query":
//                seedQuery ((String)kn.get("term"), kn, kgraph);
//                break;
//            case "disease":
//                seedQuery ((String)kn.get("term"), kn, kgraph);
//                break;
//            }
        }
    }
    void resolve (String url, Map<String, String> params,
                  KNode kn, KGraph kg, Resolver resolver) {
        WSRequest req = wsclient.url(url).setFollowRedirects(true);
        if (params != null) {
            for (Map.Entry<String, String> me : params.entrySet()) {
                Logger.debug(".."+me.getKey()+": "+me.getValue());
                req = req.setQueryParameter(me.getKey(), me.getValue());
                Logger.debug(me.getKey()+", "+me.getValue());
            }
        }
        Logger.debug("+++ resolving..."+req.getUrl());

        try {
            WSResponse res = req.get().toCompletableFuture().get();
            JsonNode json = res.asJson();
            resolver.resolve(json, kn, kg);
        }
        catch (Exception ex) {
            Logger.error("Can't resolve url: "+url, ex);
        }
    }

    void resolveDiseases (JsonNode json, KNode kn, KGraph kg) {
        if (json.isArray()) {
            for (int i = 0; i < json.size(); ++i)
                resolveDiseases(json.get(i), kn, kg);
        }
        else {
            Map<String, Object> props = new TreeMap<>();
            props.put(TYPE_P, "disease");
            props.put(URI_P, ksp.getUri()+"/concepts/"+json.get("id").asText());
            props.put(NAME_P, json.get("name").asText());
            List<String> syns = new ArrayList<>();
            JsonNode sn = json.get(SYNONYMS_P);
            if (sn != null) {
                for (int i = 0; i < sn.size(); ++i)
                    syns.add(sn.get(i).asText());
                props.put(SYNONYMS_P, syns.toArray(new String[0]));
            }
            KNode xn = kg.createNodeIfAbsent(props, URI_P);
            if (xn.getId() != kn.getId()) {
                xn.addTag("KS:"+ksp.getId());
                kg.createEdgeIfAbsent(kn, xn, "resolve");
            }
        }
    }
    void resolveQuery (JsonNode json, KNode kn, KGraph kg)
    {
        Logger.debug("resolveQuery");
        if (json.isArray()) {
            for (int i = 0; i < json.size(); ++i)
                resolveQuery(json.get(i), kn, kg);
        }
        else {
            Map<String, Object> props = new TreeMap<>();
            String semantic = json.get("semanticGroup").asText();
            String type = SEMANTIC_GROUPS.get(semantic);
            if(type == null || type.isEmpty())
            {
                if(!semantic.isEmpty()&&semantic!=null)
                    type = semantic;
                else
                    type = "UNKNOWN";
            }
            Logger.debug("TYPE: "+type);
            props.put(TYPE_P,type);
            props.put(URI_P, ksp.getUri()+"/concepts/"+json.get("id").asText());
            props.put(NAME_P, json.get("name").asText());
            List<String> syns = new ArrayList<>();
            JsonNode sn = json.get(SYNONYMS_P);
            if (sn != null) {
                for (int i = 0; i < sn.size(); ++i)
                    syns.add(sn.get(i).asText());
                props.put(SYNONYMS_P, syns.toArray(new String[0]));
            }
            KNode xn = kg.createNodeIfAbsent(props, URI_P);
            if (xn.getId() != kn.getId()) {
                xn.addTag("KS:"+ksp.getId());
                kg.createEdgeIfAbsent(kn, xn, "resolve");
            }
        }
    }
    //protected abstract void resolve (JsonNode json, KNode kn, KGraph kgraph);
}
